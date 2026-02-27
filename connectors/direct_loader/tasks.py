import asyncio
import gc
from datetime import datetime
from multiprocessing import Pool, cpu_count

from sqlalchemy.exc import InvalidRequestError, NoSuchTableError

from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase
from prefect_loader.orchestration.clickhouse_utils.config import CLICKHOUSE_POOL_SIZE
from .access import collect_direct_login_tokens
from .change_tracker import DirectChangeTracker
from .config import DATABASE_PROFILES, DEFAULT_DB_PROFILE
from .logging_utils import get_logger
from .shared_utils import convert_days_to_date_ranges, emit_prefect_event
from .uploader import YaStatUploader


def run_uploader_chunk(chunk):
    """Run uploader for a single chunk of Direct data.

    Args:
        chunk: Dictionary containing login, token, time_to, time_from,
               function name, and optional profile, year, month
    """
    uploader = YaStatUploader(
        chunk['login'],
        chunk['token'],
        chunk['time_to'],
        chunk['time_from'],
        profile=chunk.get('profile', DEFAULT_DB_PROFILE),
    )
    if chunk['function'] == 'unload_data_for_month':
        asyncio.run(
            getattr(uploader, chunk['function'])(chunk['year'], chunk['month'])
        )
    else:
        asyncio.run(getattr(uploader, chunk['function'])())
    del uploader
    gc.collect()


async def unload_data_by_day_for_all_clients(start_date, end_date, profile: str = DEFAULT_DB_PROFILE):
    """Unload Direct data for all clients across a date range, deleting existing records first."""
    profile_config = DATABASE_PROFILES[profile]
    target_db = AsyncDirectDatabase(profile_config.get('database'))
    await target_db.init_db()
    access_db = AsyncDirectDatabase()
    await access_db.init_db()
    login_token_dict = await collect_direct_login_tokens(access_db, profile=profile)
    if not login_token_dict:
        get_logger().warning("Direct: no client tokens found in Accesses; aborting unload.")
        return
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    end_of_day = end_date_dt.replace(hour=23, minute=59, second=59)
    for login in login_token_dict.keys():
        try:
            await target_db.delete_records_between_dates(login, start_date_dt, end_of_day)
        except (NoSuchTableError, InvalidRequestError):
            get_logger().info(
                f"{login}: table missing while purging {start_date}-{end_date} in profile '{profile}' — skipping delete."
            )
        except Exception as e:
            get_logger().error(f"Failed to delete records for login {login}. Error: {e}")
    chunks = [
        {
            'login': login,
            'token': token,
            'time_to': end_date,
            'time_from': start_date,
            'function': 'unload_data_by_days',
            'profile': profile,
        }
        for login, token in login_token_dict.items()
    ]
    with Pool(processes=cpu_count()) as pool:
        pool.map(run_uploader_chunk, chunks)
    get_logger().info(f"[{profile}] All data from {start_date} to {end_of_day} for all clients updated")


async def unload_data_by_day_for_single_client(login, start_date, end_date, profile: str = DEFAULT_DB_PROFILE):
    """
    Unload data for a single client login over a specified date range.

    Args:
        login (str): The client's login identifier (e.g., 'client_login_with_underscores').
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If the login is not found in agency or non-agency access tables.
    """
    profile_config = DATABASE_PROFILES[profile]

    target_db = AsyncDirectDatabase(profile_config.get('database'))
    await target_db.init_db()

    access_db = AsyncDirectDatabase()
    await access_db.init_db()

    normalized_login = AsyncDirectDatabase._normalize_login(login)
    login_tokens = await collect_direct_login_tokens(access_db, profile=profile)
    token = login_tokens.get(normalized_login)

    if not token:
        raise ValueError(f"Login {login} not found in Accesses")

    login = normalized_login
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    end_of_day = end_date_dt.replace(hour=23, minute=59, second=59)

    try:
        await target_db.delete_records_between_dates(login, start_date_dt, end_of_day)
    except (NoSuchTableError, InvalidRequestError):
        get_logger().info(
            f"{login}: table missing while purging {start_date}-{end_date} in profile '{profile}' — skipping delete."
        )
    except Exception as e:
        get_logger().error(f"{login}: failed to purge {start_date}-{end_date} — {e!s}")
        raise

    uploader = YaStatUploader(login, token, end_date, start_date, profile=profile)
    await uploader.unload_data_by_days()
    get_logger().info(f"[{profile}] Data from {start_date} to {end_date} for client {login} updated")


async def unload_data_with_changes_tracking_for_all_clients(
    profile: str = DEFAULT_DB_PROFILE,
    lookback_days: int = 60,
) -> None:
    """
    Refreshes statistics only for the days whose numbers in Postgres
    differ from those returned by the Yandex-Direct API.

    Changes w.r.t. the original version
    ───────────────────────────────────
    1.  A separate semaphore (`db_sem`) limits the *database* load
        to at most min(CLICKHOUSE_POOL_SIZE, 20) concurrent `detect_changes()` runs –
        this keeps ClickHouse HTTP pool usage under control.

    2.  All `detect_changes` work happens **inside** that semaphore
        (`async with db_sem:`).

    3.  The async engine is disposed (`await engine.dispose()`) **before**
        the code forks worker processes, so no open connections are
        duplicated into children.
    """
    profile_config = DATABASE_PROFILES[profile]
    target_db = AsyncDirectDatabase(profile_config.get('database'))
    await target_db.init_db()
    access_db = AsyncDirectDatabase()
    await access_db.init_db()

    login_tokens = await collect_direct_login_tokens(access_db, profile=profile)
    if not login_tokens:
        get_logger().warning("No clients to process — exiting.")
        return

    shared_api_sem = asyncio.Semaphore(3)
    max_db_concurrency = max(1, min(CLICKHOUSE_POOL_SIZE, 20))
    db_sem         = asyncio.Semaphore(max_db_concurrency)

    async def check_client(login: str, token: str):
        """Run change detection for one client under the shared database semaphore."""
        async with db_sem:
            client_db = AsyncDirectDatabase(profile_config.get('database'))
            await client_db.init_db()

            tracker = DirectChangeTracker(
                token,
                client_db,
                lookback_days=lookback_days,
                compare_conversions=profile_config['compare_conversions'],
                cost_tolerance=profile_config['cost_tolerance'],
                skip_cost_check_clients=profile_config['skip_cost_check_clients'],
            )
            tracker.api_semaphore = shared_api_sem
            try:
                changes = await tracker.detect_changes(login)
            except Exception as e:
                get_logger().error(f"{login}: detect_changes() failed — {e!s}")
                return None
            if not changes["days_to_update"]:
                return None
            await emit_prefect_event(
                "direct.client.changes_detected",
                login=login,
                profile=profile,
                payload={"days": changes["days_to_update"]},
            )
            return login, token, changes

    coros   = [check_client(lg, tk) for lg, tk in login_tokens.items()]
    results = [r for r in await asyncio.gather(*coros) if r]

    chunks: list[dict] = []

    for login, token, changes in results:
        ranges = convert_days_to_date_ranges(changes["days_to_update"])

        get_logger().info(f"{login}: need to refresh {ranges}")

        for start_date, end_date in ranges:
            try:
                await target_db.delete_records_between_dates(
                    login,
                    datetime.strptime(start_date, "%Y-%m-%d"),
                    datetime.strptime(end_date,   "%Y-%m-%d"),
                )
            except (NoSuchTableError, InvalidRequestError):
                get_logger().info(
                    f"{login}: table missing while purging {start_date}-{end_date}; continuing with fresh load."
                )
            except Exception as e:
                get_logger().error(f"{login}: purge {start_date}-{end_date} failed — {e!s}")

            chunks.append(dict(login=login, token=token,
                               time_to=end_date, time_from=start_date,
                               function="unload_data_by_days",
                               profile=profile))

    if not chunks:
        get_logger().info("No updates required - we're done.")
        return

    concurrency = min(cpu_count(), 8)
    worker_sem = asyncio.Semaphore(concurrency)

    async def run_chunk(chunk: dict):
        """Execute one reload chunk under the shared worker semaphore."""
        async with worker_sem:
            uploader = YaStatUploader(
                chunk["login"],
                chunk["token"],
                chunk["time_to"],
                chunk["time_from"],
                profile=chunk.get("profile", profile),
            )
            await emit_prefect_event(
                "direct.client.reload_started",
                login=chunk["login"],
                profile=chunk.get("profile", profile),
                payload={"time_from": chunk["time_from"], "time_to": chunk["time_to"]},
            )
            await uploader.unload_data_by_days()
            await emit_prefect_event(
                "direct.client.reload_finished",
                login=chunk["login"],
                profile=chunk.get("profile", profile),
                payload={"time_from": chunk["time_from"], "time_to": chunk["time_to"]},
            )

    await asyncio.gather(*(run_chunk(ch) for ch in chunks))

    get_logger().info(f"[{profile}] Finished refreshing all clients with detected changes.")


async def unload_data_with_changes_tracking_for_single_client(
    login: str,
    profile: str = DEFAULT_DB_PROFILE,
    lookback_days: int = 60,
) -> None:
    """
    1. Determines the OAuth token for the specified `login`.
    2. Using DirectChangeTracker, detects which dates (over the last 60 days) differ from the database.
    3. Deletes database rows for these dates.
    4. Reloads only changed date ranges.

    If there are no changes, it does nothing, it just writes to the log.
    """
    profile_config = DATABASE_PROFILES[profile]
    target_db = AsyncDirectDatabase(profile_config.get('database'))
    await target_db.init_db()
    access_db = AsyncDirectDatabase()
    await access_db.init_db()

    normalized_login = AsyncDirectDatabase._normalize_login(login)
    login_tokens = await collect_direct_login_tokens(access_db, profile=profile)
    token = login_tokens.get(normalized_login)
    if not token:
        get_logger().error(f"{login}: token not found in Accesses.")
        return
    login = normalized_login
    await emit_prefect_event(
        "direct.client.changes_run_started",
        login=login,
        profile=profile,
        payload={},
    )

    tracker = DirectChangeTracker(
        token,
        target_db,
        lookback_days=lookback_days,
        compare_conversions=profile_config['compare_conversions'],
        cost_tolerance=profile_config['cost_tolerance'],
        skip_cost_check_clients=profile_config['skip_cost_check_clients'],
    )

    try:
        changes = await tracker.detect_changes(login)
    except Exception as e:
        get_logger().error(f"{login}: detect_changes() failed — {e!s}")
        await emit_prefect_event(
            "direct.client.changes_run_failed",
            login=login,
            profile=profile,
            payload={"error": str(e)},
        )
        return

    days_to_update = changes.get("days_to_update") or []

    if not days_to_update:
        await emit_prefect_event(
            "direct.client.no_changes",
            login=login,
            profile=profile,
            payload={},
        )
        return

    ranges = convert_days_to_date_ranges(days_to_update)

    get_logger().info(f"{login}: will refresh {ranges}")

    for start_date, end_date in ranges:
        try:
            await target_db.delete_records_between_dates(
                login,
                datetime.strptime(start_date, "%Y-%m-%d"),
                datetime.strptime(end_date,   "%Y-%m-%d"),
            )
        except (NoSuchTableError, InvalidRequestError):
            get_logger().info(
                f"{login}: table missing while purging {start_date}-{end_date}; continuing with fresh load."
            )
        except Exception as e:
            get_logger().error(f"{login}: purge {start_date}-{end_date} failed — {e!s}")
            continue

        uploader = YaStatUploader(login, token, end_date, start_date, profile=profile)
        try:
            await emit_prefect_event(
                "direct.client.reload_started",
                login=login,
                profile=profile,
                payload={"time_from": start_date, "time_to": end_date},
            )
            await uploader.unload_data_by_days()
        except Exception as e:
            get_logger().error(f"{login}: upload {start_date}-{end_date} failed — {e!s}")
            await emit_prefect_event(
                "direct.client.reload_failed",
                login=login,
                profile=profile,
                payload={"time_from": start_date, "time_to": end_date, "error": str(e)},
            )
            continue
        await emit_prefect_event(
            "direct.client.reload_finished",
            login=login,
            profile=profile,
            payload={"time_from": start_date, "time_to": end_date},
        )

    get_logger().info(f"[{profile}] {login}: refresh finished.")
    await emit_prefect_event(
        "direct.client.changes_run_finished",
        login=login,
        profile=profile,
        payload={"ranges": ranges},
    )


async def fetch_direct_job_data(job):
    """
    Fetch Direct data for a single DirectReloadJob.
    This includes deleting old records and uploading new data.

    Args:
        job: DirectReloadJob instance with login, token, start_date, end_date, profile

    Returns:
        The data fetched by the uploader (or None on failure)
    """
    from .jobs import DirectReloadJob
    from .config import DATABASE_PROFILES

    if isinstance(job, dict):
        job = DirectReloadJob.from_dict(job)

    profile_config = DATABASE_PROFILES[job.profile]
    target_db = AsyncDirectDatabase(profile_config.get('database'))
    await target_db.init_db()

    try:
        await target_db.delete_records_between_dates(
            job.login,
            datetime.strptime(job.start_date, "%Y-%m-%d"),
            datetime.strptime(job.end_date, "%Y-%m-%d"),
        )
    except (NoSuchTableError, InvalidRequestError):
        get_logger().info(
            "[%s] %s: table missing while purging %s-%s; continuing with fresh load.",
            job.profile,
            job.login,
            job.start_date,
            job.end_date,
        )
    except Exception as e:
        get_logger().error(
            "[%s] %s: purge %s-%s failed — %s",
            job.profile,
            job.login,
            job.start_date,
            job.end_date,
            e,
        )

    await target_db.close_engine()

    uploader = YaStatUploader(
        job.login,
        job.token,
        job.end_date,
        job.start_date,
        profile=job.profile,
    )

    get_logger().info(
        "[%s] %s: fetching data for %s → %s",
        job.profile,
        job.login,
        job.start_date,
        job.end_date,
    )

    try:
        await emit_prefect_event(
            "direct.client.reload_started",
            login=job.login,
            profile=job.profile,
            payload={"time_from": job.start_date, "time_to": job.end_date},
        )
        await uploader.unload_data_by_days()
        return True
    except Exception as e:
        get_logger().error(
            "[%s] %s: fetch failed for %s → %s — %s",
            job.profile,
            job.login,
            job.start_date,
            job.end_date,
            e,
        )
        raise


async def write_direct_job_data(job, data) -> bool:
    """
    Write fetched Direct data for a job.
    Since Direct uploader handles both fetch and write in one operation,
    this function primarily handles cleanup and returns a status flag.

    Args:
        job: DirectReloadJob instance
        data: Result from fetch operation

    Returns:
        Status code (1 for success, 0 for failure)
    """
    from .jobs import DirectReloadJob

    if isinstance(job, dict):
        job = DirectReloadJob.from_dict(job)

    get_logger().info(
        "[%s] %s: write step completed (uploader already wrote data) for %s → %s",
        job.profile,
        job.login,
        job.start_date,
        job.end_date,
    )

    await emit_prefect_event(
        "direct.client.reload_finished",
        login=job.login,
        profile=job.profile,
        payload={"time_from": job.start_date, "time_to": job.end_date},
    )

    return bool(data)
