import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow, get_run_logger, task

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from prefect_loader.connectors.direct_loader import (
    DirectReloadJob,
    fetch_direct_job_data,
    unload_data_by_day_for_all_clients,
    unload_data_with_changes_tracking_for_all_clients,
    unload_data_with_changes_tracking_for_single_client,
    write_direct_job_data,
)

MAX_REPORTS_PER_LOGIN = 5
MAX_CONCURRENT_LOGINS = 15


@task(name="Direct loader: date range", retries=2, retry_delay_seconds=25, timeout_seconds=60 * 60 * 6)
async def run_direct_range(start_date: str, end_date: str, profile: str = "analytics") -> None:
    """Load Direct data for all clients in the provided date window."""
    await unload_data_by_day_for_all_clients(start_date, end_date, profile=profile)


@task(name="Direct loader: change tracker", retries=2, retry_delay_seconds=20, timeout_seconds=60 * 60 * 6)
async def run_direct_change_tracker(profile: str = "analytics", lookback_days: int = 60) -> None:
    """Run the Direct change tracker for all clients."""
    await unload_data_with_changes_tracking_for_all_clients(profile=profile, lookback_days=lookback_days)


@task(name="Direct loader: change tracker single", retries=2, retry_delay_seconds=20, timeout_seconds=60 * 60 * 6)
async def run_direct_change_tracker_single(login: str, profile: str = "analytics", lookback_days: int = 60) -> None:
    """Run the Direct change tracker for a single client."""
    await unload_data_with_changes_tracking_for_single_client(login=login, profile=profile, lookback_days=lookback_days)


@task(
    name="Direct loader: get client list",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=120,
    persist_result=False,
)
async def get_direct_clients_task(
    login: str | None = None,
    profile: str = "analytics",
) -> dict[str, list[dict]]:
    """Get list of Direct clients to process."""
    from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase
    from prefect_loader.connectors.direct_loader.access import collect_direct_login_tokens

    access_db = AsyncDirectDatabase()
    await access_db.init_db()

    try:
        login_tokens = await collect_direct_login_tokens(access_db, profile=profile)
        if not login_tokens:
            return {"clients": []}

        if login is not None:
            normalized_login = AsyncDirectDatabase._normalize_login(login)
            if normalized_login not in login_tokens:
                return {"clients": []}
            login_tokens = {normalized_login: login_tokens[normalized_login]}

        clients = [
            {"login": client_login, "token": token}
            for client_login, token in login_tokens.items()
        ]
        return {"clients": clients}
    finally:
        await access_db.close_engine()


@task(
    name="Direct loader: detect changes for client",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=600,
    persist_result=False,
)
async def detect_changes_for_client_task(
    client_data: dict,
    lookback_days: int = 60,
    profile: str = "analytics",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    """Detect changes for a single client."""
    from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase
    from prefect_loader.connectors.direct_loader.change_tracker import DirectChangeTracker
    from prefect_loader.connectors.direct_loader.config import DATABASE_PROFILES
    from prefect_loader.connectors.direct_loader.shared_utils import convert_days_to_date_ranges

    logger = get_run_logger()
    client_login = client_data["login"]
    token = client_data["token"]
    profile_config = DATABASE_PROFILES[profile]

    logger.info("%s: detecting changes (profile=%s, lookback_days=%s)", client_login, profile, lookback_days)

    effective_lookback = lookback_days
    if start_date:
        try:
            end_boundary = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else datetime.now().date() - timedelta(days=1)
            start_boundary = datetime.strptime(start_date, "%Y-%m-%d").date()
            delta_days = (end_boundary - start_boundary).days + 1
            if delta_days > 0:
                effective_lookback = delta_days
        except Exception:
            pass

    client_db = AsyncDirectDatabase(profile_config.get("database"))
    await client_db.init_db()

    try:
        tracker = DirectChangeTracker(
            token=token,
            db=client_db,
            lookback_days=effective_lookback,
            compare_conversions=profile_config["compare_conversions"],
            cost_tolerance=profile_config["cost_tolerance"],
            skip_cost_check_clients=profile_config["skip_cost_check_clients"],
        )

        changes_info = await tracker.detect_changes(client_login)
        days_to_update = changes_info.get("days_to_update") or []

        if not days_to_update:
            return {
                "login": client_login,
                "token": token,
                "has_changes": False,
                "jobs": [],
            }

        client_jobs = []
        ranges = convert_days_to_date_ranges(days_to_update)
        for start_str, end_str in ranges:
            try:
                datetime.strptime(start_str, "%Y-%m-%d")
                datetime.strptime(end_str, "%Y-%m-%d")
            except ValueError:
                logger.warning("%s: invalid date range %s-%s, skipping", client_login, start_str, end_str)
                continue
            client_jobs.append({
                "login": client_login,
                "token": token,
                "start_date": start_str,
                "end_date": end_str,
                "profile": profile,
                "source": "direct_api",
            })

        logger.info("%s: found %d job(s) to process", client_login, len(client_jobs))
        return {
            "login": client_login,
            "token": token,
            "has_changes": True,
            "jobs": client_jobs,
        }
    except Exception as exc:
        logger.error("%s: change detection failed — %s", client_login, exc)
        raise
    finally:
        await client_db.close_engine()


@task(
    name="Direct loader: fetch range",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=60 * 60 * 3,
    persist_result=False,
)
async def fetch_direct_range_task(job_payload: dict) -> object:
    """Fetch Direct data for a single job payload."""
    job = DirectReloadJob.from_dict(job_payload)
    logger = get_run_logger()
    logger.info("Fetching %s (%s → %s)", job.login, job.start_date, job.end_date)
    result = await fetch_direct_job_data(job)
    if result is None:
        raise ValueError(f"{job.login}: fetch failed for {job.start_date}-{job.end_date}")
    return result


@task(
    name="Direct loader: write range",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=60 * 60 * 2,
    persist_result=False,
)
async def write_direct_range_task(job_payload: dict, data) -> bool:
    """Write fetched Direct data for a job (no-op; uploader already wrote it)."""
    job = DirectReloadJob.from_dict(job_payload)
    logger = get_run_logger()
    success = await write_direct_job_data(job, data)
    logger.info(
        "Write step finished for %s (%s → %s); no additional rows written here",
        job.login,
        job.start_date,
        job.end_date,
    )
    return success


@flow(name="direct-loader-clickhouse")
async def direct_loader_flow(
    start_date: str | None = None,
    end_date: str | None = None,
    profile: str = "analytics",
    track_changes: bool = False,
    lookback_days: int = 60,
    login: str | None = None,
) -> None:
    """
    Prefect entrypoint for the Direct connector.
    If `track_changes` is True the change tracker is used with per-client tasks.
    With `login` it updates only one client, otherwise all clients.
    When `track_changes` is False, an explicit date range is required.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        profile: Database profile to use (default: "analytics")
        track_changes: Whether to use change tracking (default: False)
        lookback_days: Number of days to look back for changes (default: 60)
        login: Specific client login to process (optional)
    """
    logger = get_run_logger()
    if track_changes:
        logger.info(
            "Starting Direct change tracker with per-client tasks (profile=%s, lookback_days=%s, login=%s)",
            profile,
            lookback_days,
            login,
        )

        clients_future = get_direct_clients_task.submit(login=login, profile=profile)
        clients_result = clients_future.result()
        clients = clients_result.get("clients", [])

        if not clients:
            logger.info("No Direct clients found to process.")
            return

        logger.info("Found %d client(s) to process", len(clients))

        detection_futures = []
        for client_data in clients:
            future = detect_changes_for_client_task.submit(
                client_data=client_data,
                lookback_days=lookback_days,
                profile=profile,
                start_date=start_date,
                end_date=end_date,
            )
            detection_futures.append((client_data["login"], future))

        jobs = []
        clients_without_changes = []
        failed_clients = []

        for client_login, future in detection_futures:
            try:
                result = future.result()
                if result.get("has_changes"):
                    jobs.extend(result.get("jobs", []))
                else:
                    clients_without_changes.append(client_login)
            except Exception as exc:
                logger.error("%s: change detection task failed — %s", client_login, exc)
                failed_clients.append(client_login)

        if clients_without_changes:
            logger.info("Clients without changes: %s", clients_without_changes)

        if failed_clients:
            logger.error("Failed change detection for clients: %s", failed_clients)

        if not jobs:
            if failed_clients:
                raise RuntimeError(f"Change detection failed for clients: {failed_clients}")
            logger.info("No Direct changes detected; nothing to reload.")
            return

        jobs_by_login: dict[str, list[dict]] = defaultdict(list)
        for job_payload in jobs:
            jobs_by_login[job_payload["login"]].append(job_payload)

        logger.info(
            "Processing %d jobs across %d logins (up to %d reports per login)",
            len(jobs),
            len(jobs_by_login),
            MAX_REPORTS_PER_LOGIN,
        )

        inflight_by_login: dict[str, list[tuple[dict, object]]] = defaultdict(list)
        all_jobs_queue = list(jobs)
        job_index = 0

        def _complete_oldest_for_login(login: str) -> None:
            if login not in inflight_by_login or not inflight_by_login[login]:
                return
            job_payload, fetch_future = inflight_by_login[login].pop(0)
            data = fetch_future.result()
            write_direct_range_task.submit(job_payload, data).result()

        while job_index < len(all_jobs_queue) or any(inflight_by_login.values()):
            while job_index < len(all_jobs_queue):
                job = all_jobs_queue[job_index]
                login = job["login"]

                if len(inflight_by_login[login]) < MAX_REPORTS_PER_LOGIN:
                    fetch_future = fetch_direct_range_task.submit(job)
                    inflight_by_login[login].append((job, fetch_future))
                    job_index += 1
                else:
                    _complete_oldest_for_login(login)
                    break

            if job_index < len(all_jobs_queue) and any(inflight_by_login.values()):
                for login in list(inflight_by_login.keys()):
                    if inflight_by_login[login]:
                        _complete_oldest_for_login(login)
                        break
            elif job_index >= len(all_jobs_queue) and any(inflight_by_login.values()):
                for login in list(inflight_by_login.keys()):
                    if inflight_by_login[login]:
                        _complete_oldest_for_login(login)
                        break

        for login in list(inflight_by_login.keys()):
            while inflight_by_login[login]:
                _complete_oldest_for_login(login)

        if failed_clients:
            raise RuntimeError(f"Change detection failed for clients: {failed_clients}")
        return

    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required when track_changes is False")

    logger.info("Starting Direct load for %s → %s (profile=%s)", start_date, end_date, profile)
    await run_direct_range(start_date=start_date, end_date=end_date, profile=profile)
