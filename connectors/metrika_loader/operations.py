import asyncio
import gc
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from prefect_loader.orchestration.clickhouse_utils import AsyncMetrikaDatabase

from .access import collect_metrika_access_data, counters_from_access
from .auth_logging import format_auth_fingerprint
from .jobs import MetrikaReloadJob, plan_metrika_reload_jobs
from .uploader import YaMetrikaUploader


async def emit_prefect_event(
    event: str,
    *,
    counter_id: int,
    payload: dict | None = None,
) -> None:
    """
    Emit a Prefect event for the given counter if executed inside a Prefect flow.
    Acts as a no-op for CLI/bot usage outside Prefect.
    """
    try:
        from prefect.context import get_run_context
        from prefect.events import emit_event

        get_run_context()
    except Exception:
        return

    resource = {
        "prefect.resource.id": f"metrika.counter.{counter_id}",
        "metrika.counter": str(counter_id),
    }
    try:
        await emit_event(
            event=event,
            resource=resource,
            payload=payload or {},
        )
    except Exception:
        logging.debug("Failed to emit Prefect event %s", event, exc_info=True)


async def fetch_metrika_job_data(job: MetrikaReloadJob) -> pd.DataFrame:
    """Fetch raw Metrika data for a reload job and return it as a DataFrame."""
    logging.info(
        "%s: starting API fetch for %s-%s (%s)",
        job.domain_name,
        job.start_date,
        job.end_date,
        format_auth_fingerprint(job.fact_login, job.token),
    )
    uploader = YaMetrikaUploader(
        job.counter_id,
        job.start_date,
        job.end_date,
        job.domain_name,
        job.token,
        login=job.fact_login,
    )
    df = await uploader.load_metrika(
        counter_id=int(job.counter_id),
        token=job.token,
        start_date=job.start_date,
        end_date=job.end_date,
    )
    return df if df is not None else pd.DataFrame()


async def write_metrika_job_data(
    job: MetrikaReloadJob,
    df: pd.DataFrame,
    *,
    db: AsyncMetrikaDatabase | None = None,
    fail_on_empty: bool = True,
) -> int:
    """
    Persist the prepared dataframe into ClickHouse.
    Returns the number of rows written.
    """
    if df is None or df.empty:
        if fail_on_empty:
            raise ValueError(f"{job.domain_name}: empty dataset for {job.start_date}-{job.end_date}")
        logging.warning("%s: empty dataset for %s-%s; nothing to write.", job.domain_name, job.start_date, job.end_date)
        return 0

    async_db = db or AsyncMetrikaDatabase()
    created_db = db is None
    if created_db:
        await async_db.init_db()

    try:
        start_dt = datetime.strptime(job.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(job.end_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{job.domain_name}: invalid date range {job.start_date}-{job.end_date}") from exc

    await async_db.erase_data_in_interval(job.domain_name, start_dt, end_dt)
    await async_db.write_dataframe_to_table(df, job.domain_name)

    if created_db:
        await async_db.close_engine()

    return len(df.index)


async def refresh_data_with_change_tracker(
    *,
    lookback_days: int = 60,
    counter_id: Optional[int] = None,
) -> None:
    """
    Detect day-level differences via MetrikaChangeTracker and reload only the
    affected ranges for all counters (or a single counter if provided).
    """
    async_db = AsyncMetrikaDatabase()
    await async_db.init_db()
    failures: list[str] = []
    jobs: list[MetrikaReloadJob] = []
    ranges_by_counter: dict[int, list[tuple[str, str]]] = {}

    try:
        logging.info(
            "Starting Metrika change tracker (lookback_days=%s, counter_id=%s)",
            lookback_days,
            counter_id,
        )
        jobs, counters_without_changes, failed_counters, counter_diagnostics = await plan_metrika_reload_jobs(
            lookback_days=lookback_days,
            counter_id=counter_id,
            db=async_db,
        )

        for diag in counter_diagnostics:
            logging.info(
                "Change tracker diag %s (counter=%s): API visits=%s (%s days with data), "
                "DB visits=%s (empty=%s), changes_detected=%s",
                diag.get("domain_name"),
                diag.get("counter_id"),
                diag.get("api_total_visits", 0),
                diag.get("api_days_with_data", 0),
                diag.get("db_total_visits", 0),
                diag.get("db_empty", True),
                diag.get("changes_detected", False),
            )

        for failed in failed_counters:
            logging.error(
                "%s: change detection failed — %s",
                failed.get("domain_name"),
                failed.get("error"),
            )
            failures.append(
                f"{failed.get('domain_name')}: change detection failed — {failed.get('error')}"
            )
            await emit_prefect_event(
                "metrika.counter.changes_run_failed",
                counter_id=failed.get("counter_id"),
                payload={"error": failed.get("error", ""), "source": failed.get("source", "")},
            )

        for item in counters_without_changes:
            logging.info("%s: no changes detected for the last %s days.", item["domain_name"], lookback_days)
            await emit_prefect_event(
                "metrika.counter.no_changes",
                counter_id=item["counter_id"],
                payload={"lookback_days": lookback_days},
            )

        if not jobs:
            logging.info("No metrika reload jobs to run.")
            if failures:
                raise RuntimeError("Some counters failed to reload: " + "; ".join(failures))
            return

        seen_started: set[int] = set()
        for job in jobs:
            if job.counter_id not in seen_started:
                await emit_prefect_event(
                    "metrika.counter.changes_run_started",
                    counter_id=job.counter_id,
                    payload={"lookback_days": lookback_days},
                )
                seen_started.add(job.counter_id)

            logging.info("%s: fetching %s-%s (source=%s)", job.domain_name, job.start_date, job.end_date, job.source)
            await emit_prefect_event(
                "metrika.counter.reload_started",
                counter_id=job.counter_id,
                payload={"time_from": job.start_date, "time_to": job.end_date},
            )
            try:
                df = await fetch_metrika_job_data(job)
                rows = await write_metrika_job_data(job, df, db=async_db, fail_on_empty=True)
            except Exception as exc:
                msg = f"{job.domain_name}: reload failed for {job.start_date}-{job.end_date} — {exc}"
                failures.append(msg)
                logging.error(msg)
                await emit_prefect_event(
                    "metrika.counter.reload_failed",
                    counter_id=job.counter_id,
                    payload={"time_from": job.start_date, "time_to": job.end_date, "error": str(exc)},
                )
                continue

            ranges_by_counter.setdefault(job.counter_id, []).append((job.start_date, job.end_date))
            logging.info("%s: wrote %d rows for %s-%s", job.domain_name, rows, job.start_date, job.end_date)
            await emit_prefect_event(
                "metrika.counter.reload_finished",
                counter_id=job.counter_id,
                payload={"time_from": job.start_date, "time_to": job.end_date, "rows": rows},
            )

        for cid, ranges in ranges_by_counter.items():
            await emit_prefect_event(
                "metrika.counter.changes_run_finished",
                counter_id=cid,
                payload={"ranges": ranges},
            )

        if failures:
            raise RuntimeError("Some counters failed to reload: " + "; ".join(failures))

    finally:
        await async_db.close_engine()
        logging.info(
            "Change tracker finished for %d counters",
            len({job.counter_id for job in jobs}) if jobs else 0,
        )


async def erase_data_interval_for_all_counters(start, end):
    """Delete data in the given date interval for every known Metrika counter."""
    async_db = AsyncMetrikaDatabase()
    await async_db.init_db()
    df_access = await collect_metrika_access_data(async_db, favorite_only=True)
    counters_df = counters_from_access(df_access)
    if counters_df.empty:
        raise Exception("No metrika accounts found in Accesses")

    for _, row in counters_df.iterrows():
        domain_name = row['name']
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
        await async_db.erase_data_in_interval(domain_name, start_date, end_date)


async def run_uploader_chunk(chunk):
    """Run a single YaMetrikaUploader chunk and release memory afterwards."""
    uploader = YaMetrikaUploader(
        chunk['counter'],
        chunk['start'],
        chunk['end'],
        chunk['domain_name'],
        chunk['token'],
        login=chunk.get('fact_login'),
    )
    await uploader.upload_data()
    del uploader
    gc.collect()


async def run_uploader_chunk_with_semaphore(chunk, global_semaphore, token_semaphore):
    """Run an uploader chunk respecting the global and per-token concurrency semaphores."""
    async with global_semaphore:
        async with token_semaphore:
            await asyncio.sleep(1)
            return await run_uploader_chunk(chunk)


async def process_chunks_in_parallel(df_full_access, date_chunks, global_semaphore):
    """Dispatch all counter/date-range combinations concurrently using per-token semaphores."""
    tasks = []
    token_semaphores = {}

    for _, row in df_full_access.iterrows():
        token = row['token']
        if token not in token_semaphores:
            token_semaphores[token] = asyncio.Semaphore(3)

        for start_chunk, end_chunk in date_chunks:
            chunk = {'counter': row['id'], 'start': start_chunk, 'end': end_chunk, 'domain_name': row['name'], 'token': token}
            chunk['fact_login'] = row.get('fact_login')
            tasks.append(run_uploader_chunk_with_semaphore(chunk, global_semaphore, token_semaphores[token]))

    await asyncio.gather(*tasks)


async def upload_data_for_all_counters(start, end):
    """Upload Metrika data for all counters over the given date range in parallel chunks."""
    async_db = AsyncMetrikaDatabase()
    await async_db.init_db()
    df_full_access = await collect_metrika_access_data(async_db, favorite_only=True)
    if df_full_access.empty:
        raise Exception("No metrika tokens found after filtering Accesses for upload")
    logging.info(
        "Starting full upload for %d counters from %s to %s",
        len(df_full_access),
        start,
        end,
    )
    logging.info("Upload sources: %s", df_full_access[["id", "source"]].head(20).to_dict("records"))
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    date_chunks = []

    while start_date < end_date:
        chunk_end_date = min(start_date + timedelta(days=5), end_date)
        date_chunks.append((start_date.strftime("%Y-%m-%d"), chunk_end_date.strftime("%Y-%m-%d")))
        start_date = chunk_end_date + timedelta(days=1)

    await erase_data_interval_for_all_counters(start, end)

    global_semaphore = asyncio.Semaphore(15)

    await process_chunks_in_parallel(df_full_access, date_chunks, global_semaphore)



async def upload_data_for_single_counter(counter_id, start, end):
    """Upload Metrika data for one specific counter over the given date range."""
    async_db = AsyncMetrikaDatabase()
    await async_db.init_db()
    df_full_access = await collect_metrika_access_data(async_db, favorite_only=True)
    if df_full_access.empty:
        raise Exception("No metrika accounts found in Accesses")

    counter_row = df_full_access[df_full_access['id'] == counter_id]
    if counter_row.empty:
        raise Exception(f"Counter {counter_id} not found in Accesses")
    token = counter_row.iloc[0]['token']
    fact_login = counter_row.iloc[0].get('fact_login')
    logging.info(
        "Starting upload for counter %s from %s to %s",
        counter_id,
        start,
        end,
    )
    logging.info("Upload source: %s", counter_row[["id", "source"]].to_dict())

    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    await async_db.erase_data_in_interval(f'm_{counter_id}', start_date, end_date)

    date_chunks = []

    while start_date < end_date:
        chunk_end_date = min(start_date + timedelta(days=5), end_date)
        date_chunks.append((start_date.strftime("%Y-%m-%d"), chunk_end_date.strftime("%Y-%m-%d")))
        start_date = chunk_end_date + timedelta(days=1)

    semaphore = asyncio.Semaphore(3)

    async def run_chunk_with_semaphore(chunk, semaphore):
        """Run an uploader chunk with concurrency limited by the given semaphore."""
        async with semaphore:
            return await run_uploader_chunk(chunk)

    for start_chunk, end_chunk in date_chunks:
        chunk = {'counter': counter_id, 'start': start_chunk, 'end': end_chunk, 'domain_name': f"m_{counter_id}", 'token': token, 'fact_login': fact_login}
        result = await run_chunk_with_semaphore(chunk, semaphore)
        if result == "split":
            smaller_date_chunks = YaMetrikaUploader(counter_id, start_chunk, end_chunk, f"m_{counter_id}", token).split_date_range(start_chunk, end_chunk, 5)
            for smaller_start, smaller_end in smaller_date_chunks:
                smaller_chunk = {'counter': counter_id, 'start': smaller_start, 'end': smaller_end, 'domain_name': f"m_{counter_id}", 'token': token, 'fact_login': fact_login}
                await run_chunk_with_semaphore(smaller_chunk, semaphore)



async def continue_upload_data_for_counters(counter_id, start, end):
    """Resume a full upload starting from the specified counter_id through the end of the access list."""
    async_db = AsyncMetrikaDatabase()
    await async_db.init_db()
    counters_df = await collect_metrika_access_data(async_db, favorite_only=True)
    if counters_df.empty:
        raise Exception("No metrika accounts found in Accesses")

    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    date_chunks = []

    while start_date < end_date:
        chunk_end_date = min(start_date + timedelta(days=5), end_date)
        date_chunks.append((start_date.strftime("%Y-%m-%d"), chunk_end_date.strftime("%Y-%m-%d")))
        start_date = chunk_end_date + timedelta(days=1)

    df_full_access = counters_df.dropna(subset=["token"]).drop_duplicates(subset=["id"])

    try:
        counter_index = df_full_access[df_full_access['id'] == counter_id].index[0]
    except IndexError:
        logging.error(f"Counter ID {counter_id} not found in the list of counters.")
        return
    logging.info(
        "Continuing upload for counters starting from ID %s (%d counters), range %s-%s",
        counter_id,
        len(df_full_access),
        start,
        end,
    )
    logging.info("Continue upload sources: %s", df_full_access[["id", "source"]].head(20).to_dict("records"))

    df_full_access = df_full_access.iloc[counter_index:]

    semaphore = asyncio.Semaphore(3)

    async def run_chunk_with_semaphore(chunk, semaphore):
        """Run an uploader chunk with concurrency limited by the given semaphore."""
        async with semaphore:
            return await run_uploader_chunk(chunk)

    for _, row in df_full_access.iterrows():
        token = row['token']
        for start_chunk, end_chunk in date_chunks:
            chunk = {'counter': row['id'], 'start': start_chunk, 'end': end_chunk, 'domain_name': row['name'], 'token': token, 'fact_login': row.get('fact_login')}
            result = await run_chunk_with_semaphore(chunk, semaphore)
            if result == "split":
                smaller_date_chunks = YaMetrikaUploader(row['id'], start_chunk, end_chunk, row['name'], token).split_date_range(start_chunk, end_chunk, 5)
                for smaller_start, smaller_end in smaller_date_chunks:
                    smaller_chunk = {'counter': row['id'], 'start': smaller_start, 'end': smaller_end, 'domain_name': row['name'], 'token': token, 'fact_login': row.get('fact_login')}
                    await run_chunk_with_semaphore(smaller_chunk, semaphore)



__all__ = [
    "fetch_metrika_job_data",
    "write_metrika_job_data",
    "refresh_data_with_change_tracker",
    "erase_data_interval_for_all_counters",
    "upload_data_for_all_counters",
    "upload_data_for_single_counter",
    "continue_upload_data_for_counters",
]
