import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from prefect_loader.connectors.roistat_loader.loader_service import (
    process_all_clients,
    process_single_client,
)


@task(name="Roistat loader: all clients", retries=2, retry_delay_seconds=25, timeout_seconds=60 * 60 * 4)
async def run_roistat_all(tdelta: int = 10) -> None:
    await process_all_clients(tdelta=tdelta)


@task(name="Roistat loader: single client", retries=2, retry_delay_seconds=25, timeout_seconds=60 * 60 * 2)
async def run_roistat_single(site_id: int, tdelta: int = 10) -> None:
    await process_single_client(site_id=site_id, tdelta=tdelta)


@flow(name="roistat-loader-clickhouse")
async def roistat_loader_flow(
    site_id: int | None = None,
    tdelta: int = 10,
) -> None:
    logger = get_run_logger()
    if site_id is not None:
        logger.info("Starting Roistat load for site_id=%s (tdelta=%s)", site_id, tdelta)
        await run_roistat_single(site_id=site_id, tdelta=tdelta)
    else:
        logger.info("Starting Roistat load for all clients (tdelta=%s)", tdelta)
        await run_roistat_all(tdelta=tdelta)
