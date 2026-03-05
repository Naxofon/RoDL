import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from prefect_loader.connectors.vk_loader.loader_service import (
    upload_data_for_all_agencies as upload_vk_data_for_all_agencies,
)


@task(name="VK loader: all agencies", retries=2, retry_delay_seconds=25, timeout_seconds=60 * 60 * 4)
async def run_vk_all(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 10,
) -> None:
    """Load VK data for all agencies and their clients."""
    await upload_vk_data_for_all_agencies(
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )


@flow(name="vk-loader-clickhouse")
async def vk_loader_flow(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 10,
) -> None:
    """
    Prefect entrypoint for the VK Ads connector.

    Loads data for all VK agencies and their active clients.

    Args:
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional, defaults to yesterday)
        lookback_days: Number of days to look back if start_date not specified (default: 10)
    """
    logger = get_run_logger()
    logger.info(
        "Starting VK load for all agencies (start_date=%s, end_date=%s, lookback_days=%s)",
        start_date,
        end_date,
        lookback_days,
    )
    await run_vk_all(start_date=start_date, end_date=end_date, lookback_days=lookback_days)
