import logging
import ast
import json
from collections.abc import MutableMapping
from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd

from prefect_loader.orchestration.clickhouse_utils import AsyncVkDatabase
from prefect_loader.orchestration.clickhouse_utils.config import CLICKHOUSE_DB_VK

from .access import get_agency_clients_with_activity, get_vk_agencies
from .api import VkApiClient
from .config import (
    VK_DEFAULT_LOOKBACK_DAYS,
    VK_FLOAT_COLUMNS,
    VK_INT_COLUMNS,
    VK_REQUIRED_COLUMNS,
    VK_STRING_COLUMNS,
)

VK_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "base_vk_goals": (
        "base_vk_goals",
        "base_goals",
        "base_goal",
        "base_conversions",
    ),
}


def get_logger():
    """Get Prefect logger if available, otherwise use module logger."""
    try:
        from prefect import get_run_logger
        from prefect.context import MissingContextError
        return get_run_logger()
    except (ImportError, MissingContextError, RuntimeError):
        return logging.getLogger(__name__)


def flatten_stats_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten nested dictionary structures in DataFrame columns.

    Args:
        df: Input DataFrame with potentially nested structures

    Returns:
        Flattened DataFrame
    """
    if df.empty:
        return df

    def parse_mapping(value: Any) -> MutableMapping[str, Any] | None:
        if isinstance(value, MutableMapping):
            return value
        if not isinstance(value, str):
            return None

        for parser in (ast.literal_eval, json.loads):
            try:
                parsed = parser(value)
            except (ValueError, SyntaxError, TypeError, MemoryError, json.JSONDecodeError):
                continue
            if isinstance(parsed, MutableMapping):
                return parsed
        return None

    def flatten_mapping(
        mapping: MutableMapping[str, Any],
        parent_key: str,
        sep: str = "_",
    ) -> dict[str, Any]:
        items: dict[str, Any] = {}
        for k, v in mapping.items():
            key = f"{parent_key}{sep}{k}" if parent_key else str(k)
            nested = parse_mapping(v)
            if nested:
                items.update(flatten_mapping(nested, key, sep=sep))
            else:
                items[key] = v
        return items

    processed_rows = []
    non_nested_cols = ["campaign_name", "campaign_id", "group_name", "group_id", "date"]

    for _, row in df.iterrows():
        flat_row = {}

        for col in non_nested_cols:
            if col in row:
                flat_row[col] = row[col]

        for col_name in df.columns:
            if col_name in non_nested_cols:
                continue

            if col_name in row and pd.notna(row[col_name]):
                cell_value = row[col_name]

                parsed_mapping = parse_mapping(cell_value)
                if parsed_mapping:
                    flat_row.update(flatten_mapping(parsed_mapping, col_name))
                else:
                    flat_row[col_name] = cell_value

        processed_rows.append(flat_row)

    return pd.DataFrame(processed_rows)


def apply_metric_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Populate canonical metric columns from known API aliases."""
    out = df.copy()
    for target_col, source_cols in VK_METRIC_ALIASES.items():
        available = [col for col in source_cols if col in out.columns]
        if not available:
            continue
        merged = out[available[0]]
        for col in available[1:]:
            merged = merged.combine_first(out[col])
        out[target_col] = merged
    return out


def normalize_vk_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize VK statistics DataFrame to match expected schema.

    Args:
        df: Raw statistics DataFrame

    Returns:
        Normalized DataFrame with correct column types
    """
    if df.empty:
        return pd.DataFrame(columns=VK_REQUIRED_COLUMNS)

    df = flatten_stats_dataframe(df)
    df = apply_metric_aliases(df)

    df = df.reindex(columns=VK_REQUIRED_COLUMNS, fill_value=0)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    if "base_vk_goals" in df.columns:
        df["base_vk_goals"] = df["base_vk_goals"].fillna(0)

    for col in VK_INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(pd.Int64Dtype())

    for col in VK_FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(pd.Float64Dtype())

    for col in VK_STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(pd.StringDtype())

    return df


async def upload_vk_data_for_agency_client(
    client_id: str,
    client_secret: str,
    user_id: str,
    start_date: date | str,
    end_date: date | str,
    *,
    db: AsyncVkDatabase | None = None,
) -> dict[str, Any]:
    """
    Upload VK data for a single agency client.

    Args:
        client_id: VK agency client ID
        client_secret: VK agency client secret
        user_id: Agency client user ID
        start_date: Start date
        end_date: End date
        db: Optional database instance (will create if not provided)

    Returns:
        Dictionary with upload results
    """
    logger = get_logger()
    logger.info("%s (agency %s): Starting data upload (%s → %s)", user_id, client_id, start_date, end_date)

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    api_client = VkApiClient(client_id, client_secret)

    created_db = db is None
    if created_db:
        db = AsyncVkDatabase(CLICKHOUSE_DB_VK)
        await db.init_db()

    try:
        client_token = await api_client.get_access_token(user_id)

        logger.info("%s: Fetching ad plans and groups", user_id)
        ad_plans = await api_client.get_ad_plans_with_groups(client_token)

        if not ad_plans:
            logger.info("%s: No ad plans found", user_id)
            await api_client.delete_access_token(user_id)
            return {"user_id": user_id, "success": True, "rows": 0}

        if "error" in ad_plans:
            error_msg = ad_plans["error"]
            logger.error("%s: Error fetching ad plans: %s", user_id, error_msg)
            await api_client.delete_access_token(user_id)
            return {"user_id": user_id, "success": False, "error": error_msg}

        logger.info("%s: Fetching statistics for %d plans", user_id, len(ad_plans))
        stats_df = await api_client.get_ad_group_daily_statistics(
            client_token, ad_plans, start_date, end_date
        )

        await api_client.delete_access_token(user_id)

        if stats_df.empty:
            logger.info("%s: No statistics data found", user_id)
            return {"user_id": user_id, "success": True, "rows": 0}

        stats_df = normalize_vk_dataframe(stats_df)

        if stats_df.empty:
            logger.info("%s: No data after normalization", user_id)
            return {"user_id": user_id, "success": True, "rows": 0}

        table_name = f"vk_{user_id}".replace("-", "_").replace(".", "_")

        db_start = datetime.combine(start_date, time.min)
        db_end = datetime.combine(end_date, time(23, 59, 59))

        logger.info("%s: Deleting existing data in range", user_id)
        await db.delete_records_between_dates(table_name, db_start, db_end)

        logger.info("%s: Writing %d rows to table %s", user_id, len(stats_df), table_name)
        await db.write_dataframe_to_table(stats_df, table_name)

        logger.info("%s: Upload completed successfully", user_id)
        return {"user_id": user_id, "success": True, "rows": len(stats_df)}

    except Exception as exc:
        logger.error("%s: Upload failed: %s", user_id, exc)
        try:
            await api_client.delete_access_token(user_id)
        except Exception:
            pass
        return {"user_id": user_id, "success": False, "error": str(exc)}

    finally:
        if created_db:
            await db.close_engine()


async def upload_data_for_all_agencies(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    *,
    lookback_days: int = VK_DEFAULT_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """
    Upload VK data for all configured agencies and their clients.

    Args:
        start_date: Start date (defaults to lookback_days ago)
        end_date: End date (defaults to yesterday)
        lookback_days: Number of days to look back if start_date not specified

    Returns:
        Dictionary with upload results
    """
    logger = get_logger()

    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start_date is None:
        start_date = end_date - timedelta(days=lookback_days)
    elif isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    logger.info("Starting VK data upload for all agencies (%s → %s)", start_date, end_date)

    access_db = AsyncVkDatabase()
    await access_db.init_db()

    try:
        agencies = await get_vk_agencies(access_db)
    finally:
        await access_db.close_engine()

    if not agencies:
        logger.warning("No VK agencies found in Accesses; nothing to upload.")
        return {"success": True, "agencies_processed": 0, "clients_processed": 0, "results": []}

    logger.info("Found %d VK agencies to process", len(agencies))

    db = AsyncVkDatabase(CLICKHOUSE_DB_VK)
    await db.init_db()

    try:
        all_results = []
        total_clients = 0

        for agency_id, agency_info in agencies.items():
            client_id = str(agency_id)
            payload_client_id = str(agency_info.get("client_id") or "").strip()
            if payload_client_id and payload_client_id != client_id:
                logger.warning(
                    "Agency key/client_id mismatch: key=%s payload=%s. Using key value.",
                    client_id,
                    payload_client_id,
                )
            client_secret = agency_info["client_secret"]
            container = agency_info.get("container") or client_id

            logger.info("Processing agency: %s (container: %s)", client_id, container)

            active_clients = await get_agency_clients_with_activity(
                client_id, client_secret, lookback_days=lookback_days
            )

            if not active_clients:
                logger.info("%s: No active clients found", client_id)
                continue

            logger.info("%s: Processing %d active clients", client_id, len(active_clients))

            for client_info in active_clients:
                user_id = client_info["user_id"]
                total_clients += 1

                result = await upload_vk_data_for_agency_client(
                    client_id,
                    client_secret,
                    user_id,
                    start_date,
                    end_date,
                    db=db,
                )
                result["agency"] = container
                all_results.append(result)

        successful = sum(1 for r in all_results if r.get("success"))
        total_rows = sum(r.get("rows", 0) for r in all_results if r.get("success"))

        logger.info(
            "VK upload complete: %d agencies, %d clients, %d successful, %d total rows",
            len(agencies),
            total_clients,
            successful,
            total_rows,
        )

        return {
            "success": True,
            "agencies_processed": len(agencies),
            "clients_processed": total_clients,
            "clients_successful": successful,
            "total_rows": total_rows,
            "results": all_results,
        }

    finally:
        await db.close_engine()


__all__ = [
    "flatten_stats_dataframe",
    "normalize_vk_dataframe",
    "upload_vk_data_for_agency_client",
    "upload_data_for_all_agencies",
]
