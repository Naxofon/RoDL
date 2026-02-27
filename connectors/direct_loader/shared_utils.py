from __future__ import annotations

import asyncio
import json
import random as rnd
import secrets
from datetime import datetime
from typing import Optional

import aiohttp
import pandas as pd

from .config import (
    DEFAULT_DB_PROFILE,
    DIRECT_BACKOFF_BASE,
    DIRECT_MAX_RETRIES,
    DIRECT_TIMEOUT,
)
from .logging_utils import get_logger


def normalize_login_to_db(login: str) -> str:
    """Convert login to database table format (hyphens to underscores).

    Args:
        login: Client login (e.g., 'client-name' or 'client_name')

    Returns:
        Normalized login for database use (e.g., 'client_name')
    """
    return login.replace("-", "_") if login else ""


def normalize_login_to_api(login: str) -> str:
    """Convert login to Yandex Direct API format (underscores to hyphens).

    Args:
        login: Client login (e.g., 'client_name' or 'client-name')

    Returns:
        Normalized login for API use (e.g., 'client-name')
    """
    return login.replace("_", "-") if login else ""


def build_direct_headers(token: str, client_login: str | None = None) -> dict[str, str]:
    """Build headers for Yandex Direct API requests.

    Args:
        token: Yandex Direct API token
        client_login: Optional client login (will be normalized to API format)

    Returns:
        Dictionary of HTTP headers for Direct API requests
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Client-Login": normalize_login_to_api(client_login) if client_login else "",
        "Use-Operator-Units": "true",
        "RequestId": secrets.token_hex(8),
    }
    if not client_login:
        headers.pop("Client-Login", None)
    return headers


async def direct_post_json(
    session: aiohttp.ClientSession,
    url: str,
    body: dict,
    *,
    token: str,
    client_login: str | None = None,
    semaphore: asyncio.Semaphore | None = None,
    max_retries: int = DIRECT_MAX_RETRIES,
) -> dict:
    """POST JSON to Yandex Direct API with retry and backoff logic.

    Handles rate limiting (429), server errors (5xx), queue full errors (9000),
    and network issues with exponential backoff.

    Args:
        session: aiohttp ClientSession
        url: API endpoint URL
        body: Request body as dictionary
        token: Yandex Direct API token
        client_login: Optional client login
        semaphore: Optional semaphore for concurrency control
        max_retries: Maximum number of retry attempts

    Returns:
        Parsed JSON response as dictionary

    Raises:
        RuntimeError: On non-recoverable errors or after max retries
    """
    headers = build_direct_headers(token, client_login)
    headers.setdefault("processingMode", "auto")
    attempt = 0
    sem = semaphore or asyncio.Semaphore(10)

    while True:
        attempt += 1
        try:
            async with sem:
                async with session.post(url, json=body, headers=headers, timeout=DIRECT_TIMEOUT) as resp:
                    status = resp.status
                    text = await resp.text()
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError:
                        payload = {"raw": text}

                    if status in (429, 500, 502, 503, 504):
                        retry_in = resp.headers.get("RetryIn") or resp.headers.get("retryin")
                        if retry_in:
                            delay = float(retry_in)
                        else:
                            delay = min(60, DIRECT_BACKOFF_BASE ** attempt + rnd.random())
                        get_logger().warning(f"Direct API {url} throttled ({status}), retry in {delay}s")
                        if attempt >= max_retries:
                            raise RuntimeError(f"Direct API throttled ({status}) after {attempt} attempts: {text}")
                        await asyncio.sleep(delay)
                        continue

                    if isinstance(payload, dict) and "error" in payload:
                        error = payload["error"]
                        if not isinstance(error, dict):
                            raise RuntimeError(f"Direct API error (malformed): {error}")
                        code = error.get("error_code") or error.get("errorCode")
                        message = error.get("error_detail") or error.get("error_string") or error
                        if code == "9000":
                            delay = 60
                            get_logger().info(f"Direct queue full, retry in {delay}s")
                            if attempt >= max_retries:
                                raise RuntimeError(f"Direct queue full after {attempt} attempts: {message}")
                            await asyncio.sleep(delay)
                            continue
                        raise RuntimeError(f"Direct API error ({code}): {message}")

                    if status >= 400:
                        raise RuntimeError(f"Direct API HTTP {status}: {text}")
                    return payload
        except Exception as exc:
            if attempt >= max_retries:
                raise
            delay = min(60, DIRECT_BACKOFF_BASE ** attempt + rnd.random())
            get_logger().warning(f"Direct request retry {attempt}/{max_retries} after error {exc}; sleeping {delay}s")
            await asyncio.sleep(delay)


async def get_goal_ids_by_client(
    token: str,
    client_login: str,
    *,
    timeout: Optional[aiohttp.ClientTimeout] = None,
) -> list[int]:
    """Retrieve goal IDs for a client from Yandex Direct campaigns.

    This function fetches all campaigns for a client and extracts goal IDs
    from PriorityGoals in TextCampaign, DynamicTextCampaign, and SmartCampaign types.
    Uses fully defensive parsing to handle API response variations.

    Args:
        token: Yandex Direct API token
        client_login: Client login (will be normalized to API format)
        timeout: Optional aiohttp timeout (defaults to DIRECT_TIMEOUT)

    Returns:
        Sorted list of unique goal IDs (excluding goal ID 12)
    """
    client_login = normalize_login_to_api(client_login)
    url = "https://api.direct.yandex.com/json/v5/campaigns"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Client-Login": client_login,
    }
    body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "Types": [
                    "TEXT_CAMPAIGN",
                    "MOBILE_APP_CAMPAIGN",
                    "DYNAMIC_TEXT_CAMPAIGN",
                    "CPM_BANNER_CAMPAIGN",
                    "SMART_CAMPAIGN",
                    "UNIFIED_CAMPAIGN",
                ],
                "States": ["ON", "OFF", "SUSPENDED", "ARCHIVED", "CONVERTED", "ENDED"],
                "Statuses": ["ACCEPTED", "DRAFT", "MODERATION", "REJECTED"],
            },
            "FieldNames": ["Id", "Name"],
            "TextCampaignFieldNames": ["PriorityGoals"],
            "DynamicTextCampaignFieldNames": ["PriorityGoals"],
            "SmartCampaignFieldNames": ["PriorityGoals"],
            "Page": {"Limit": 10_000, "Offset": 0},
        },
    }

    goal_ids: set[int] = set()
    offset = 0
    session_timeout = timeout or DIRECT_TIMEOUT

    while True:
        body["params"]["Page"]["Offset"] = offset
        try:
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                async with session.post(url, json=body, headers=headers) as r:
                    if r.status != 200:
                        get_logger().warning(
                            f"{client_login}: Campaigns HTTP {r.status}: {await r.text()}"
                        )
                        return sorted(goal_ids)
                    try:
                        js = await r.json(content_type=None)
                    except Exception as e:
                        get_logger().warning(
                            f"{client_login}: Campaigns non-JSON payload ({e})"
                        )
                        return sorted(goal_ids)
        except Exception as exc:
            get_logger().error(f"{client_login}: Campaigns fetch error — {exc}")
            return sorted(goal_ids)

        if not isinstance(js, dict):
            get_logger().warning(f"{client_login}: Campaigns payload not a dict")
            return sorted(goal_ids)

        result_obj = js.get("result")
        if not isinstance(result_obj, dict):
            get_logger().warning(f"{client_login}: 'result' not a dict")
            return sorted(goal_ids)

        campaigns_raw = result_obj.get("Campaigns") or []
        campaigns: list[dict] = (
            campaigns_raw if isinstance(campaigns_raw, list) else []
        )

        for camp in campaigns:
            if not isinstance(camp, dict):
                continue

            for capkey in ("TextCampaign", "DynamicTextCampaign", "SmartCampaign"):
                cap = camp.get(capkey)
                if not isinstance(cap, dict):
                    continue
                prio = cap.get("PriorityGoals")
                if not isinstance(prio, dict):
                    continue
                items = prio.get("Items") or []
                if not isinstance(items, list):
                    continue
                for g in items:
                    if isinstance(g, dict):
                        gid = g.get("GoalId")
                        if isinstance(gid, int):
                            goal_ids.add(gid)

        limited_by = result_obj.get("LimitedBy")
        if isinstance(limited_by, int):
            offset = limited_by
        else:
            break

    goal_ids.discard(12)
    return sorted(goal_ids)


def get_conversion_columns(df: pd.DataFrame, prefix: str = "Conv") -> list[str]:
    """Get list of conversion column names from a DataFrame.

    Args:
        df: DataFrame to search
        prefix: Column name prefix to match (case-sensitive)

    Returns:
        List of column names starting with the specified prefix
    """
    return [col for col in df.columns if col.startswith(prefix)]


def fill_na_for_conversion_columns(df: pd.DataFrame, prefix: str = "Conv") -> pd.DataFrame:
    """Fill NA values in conversion columns with 0.

    Args:
        df: DataFrame to process
        prefix: Conversion column prefix (default: "Conv")

    Returns:
        DataFrame with NA values filled
    """
    conversion_columns = get_conversion_columns(df, prefix)
    if conversion_columns:
        df[conversion_columns] = df[conversion_columns].fillna(0)
    return df


def cast_conversion_columns_to_int(df: pd.DataFrame, prefix: str = "Conv") -> pd.DataFrame:
    """Cast conversion columns to integer type.

    Args:
        df: DataFrame to process
        prefix: Conversion column prefix (default: "Conv")

    Returns:
        DataFrame with conversion columns as integers
    """
    conversion_columns = get_conversion_columns(df, prefix)
    if conversion_columns:
        df[conversion_columns] = df[conversion_columns].astype(int)
    return df


def process_conversion_columns(
    df: pd.DataFrame,
    prefix: str = "Conv",
    *,
    add_sum: bool = False,
) -> pd.DataFrame:
    """Process conversion columns: fill NA with 0 and cast to int.

    This is a convenience function that combines fill_na and cast operations.

    Args:
        df: DataFrame to process
        prefix: Conversion column prefix (default: "Conv")
        add_sum: If True, add a SumConversion column with row totals

    Returns:
        Processed DataFrame
    """
    df = fill_na_for_conversion_columns(df, prefix)
    df = cast_conversion_columns_to_int(df, prefix)

    if add_sum:
        conversion_columns = get_conversion_columns(df, prefix)
        if conversion_columns:
            df['SumConversion'] = df[conversion_columns].sum(axis=1)

    return df


def normalize_conversion_columns_case_insensitive(
    df: pd.DataFrame,
    target_prefix: str = "Conversions",
) -> list[str]:
    """Get conversion columns with case-insensitive matching.

    Args:
        df: DataFrame to search
        target_prefix: Prefix to match (case-insensitive)

    Returns:
        List of column names starting with target_prefix (case-insensitive)
    """
    return sorted(c for c in df.columns if c.lower().startswith(target_prefix.lower()))


def convert_days_to_date_ranges(days: list[str]) -> list[tuple[str, str]]:
    """Convert a list of dates into continuous date ranges.

    Takes a list of date strings and groups consecutive dates into ranges.
    For example: ['2024-01-01', '2024-01-02', '2024-01-04'] becomes
    [('2024-01-01', '2024-01-02'), ('2024-01-04', '2024-01-04')]

    Args:
        days: List of date strings in 'YYYY-MM-DD' format

    Returns:
        List of (start_date, end_date) tuples as strings
    """
    if not days:
        return []

    dates = sorted({datetime.strptime(d, "%Y-%m-%d") for d in days})
    ranges: list[tuple[str, str]] = []
    start = end = dates[0]

    for d in dates[1:]:
        if (d - end).days == 1:
            end = d
        else:
            ranges.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
            start = end = d

    ranges.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    return ranges


def parse_date(date_string: str, format: str = "%Y-%m-%d") -> datetime:
    """Parse a date string to datetime object.

    Args:
        date_string: Date string to parse
        format: Date format (default: '%Y-%m-%d')

    Returns:
        Parsed datetime object
    """
    return datetime.strptime(date_string, format)


def format_date(date_obj: datetime, format: str = "%Y-%m-%d") -> str:
    """Format a datetime object to string.

    Args:
        date_obj: Datetime object to format
        format: Date format (default: '%Y-%m-%d')

    Returns:
        Formatted date string
    """
    return date_obj.strftime(format)


async def emit_prefect_event(
    event: str,
    *,
    login: str,
    profile: str = DEFAULT_DB_PROFILE,
    payload: dict | None = None,
) -> None:
    """Emit a Prefect event if running inside a flow.

    This function only emits events when running within a Prefect flow context.
    Outside of Prefect, it's a no-op. Errors are logged but don't raise.

    Args:
        event: Event name (e.g., 'direct.client.reload_started')
        login: Client login identifier
        profile: Database profile name
        payload: Optional event payload dictionary
    """
    try:
        from prefect.events import emit_event as prefect_emit
        from prefect.context import get_run_context

        get_run_context()
    except Exception:
        return

    resource = {
        "prefect.resource.id": f"direct.client.{login}",
        "direct.client": login,
        "direct.profile": profile,
    }
    try:
        await prefect_emit(
            event=event,
            resource=resource,
            payload=payload or {},
        )
    except Exception:
        get_logger().debug("Failed to emit Prefect event %s", event, exc_info=True)
