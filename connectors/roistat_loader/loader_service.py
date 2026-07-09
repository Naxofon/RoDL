from __future__ import annotations

import asyncio
import ast
import hashlib
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover - test environment fallback
    np = None

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - test environment fallback
    pd = None

from .access import collect_roistat_clients
from .flags import enabled_roistat_sections

ROISTAT_TZ = timezone(timedelta(hours=3))
ROISTAT_VISIT_TZ = timezone.utc
ROISTAT_ANALYTICS_URL = "https://cloud.roistat.com/api/v1/project/analytics/data"
ROISTAT_CALLS_URL = "https://cloud.roistat.com/api/v1/project/calltracking/call/list"
ROISTAT_VISITS_URL = "https://cloud.roistat.com/api/v1/project/site/visit/list"

ROISTAT_ANALYTICS_DIMENSIONS = [
    "daily",
    "marker_level_1",
    "referrer",
    "landing_page",
    "country",
    "region",
    "city",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
]
ROISTAT_ANALYTICS_METRICS = [
    "visits",
    "clicks",
    "unique_visits",
    "bounced_visits",
    "leads",
    "uniqueCalls",
    "calls",
    "sales",
    "revenue",
    "marketing_cost",
]
ROISTAT_SECTIONS = ("analytics", "calls", "visits")
ROISTAT_VISIT_COLUMNS: dict[str, str] = {
    "id": "string",
    "first_visit_id": "string",
    "date": "datetime64[ns]",
    "landing_page": "string",
    "host": "string",
    "google_client_id": "string",
    "metrika_client_id": "string",
    "ip": "string",
    "order_ids": "object",
    "cost": "float",
    "device.os": "string",
    "device.agent": "string",
    "device.agent_icon": "string",
    "device.is_mobile": "boolean",
    "source.referrer": "string",
    "source.display_name": "string",
    "source.utm_source": "string",
    "source.utm_medium": "string",
    "source.utm_campaign": "string",
    "source.utm_term": "string",
    "source.utm_content": "string",
    "geo.country": "string",
    "geo.region": "string",
    "geo.city": "string",
    "geo.country_iso": "string",
}


def get_logger():
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def build_target_table_name(section: str, site_id: int) -> str:
    normalized = str(section).strip().lower()
    if normalized not in ROISTAT_SECTIONS:
        raise ValueError(f"Unsupported Roistat section: {section}")
    return f"{normalized}_{site_id}"


def compute_date_window(tdelta: int, *, today: date | None = None) -> tuple[date, date]:
    window_days = max(int(tdelta or 1), 1)
    base_day = today or datetime.now().date()
    end_date = base_day - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)
    return start_date, end_date


def _datetime_range_for_local_days(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, time.min).replace(tzinfo=ROISTAT_TZ)
    end_dt = datetime.combine(end_date, time(23, 59, 59)).replace(tzinfo=ROISTAT_TZ)
    return start_dt, end_dt


def _datetime_range_for_visit_days(start_date: date, end_date: date) -> tuple[str, str]:
    start_dt = datetime.combine(start_date, time.min).replace(tzinfo=ROISTAT_VISIT_TZ)
    end_dt = datetime.combine(end_date, time(23, 59, 59)).replace(tzinfo=ROISTAT_VISIT_TZ)
    return start_dt.isoformat(), end_dt.isoformat()


def _build_headers(token: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Api-Key": token,
    }


async def _post_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any],
    payload: dict[str, Any],
) -> tuple[int, dict[str, Any], str]:
    async with session.post(url, headers=headers, params=params, json=payload) as response:
        text = await response.text()
        try:
            data = await response.json(content_type=None)
        except Exception:
            data = {}
        return response.status, data if isinstance(data, dict) else {}, text


def _analytics_response_to_dataframe(api_response: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in api_response.get("items", []):
        row: dict[str, Any] = {}
        for dim_name, dim_value in item.get("dimensions", {}).items():
            row[dim_name] = dim_value.get("title") if isinstance(dim_value, dict) else dim_value
        for metric in item.get("metrics", []):
            row[metric.get("metric_name")] = metric.get("value")
        rows.append(row)
    return pd.DataFrame(rows)


async def _download_analytics_interval(
    session: aiohttp.ClientSession,
    *,
    token: str,
    site_id: int,
    start_dt: datetime,
    end_dt: datetime,
    min_interval: timedelta = timedelta(hours=1),
) -> pd.DataFrame:
    headers = _build_headers(token)
    params = {"project": str(site_id)}
    payload = {
        "dimensions": ROISTAT_ANALYTICS_DIMENSIONS,
        "metrics": ROISTAT_ANALYTICS_METRICS,
        "period": {"from": start_dt.isoformat(), "to": end_dt.isoformat()},
    }

    status, data, text = await _post_json(
        session,
        ROISTAT_ANALYTICS_URL,
        headers=headers,
        params=params,
        payload=payload,
    )
    if status != 200:
        get_logger().warning(
            "Roistat analytics request failed for site_id=%s interval=%s..%s status=%s body=%s",
            site_id,
            start_dt.isoformat(),
            end_dt.isoformat(),
            status,
            text[:500],
        )
        return pd.DataFrame()

    if data.get("error"):
        description = str(data.get("description") or "")
        current_interval = end_dt - start_dt
        if "Try smaller period" in description and current_interval > min_interval:
            midpoint = start_dt + current_interval / 2
            left_df = await _download_analytics_interval(
                session,
                token=token,
                site_id=site_id,
                start_dt=start_dt,
                end_dt=midpoint,
                min_interval=min_interval,
            )
            right_df = await _download_analytics_interval(
                session,
                token=token,
                site_id=site_id,
                start_dt=midpoint + timedelta(seconds=1),
                end_dt=end_dt,
                min_interval=min_interval,
            )
            frames = [frame for frame in (left_df, right_df) if not frame.empty]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        get_logger().warning(
            "Roistat analytics API error for site_id=%s interval=%s..%s: %s",
            site_id,
            start_dt.isoformat(),
            end_dt.isoformat(),
            description or data.get("error"),
        )
        return pd.DataFrame()

    raw_data = data.get("data") or []
    if not raw_data:
        return pd.DataFrame()

    frame = _analytics_response_to_dataframe(raw_data[0])
    if frame.empty:
        return frame

    if "daily" in frame.columns:
        frame["daily"] = pd.to_datetime(frame["daily"], errors="coerce")
        frame = frame.rename(columns={"daily": "Date"})
    else:
        frame["Date"] = pd.NaT

    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.normalize()
    frame["site_id"] = int(site_id)
    return frame


async def download_analytics_data(
    session: aiohttp.ClientSession,
    *,
    token: str,
    site_id: int,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    start_dt, end_dt = _datetime_range_for_local_days(start_date, end_date)
    day_frames: list[pd.DataFrame] = []
    for offset in range((end_date - start_date).days + 1):
        day = start_date + timedelta(days=offset)
        day_start = datetime.combine(day, time.min).replace(tzinfo=ROISTAT_TZ)
        day_end = datetime.combine(day, time(23, 59, 59)).replace(tzinfo=ROISTAT_TZ)
        frame = await _download_analytics_interval(
            session,
            token=token,
            site_id=site_id,
            start_dt=day_start,
            end_dt=day_end,
        )
        if not frame.empty:
            day_frames.append(frame)

    if not day_frames:
        return pd.DataFrame(columns=["Date", "site_id"])

    result = pd.concat(day_frames, ignore_index=True)
    result = result[(result["Date"].dt.date >= start_date) & (result["Date"].dt.date <= end_date)]
    return result


async def download_calls_data(
    session: aiohttp.ClientSession,
    *,
    token: str,
    site_id: int,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    start_dt, end_dt = _datetime_range_for_local_days(start_date, end_date)
    payload = {
        "filters": {
            "and": [
                ["date", ">", start_dt.isoformat()],
                ["date", "<", end_dt.isoformat()],
            ]
        },
        "extend": ["visit", "order"],
    }
    status, data, text = await _post_json(
        session,
        ROISTAT_CALLS_URL,
        headers=_build_headers(token),
        params={"project": str(site_id)},
        payload=payload,
    )
    if status != 200:
        get_logger().warning("Roistat calls request failed for site_id=%s status=%s body=%s", site_id, status, text[:500])
        return pd.DataFrame(columns=["Date", "site_id"])
    if data.get("error"):
        get_logger().warning("Roistat calls API error for site_id=%s: %s", site_id, data.get("description") or data.get("error"))
        return pd.DataFrame(columns=["Date", "site_id"])

    records = data.get("data") or []
    if not records:
        return pd.DataFrame(columns=["Date", "site_id"])

    frame = pd.json_normalize(records)
    if frame.empty:
        return pd.DataFrame(columns=["Date", "site_id"])
    if "caller" not in frame.columns:
        frame["caller"] = pd.NA
    if "date" not in frame.columns:
        frame["date"] = pd.NaT

    cols_all_nan = frame.columns[frame.isna().all()].tolist()
    drop_columns = [
        "tags",
        "static_source.icon_url",
        "static_source.display_name_by_level",
        "static_source.system_name_by_level",
        "visit.geo.icon_url",
        "visit.source.icon_url",
        "visit.source.display_name_by_level",
        "visit.source.system_name_by_level",
        "link",
        "static_source.system_name",
        "visit.google_client_id",
        "visit.source.system_name",
        "visit.agent",
        "comment",
        "visit.id",
        "visit.date",
    ]
    frame = frame.drop(columns=cols_all_nan + drop_columns, errors="ignore")
    frame = frame.replace("", np.nan)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame["hash_tel"] = frame["caller"].apply(lambda value: hashlib.md5(str(value).encode()).hexdigest())
    frame = frame.drop(columns=["caller"], errors="ignore")
    frame.columns = frame.columns.str.replace(".", "_", regex=False)
    frame = frame.rename(columns={"date": "Date"})
    frame["site_id"] = int(site_id)
    return frame


def normalize_order_ids(value):
    if isinstance(value, list):
        return value or [pd.NA]
    if value is None:
        return [pd.NA]
    try:
        if pd.isna(value):
            return [pd.NA]
    except Exception:
        pass
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in {"", "[]"}:
            return [pd.NA]
        try:
            parsed = ast.literal_eval(stripped)
        except Exception:
            return [stripped]
        if isinstance(parsed, list):
            return parsed or [pd.NA]
        if parsed is None:
            return [pd.NA]
        return [parsed]
    return [value]


async def download_visits_data(
    session: aiohttp.ClientSession,
    *,
    token: str,
    site_id: int,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    offset = 0
    limit = 10000
    frames: list[pd.DataFrame] = []
    date_from, date_to = _datetime_range_for_visit_days(start_date, end_date)
    headers = _build_headers(token)
    params = {"project": str(site_id)}

    while True:
        payload = {
            "filters": {
                "and": [
                    ["date", ">", date_from],
                    ["date", "<", date_to],
                ]
            },
            "limit": limit,
            "offset": offset,
        }
        status, data, text = await _post_json(
            session,
            ROISTAT_VISITS_URL,
            headers=headers,
            params=params,
            payload=payload,
        )
        if status != 200:
            get_logger().warning("Roistat visits request failed for site_id=%s status=%s body=%s", site_id, status, text[:500])
            break
        if data.get("error"):
            get_logger().warning("Roistat visits API error for site_id=%s: %s", site_id, data.get("description") or data.get("error"))
            break
        batch = data.get("data") or []
        batch_df = pd.json_normalize(batch)
        size = len(batch_df)
        if size:
            frames.append(batch_df)
        offset += size
        if size < limit:
            break

    if not frames:
        return pd.DataFrame(columns=["Date", "site_id"])

    result = pd.concat(frames, ignore_index=True)
    for column in ROISTAT_VISIT_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    result = result[list(ROISTAT_VISIT_COLUMNS.keys())]

    for column, dtype in ROISTAT_VISIT_COLUMNS.items():
        if dtype == "datetime64[ns]":
            result[column] = pd.to_datetime(result[column], errors="coerce")
        elif dtype == "float":
            result[column] = pd.to_numeric(result[column], errors="coerce")
        elif dtype == "object":
            continue
        else:
            result[column] = result[column].astype(dtype)

    result["order_ids"] = result["order_ids"].apply(normalize_order_ids)
    result = result.explode("order_ids", ignore_index=True)
    result["metrika_client_id"] = result["metrika_client_id"].astype("string")
    result["google_client_id"] = result["google_client_id"].astype("string")
    result.columns = result.columns.str.replace(".", "_", regex=False)
    result = result.rename(columns={"date": "Date"})
    result["site_id"] = int(site_id)
    return result


async def reload_roistat_section(
    db,
    session: aiohttp.ClientSession,
    *,
    site_id: int,
    token: str,
    section: str,
    start_date: date,
    end_date: date,
) -> int:
    if section == "analytics":
        frame = await download_analytics_data(
            session,
            token=token,
            site_id=site_id,
            start_date=start_date,
            end_date=end_date,
        )
    elif section == "calls":
        frame = await download_calls_data(
            session,
            token=token,
            site_id=site_id,
            start_date=start_date,
            end_date=end_date,
        )
    elif section == "visits":
        frame = await download_visits_data(
            session,
            token=token,
            site_id=site_id,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        raise ValueError(f"Unsupported Roistat section: {section}")

    table_name = build_target_table_name(section, site_id)
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time(23, 59, 59))
    await db.delete_between_dates(table_name, start_dt, end_dt, date_column="Date")
    if frame is None or frame.empty:
        return 0
    await db.write_dataframe(table_name, frame, order=["Date"])
    return len(frame)


async def process_roistat_client(
    client: dict[str, Any],
    *,
    tdelta: int = 10,
) -> dict[str, int]:
    logger = get_logger()
    site_id = int(client["site_id"])
    token = str(client.get("token") or "").strip()
    if not token:
        raise ValueError(f"Roistat site_id={site_id} has empty token")
    sections = enabled_roistat_sections(client.get("flags"))
    if not sections:
        logger.info("Roistat site_id=%s skipped: all sections disabled", site_id)
        return {}

    start_date, end_date = compute_date_window(tdelta)
    logger.info(
        "Roistat site_id=%s processing sections=%s window=%s..%s",
        site_id,
        ",".join(sections),
        start_date.isoformat(),
        end_date.isoformat(),
    )

    from .prefect.clickhouse_utils import AsyncRoistatDatabase

    db = AsyncRoistatDatabase()
    await db.init_db()
    results: dict[str, int] = {}
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for section in sections:
            try:
                results[section] = await reload_roistat_section(
                    db,
                    session,
                    site_id=site_id,
                    token=token,
                    section=section,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:
                logger.error("Roistat site_id=%s section=%s failed: %s", site_id, section, exc, exc_info=True)
    return results


async def process_single_client(
    site_id: int,
    tdelta: int = 10,
    api_token: str | None = None,
) -> dict[str, int]:
    from .prefect.clickhouse_utils import AsyncRoistatDatabase

    db = AsyncRoistatDatabase()
    client = await db.get_client(site_id)
    if client is None:
        raise ValueError(f"Roistat site_id {site_id} not found in Accesses")
    if api_token:
        client["token"] = api_token
    return await process_roistat_client(client, tdelta=tdelta)


async def process_all_clients(tdelta: int = 10) -> dict[int, dict[str, int]]:
    logger = get_logger()
    from .prefect.clickhouse_utils import AsyncRoistatDatabase

    db = AsyncRoistatDatabase()
    rows = await db.fetch_roistat_access_rows()
    clients = collect_roistat_clients(rows)
    if not clients:
        logger.warning("No Roistat clients found in Accesses")
        return {}

    semaphore = asyncio.Semaphore(5)
    results: dict[int, dict[str, int]] = {}

    async def _run_client(client: dict[str, Any]):
        async with semaphore:
            site_id = int(client["site_id"])
            try:
                results[site_id] = await process_roistat_client(client, tdelta=tdelta)
            except Exception as exc:
                logger.error("Roistat site_id=%s failed: %s", site_id, exc, exc_info=True)

    await asyncio.gather(*[_run_client(client) for client in clients])
    return results


__all__ = [
    "build_target_table_name",
    "compute_date_window",
    "process_all_clients",
    "process_single_client",
    "process_roistat_client",
]
