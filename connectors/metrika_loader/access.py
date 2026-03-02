import asyncio
from typing import Any, Optional

import aiohttp
import pandas as pd

from prefect_loader.orchestration.clickhouse_utils import AsyncMetrikaDatabase

from prefect import get_run_logger as get_logger

METRIKA_AGENCY_SUBTYPES = {"agency", "agency_token"}
METRIKA_COUNTER_PAGE_LIMIT = 200


def _normalize_counter_id(value: Any) -> Optional[int]:
    """Normalize a counter ID value to int, returning None on failure."""
    try:
        return int(str(value))
    except Exception:
        return None


async def _fetch_favorite_counters(
    token: str,
    *,
    label: str,
    favorite_only: bool = True,
) -> list[dict]:
    """
    Fetch favourite counters for the given agency token. Returns list of dicts with id/owner_login.
    """
    url = "https://api-metrika.yandex.ru/management/v1/counters"
    headers = {"Authorization": f"OAuth {token}"}
    counters: list[dict] = []
    params = {"limit": METRIKA_COUNTER_PAGE_LIMIT, "offset": 1}
    if favorite_only:
        params["favorite"] = "1"

    offset = 1
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params["offset"] = offset
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        get_logger().error(
                            "%s: failed to list Metrika counters (HTTP %s): %s",
                            label,
                            resp.status,
                            await resp.text(),
                        )
                        break
                    payload = await resp.json()
                    chunk = payload.get("counters") or []
                    if not isinstance(chunk, list):
                        get_logger().error("%s: unexpected counters payload shape", label)
                        break
                    counters.extend(
                        {
                            "id": _normalize_counter_id(item.get("id")),
                            "owner_login": item.get("owner_login") or item.get("login"),
                            "favorite": item.get("favorite"),
                        }
                        for item in chunk
                    )
                    if len(chunk) < METRIKA_COUNTER_PAGE_LIMIT:
                        break
                    offset += METRIKA_COUNTER_PAGE_LIMIT
    except Exception as exc:
        get_logger().error("%s: error while listing counters — %s", label, exc)
    return [c for c in counters if c.get("id") is not None]


async def collect_metrika_access_data(
    async_db: AsyncMetrikaDatabase,
    *,
    favorite_only: bool = True,
    force_include_ids: set[int] | None = None,
) -> pd.DataFrame:
    """
    Build counter→token mapping from Accesses, supporting multiple agencies.

    Rules:
        - Only counters explicitly registered as clients (numeric login/container) may be loaded.
        - Agency tokens are used *only* to fetch the favorite list; data is loaded with the client token.
        - Favorites from multiple agency tokens are merged; counters not present as explicit clients are skipped.
    """
    try:
        access_rows = await async_db._access_db.fetch_access_rows(
            service_type="metrika",
            include_null_type=True,
        )
    except Exception as exc:
        get_logger().error("Failed to fetch metrika Accesses rows: %s", exc)
        access_rows = []

    get_logger().info("Metrika Accesses rows fetched: %d", len(access_rows))

    explicit_tokens: dict[int, dict] = {}
    agency_rows: list[dict] = []

    for row in access_rows:
        token = row.get("token")
        if not token:
            continue

        login_val = row.get("login")
        container_val = row.get("container")
        subtype = row.get("subtype")

        counter_id = _normalize_counter_id(login_val)
        if counter_id is None:
            counter_id = _normalize_counter_id(container_val)
        if counter_id is not None:
            explicit_tokens[counter_id] = {
                "token": token,
                "fact_login": login_val or container_val or "",
            }
            continue

        if subtype in METRIKA_AGENCY_SUBTYPES or login_val is None:
            agency_rows.append(row)

    agency_rows = [r for r in agency_rows if r.get("token")]
    get_logger().info(
        "Metrika Accesses: %d explicit counter tokens, %d agency tokens",
        len(explicit_tokens),
        len(agency_rows),
    )

    agency_sem = asyncio.Semaphore(3)

    async def _pull_agency(row: dict):
        """Fetch favorite counters for a single agency row under the shared semaphore."""
        async with agency_sem:
            label = row.get("container") or row.get("login") or "agency"
            counters = await _fetch_favorite_counters(row["token"], label=label, favorite_only=favorite_only)
            return label, row["token"], counters

    agency_results = await asyncio.gather(*(_pull_agency(r) for r in agency_rows), return_exceptions=True)

    favorite_ids: set[int] = set()
    for res in agency_results:
        if isinstance(res, Exception):
            get_logger().error("Agency counters fetch failed: %s", res)
            continue
        label, _agency_token, counters = res
        ids = [c.get("id") for c in counters if c.get("id") is not None]
        favorite_ids.update(ids)
        get_logger().info("%s: favorites discovered: %s%s", label, ids[:20], " …" if len(ids) > 20 else "")

    if force_include_ids:
        favorite_ids.update(force_include_ids)

    records: dict[int, dict] = {}
    if favorite_ids:
        for cid, token_info in explicit_tokens.items():
            if cid not in favorite_ids:
                continue
            records[cid] = {
                "id": cid,
                "name": f"m_{cid}",
                "token": token_info.get("token"),
                "fact_login": token_info.get("fact_login"),
                "source": "explicit_token",
            }
        skipped = sorted(set(explicit_tokens.keys()) - favorite_ids)
        if skipped:
            get_logger().info(
                "Skipping %d explicit counters not in favorites: %s%s",
                len(skipped),
                skipped[:20],
                " …" if len(skipped) > 20 else "",
            )
    else:
        for cid, token_info in explicit_tokens.items():
            records[cid] = {
                "id": cid,
                "name": f"m_{cid}",
                "token": token_info.get("token"),
                "fact_login": token_info.get("fact_login"),
                "source": "explicit_token",
            }

    if records:
        get_logger().info(
            "Metrika Accesses ready: %d counters after applying favorites (agency tokens checked: %d)",
            len(records),
            len(agency_rows),
        )
        return pd.DataFrame(records.values())

    try:
        legacy_df = await async_db.get_metrika_config_data()
        if isinstance(legacy_df, pd.DataFrame) and not legacy_df.empty:
            legacy_df = legacy_df.rename(columns={"counter_metric": "id"})
            legacy_df["id"] = legacy_df["id"].apply(_normalize_counter_id)
            legacy_df = legacy_df.dropna(subset=["id", "token"])
            if not legacy_df.empty:
                legacy_df["id"] = legacy_df["id"].astype(int)
                legacy_df["name"] = "m_" + legacy_df["id"].astype(str)
                legacy_df["source"] = "legacy_config"
                return legacy_df[["id", "name", "token", "fact_login", "source"]]
    except Exception as exc:
        get_logger().error("Failed to read legacy metrika_config: %s", exc)

    get_logger().warning("No metrika Accesses or legacy config found.")
    return pd.DataFrame(columns=["id", "name", "token", "fact_login", "source"])


def counters_from_access(df_access: pd.DataFrame) -> pd.DataFrame:
    """
    Build counters dataframe from Accesses rows of service 'metrika'.
    Supports both legacy columns (counter_metric/token) and new ones (id/name/token).
    """
    if df_access is None or df_access.empty:
        raise Exception("No metrika accounts found in Accesses")

    df = df_access.copy()
    if "counter_metric" in df.columns and "id" not in df.columns:
        df["id"] = df["counter_metric"]
    if "name" not in df.columns and "id" in df.columns:
        df["name"] = "m_" + df["id"].astype(str)

    required = {"id", "token"}
    if not required.issubset(df.columns):
        raise Exception("Accesses metrika rows missing required columns (id/token)")

    df = df.dropna(subset=list(required))
    df["id"] = df["id"].astype(int)
    return df[["id", "name", "token"]]


__all__ = [
    "_normalize_counter_id",
    "_fetch_favorite_counters",
    "collect_metrika_access_data",
    "counters_from_access",
]
