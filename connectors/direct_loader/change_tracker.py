from __future__ import annotations

import asyncio
import aiohttp
import io
import json
import random
import string
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd

from .config import SKIP_COST_CHECK_CLIENTS, global_rate_limiter, offline_report_tracker
from .logging_utils import get_logger
from .shared_utils import (
    get_goal_ids_by_client,
    normalize_conversion_columns_case_insensitive,
)


class DirectChangeTracker:
    """Detects day-level changes between Yandex-Direct and Postgres."""

    def __init__(
        self,
        token: str,
        db,
        *,
        lookback_days: int = 60,
        compare_conversions: bool = True,
        cost_tolerance: int = 5,
        skip_cost_check_clients: Optional[set[str]] = None,
    ) -> None:
        self.token = token
        self.db = db
        self.lookback_days = max(1, int(lookback_days))
        self.compare_conversions = compare_conversions
        self.cost_tolerance = cost_tolerance
        if skip_cost_check_clients is None:
            skip_cost_check_clients = SKIP_COST_CHECK_CLIENTS
        self.skip_cost_check_clients = set(skip_cost_check_clients)
        self._api_semaphore: asyncio.Semaphore | None = None
        self._api_semaphore_loop: asyncio.AbstractEventLoop | None = None

    @property
    def api_semaphore(self) -> asyncio.Semaphore:
        """Lazily create semaphore in the current event loop, recreating if loop changed."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._api_semaphore is None or self._api_semaphore_loop is not current_loop:
            self._api_semaphore = asyncio.Semaphore(5) if current_loop else None
            self._api_semaphore_loop = current_loop
        return self._api_semaphore

    @api_semaphore.setter
    def api_semaphore(self, value: asyncio.Semaphore):
        """Allow external override of semaphore."""
        self._api_semaphore = value
        try:
            self._api_semaphore_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._api_semaphore_loop = None

    async def _fetch_report_slice(
        self,
        client_login: str,
        start_date: str,
        end_date: str,
        goals: Optional[list[int]],
    ) -> Optional[pd.DataFrame]:
        url = "https://api.direct.yandex.com/json/v5/reports"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Client-Login": client_login.replace("_", "-"),
            "Accept-Language": "ru",
            "processingMode": "auto",
            "skipReportHeader": "true",
            "skipReportSummary": "true",
        }

        rnd = f"{random.randint(1, 99_999_999_999)}{random.choice(string.ascii_lowercase)}"
        params = {
            "SelectionCriteria": {"DateFrom": start_date, "DateTo": end_date},
            "FieldNames": ["Date", "Impressions", "Clicks", "Cost"],
            "ReportName": f"CHANGE_CHECK_{client_login}_{start_date}_{end_date}_{rnd}",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO",
        }
        if goals:
            params["Goals"] = goals
            params["FieldNames"].append("Conversions")
            params["AttributionModels"] = ["LSC"]

        body = json.dumps({"method": "get", "params": params}).encode()

        attempt = 0
        async with offline_report_tracker.reserve(self.token, owner=client_login):
            while attempt < 60:
                attempt += 1
                waited = await global_rate_limiter.acquire(client_login)
                if waited:
                    limit = global_rate_limiter.max_requests
                    window = global_rate_limiter.window
                    get_logger().debug(
                        "%s: waited %.2fs to respect GLOBAL Reports %d/%ss limit",
                        client_login,
                        waited,
                        limit,
                        window,
                    )
                async with self.api_semaphore:
                    try:
                        async with aiohttp.ClientSession() as ses:
                            async with ses.post(url, data=body, headers=headers) as resp:
                                st = resp.status
                                if st == 200:
                                    raw = await resp.read()
                                    df = pd.read_table(
                                        io.StringIO(raw.decode()), delimiter="\t"
                                    ).replace("--", 0)
                                    if "Date" in df.columns:
                                        df["Date"] = pd.to_datetime(df["Date"])
                                    if "Cost" in df.columns:
                                        df["Cost"] = (
                                            pd.to_numeric(df["Cost"], errors="coerce")
                                            .fillna(0)
                                            .astype("float64")
                                            / 1_000_000
                                        )
                                    return df
                                if st not in (201, 202):
                                    get_logger().warning(
                                        f"{client_login}: report HTTP {st}: {await resp.text()}"
                                    )
                                    return None
                    except Exception as e:
                        get_logger().error(f"{client_login}: report slice error — {e}")
                        return None
                await asyncio.sleep(10)

        get_logger().error(f"{client_login}: report slice timeout")
        return None

    async def get_report_data_for_period(
        self,
        client_login: str,
        start_date: str,
        end_date: str,
        goal_ids: list[int],
    ) -> Optional[pd.DataFrame]:
        batches: List[Optional[List[int]]] = (
            [goal_ids[i : i + 10] for i in range(0, len(goal_ids), 10)]
            if goal_ids
            else [None]
        )
        frames: list[pd.DataFrame] = []
        for batch in batches:
            df = await self._fetch_report_slice(client_login, start_date, end_date, batch)
            if df is None:
                return None
            frames.append(df)

        api_df = pd.concat(frames, axis=1, join="outer").loc[:, lambda d: ~d.columns.duplicated()]

        conv_cols = [c for c in api_df.columns if c.lower().startswith("conversions")]
        for col in conv_cols:
            api_df[col] = pd.to_numeric(api_df[col], errors="coerce").fillna(0).astype("int64")
        api_df["SumConversion"] = (
            api_df[conv_cols].sum(axis=1).astype("int64") if conv_cols else 0
        )

        if "Cost" in api_df.columns:
            api_df["Cost"] = pd.to_numeric(api_df["Cost"], errors="coerce").fillna(0).astype("float64")

        return api_df

    async def detect_changes(self, client_login: str) -> dict:
        table = client_login.replace("-", "_")

        today     = datetime.now().date()
        yesterday = today - timedelta(days=1)
        start     = yesterday - timedelta(days=self.lookback_days - 1)

        goal_ids = await get_goal_ids_by_client(self.token, client_login)
        api_df   = await self.get_report_data_for_period(
            client_login, start.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"), goal_ids
        )
        if api_df is None or api_df.empty:
            get_logger().warning(f"{client_login}: no API data")
            return {"changes_detected": False, "days_to_update": []}
        api_by_date = api_df.groupby(api_df["Date"].dt.date)

        db_df = await self.db.get_daily_summary(
            table, start.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
        )
        db_by_date = db_df.groupby(db_df["Date"].dt.date) if not db_df.empty else {}

        conv_cols_api = normalize_conversion_columns_case_insensitive(api_df, "conversions_")
        conv_cols_db  = normalize_conversion_columns_case_insensitive(db_df, "conversions_")
        conv_cols_all = sorted(set(conv_cols_api) | set(conv_cols_db))
        conv_cols_new = sorted(set(conv_cols_api) - set(conv_cols_db))

        def _aggregate(df):
            """Return totals for base metrics + every conversion column."""
            impressions = int(df["Impressions"].sum()) if "Impressions" in df else 0
            clicks = int(df["Clicks"].sum()) if "Clicks" in df else 0
            cost = float(df["Cost"].sum()) if "Cost" in df else .0
            conv_totals = {
                col: int(df[col].sum()) if col in df else 0
                for col in conv_cols_all
            }
            sum_conv = sum(conv_totals.values())
            has_metrics = impressions + clicks + cost + sum_conv > 0
            return impressions, clicks, cost, conv_totals, has_metrics

        days_to_update: list[str] = []
        for current_date in (start + timedelta(days=i) for i in range((yesterday - start).days + 1)):
            date_str = current_date.strftime("%Y-%m-%d")

            api_has_data = current_date in api_by_date.groups
            db_has_data = db_by_date and current_date in db_by_date.groups

            if not api_has_data and not db_has_data:
                continue

            if api_has_data:
                api_impressions, api_clicks, api_cost, api_conversions, api_has_metrics = _aggregate(api_by_date.get_group(current_date))
            else:
                api_impressions = api_clicks = 0
                api_cost = 0.0
                api_conversions = {}
                api_has_metrics = False

            if db_has_data:
                db_impressions, db_clicks, db_cost, db_conversions, db_has_metrics = _aggregate(db_by_date.get_group(current_date))
            else:
                db_impressions = db_clicks = 0
                db_cost = 0.0
                db_conversions = {}
                db_has_metrics = False

            if api_has_metrics != db_has_metrics:
                get_logger().debug(
                    f"{client_login} {date_str}: data presence mismatch - API: {api_has_metrics}, DB: {db_has_metrics}"
                )
                days_to_update.append(date_str)
                continue

            skip_cost_check = client_login in self.skip_cost_check_clients

            conversions_mismatch = False
            mismatched_conversions = []
            if self.compare_conversions and conv_cols_db:
                for col in conv_cols_db:
                    api_val = api_conversions.get(col, 0)
                    db_val = db_conversions.get(col, 0)
                    if api_val != db_val:
                        conversions_mismatch = True
                        mismatched_conversions.append(f"{col}(API:{api_val} DB:{db_val})")

            if api_has_metrics:
                reasons = []
                if api_impressions != db_impressions:
                    reasons.append(f"Impressions(API:{api_impressions} DB:{db_impressions})")
                if api_clicks != db_clicks:
                    reasons.append(f"Clicks(API:{api_clicks} DB:{db_clicks})")
                if not skip_cost_check and abs(api_cost - db_cost) > self.cost_tolerance:
                    reasons.append(f"Cost(API:{api_cost} DB:{db_cost} diff:{abs(api_cost - db_cost)})")
                if conversions_mismatch:
                    reasons.extend(mismatched_conversions)

                if reasons:
                    get_logger().debug(
                        f"{client_login} {date_str}: detected changes - {', '.join(reasons)}"
                    )
                    days_to_update.append(date_str)

        if table not in self.db.metadata.tables:
            yesterday_str = yesterday.strftime("%Y-%m-%d")
            if yesterday_str not in days_to_update:
                get_logger().info(
                    f"{client_login}: table '{table}' does not exist, forcing reload of {yesterday_str}"
                )
                days_to_update.append(yesterday_str)

        if conv_cols_new:
            get_logger().info(
                "%s: detected %d new conversion column(s) without DB columns: %s",
                client_login,
                len(conv_cols_new),
                sorted(conv_cols_new),
            )
            cols_with_data: set[str] = set()
            cols_without_data: set[str] = set()
            days_needing_reload: set[str] = set()

            for col in conv_cols_new:
                if col in api_df.columns:
                    col_data = api_df.groupby(api_df["Date"].dt.date)[col].sum()
                    days_with_col = col_data[col_data > 0]
                    if not days_with_col.empty:
                        cols_with_data.add(col)
                        for day_idx in days_with_col.index:
                            day_str = day_idx.strftime("%Y-%m-%d") if hasattr(day_idx, 'strftime') else str(day_idx)
                            days_needing_reload.add(day_str)
                    else:
                        cols_without_data.add(col)
                else:
                    cols_without_data.add(col)

            if cols_without_data:
                get_logger().info(
                    "%s: adding %d empty conversion column(s) (no data): %s",
                    client_login,
                    len(cols_without_data),
                    sorted(cols_without_data),
                )
                await self.db.add_empty_conversion_columns(table, cols_without_data)

            if days_needing_reload:
                get_logger().info(
                    "%s: new column(s) %s have data on %d day(s): %s",
                    client_login,
                    sorted(cols_with_data),
                    len(days_needing_reload),
                    sorted(days_needing_reload),
                )
                for day_str in days_needing_reload:
                    if day_str not in days_to_update:
                        days_to_update.append(day_str)

        if days_to_update:
            get_logger().info(
                f"{client_login}: SUMMARY - {len(days_to_update)} day(s) need reload: {sorted(days_to_update)}"
            )
        else:
            get_logger().info(f"{client_login}: SUMMARY - no changes detected, all data up to date")

        return {"changes_detected": bool(days_to_update), "days_to_update": sorted(days_to_update)}
