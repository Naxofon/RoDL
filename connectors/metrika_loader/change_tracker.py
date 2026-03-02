import asyncio
import re
from datetime import datetime, timedelta, date
from typing import Sequence, TypeVar

import aiohttp
import pandas as pd

from prefect_loader.orchestration.clickhouse_utils import AsyncMetrikaDatabase
from prefect import get_run_logger
from .change_utils import AsyncRequestLimiter, GoalMetadata, classify_goals, format_auth_fingerprint

T = TypeVar("T")


class _LazyLogger:
    """Defer get_run_logger() until first use so module-level import succeeds outside Prefect."""
    def __getattr__(self, name: str):
        return getattr(get_run_logger(), name)


logger = _LazyLogger()


class MetrikaChangeTracker:
    """Compare day-level visits and conversions between Metrika API and the DB."""

    REPORT_URL = "https://api-metrika.yandex.net/stat/v1/data"
    GOALS_URL = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/goals"
    MAX_ATTEMPTS = 5
    _REQUEST_LIMITERS: dict[str, AsyncRequestLimiter] = {}
    _REQUEST_LIMITERS_LOCK = asyncio.Lock()

    def __init__(
        self,
        counter_id: int,
        token: str,
        db: AsyncMetrikaDatabase,
        *,
        lookback_days: int = 60,
        compare_conversions: bool = True,
        login: str | None = None,
    ) -> None:
        """Initialise the change tracker for a single Metrika counter."""
        self.counter_id = int(counter_id)
        self.token = token
        self.login = login
        self.db = db
        self.lookback_days = lookback_days
        self.compare_conversions = compare_conversions
        self._goal_metadata: list[GoalMetadata] | None = None
        self._request_limiter: AsyncRequestLimiter | None = None

    @staticmethod
    def _chunk(seq: Sequence[T], size: int) -> list[list[T]]:
        """Split a sequence into sublists of at most size elements."""
        return [list(seq[i : i + size]) for i in range(0, len(seq), size)]

    async def _get_request_limiter(self) -> AsyncRequestLimiter:
        """
        Return a per-token limiter respecting Metrika's "3 concurrent requests per user" quota.
        This allows us to parallelise across different logins without throttling everyone to 3 total.
        """
        if self._request_limiter is not None:
            return self._request_limiter

        async with self._REQUEST_LIMITERS_LOCK:
            limiter = self._REQUEST_LIMITERS.get(self.token)
            if limiter is None:
                limiter = AsyncRequestLimiter(max_concurrent=3, min_interval=0.05)
                self._REQUEST_LIMITERS[self.token] = limiter
        self._request_limiter = limiter
        return limiter

    async def _get_goal_metadata(self) -> list[GoalMetadata]:
        """Fetch and cache goal metadata for this counter from the Metrika API."""
        if self._goal_metadata is not None:
            return self._goal_metadata

        url = self.GOALS_URL.format(counter_id=self.counter_id)
        headers = {"Authorization": f"OAuth {self.token}"}
        limiter = await self._get_request_limiter()

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with limiter:
                        async with session.get(url, params={"useDeleted": "0"}) as resp:
                            if resp.status == 200:
                                payload = await resp.json()
                                goals = payload.get("goals") or []
                                metadata = classify_goals(goals)
                                self._goal_metadata = metadata
                                return metadata

                            if resp.status in (401, 403):
                                text = await resp.text()
                                logger.warning(
                                    "Counter %s: unable to fetch goals (HTTP %s): %s",
                                    self.counter_id,
                                    resp.status,
                                    text,
                                )
                                self._goal_metadata = []
                                return []

                            text = await resp.text()
                            logger.warning(
                                "Counter %s: goals request HTTP %s attempt %s/%s — %s",
                                self.counter_id,
                                resp.status,
                                attempt,
                                self.MAX_ATTEMPTS,
                                text,
                            )
            except aiohttp.ClientError as exc:
                logger.error(
                    "Counter %s: goals request error attempt %s/%s — %s",
                    self.counter_id,
                    attempt,
                    self.MAX_ATTEMPTS,
                    exc,
                )

            await asyncio.sleep(min(5, attempt * 2))

        logger.error("Counter %s: failed to fetch goals after retries", self.counter_id)
        self._goal_metadata = []
        return []

    async def _fetch_metrics_slice(
        self,
        session: aiohttp.ClientSession,
        metrics: list[str],
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, set[str]]:
        """Fetch a single batch of metrics from the Metrika Stat API, returning a DataFrame and any invalid metric names."""
        if not metrics:
            return pd.DataFrame(), set()

        limiter = await self._get_request_limiter()
        params = {
            "ids": self.counter_id,
            "metrics": ",".join(metrics),
            "dimensions": "ym:s:date",
            "date1": start_date,
            "date2": end_date,
            "accuracy": "1",
            "proposed_accuracy": "true",
            "include_undefined": "false",
            "lang": "ru",
            "limit": "100000",
            "attribution": "lastsign",
        }

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                async with limiter:
                    async with session.get(self.REPORT_URL, params=params) as resp:
                        status = resp.status
                        if status == 200:
                            payload = await resp.json()
                            return self._parse_report(payload, metrics), set()

                        if status in (429, 500, 503):
                            await asyncio.sleep(min(10, attempt * 2))
                            continue

                        text = await resp.text()
                        invalid_metrics: set[str] = set()
                        if status == 400:
                            matches = re.findall(r"ym:s:goal[0-9]+visits", text)
                            invalid_metrics.update(matches)
                        logger.warning(
                            "Counter %s: report slice HTTP %s (%s/%s metrics: %s): %s",
                            self.counter_id,
                            status,
                            attempt,
                            self.MAX_ATTEMPTS,
                            metrics,
                            text,
                        )
                        return pd.DataFrame(), invalid_metrics
            except aiohttp.ClientError as exc:
                logger.error(
                    "Counter %s: report slice error attempt %s/%s (%s): %s",
                    self.counter_id,
                    attempt,
                    self.MAX_ATTEMPTS,
                    metrics,
                    exc,
                )

            await asyncio.sleep(min(10, attempt * 2))

        logger.error(
            "Counter %s: failed to fetch metrics slice after %s attempts (%s)",
            self.counter_id,
            self.MAX_ATTEMPTS,
            metrics,
        )
        return pd.DataFrame(), set()

    @staticmethod
    def _parse_report(payload: dict, metrics: list[str]) -> pd.DataFrame:
        """Parse a Metrika Stat API response payload into a DataFrame indexed by date."""
        data = payload.get("data") or []
        rows = []
        for item in data:
            dimensions = item.get("dimensions") or []
            if not dimensions:
                continue

            dim_obj = dimensions[0]
            date_str = dim_obj.get("name") or dim_obj.get("id")
            if not date_str:
                continue

            try:
                parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            metrics_values = item.get("metrics") or []
            row = {"Date": parsed_date}
            for idx, metric_name in enumerate(metrics):
                try:
                    value = metrics_values[idx]
                except IndexError:
                    value = 0
                row[metric_name] = value if value is not None else 0
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        metric_columns = [c for c in df.columns if c != "Date"]
        for col in metric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df

    async def _get_api_summary(
        self,
        start_date: str,
        end_date: str,
        table_name: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, set[int]]:
        """Retrieve visit and conversion summaries from the Metrika API for the given date range."""
        goal_metadata = await self._get_goal_metadata()
        goal_metrics: list[str] = []

        try:
            cols = await self.db._data_db.get_columns(table_name)
            db_goal_ids: set[int] = set()
            for col in cols:
                if col.startswith("u_goal_"):
                    try:
                        db_goal_ids.add(int(col.split("_", 2)[2]))
                    except (IndexError, ValueError):
                        continue
            existing_ids = {meta.goal_id for meta in goal_metadata}
            for missing_id in sorted(db_goal_ids - existing_ids):
                goal_metadata.append(GoalMetadata(goal_id=missing_id, identifier="u"))
        except Exception:
            pass
        headers = {"Authorization": f"OAuth {self.token}"}
        frames: list[pd.DataFrame] = []
        invalid_metrics_agg: set[str] = set()
        ignored_goal_ids: set[int] = set()
        failed_goal_ids: set[int] = set()

        metric_batches: list[list[str]] = []
        if goal_metadata:
            goal_metrics = []
            for meta in goal_metadata:
                goal_metrics.append(f"ym:s:goal{meta.goal_id}visits")

            chunks = self._chunk(goal_metrics, 9)
            if chunks:
                first_chunk = ["ym:s:visits", *chunks[0]]
                metric_batches.append(first_chunk)
                metric_batches.extend(chunks[1:])
        else:
            metric_batches.append(["ym:s:visits"])

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [
                self._fetch_metrics_slice(session, metrics, start_date, end_date)
                for metrics in metric_batches
            ]
            if tasks:
                for df_chunk, invalid_metrics in await asyncio.gather(*tasks):
                    if invalid_metrics:
                        invalid_metrics_agg.update(invalid_metrics)
                    if not df_chunk.empty:
                        frames.append(df_chunk)

            if invalid_metrics_agg:
                ignored_goal_ids = {
                    int(re.search(r"goal([0-9]+)visits", m).group(1))
                    for m in invalid_metrics_agg
                    if re.search(r"goal([0-9]+)visits", m)
                }
                logger.warning(
                    "Counter %s: dropping %d invalid goal metric(s) after HTTP 400: %s",
                    self.counter_id,
                    len(ignored_goal_ids),
                    sorted(ignored_goal_ids),
                )
                goal_metrics = [m for m in goal_metrics if m not in invalid_metrics_agg]

            if goal_metrics:
                existing_metrics = {col for df in frames for col in df.columns if col != "Date"}
                missing_metrics = [m for m in goal_metrics if m not in existing_metrics]
                if missing_metrics:
                    logger.warning(
                        "Counter %s: retrying %d missing goal metric(s) individually after batch errors",
                        self.counter_id,
                        len(missing_metrics),
                    )
                    for metric in missing_metrics:
                        df_single, invalid_single = await self._fetch_metrics_slice(session, [metric], start_date, end_date)
                        if invalid_single:
                            invalid_metrics_agg.update(invalid_single)
                            try:
                                ignored_goal_ids.update(
                                    int(re.search(r"goal([0-9]+)visits", m).group(1))
                                    for m in invalid_single
                                    if re.search(r"goal([0-9]+)visits", m)
                                )
                            except Exception:
                                pass
                        if not df_single.empty:
                            frames.append(df_single)
                        else:
                            try:
                                mid = int(re.search(r"goal([0-9]+)visits", metric).group(1))
                                failed_goal_ids.add(mid)
                            except Exception:
                                pass
                if invalid_metrics_agg:
                    goal_metrics = [m for m in goal_metrics if m not in invalid_metrics_agg]
                if failed_goal_ids:
                    ignored_goal_ids.update(failed_goal_ids)

            has_visits = any("ym:s:visits" in df.columns for df in frames)
            if not has_visits:
                visits_only, _ = await self._fetch_metrics_slice(session, ["ym:s:visits"], start_date, end_date)
                if not visits_only.empty:
                    frames.append(visits_only)

        if not frames:
            empty_summary = pd.DataFrame(columns=["Date", "APIVisits", "APIConversions"])
            empty_goals = pd.DataFrame(columns=["Date"])
            return empty_summary, empty_goals, ignored_goal_ids

        merged = frames[0]
        for df in frames[1:]:
            merged = pd.merge(merged, df, on="Date", how="outer")

        merged = merged.fillna(0)

        goal_columns = []
        if "ym:s:visits" in merged:
            visits_series = pd.to_numeric(merged["ym:s:visits"], errors="coerce").fillna(0)
        else:
            visits_series = pd.Series(0, index=merged.index, dtype="float64")

        visit_conversions: list[str] = []
        for meta in goal_metadata:
            base_column = f"goal_{meta.goal_id}"
            visits_metric = f"ym:s:goal{meta.goal_id}visits"

            if visits_metric in merged:
                merged[base_column] = pd.to_numeric(merged[visits_metric], errors="coerce").fillna(0)
                visit_conversions.append(base_column)
                goal_columns.append(base_column)

        visit_conversions = sorted(set(visit_conversions))
        merged["APIConversions"] = (
            merged[visit_conversions]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum(axis=1)
            if visit_conversions
            else pd.Series(0, index=merged.index, dtype="int64")
        ).round().astype("int64")

        merged["APIVisits"] = visits_series.round().astype("int64")

        goal_breakdown = pd.DataFrame(columns=["Date"])
        if goal_columns:
            goal_breakdown = merged[["Date", *sorted(set(goal_columns))]].copy()
            goal_breakdown.columns = goal_breakdown.columns.map(str)

        summary = merged[["Date", "APIVisits", "APIConversions"]]
        return summary, goal_breakdown, ignored_goal_ids

    async def _get_db_goal_breakdown(
        self,
        table_name: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch per-goal daily sums from ClickHouse (u_goal_* only) for debug logging.
        """
        try:
            df = await self.db._data_db.aggregate_daily(
                table_name,
                start_date,
                end_date,
                conversion_prefixes=("u_goal_",),
                date_column_override="dateTime",
            )
        except Exception as exc:
            logger.warning("Counter %s: failed to read DB goal breakdown — %s", self.counter_id, exc)
            return pd.DataFrame(columns=["Date"])
        return df

    def _log_goal_deltas(
        self,
        day: date,
        api_goals_df: pd.DataFrame,
        db_goals_df: pd.DataFrame,
    ) -> None:
        """Log per-goal deltas for a specific day when mismatches are detected."""
        day_key = pd.to_datetime(day).date()
        if api_goals_df.empty and db_goals_df.empty:
            logger.debug("Counter %s: no goal breakdown data available for %s", self.counter_id, day_key)
            return

        def _row_to_map(df: pd.DataFrame, prefix: str) -> dict[int, int]:
            if df.empty or day_key not in df.index:
                return {}
            row = df.loc[day_key]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            mapping: dict[int, int] = {}
            for col, val in row.items():
                if not col.startswith(prefix):
                    continue
                try:
                    goal_id = int(col.split("_", 2)[-1])
                except (IndexError, ValueError):
                    continue
                try:
                    mapping[goal_id] = int(pd.to_numeric(val, errors="coerce") or 0)
                except Exception:
                    mapping[goal_id] = 0
            return mapping

        api_map = _row_to_map(api_goals_df, "goal_")
        db_map = _row_to_map(db_goals_df, "u_goal_")
        ids = sorted(set(api_map) | set(db_map))

        if not ids:
            logger.debug("Counter %s: goal breakdown empty for %s", self.counter_id, day_key)
            return

        entries: list[str] = []
        for goal_id in ids:
            api_val = api_map.get(goal_id, 0)
            db_val = db_map.get(goal_id, 0)
            if api_val == db_val:
                continue
            delta = api_val - db_val
            entries.append(f"{goal_id}:api={api_val} db={db_val} Δ={delta}")

        api_sum = sum(api_map.values())
        db_sum = sum(db_map.values())
        detail = "; ".join(entries[:15]) if entries else "all goals match"
        if len(entries) > 15:
            detail += f"; …(+{len(entries)-15} more)"

        logger.debug(
            "Counter %s: per-goal delta %s api_sum=%s db_sum=%s :: %s",
            self.counter_id,
            day_key,
            api_sum,
            db_sum,
            detail,
        )

    async def _log_db_day_snapshot(self, table_name: str, day: date) -> None:
        """
        Log raw row counts/time span for a specific day to detect missing data in DB.
        """
        try:
            sql = (
                f"SELECT count() AS rows, "
                f"min(dateTime) AS min_dt, "
                f"max(dateTime) AS max_dt "
                f"FROM {self.db._data_db.database}.{table_name} "
                f"WHERE toDate(dateTime) = toDate('{day:%Y-%m-%d}')"
            )
            result = await self.db._data_db._query_with_retry(sql)
            if not result or not getattr(result, "result_rows", None):
                logger.debug("Counter %s: DB snapshot %s — no rows returned", self.counter_id, day)
                return
            rows, min_dt, max_dt = result.result_rows[0][:3]
            logger.debug(
                "Counter %s: DB snapshot %s rows=%s min_dt=%s max_dt=%s",
                self.counter_id,
                day,
                rows,
                min_dt,
                max_dt,
            )
        except Exception as exc:
            logger.debug("Counter %s: DB snapshot %s failed: %s", self.counter_id, day, exc)

    async def _log_db_day_aggregate(self, table_name: str, day: date, *, max_goals: int = 20) -> None:
        """
        Log aggregated visits and goal counts for a specific day to diagnose zero summaries.
        """
        try:
            cols = await self.db._data_db.get_columns(table_name)
            visits_col = None
            for candidate in ("visits", "Visits", "sessions", "Sessions"):
                if candidate in cols:
                    visits_col = candidate
                    break

            def _is_numeric(click_type: str) -> bool:
                t = (click_type or "").strip().lower()
                if t.startswith("nullable(") and t.endswith(")"):
                    t = t[len("nullable("):-1].strip()
                return t.startswith(("int", "uint", "float", "decimal"))

            def _sum_expr(col: str) -> str:
                if _is_numeric(cols.get(col, "")):
                    return f"sum(`{col}`)"
                return f"sum(toInt64OrZero(`{col}`))"

            u_goal_cols = [c for c in cols if c.startswith("u_goal_")]
            legacy_goal_cols = [c for c in cols if c.startswith("goal_")]

            select_parts = ["count() AS rows"]
            select_parts.append(_sum_expr(visits_col) + " AS visits_sum" if visits_col else "0 AS visits_sum")

            for col in u_goal_cols[:max_goals]:
                select_parts.append(_sum_expr(col) + f" AS `{col}`")
            remaining = max(0, max_goals - len(u_goal_cols))
            for col in legacy_goal_cols[:remaining]:
                select_parts.append(_sum_expr(col) + f" AS `{col}`")

            select_clause = ", ".join(select_parts)
            sql = (
                f"SELECT {select_clause} "
                f"FROM {self.db._data_db.database}.{table_name} "
                f"WHERE toDate(dateTime) = toDate('{day:%Y-%m-%d}')"
            )
            result = await self.db._data_db._query_with_retry(sql)
            if not getattr(result, "result_rows", None):
                logger.debug("Counter %s: DB aggregate %s — no rows", self.counter_id, day)
                return
            row = result.result_rows[0]
            columns = getattr(result, "column_names", None)
            if not columns:
                columns = [f"col{i}" for i in range(len(row))]
            payload = dict(zip(columns, row))
            logger.debug(
                "Counter %s: DB aggregate %s cols=%s payload=%s",
                self.counter_id,
                day,
                {
                    "visits_col": visits_col,
                    "u_goal_cols": u_goal_cols[:max_goals],
                    "legacy_goal_cols": legacy_goal_cols[:max_goals],
                },
                payload,
            )
        except Exception as exc:
            logger.debug("Counter %s: DB aggregate %s failed: %s", self.counter_id, day, exc)

    async def detect_changes(self, table_name: str) -> dict:
        """Return a dict with `changes_detected` and `days_to_update`."""
        today = date.today()
        yesterday = today - timedelta(days=1)
        start = yesterday - timedelta(days=self.lookback_days - 1)

        start_str = start.strftime("%Y-%m-%d")
        end_str = yesterday.strftime("%Y-%m-%d")
        logger.info(
            "Counter %s: change tracker API auth (%s), window %s → %s",
            self.counter_id,
            format_auth_fingerprint(self.login, self.token),
            start_str,
            end_str,
        )

        api_df, api_goals_df, ignored_goal_ids = await self._get_api_summary(start_str, end_str, table_name)
        db_goals_df = await self._get_db_goal_breakdown(table_name, start_str, end_str)

        def _goal_id_from_col(col: str) -> int | None:
            """
            Extract numeric goal id from column names like goal_<id>, u_goal_<id> or g_goal_<id>.
            """
            match = re.search(r"goal_([0-9]+)$", str(col))
            if not match:
                return None
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                return None

        try:
            db_df = await self.db.get_daily_summary(table_name, start_str, end_str)
        except Exception as exc:
            logger.error(
                "Counter %s: failed to read DB summary for %s — %s",
                self.counter_id,
                table_name,
                exc,
            )
            db_df = pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])

        if not api_df.empty:
            api_df = api_df.set_index("Date")
            api_df.index = pd.to_datetime(api_df.index).date
        if not db_df.empty:
            db_df = db_df.set_index("Date")
            db_df.index = pd.to_datetime(db_df.index).date
        if not api_goals_df.empty:
            api_goals_df = api_goals_df.set_index("Date")
            api_goals_df.index = pd.to_datetime(api_goals_df.index).date
        if not db_goals_df.empty:
            db_goals_df = db_goals_df.set_index("Date")
            db_goals_df.index = pd.to_datetime(db_goals_df.index).date

        # Diagnostic summary for both API and DB
        if api_df.empty:
            logger.warning(
                "Counter %s: API returned no data for window %s → %s "
                "(counter may be brand-new, have no traffic, or the token may lack access)",
                self.counter_id, start_str, end_str,
            )
        else:
            _api_total_visits = int(api_df["APIVisits"].sum())
            _api_total_conv = int(api_df["APIConversions"].sum())
            _api_days_with_data = int(
                (api_df["APIVisits"].gt(0) | api_df["APIConversions"].gt(0)).sum()
            )
            logger.info(
                "Counter %s: API window %s → %s — %d days fetched, "
                "%d days with data (visits=%d, conversions=%d)",
                self.counter_id, start_str, end_str,
                len(api_df), _api_days_with_data, _api_total_visits, _api_total_conv,
            )

        if db_df.empty:
            logger.info(
                "Counter %s: DB has no data for table '%s' in window %s → %s "
                "(table is new or empty)",
                self.counter_id, table_name, start_str, end_str,
            )
        else:
            _db_total_visits = int(db_df["DBVisits"].sum())
            _db_total_conv = int(db_df["DBConversions"].sum())
            logger.info(
                "Counter %s: DB window %s → %s — %d days with data "
                "(visits=%d, conversions=%d)",
                self.counter_id, start_str, end_str,
                len(db_df), _db_total_visits, _db_total_conv,
            )

        columns_cache: dict[str, str] | None = None
        db_goal_ids: set[int] = set()
        try:
            columns_cache = await self.db._data_db.get_columns(table_name)
            for col in columns_cache:
                gid = _goal_id_from_col(col)
                if gid is not None:
                    db_goal_ids.add(gid)
        except Exception as exc:
            logger.debug(
                "Counter %s: failed to read column list for goal comparison — %s",
                self.counter_id,
                exc,
            )

        if ignored_goal_ids:
            db_goal_ids -= ignored_goal_ids

        try:
            current_g_goal_ids = {meta.goal_id for meta in goal_metadata if meta.identifier == "g"}
        except Exception:
            current_g_goal_ids = set()
        stale_g_cols: list[str] = []
        stale_g_ids: set[int] = set()
        if columns_cache:
            for col in list(columns_cache.keys()):
                if not col.startswith("g_goal_"):
                    continue
                gid = _goal_id_from_col(col)
                if gid is None or gid not in current_g_goal_ids:
                    stale_g_cols.append(col)
                    if gid is not None:
                        stale_g_ids.add(gid)

        if stale_g_cols:
            logger.info(
                "Counter %s: removing %d stale g_goal column(s) (marker removed): %s",
                self.counter_id,
                len(stale_g_cols),
                stale_g_cols,
            )
            try:
                await self.db.drop_goal_columns(table_name, stale_g_ids or set(), prefixes=("g_goal_",))
                columns_cache = await self.db._data_db.get_columns(table_name)
            except Exception as exc:
                logger.warning(
                    "Counter %s: failed to drop stale g_goal columns %s — %s",
                    self.counter_id,
                    stale_g_cols,
                    exc,
                )

        api_goal_cols_for_db: list[str] = []
        if db_goal_ids and not api_goals_df.empty:
            api_goal_cols_for_db = [
                col for col in api_goals_df.columns
                if _goal_id_from_col(col) in db_goal_ids
            ]

        db_goal_cols_existing: list[str] = []
        if db_goal_ids and not db_goals_df.empty:
            db_goal_cols_existing = [
                col for col in db_goals_df.columns
                if _goal_id_from_col(col) in db_goal_ids
            ]

        api_goal_ids_present = {gid for gid in (_goal_id_from_col(c) for c in api_goal_cols_for_db) if gid is not None}
        db_goal_ids_present = {gid for gid in (_goal_id_from_col(c) for c in db_goal_cols_existing) if gid is not None}
        missing_goal_ids = sorted(db_goal_ids_present - api_goal_ids_present - ignored_goal_ids)
        missing_goal_cols = [f"u_goal_{gid}" for gid in missing_goal_ids]
        if missing_goal_cols:
            logger.warning(
                "Counter %s: API report missing metrics for %d DB goal column(s); skipping conversion diff for those: %s",
                self.counter_id,
                len(missing_goal_cols),
                missing_goal_cols,
            )

        days_with_changes: list[str] = []

        cursor = start
        while cursor <= yesterday:
            api_visits = api_conversions = 0
            db_visits = db_conversions = 0

            if not api_df.empty and cursor in api_df.index:
                api_visits = int(api_df.loc[cursor, "APIVisits"])
                api_conversions = int(api_df.loc[cursor, "APIConversions"])

            if not db_df.empty and cursor in db_df.index:
                db_visits = int(db_df.loc[cursor, "DBVisits"])
                db_conversions = int(db_df.loc[cursor, "DBConversions"])

            api_present = (api_visits + api_conversions) > 0
            db_present = (db_visits + db_conversions) > 0

            if not api_present and not db_present:
                cursor += timedelta(days=1)
                continue

            mismatched = api_visits != db_visits
            api_conv_known = api_conversions
            db_conv_known = db_conversions
            conversions_mismatch = False
            if self.compare_conversions:
                if db_goal_ids:
                    if missing_goal_cols:
                        conversions_mismatch = False
                    else:
                        if cursor in api_goals_df.index and api_goal_cols_for_db:
                            api_conv_known = int(
                                pd.to_numeric(
                                    api_goals_df.loc[cursor, api_goal_cols_for_db],
                                    errors="coerce",
                                )
                                .fillna(0)
                                .sum()
                            )
                        else:
                            api_conv_known = 0

                        if cursor in db_goals_df.index and db_goal_cols_existing:
                            db_conv_known = int(
                                pd.to_numeric(
                                    db_goals_df.loc[cursor, db_goal_cols_existing],
                                    errors="coerce",
                                )
                                .fillna(0)
                                .sum()
                            )
                        else:
                            db_conv_known = 0
                        conversions_mismatch = api_conv_known != db_conv_known
                else:
                    conversions_mismatch = api_conversions != db_conversions

            if mismatched or conversions_mismatch or api_present != db_present:
                days_with_changes.append(cursor.strftime("%Y-%m-%d"))
                self._log_goal_deltas(cursor, api_goals_df, db_goals_df)
                if not db_present and api_present:
                    await self._log_db_day_snapshot(table_name, cursor)
                    await self._log_db_day_aggregate(table_name, cursor)

            cursor += timedelta(days=1)

        try:
            goal_metadata = await self._get_goal_metadata()
            api_goal_ids = {meta.goal_id for meta in goal_metadata}
            if columns_cache is None:
                columns_cache = await self.db._data_db.get_columns(table_name)
            db_goal_ids_existing: set[int] = set()
            for col in columns_cache:
                gid = _goal_id_from_col(col)
                if gid is not None:
                    db_goal_ids_existing.add(gid)
            db_goal_ids_existing -= ignored_goal_ids
            new_goals = api_goal_ids - db_goal_ids_existing
            if new_goals:
                logger.info(
                    "Counter %s: detected %d new goal(s) without DB columns: %s",
                    self.counter_id,
                    len(new_goals),
                    sorted(new_goals),
                )
                goals_with_data: set[int] = set()
                goals_without_data: set[int] = set()
                days_needing_reload: set[str] = set()

                for goal_id in new_goals:
                    goal_col = f"goal_{goal_id}"
                    if not api_goals_df.empty and goal_col in api_goals_df.columns:
                        goal_data = api_goals_df[goal_col]
                        days_with_goal = goal_data[goal_data > 0]
                        if not days_with_goal.empty:
                            goals_with_data.add(goal_id)
                            for day_idx in days_with_goal.index:
                                day_str = day_idx.strftime("%Y-%m-%d") if hasattr(day_idx, 'strftime') else str(day_idx)
                                days_needing_reload.add(day_str)
                        else:
                            goals_without_data.add(goal_id)
                    else:
                        goals_without_data.add(goal_id)

                if goals_without_data:
                    logger.info(
                        "Counter %s: adding %d empty goal column(s) (no conversions): %s",
                        self.counter_id,
                        len(goals_without_data),
                        sorted(goals_without_data),
                    )
                    g_goals = {m.goal_id for m in goal_metadata if m.identifier == "g"}
                    for goal_id in goals_without_data:
                        prefixes = ["u_goal_"]
                        if goal_id in g_goals:
                            prefixes.append("g_goal_")
                        await self.db.add_empty_goal_columns(table_name, [goal_id], prefixes)

                if days_needing_reload:
                    logger.info(
                        "Counter %s: new goal(s) %s have conversions on %d day(s): %s",
                        self.counter_id,
                        sorted(goals_with_data),
                        len(days_needing_reload),
                        sorted(days_needing_reload),
                    )
                    for day_str in days_needing_reload:
                        if day_str not in days_with_changes:
                            days_with_changes.append(day_str)
        except Exception as exc:
            logger.warning("Counter %s: failed to check for new goal columns: %s", self.counter_id, exc)

        if days_with_changes:
            logger.info(
                "Counter %s: changes detected (%d days): %s",
                self.counter_id,
                len(days_with_changes),
                ", ".join(sorted(days_with_changes)),
            )
        else:
            if api_df.empty:
                logger.info(
                    "Counter %s: no changes detected — API returned no data for window %s → %s",
                    self.counter_id, start_str, end_str,
                )
            elif db_df.empty:
                logger.warning(
                    "Counter %s: no changes detected but DB is empty for table '%s' "
                    "and API returned %d days of data (visits=%d) — "
                    "all API days have 0 visits/conversions, nothing to export",
                    self.counter_id, table_name, len(api_df),
                    int(api_df["APIVisits"].sum()),
                )
            else:
                logger.info(
                    "Counter %s: no changes detected in window %s → %s "
                    "(API visits=%d, DB visits=%d)",
                    self.counter_id, start_str, end_str,
                    int(api_df["APIVisits"].sum()),
                    int(db_df["DBVisits"].sum()),
                )

        return {
            "changes_detected": bool(days_with_changes),
            "days_to_update": days_with_changes,
            "api_total_visits": int(api_df["APIVisits"].sum()) if not api_df.empty else 0,
            "api_days_with_data": int(
                (api_df["APIVisits"].gt(0) | api_df["APIConversions"].gt(0)).sum()
            ) if not api_df.empty else 0,
            "db_empty": db_df.empty,
            "db_total_visits": int(db_df["DBVisits"].sum()) if not db_df.empty else 0,
        }
