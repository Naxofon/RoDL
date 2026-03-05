import logging
from datetime import date, datetime
from typing import Iterable

import pandas as pd

try:
    from prefect_loader.orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DB_METRIKA,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from prefect_loader.orchestration.clickhouse_utils.database import ClickhouseDatabase, _AsyncEngine
    from prefect_loader.orchestration.clickhouse_utils.helpers import (
        _get_column_names,
        _is_numeric_type,
        detect_date_column,
        detect_visits_column,
        sanitize_login,
    )
except Exception:  # pragma: no cover - local fallback
    from orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DB_METRIKA,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from orchestration.clickhouse_utils.database import ClickhouseDatabase, _AsyncEngine
    from orchestration.clickhouse_utils.helpers import (
        _get_column_names,
        _is_numeric_type,
        detect_date_column,
        detect_visits_column,
        sanitize_login,
    )


class AsyncMetrikaDatabase:
    """
    Metrika loader wrapper retaining the historic AsyncMetrikaDatabase interface.
    """

    def __init__(self):
        target_db = CLICKHOUSE_DB_METRIKA or CLICKHOUSE_DATABASE
        self._data_db = ClickhouseDatabase(database=target_db, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
        self._access_db = ClickhouseDatabase(
            database=CLICKHOUSE_ACCESS_DATABASE,
            user=CLICKHOUSE_ACCESS_USER,
            password=CLICKHOUSE_ACCESS_PASSWORD,
        )
        self.engine = _AsyncEngine()
        self.metadata = type("Meta", (), {"tables": {}})()

    async def init_db(self):
        """Create the Metrika database if needed."""
        await self._data_db.ensure_db()

    async def close_engine(self):
        """Compatibility stub for interface parity."""
        return None

    async def table_exists(self, table_name: str) -> bool:
        """Return True when the Metrika table exists."""
        exists = await self._data_db.table_exists(table_name)
        if exists:
            self.metadata.tables.setdefault(table_name, True)
        return exists

    async def get_metrika_config_data(self) -> pd.DataFrame:
        """Primary source is Accesses where service='metrika'."""
        access_rows = await self._access_db.fetch_access_rows(service_type="metrika", include_null_type=True)
        if not access_rows:
            return pd.DataFrame(columns=["counter_metric", "token", "fact_login", "date"])

        parsed: list[dict[str, object]] = []
        for row in access_rows:
            login = row.get("login")
            token = row.get("token")
            container = (row.get("container") or "").strip()
            counter_id = None
            try:
                counter_id = int(container) if container else None
            except (TypeError, ValueError):
                counter_id = None
            if login is None:
                continue
            parsed.append(
                {
                    "counter_metric": counter_id,
                    "token": token,
                    "fact_login": str(login),
                }
            )

        if not parsed:
            return pd.DataFrame(columns=["counter_metric", "token", "fact_login", "date"])

        df = pd.DataFrame(parsed)
        df["date"] = pd.to_datetime("today").date()
        self.metadata.tables.setdefault("Accesses", True)
        return df[["date", "fact_login", "counter_metric", "token"]]

    async def write_dataframe_to_table(self, df: pd.DataFrame, table_name: str):
        """Insert a DataFrame into the Metrika table and track metadata."""
        await self._data_db.write_dataframe(table_name, df)
        if not df.empty:
            self.metadata.tables.setdefault(table_name, True)

    async def erase_data_in_interval(
        self,
        table_name: str,
        start_date: datetime | date,
        end_date: datetime | date,
    ):
        """Erase rows from a Metrika table within the date interval."""
        await self._data_db.delete_between_dates(
            table_name, start_date, end_date, date_column="dateTime"
        )

    async def add_empty_goal_columns(
        self,
        table_name: str,
        goal_ids: Iterable[int],
        prefixes: Iterable[str] = ("u_goal_", "g_goal_"),
    ) -> int:
        """Add empty goal columns (Int8 default 0) without reloading data."""
        if not await self._data_db.table_exists(table_name):
            return 0
        added = 0
        for goal_id in goal_ids:
            for prefix in prefixes:
                col_name = f"{prefix}{goal_id}"
                try:
                    await self._data_db._command_with_retry(
                        f"ALTER TABLE {self._data_db.database}.{table_name} "
                        f"ADD COLUMN IF NOT EXISTS `{col_name}` "
                        f"Int8 DEFAULT 0"
                    )
                    added += 1
                except Exception:
                    pass
        return added

    async def drop_goal_columns(
        self,
        table_name: str,
        goal_ids: Iterable[int],
        prefixes: Iterable[str] = ("g_goal_",),
    ) -> int:
        """Drop goal columns for specified goal IDs."""
        if not await self._data_db.table_exists(table_name):
            return 0
        dropped = 0
        for goal_id in goal_ids:
            for prefix in prefixes:
                col_name = f"{prefix}{goal_id}"
                try:
                    await self._data_db._command_with_retry(
                        f"ALTER TABLE {self._data_db.database}.{table_name} "
                        f"DROP COLUMN IF EXISTS `{col_name}`"
                    )
                    dropped += 1
                except Exception:
                    pass
        return dropped

    async def add_client_to_metrika_config(
        self,
        fact_login: str,
        counter_metric: int | None,
        token: str,
        *,
        subtype: str | None = None,
    ):
        """
        Store Metrika access tokens.
        - Login (fact_login) is always required.
        - For client accounts counter_metric is required and stored as container.
        - For agency accounts counter_metric may be None/placeholder; container is left blank.
        """
        login_value = sanitize_login(fact_login)
        is_agency = subtype == "agency"
        if not is_agency and counter_metric is None:
            raise ValueError("counter_required")
        container_value = "" if is_agency else str(counter_metric)
        await self._access_db.upsert_accesses(
            [login_value],
            token,
            container=container_value,
            service_type="metrika",
            type_value=subtype,
            replace=False,
        )
        self.metadata.tables.setdefault("Accesses", True)

    async def delete_client_by_counter_metric(self, counter_metric: int):
        await self._access_db.upsert_accesses(
            [],
            "",
            container=str(counter_metric),
            service_type="metrika",
            replace=True,
        )

    async def get_daily_summary(
        self,
        table_name: str,
        start_date: datetime | date | str,
        end_date: datetime | date | str,
    ) -> pd.DataFrame:
        try:
            cols = await self._data_db.get_columns(table_name)
        except Exception:
            return pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])
        if not cols:
            return pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])

        date_col = (
            "dateTime"
            if "dateTime" in cols
            else detect_date_column(cols.keys())
        )
        if not date_col:
            return pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])

        def _sum_expr(col: str | None) -> str:
            if not col:
                return "0"
            col_type = cols.get(col, "")
            if _is_numeric_type(col_type):
                return f"sum(`{col}`)"
            return f"sum(toInt64OrZero(`{col}`))"

        visits_col = detect_visits_column(cols.keys())
        visits_expr = _sum_expr(visits_col)

        if "u_sum_goal" in cols:
            conv_expr = _sum_expr("u_sum_goal")
        else:
            u_goal_cols = [c for c in cols if c.startswith("u_goal_")]
            conv_expr = (
                " + ".join(_sum_expr(c) for c in u_goal_cols)
                if u_goal_cols
                else "0"
            )

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        try:
            logging.getLogger("metrika.change_tracker").debug(
                "daily_summary query table=%s date_col=%s "
                "visits_col=%s conv_expr_prefix=u_goal count=%s "
                "window=%s..%s",
                table_name,
                date_col,
                visits_col,
                len([c for c in cols if c.startswith('u_goal_')]),
                start_dt.date(),
                end_dt.date(),
            )
        except Exception:
            pass

        query = (
            f"SELECT toDate(`{date_col}`) AS Date, "
            f"{visits_expr} AS DBVisits, "
            f"{conv_expr} AS DBConversions "
            f"FROM {self._data_db.database}.{table_name} "
            f"WHERE toDate(`{date_col}`) >= "
            f"toDate('{start_dt:%Y-%m-%d}') "
            f"AND toDate(`{date_col}`) <= "
            f"toDate('{end_dt:%Y-%m-%d}') "
            f"GROUP BY Date ORDER BY Date"
        )

        try:
            result = await self._data_db._query_with_retry(query)
        except Exception as exc:
            logging.getLogger("metrika.change_tracker").info(
                "daily_summary query failed table=%s err=%s",
                table_name,
                exc,
            )
            return pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])

        if not getattr(result, "result_rows", None):
            return pd.DataFrame(columns=["Date", "DBVisits", "DBConversions"])

        df = pd.DataFrame(
            result.result_rows,
            columns=_get_column_names(
                result, default=["Date", "DBVisits", "DBConversions"]
            ),
        )
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["DBVisits"] = (
            pd.to_numeric(df.get("DBVisits", 0), errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        df["DBConversions"] = (
            pd.to_numeric(df.get("DBConversions", 0), errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        return df[["Date", "DBVisits", "DBConversions"]]

    async def reset_database(self) -> dict[str, int]:
        """
        Reset the Metrika database - truncate all tables.

        Returns:
            dict with count of tables affected per database.
        """
        data_tables = await self._data_db.truncate_all_tables()
        return {"data_db": data_tables}

    async def get_stale_clients(self, days_threshold: int = 30):
        """
        Get list of Metrika clients with stale data (oldest date > threshold days old).

        Returns:
            List of dicts with: table_name, min_date, days_since_update
        """
        return await self._data_db.get_stale_tables("dateTime", days_threshold)

    async def cleanup_clients(self, table_names: list[str]) -> int:
        """
        Drop tables for specified table names.

        Returns:
            Count of tables dropped
        """
        dropped = 0
        for table_name in table_names:
            try:
                await self._data_db.drop_table(table_name)
                dropped += 1
            except Exception:
                pass
        return dropped


__all__ = ["AsyncMetrikaDatabase"]
