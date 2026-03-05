import logging
from datetime import datetime

import pandas as pd

try:
    from prefect_loader.orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DB_DIRECT,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from prefect_loader.orchestration.clickhouse_utils.database import ClickhouseDatabase, _AsyncEngine
except Exception:  # pragma: no cover - local fallback
    from orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DB_DIRECT,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from orchestration.clickhouse_utils.database import ClickhouseDatabase, _AsyncEngine


class AsyncDirectDatabase:
    """
    Direct loader wrapper retaining the historic AsyncDatabase interface (renamed).
    """

    def __init__(self, database: str | None = None):
        target_db = database or CLICKHOUSE_DB_DIRECT or CLICKHOUSE_DATABASE
        self._data_db = ClickhouseDatabase(database=target_db, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
        self._access_db = ClickhouseDatabase(
            database=CLICKHOUSE_ACCESS_DATABASE,
            user=CLICKHOUSE_ACCESS_USER,
            password=CLICKHOUSE_ACCESS_PASSWORD,
        )
        self.engine = _AsyncEngine()
        self.metadata = type("Meta", (), {"tables": {}})()

    async def table_exists(self, table_name: str) -> bool:
        exists = await self._data_db.table_exists(table_name)
        if exists:
            self.metadata.tables.setdefault(table_name, True)
        return exists

    async def init_db(self):
        """Create the Direct database if it is missing."""
        await self._data_db.ensure_db()

    async def close_engine(self):
        """Compatibility stub for interface parity."""
        return None

    async def write_dataframe_to_table_fast(self, df: pd.DataFrame, table_name: str, min_rows_for_copy: int = 500):
        """Compatibility wrapper that writes a DataFrame to ClickHouse."""
        del min_rows_for_copy
        if df is None or df.empty:
            return
        await self._data_db.write_dataframe(table_name, df)

    @staticmethod
    def _container_from_legacy(table_name: str) -> str:
        """Infer a container label from legacy table naming."""
        name = (table_name or "").lower()
        if "make" in name or "agency" in name:
            return "agency"
        if "not" in name or "client" in name:
            return "client"
        return name or "default"

    @staticmethod
    def _normalize_identifier(value: str | None) -> str | None:
        """Strip whitespace and normalize empty strings to None."""
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @staticmethod
    def _normalize_login(value: str | None) -> str | None:
        """Normalize Direct logins to a canonical API form (underscores/dots→hyphens)."""
        normalized = AsyncDirectDatabase._normalize_identifier(value)
        if not normalized:
            return None
        return normalized.replace("_", "-").replace(".", "-")

    async def upsert_access_records(
        self,
        logins: list[str | None],
        token: str,
        *,
        container: str | None,
        type_value: str | None,
        replace: bool,
    ):
        """Upsert Direct access tokens for a set of logins."""
        normalized_logins = [self._normalize_login(login) for login in logins]
        await self._access_db.upsert_accesses(
            normalized_logins,
            token,
            container=self._normalize_identifier(container),
            service_type="direct",
            replace=replace,
            type_value=type_value,
        )
        self.metadata.tables.setdefault("Accesses", True)

    async def fetch_direct_access_rows(
        self,
        *,
        container: str | None = None,
        type_values: list[str] | None = None,
        include_null_type: bool = False,
    ):
        """Return Accesses rows for Direct with optional container/type filtering."""
        rows = await self._access_db.fetch_access_rows(
            service_type="direct",
            container=self._normalize_identifier(container),
            type_values=type_values,
            include_null_type=include_null_type,
        )
        if rows:
            self.metadata.tables.setdefault("Accesses", True)
        return rows

    async def add_agency_client_list_to_table(self, client_list, token: str, table: str):
        """Replace Direct tokens for an agency client list."""
        container = self._container_from_legacy(table)
        await self._access_db.upsert_accesses(
            client_list,
            token,
            container=container,
            service_type="direct",
            replace=True,
        )

    async def add_other_client_list_to_table(self, client_list, token: str, table: str, type_value: str | None = None):
        """Append Direct tokens for a client list, optionally scoped by type."""
        container = self._container_from_legacy(table)
        await self._access_db.upsert_accesses(
            client_list,
            token,
            container=container,
            service_type="direct",
            replace=False,
            type_value=type_value,
        )

    async def add_agency_token(self, agency_login: str, token: str, container: str | None = None):
        """
        Register/replace an agency-level token. Stored in Accesses with type='direct'.
        """
        effective_container = container or agency_login or "agency"
        await self._access_db.upsert_accesses(
            [agency_login],
            token,
            container=effective_container,
            service_type="direct",
            replace=True,
        )

    async def add_client_token(self, client_login: str, token: str, container: str | None = None):
        """
        Append or replace a client-level token; if container provided it scopes the token.
        """
        effective_container = container or client_login or "client"
        await self._access_db.upsert_accesses(
            [client_login],
            token,
            container=effective_container,
            service_type="direct",
            replace=True,
        )

    async def get_login_key_dictionary(
        self,
        table_name: str | None = None,
        *,
        include_null_type: bool = False,
    ) -> dict[str, str]:
        """Return Direct login→token pairs, optionally constrained by container."""
        container = self._container_from_legacy(table_name) if table_name else None
        tokens = await self._access_db.fetch_access_tokens(
            service_type="direct",
            container=container,
            include_null_type=include_null_type,
        )
        tokens_by_login = {
            login: token_value
            for login, token_value in tokens.items()
            if login
        }
        if tokens_by_login:
            self.metadata.tables.setdefault("Accesses", True)
        return tokens_by_login

    async def delete_records_between_dates(
        self, table_name: str, start_date: datetime, end_date: datetime
    ):
        """Delete Direct records within a date range."""
        safe_table_name = table_name.replace("-", "_")
        await self._data_db.delete_between_dates(
            safe_table_name, start_date, end_date
        )

    async def get_daily_summary(
        self, table: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Aggregate core Direct metrics by day for a table."""
        safe_table = table.replace("-", "_")
        daily_summary_df = await self._data_db.aggregate_daily(
            safe_table,
            start_date,
            end_date,
            metric_allowlist=("impressions", "clicks", "cost", "sumconversion"),
            conversion_prefixes=("conversions_",),
        )
        if not daily_summary_df.empty:
            self.metadata.tables.setdefault(safe_table, True)
        return daily_summary_df

    async def remove_client_access(self, login: str, table_name: str):
        """Delete Direct access rows for a login and container."""
        container = self._container_from_legacy(table_name)
        await self._access_db.delete_access(
            login, service_type="direct", container=container
        )

    async def add_empty_conversion_columns(
        self, table_name: str, column_names: list[str]
    ) -> int:
        """Add empty conversion columns (Int64 default 0) without reloading data."""
        safe_table = table_name.replace("-", "_")
        if not await self._data_db.table_exists(safe_table):
            return 0
        added_columns = 0
        for col_name in column_names:
            try:
                await self._data_db._command_with_retry(
                    f"ALTER TABLE {self._data_db.database}.{safe_table} "
                    f"ADD COLUMN IF NOT EXISTS `{col_name}` Int64 DEFAULT 0"
                )
                added_columns += 1
            except Exception as exc:
                logging.error(
                    "%s: Failed to add column %s: %s", safe_table, col_name, exc
                )
        return added_columns

    async def delete_access(self, login: str | None, *, container: str | None = None, type_value: str | None = None):
        """Delete access rows for a login and optional container/type."""
        await self._access_db.delete_access(
            self._normalize_login(login) if login else None,
            service_type="direct",
            container=self._normalize_identifier(container) if container else None,
            type_value=type_value,
        )

    async def reset_database(self) -> dict[str, int]:
        """
        Reset the Direct database - truncate all tables.

        Returns:
            dict with count of tables affected per database.
        """
        data_tables = await self._data_db.truncate_all_tables()
        return {"data_db": data_tables}

    async def get_stale_clients(self, days_threshold: int = 30):
        """
        Get list of Direct clients with stale data (oldest date > threshold days old).

        Returns:
            List of dicts with: table_name, min_date, days_since_update
        """
        return await self._data_db.get_stale_tables("Date", days_threshold)

    async def cleanup_clients(self, client_logins: list[str]) -> int:
        """
        Drop tables for specified client logins.

        Returns:
            Count of tables dropped
        """
        dropped_tables = 0
        for login in client_logins:
            safe_login = self._normalize_login(login)
            try:
                await self._data_db.drop_table(safe_login)
                dropped_tables += 1
            except Exception:
                pass
        return dropped_tables


__all__ = ["AsyncDirectDatabase"]
