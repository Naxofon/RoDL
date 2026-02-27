from datetime import date, datetime

import pandas as pd

from .config import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_DB_VK,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)
from .database import ClickhouseDatabase, _AsyncEngine


class AsyncVkDatabase:
    """
    VK Ads loader wrapper providing an async interface for ClickHouse operations.
    """

    def __init__(self, database: str | None = None):
        target_db = database or CLICKHOUSE_DB_VK or CLICKHOUSE_DATABASE
        self._data_db = ClickhouseDatabase(
            database=target_db, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
        )
        self._access_db = ClickhouseDatabase(
            database=CLICKHOUSE_ACCESS_DATABASE,
            user=CLICKHOUSE_ACCESS_USER,
            password=CLICKHOUSE_ACCESS_PASSWORD,
        )
        self.engine = _AsyncEngine()
        self.metadata = type("Meta", (), {"tables": {}})()

    async def table_exists(self, table_name: str) -> bool:
        """Return True if the table exists in the current database."""
        exists = await self._data_db.table_exists(table_name)
        if exists:
            self.metadata.tables.setdefault(table_name, True)
        return exists

    async def init_db(self):
        """Create the VK database if it is missing."""
        await self._data_db.ensure_db()

    async def close_engine(self):
        """Compatibility stub for interface parity."""
        return None

    async def write_dataframe_to_table(
        self, df: pd.DataFrame, table_name: str
    ):
        """Write a DataFrame to ClickHouse table."""
        if df is None or df.empty:
            return
        await self._data_db.write_dataframe(table_name, df)

    @staticmethod
    def _normalize_identifier(value: str | None) -> str | None:
        """Strip whitespace and normalize empty strings to None."""
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @staticmethod
    def _normalize_user_id(value: str | int | None) -> str | None:
        """Normalize VK user IDs to canonical string form."""
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    async def upsert_access_records(
        self,
        user_ids: list[str | int | None],
        token: str,
        *,
        container: str | None,
        type_value: str | None,
        replace: bool,
    ):
        """Upsert VK access tokens for a set of user IDs."""
        normalized_ids = [self._normalize_user_id(uid) for uid in user_ids]
        await self._access_db.upsert_accesses(
            normalized_ids,
            token,
            container=self._normalize_identifier(container),
            service_type="vk",
            replace=replace,
            type_value=type_value,
        )
        self.metadata.tables.setdefault("Accesses", True)

    async def fetch_vk_access_rows(
        self,
        *,
        container: str | None = None,
        type_values: list[str] | None = None,
        include_null_type: bool = False,
    ):
        """Return Accesses rows for VK with optional container/type filtering."""
        rows = await self._access_db.fetch_access_rows(
            service_type="vk",
            container=self._normalize_identifier(container),
            type_values=type_values,
            include_null_type=include_null_type,
        )
        if rows:
            self.metadata.tables.setdefault("Accesses", True)
        return rows

    async def add_agency_token(
        self, agency_user_id: str, token: str, container: str | None = None
    ):
        """
        Register/replace an agency-level token.

        Args:
            agency_user_id: Agency user ID
            token: API authentication token
            container: Optional container identifier
        """
        cont = container or agency_user_id or "agency"
        await self._access_db.upsert_accesses(
            [agency_user_id],
            token,
            container=cont,
            service_type="vk",
            replace=True,
        )

    async def add_client_token(
        self, client_user_id: str, token: str, container: str | None = None
    ):
        """
        Append or replace a client-level token.

        Args:
            client_user_id: Client user ID
            token: API authentication token
            container: Optional container identifier
        """
        cont = container or client_user_id or "client"
        await self._access_db.upsert_accesses(
            [client_user_id],
            token,
            container=cont,
            service_type="vk",
            replace=True,
        )

    async def add_agency_client_list(
        self, client_list: list[str], token: str, container: str
    ):
        """
        Replace VK tokens for an agency client list.

        Args:
            client_list: List of client user IDs
            token: API authentication token
            container: Container identifier
        """
        await self._access_db.upsert_accesses(
            client_list,
            token,
            container=container,
            service_type="vk",
            replace=True,
        )

    async def get_user_id_token_dictionary(
        self, container: str | None = None, *, include_null_type: bool = False
    ) -> dict[str, str]:
        """
        Return VK user_id→token pairs.

        Args:
            container: Optional container filter
            include_null_type: Include records with null type

        Returns:
            Dictionary mapping user_id -> token
        """
        tokens = await self._access_db.fetch_access_tokens(
            service_type="vk",
            container=container,
            include_null_type=include_null_type,
        )
        cleaned = {k: v for k, v in tokens.items() if k}
        if cleaned:
            self.metadata.tables.setdefault("Accesses", True)
        return cleaned

    async def delete_records_between_dates(
        self, table_name: str, start_date: datetime | date, end_date: datetime | date
    ):
        """
        Delete VK records within a date range.

        Args:
            table_name: Table name
            start_date: Start date
            end_date: End date
        """
        safe_table_name = table_name.replace("-", "_").replace(".", "_")
        await self._data_db.delete_between_dates(
            safe_table_name, start_date, end_date
        )

    async def delete_access(
        self,
        user_id: str | None,
        *,
        container: str | None = None,
        type_value: str | None = None,
    ):
        """
        Delete access rows for a user ID and optional container/type.

        Args:
            user_id: VK user ID
            container: Optional container
            type_value: Optional type value
        """
        await self._access_db.delete_access(
            self._normalize_user_id(user_id) if user_id else None,
            service_type="vk",
            container=self._normalize_identifier(container) if container else None,
            type_value=type_value,
        )

    async def reset_database(self) -> dict[str, int]:
        """
        Reset the VK database - truncate all tables.

        Returns:
            dict with count of tables affected
        """
        data_tables = await self._data_db.truncate_all_tables()
        return {"data_db": data_tables}

    async def get_stale_clients(self, days_threshold: int = 30):
        """
        Get list of VK clients with stale data.

        Args:
            days_threshold: Number of days to consider stale

        Returns:
            List of dicts with: table_name, max_date, days_since_update
        """
        return await self._data_db.get_stale_tables("date", days_threshold)

    async def cleanup_clients(self, client_user_ids: list[str]) -> int:
        """
        Drop tables for specified client user IDs.

        Args:
            client_user_ids: List of VK user IDs

        Returns:
            Count of tables dropped
        """
        dropped = 0
        for user_id in client_user_ids:
            safe_user_id = self._normalize_user_id(user_id)
            if not safe_user_id:
                continue
            table_name = f"vk_{safe_user_id}".replace("-", "_").replace(".", "_")
            try:
                await self._data_db.drop_table(table_name)
                dropped += 1
            except Exception:
                pass
        return dropped


__all__ = ["AsyncVkDatabase"]
