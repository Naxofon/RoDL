from datetime import date, datetime

import pandas as pd

from .config import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_DB_CALLTOUCH,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)
from .database import ClickhouseDatabase


class AsyncCalltouchDatabase:
    """
    Calltouch loader wrapper for ClickHouse operations.

    Configuration:
    - Uses Accesses table with service='calltouch'
    - login: site_id (as string)
    - token: Calltouch API token
    - container: account name/description (optional)
    - type: unused (can be None)
    """

    def __init__(self, *, database: str | None = None):
        target_db = database or CLICKHOUSE_DB_CALLTOUCH or CLICKHOUSE_DATABASE
        self._data_db = ClickhouseDatabase(database=target_db, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
        self._access_db = ClickhouseDatabase(
            database=CLICKHOUSE_ACCESS_DATABASE,
            user=CLICKHOUSE_ACCESS_USER,
            password=CLICKHOUSE_ACCESS_PASSWORD,
        )
        self.metadata = type("Meta", (), {"tables": {}})()

    async def init_db(self):
        """Create the Calltouch database if it does not exist."""
        await self._data_db.ensure_db()

    async def add_client_to_calltouch_config(
        self,
        site_id: int,
        token: str,
        account: str | None = None,
    ):
        """
        Add a Calltouch client to the Accesses table.

        Args:
            site_id: Calltouch site ID (stored as login)
            token: API token
            account: Optional account name/description (stored as container)
        """
        site_id_str = str(site_id)
        container_value = (account or "").strip() or None

        await self._access_db.upsert_accesses(
            [site_id_str],
            token,
            container=container_value,
            service_type="calltouch",
            type_value=None,
            replace=False,
        )
        self.metadata.tables.setdefault("Accesses", True)

    async def delete_client_by_site_id(self, site_id: int):
        """Delete a Calltouch client from the Accesses table."""
        site_id_str = str(site_id)
        await self._access_db.delete_access(site_id_str, service_type="calltouch")

    async def get_calltouch_config_data(self) -> pd.DataFrame:
        """
        Retrieve all Calltouch clients from the Accesses table.

        Returns:
            DataFrame with columns: site_id, token, account
        """
        access_rows = await self._access_db.fetch_access_rows(service_type="calltouch", include_null_type=True)

        if not access_rows:
            return pd.DataFrame(columns=["site_id", "token", "account"])

        parsed: list[dict[str, object]] = []
        for row in access_rows:
            site_id_str = row.get("login")
            token = row.get("token")
            account = (row.get("container") or "").strip()

            if not site_id_str:
                continue

            try:
                site_id = int(site_id_str)
            except (TypeError, ValueError):
                continue

            parsed.append({
                "site_id": site_id,
                "token": token,
                "account": account,
            })

        df = pd.DataFrame(parsed)
        if not df.empty:
            self.metadata.tables.setdefault("Accesses", True)
        return df

    async def write_dataframe_to_table(self, df: pd.DataFrame, table_name: str):
        """Write DataFrame to ClickHouse table."""
        if df is None or df.empty:
            return
        await self._data_db.write_dataframe(table_name, df, order=["Date"])

    async def delete_between_dates(
        self,
        table_name: str,
        start_date: datetime | date,
        end_date: datetime | date,
    ):
        """Delete records in a date range."""
        await self._data_db.delete_between_dates(table_name, start_date, end_date, date_column="Date")

    async def reset_database(self) -> dict[str, int]:
        """
        Reset the Calltouch database - truncate all tables.

        Returns:
            dict with count of tables affected per database.
        """
        data_tables = await self._data_db.truncate_all_tables()
        return {"data_db": data_tables}

    async def get_stale_clients(self, days_threshold: int = 30):
        """
        Get list of Calltouch clients with stale data (oldest date > threshold days old).

        Returns:
            List of dicts with: table_name, min_date, days_since_update
        """
        return await self._data_db.get_stale_tables("Date", days_threshold)

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


__all__ = ["AsyncCalltouchDatabase"]
