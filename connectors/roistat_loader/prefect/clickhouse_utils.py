from __future__ import annotations

from datetime import date, datetime

import pandas as pd

try:
    from prefect_loader.orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_DB_ROISTAT,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from prefect_loader.orchestration.clickhouse_utils.database import ClickhouseDatabase
except Exception:  # pragma: no cover - local fallback
    from orchestration.clickhouse_utils.config import (
        CLICKHOUSE_ACCESS_DATABASE,
        CLICKHOUSE_ACCESS_PASSWORD,
        CLICKHOUSE_ACCESS_USER,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_DB_ROISTAT,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_USER,
    )
    from orchestration.clickhouse_utils.database import ClickhouseDatabase

from connectors.roistat_loader.flags import (
    DEFAULT_ROISTAT_FLAGS,
    parse_roistat_flags,
    serialize_roistat_payload,
)


class AsyncRoistatDatabase:
    def __init__(self, *, database: str | None = None):
        target_db = database or CLICKHOUSE_DB_ROISTAT or CLICKHOUSE_DATABASE
        self._data_db = ClickhouseDatabase(database=target_db, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
        self._access_db = ClickhouseDatabase(
            database=CLICKHOUSE_ACCESS_DATABASE,
            user=CLICKHOUSE_ACCESS_USER,
            password=CLICKHOUSE_ACCESS_PASSWORD,
        )
        self.metadata = type("Meta", (), {"tables": {}})()

    async def init_db(self):
        await self._data_db.ensure_db()

    async def close_engine(self):
        return None

    async def add_or_update_client(
        self,
        *,
        site_id: int,
        token: str,
        account: str | None = None,
        flags: dict[str, bool] | None = None,
    ) -> None:
        site_id_str = str(site_id)
        await self._access_db.delete_access(site_id_str, service_type="roistat")
        await self._access_db.upsert_accesses(
            [site_id_str],
            token,
            container=(account or "").strip() or None,
            service_type="roistat",
            type_value=serialize_roistat_payload(flags or DEFAULT_ROISTAT_FLAGS),
            replace=False,
        )
        self.metadata.tables.setdefault("Accesses", True)

    async def update_client_flags(self, site_id: int, flags: dict[str, bool]) -> None:
        client = await self.get_client(site_id)
        if client is None:
            raise ValueError(f"Client {site_id} not found")
        await self.add_or_update_client(
            site_id=site_id,
            token=client["token"],
            account=client.get("account"),
            flags=flags,
        )

    async def delete_client_by_site_id(self, site_id: int) -> None:
        await self._access_db.delete_access(str(site_id), service_type="roistat")

    async def get_client(self, site_id: int) -> dict | None:
        rows = await self._access_db.fetch_access_rows(service_type="roistat", include_null_type=True)
        for row in rows:
            if str(row.get("login")) != str(site_id):
                continue
            return {
                "site_id": int(site_id),
                "token": row.get("token"),
                "account": (row.get("container") or "").strip(),
                "flags": parse_roistat_flags(row.get("subtype")),
                "type": row.get("type"),
            }
        return None

    async def get_roistat_config_data(self) -> pd.DataFrame:
        rows = await self._access_db.fetch_access_rows(service_type="roistat", include_null_type=True)
        parsed: list[dict[str, object]] = []
        for row in rows:
            login = row.get("login")
            token = row.get("token")
            if not login or not token:
                continue
            try:
                site_id = int(str(login).strip())
            except (TypeError, ValueError):
                continue
            flags = parse_roistat_flags(row.get("subtype"))
            parsed.append(
                {
                    "site_id": site_id,
                    "token": token,
                    "account": (row.get("container") or "").strip(),
                    "analytics": flags["analytics"],
                    "calls": flags["calls"],
                    "visits": flags["visits"],
                    "type": row.get("type"),
                }
            )
        return pd.DataFrame(parsed)

    async def fetch_roistat_access_rows(self) -> list[dict]:
        rows = await self._access_db.fetch_access_rows(service_type="roistat", include_null_type=True)
        if rows:
            self.metadata.tables.setdefault("Accesses", True)
        return rows

    async def write_dataframe(self, table_name: str, df: pd.DataFrame, order: list[str] | None = None):
        if df is None or df.empty:
            return
        await self._data_db.write_dataframe(table_name, df, order=order or ["Date"])

    async def delete_between_dates(
        self,
        table_name: str,
        start_date: datetime | date,
        end_date: datetime | date,
        *,
        date_column: str = "Date",
    ) -> None:
        await self._data_db.delete_between_dates(table_name, start_date, end_date, date_column=date_column)

    async def reset_database(self) -> dict[str, int]:
        data_tables = await self._data_db.truncate_all_tables()
        return {"data_db": data_tables}


__all__ = ["AsyncRoistatDatabase"]
