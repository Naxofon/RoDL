import asyncio
from typing import Any

from prefect_loader.orchestration.clickhouse_utils import (
    ClickhouseDatabase,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_ACCESS_PASSWORD,
)


_DATABASE = CLICKHOUSE_ACCESS_DATABASE
_TABLE_NAME = "AdminUsers"


def _coerce_id(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


class UserDatabaseConnector:
    """ClickHouse-backed repository for admin bot users."""

    _shared_db: ClickhouseDatabase | None = None

    def __init__(self, db_name: str | None = None):
        if UserDatabaseConnector._shared_db is None:
            UserDatabaseConnector._shared_db = ClickhouseDatabase(
                database=CLICKHOUSE_DATABASE,
                user=CLICKHOUSE_ACCESS_USER,
                password=CLICKHOUSE_ACCESS_PASSWORD,
            )
        self.db = UserDatabaseConnector._shared_db
        self.table = f"{_DATABASE}.{_TABLE_NAME}"

    async def initialize(self):
        try:
            await self.db._query_with_retry(f"SELECT 1 FROM {self.table} LIMIT 1")
            return
        except Exception as exc:
            msg = str(exc)
            if "UNKNOWN_DATABASE" in msg:
                raise RuntimeError(
                    f"ClickHouse: database {_DATABASE} not found. "
                    f"Create {_DATABASE} and {_DATABASE}.{_TABLE_NAME} manually (or via init.sh)."
                ) from exc
            if "UNKNOWN_TABLE" in msg or ("Table" in msg and "doesn't exist" in msg):
                raise RuntimeError(
                    f"ClickHouse: table {_DATABASE}.{_TABLE_NAME} not found. "
                    f"Create {_DATABASE}.{_TABLE_NAME} manually (or via init.sh)."
                ) from exc
            if "ACCESS_DENIED" in msg or "Not enough privileges" in msg:
                raise RuntimeError(
                    f"ClickHouse: insufficient privileges to read {_DATABASE}.{_TABLE_NAME}. "
                    f"Grant SELECT on {_DATABASE}.{_TABLE_NAME} to CLICKHOUSE_ACCESS_USER."
                ) from exc
            raise

    async def _insert_row(
        self,
        *,
        user_id: int,
        name: str,
        role: str,
    ):
        data = [[user_id, name, role]]
        await asyncio.to_thread(
            self.db.client.insert,
            self.table,
            data,
            column_names=["id", "name", "role"],
        )

    async def _fetch_user_row(self, user_id: int) -> tuple[int, str, str] | None:
        sql = (
            f"SELECT id, name, role "
            f"FROM {self.table} FINAL WHERE id = {user_id} LIMIT 1"
        )
        result = await self.db._query_with_retry(sql)
        rows = result.result_rows
        return rows[0] if rows else None

    async def add_user(self, user_id, name, role):
        user_id_int = _coerce_id(user_id)
        if user_id_int is None:
            return

        existing = await self._fetch_user_row(user_id_int)
        if existing:
            return

        await self._insert_row(
            user_id=user_id_int,
            name=str(name or "").strip(),
            role=str(role or "").strip(),
        )

    async def delete_user(self, user_identifier):
        user_id_int = _coerce_id(user_identifier)
        if user_id_int is not None:
            await self.db._command_with_retry(
                f"ALTER TABLE {self.table} DELETE WHERE id = {user_id_int}"
            )
            return

        name = str(user_identifier or "").strip()
        if not name:
            return

        escaped_name = name.replace("'", "''")
        await self.db._command_with_retry(
            f"ALTER TABLE {self.table} DELETE WHERE name = '{escaped_name}'"
        )

    async def get_user_ids_by_role(self, role):
        role_value = str(role or "").strip()
        if not role_value:
            return []

        escaped_role = role_value.replace("'", "''")
        sql = f"SELECT id FROM {self.table} FINAL WHERE role = '{escaped_role}'"
        result = await self.db._query_with_retry(sql)
        return [str(row[0]) for row in result.result_rows]

    async def search_users_by_id(self, user_id):
        user_id_int = _coerce_id(user_id)
        if user_id_int is None:
            return None

        row = await self._fetch_user_row(user_id_int)
        if not row:
            return None

        return row[0], row[1], row[2]

    async def get_administrators(self):
        sql = f"SELECT id, name FROM {self.table} FINAL WHERE role = 'Admin'"
        result = await self.db._query_with_retry(sql)
        return [{"id": row[0], "name": row[1]} for row in result.result_rows]

    async def get_alpha(self):
        sql = f"SELECT id, name FROM {self.table} FINAL WHERE role = 'Alpha'"
        result = await self.db._query_with_retry(sql)
        return [{"id": row[0], "name": row[1]} for row in result.result_rows]

    async def get_admin_user_ids(self):
        return await self.get_user_ids_by_role("Admin")

    async def get_regular_user_ids(self):
        return await self.get_user_ids_by_role("User")

    async def get_alpha_user_ids(self):
        return await self.get_user_ids_by_role("Alpha")

    async def get_users(self):
        sql = f"SELECT id, name, role FROM {self.table} FINAL"
        result = await self.db._query_with_retry(sql)
        return [{"id": row[0], "name": row[1], "role": row[2]} for row in result.result_rows]

    async def _set_role(self, user_id, role: str):
        user_id_int = _coerce_id(user_id)
        if user_id_int is None:
            return

        row = await self._fetch_user_row(user_id_int)
        if not row:
            return

        name = row[1] or ""
        await self._insert_row(
            user_id=user_id_int,
            name=name,
            role=role,
        )

    async def set_user_to_admin(self, user_id):
        await self._set_role(user_id, "Admin")

    async def set_user_to_user(self, user_id):
        await self._set_role(user_id, "User")

    async def set_user_to_alpha(self, user_id):
        await self._set_role(user_id, "Alpha")