import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any, Callable, Mapping, Sequence

import clickhouse_connect
import pandas as pd

from .config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_ROOT_PASSWORD,
    CLICKHOUSE_ROOT_USER,
    CLICKHOUSE_SECURE,
)

from .connection import ensure_database, get_client
from .helpers import (
    _as_nullable,
    _build_type_predicate,
    _compose_type,
    _get_column_names,
    _is_numeric_type,
    _quote_nullable,
    _split_type,
    detect_date_column
)

from .schema import (
    delete_by_date_range,
    describe_columns,
    ensure_table,
    insert_dataframe,
)


class ClickhouseDatabase:
    """Async helpers for the ClickHouse HTTP client."""

    def __init__(
        self,
        *,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        self.client = get_client(
            database=database,
            username=user,
            password=password,
        )
        self.database = self.client.database

    async def _run_with_retry(
        self,
        action: Callable[[str], Any],
        sql: str,
        *,
        retries: int = 3,
    ):
        """Run a ClickHouse operation with simple retry backoff."""
        delay = 1.0
        for attempt in range(1, retries + 1):
            try:
                return await asyncio.to_thread(action, sql)
            except Exception as exc:
                msg = str(exc)
                if attempt >= retries or "ACCESS_DENIED" in msg:
                    raise
                transient = (
                    "Server disconnected" in msg
                    or "Timeout" in msg
                    or "connection was closed" in msg
                )
                if transient:
                    await asyncio.sleep(delay)
                    delay = min(10.0, delay * 2)
                    continue
                raise

    async def _command_with_retry(self, sql: str, *, retries: int = 3):
        """Execute a command with retries on transient errors."""
        return await self._run_with_retry(
            self.client.command,
            sql,
            retries=retries,
        )

    async def _query_with_retry(self, sql: str, *, retries: int = 3):
        """Execute a query with retries on transient errors."""
        return await self._run_with_retry(
            self.client.query,
            sql,
            retries=retries,
        )

    async def ensure_db(self):
        """Create the configured database if it does not exist."""
        await asyncio.to_thread(ensure_database, self.client)

    async def get_all_tables(self) -> list[str]:
        """Get list of all tables in the database."""
        result = await self._query_with_retry(f"SHOW TABLES FROM {self.database}")
        return [row[0] for row in result.result_rows]

    async def truncate_table(self, table_name: str) -> None:
        """Truncate a table (remove all data but keep structure)."""
        await self.ensure_db()
        await self._command_with_retry(
            f"TRUNCATE TABLE IF EXISTS {self.database}.{table_name}"
        )

    async def drop_table(self, table_name: str) -> None:
        """
        Drop a table from the database with elevated credentials.

        Removes the table and its data using the root user to avoid privilege issues.
        """
        root_client = clickhouse_connect.create_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_ROOT_USER,
            password=CLICKHOUSE_ROOT_PASSWORD,
            database=self.database,
            secure=CLICKHOUSE_SECURE,
        )
        try:
            await asyncio.to_thread(root_client.command, f"DROP TABLE IF EXISTS {self.database}.{table_name}")
        finally:
            root_client.close()

    async def truncate_all_tables(self) -> int:
        """Truncate all tables in the database. Returns count of truncated tables."""
        tables = await self.get_all_tables()
        for table in tables:
            await self.truncate_table(table)
        return len(tables)

    async def drop_all_tables(self) -> int:
        """Drop all tables in the database. Returns count of dropped tables."""
        tables = await self.get_all_tables()
        for table in tables:
            await self.drop_table(table)
        return len(tables)

    async def get_stale_tables(self, date_column: str, days_threshold: int = 30) -> list[dict[str, Any]]:
        """
        Find tables where the maximum (most recent) date is older than X days.

        Returns sorted list of tables with max_date and age in days.
        """
        tables = await self.get_all_tables()
        stale_tables = []
        cutoff_date = datetime.now().date() - timedelta(days=days_threshold)

        for table in tables:
            try:
                query = (
                    f"SELECT MAX({date_column}) as max_date "
                    f"FROM {self.database}.{table}"
                )
                result = await self._query_with_retry(query)

                if not result.result_rows or not result.result_rows[0][0]:
                    continue

                max_date = result.result_rows[0][0]

                if isinstance(max_date, str):
                    try:
                        max_date = datetime.strptime(
                            max_date.split()[0], "%Y-%m-%d"
                        ).date()
                    except (ValueError, IndexError):
                        continue
                elif hasattr(max_date, "date"):
                    max_date = max_date.date()

                if max_date < cutoff_date:
                    days_old = (datetime.now().date() - max_date).days
                    stale_tables.append({
                        "table_name": table,
                        "max_date": max_date.isoformat(),
                        "days_since_update": days_old,
                    })
            except Exception:
                continue

        return sorted(stale_tables, key=lambda x: x["days_since_update"], reverse=True)

    async def table_exists(self, table_name: str) -> bool:
        """Return True if the table exists in the current database."""
        result = await self._command_with_retry(f"EXISTS TABLE {self.database}.{table_name}")
        return bool(int(str(result).strip()))

    async def get_columns(self, table_name: str) -> Mapping[str, str]:
        """Describe table columns as a mapping of name to ClickHouse type."""
        return await asyncio.to_thread(describe_columns, self.client, table_name)

    async def ensure_table(self, table_name: str, df: pd.DataFrame, order: Sequence[str] | None = None):
        """Create the table if needed and align its schema to the DataFrame."""
        await asyncio.to_thread(ensure_table, self.client, table_name, df, order)

    async def insert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        order: Sequence[str] | None = None,
        ensure_schema: bool = True,
    ):
        """Insert a DataFrame, optionally ensuring schema before write."""
        await asyncio.to_thread(
            insert_dataframe,
            self.client,
            table_name,
            df,
            order,
            ensure_schema=ensure_schema,
        )

    async def write_dataframe(self, table_name: str, df: pd.DataFrame, order: Sequence[str] | None = None):
        """Ensure the table exists and append the DataFrame rows."""
        if df is None or df.empty:
            return
        await self.insert_dataframe(
            table_name,
            df,
            order=order,
            ensure_schema=True,
        )

    async def delete_between_dates(
        self,
        table_name: str,
        start: datetime | date,
        end: datetime | date,
        date_column: str | None = None,
    ):
        """Delete rows in a date range if the table and date column exist."""
        if not await self.table_exists(table_name):
            return
        cols = await self.get_columns(table_name)
        col = date_column or detect_date_column(cols.keys())
        if not col:
            return
        await asyncio.to_thread(delete_by_date_range, self.client, table_name, col, start, end)

    def _ensure_accesses_table(self):
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.database}.Accesses ("
            " login Nullable(String),"
            " token Nullable(String),"
            " container Nullable(String),"
            " type Nullable(String)"
            ") ENGINE = MergeTree ORDER BY (type, container, login) SETTINGS allow_nullable_key = 1"
        )
        self.client.command(ddl)

    async def _prepare_accesses(self):
        """Ensure the Accesses helper table and its database exist."""
        await self.ensure_db()
        await asyncio.to_thread(self._ensure_accesses_table)

    async def upsert_accesses(
        self,
        logins: Sequence[str],
        token: str,
        container: str | None,
        service_type: str | None,
        *,
        replace: bool,
        type_value: str | None = None,
    ) -> None:
        await self._prepare_accesses()
        stored_type = _compose_type(service_type, type_value)

        def _where_container(cont: str | None) -> str | None:
            if cont is None or pd.isna(cont):
                return "container IS NULL"
            escaped = str(cont).replace("'", "''")
            return f"container = '{escaped}'"

        cont_clause = _where_container(container)
        type_clause = _build_type_predicate(
            service_type, type_value, include_null=stored_type is None
        )

        seen: set = set()
        ordered_logins: list[str | None] = []
        for login in logins:
            norm = _as_nullable(login)
            if norm in seen:
                continue
            seen.add(norm)
            ordered_logins.append(norm)

        if replace and not ordered_logins:
            clauses = [cl for cl in (cont_clause, type_clause) if cl]
            where_clause = " AND ".join(clauses) if clauses else "1"
            await asyncio.to_thread(
                self.client.command,
                f"ALTER TABLE {self.database}.Accesses DELETE WHERE {where_clause}",
                settings={"mutations_sync": 1},
            )
            return

        if replace and ordered_logins:
            login_filter_parts: list[str] = []
            has_null_login = any(login is None for login in ordered_logins)
            non_null_logins = [
                login for login in ordered_logins if login is not None
            ]
            if non_null_logins:
                quoted_logins = ", ".join(
                    _quote_nullable(l) for l in non_null_logins
                )
                login_filter_parts.append(f"login IN ({quoted_logins})")
            if has_null_login:
                login_filter_parts.append("login IS NULL")
            allowed_clause = (
                " OR ".join(login_filter_parts)
                if login_filter_parts
                else "0"
            )
            stale_where = " AND ".join(
                cl
                for cl in (
                    cont_clause,
                    type_clause,
                    f"NOT ({allowed_clause})",
                )
                if cl
            )
            await asyncio.to_thread(
                self.client.command,
                f"ALTER TABLE {self.database}.Accesses "
                f"DELETE WHERE {stale_where}",
                settings={"mutations_sync": 1},
            )

        if not ordered_logins:
            return

        existing_map: dict[str | None, str | None] = {}
        existing_predicates: list[str] = []
        non_null_logins = [
            login for login in ordered_logins if login is not None
        ]
        if non_null_logins:
            quoted_logins = ", ".join(
                _quote_nullable(l) for l in non_null_logins
            )
            existing_predicates.append(f"login IN ({quoted_logins})")
        if any(login is None for login in ordered_logins):
            existing_predicates.append("login IS NULL")
        if existing_predicates:
            filter_clause = " OR ".join(existing_predicates)
            where_clause = " AND ".join(
                cl
                for cl in (
                    cont_clause,
                    type_clause,
                    f"({filter_clause})",
                )
                if cl
            )
            result = await asyncio.to_thread(
                self.client.query,
                f"SELECT login, token FROM {self.database}.Accesses "
                f"WHERE {where_clause}",
            )
            for row in result.result_rows:
                existing_map[row[0]] = row[1]

        rows_to_insert: list[dict[str, str | None]] = []
        clean_container = _as_nullable(container)
        clean_type = _as_nullable(stored_type)
        for login in ordered_logins:
            current = existing_map.get(login, "__missing__")
            if current == token:
                continue
            rows_to_insert.append(
                {
                    "login": _as_nullable(login),
                    "token": _as_nullable(token),
                    "container": clean_container,
                    "type": clean_type,
                }
            )

        if not rows_to_insert:
            return

        changed_predicates: list[str] = []
        non_null_changed = [
            row["login"]
            for row in rows_to_insert
            if row["login"] is not None
        ]
        if non_null_changed:
            quoted_changed = ", ".join(
                _quote_nullable(l) for l in non_null_changed
            )
            changed_predicates.append(f"login IN ({quoted_changed})")
        if any(row["login"] is None for row in rows_to_insert):
            changed_predicates.append("login IS NULL")
        if changed_predicates:
            delete_clause = " AND ".join(
                cl
                for cl in (
                    cont_clause,
                    type_clause,
                    f"({' OR '.join(changed_predicates)})",
                )
                if cl
            )
            await asyncio.to_thread(
                self.client.command,
                f"ALTER TABLE {self.database}.Accesses "
                f"DELETE WHERE {delete_clause}",
                settings={"mutations_sync": 1},
            )

        df = pd.DataFrame(rows_to_insert)
        await asyncio.to_thread(
            insert_dataframe, self.client, "Accesses", df
        )

    async def fetch_access_tokens(
        self,
        service_type: str | None,
        container: str | None = None,
        type_values: Sequence[str] | None = None,
        include_null_type: bool = False,
    ) -> dict[str | None, str | None]:
        """
        Fetch Accesses rows as a login→token map.

        - `service_type` may be None to fetch all types.
        - `type_values` narrows results to specific subtypes (stored as "<service>:<subtype>").
        - `include_null_type` includes rows where `type` is NULL.
        """
        await self._prepare_accesses()
        clauses: list[str] = []

        type_predicates: list[str] = []
        if type_values:
            for subtype in type_values:
                predicate = _build_type_predicate(service_type, subtype, include_null=include_null_type)
                if predicate:
                    type_predicates.append(predicate)
        else:
            predicate = _build_type_predicate(service_type, None, include_null=include_null_type)
            if predicate:
                type_predicates.append(predicate)

        if type_predicates:
            clauses.append(f"({' OR '.join(type_predicates)})")

        if container is not None:
            clauses.append(f"container = {_quote_nullable(container)}")

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        result = await asyncio.to_thread(
            self.client.query,
            f"SELECT login, token FROM {self.database}.Accesses{where}",
        )
        return {row[0]: row[1] for row in result.result_rows}

    async def fetch_access_rows(
        self,
        service_type: str | None = None,
        container: str | None = None,
        type_values: Sequence[str] | None = None,
        include_null_type: bool = False,
    ) -> list[dict[str, Any]]:
        """Return Accesses rows with parsed service/subtype metadata."""
        await self._prepare_accesses()
        clauses: list[str] = []
        predicates: list[str] = []
        if type_values:
            for subtype in type_values:
                predicate = _build_type_predicate(service_type, subtype, include_null=include_null_type)
                if predicate:
                    predicates.append(predicate)
        else:
            predicate = _build_type_predicate(service_type, None, include_null=include_null_type)
            if predicate:
                predicates.append(predicate)
        if predicates:
            clauses.append(f"({' OR '.join(predicates)})")
        if container is not None:
            clauses.append(f"container = {_quote_nullable(container)}")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        result = await asyncio.to_thread(
            self.client.query,
            f"SELECT login, token, container, type FROM {self.database}.Accesses{where}",
        )
        rows: list[dict[str, Any]] = []
        for login, token, cont, type_value in result.result_rows:
            service, subtype = _split_type(type_value)
            rows.append(
                {
                    "login": login,
                    "token": token,
                    "container": cont,
                    "type": type_value,
                    "service": service,
                    "subtype": subtype,
                }
            )
        return rows

    async def delete_access(
        self,
        login: str,
        service_type: str | None,
        container: str | None = None,
        type_value: str | None = None,
    ) -> None:
        await self._prepare_accesses()
        clauses = [f"login = {_quote_nullable(login)}"]
        type_clause = _build_type_predicate(service_type, type_value)
        if type_clause:
            clauses.append(type_clause)
        if container is not None:
            clauses.append(f"container = {_quote_nullable(container)}")
        where = " AND ".join(clauses) if clauses else "1"
        await asyncio.to_thread(
            self.client.command,
            f"ALTER TABLE {self.database}.Accesses DELETE WHERE {where}",
            settings={"mutations_sync": 1},
        )

    async def aggregate_daily(
        self,
        table_name: str,
        start_date: str | datetime | date,
        end_date: str | datetime | date,
        metric_allowlist: Sequence[str] | None = None,
        conversion_prefixes: Sequence[str] | None = None,
        date_column_override: str | None = None,
    ) -> pd.DataFrame:
        if not await self.table_exists(table_name):
            return pd.DataFrame()

        cols = await self.get_columns(table_name)
        date_col = (
            date_column_override
            if date_column_override in cols
            else detect_date_column(cols.keys())
        )
        if not date_col:
            return pd.DataFrame()

        conversions = conversion_prefixes or []
        metrics: list[str] = []
        for col in cols.keys():
            lower = col.lower()
            if metric_allowlist and lower in metric_allowlist:
                metrics.append(col)
            elif any(
                lower.startswith(prefix.lower()) for prefix in conversions
            ):
                metrics.append(col)

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        try:
            debug_logger = logging.getLogger("metrika.change_tracker")
            debug_logger.debug(
                "aggregate_daily: table=%s date_col=%s metrics=%s window=%s..%s",
                table_name,
                date_col,
                metrics,
                start_dt.date(),
                end_dt.date(),
            )
        except Exception:
            pass

        selects = [f"toDate(`{date_col}`) AS Date"]
        for col in metrics:
            col_type = cols.get(col, "")
            if _is_numeric_type(col_type):
                agg_expr = f"sum(`{col}`)"
            else:
                agg_expr = f"sum(toInt64OrZero(`{col}`))"
            selects.append(f"{agg_expr} AS `{col}`")

        query = (
            f"SELECT {', '.join(selects)} "
            f"FROM {self.database}.{table_name} "
            f"WHERE toDate(`{date_col}`) >= "
            f"toDate('{start_dt:%Y-%m-%d}') "
            f"AND toDate(`{date_col}`) <= "
            f"toDate('{end_dt:%Y-%m-%d}') "
            f"GROUP BY Date ORDER BY Date"
        )
        result = await asyncio.to_thread(self.client.query, query)
        if not result.result_rows:
            return pd.DataFrame()
        default_cols = (
            ["Date", *metrics] if selects else None
        )
        df = pd.DataFrame(
            result.result_rows,
            columns=_get_column_names(result, default=default_cols),
        )
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        try:
            debug_logger.debug(
                "aggregate_daily result: rows=%s cols=%s sample=%s",
                len(df),
                list(df.columns),
                df.head(3).to_dict("records"),
            )
        except Exception:
            pass
        return df


class _AsyncEngine:
    async def dispose(self) -> None:
        return None


__all__ = ["ClickhouseDatabase", "_AsyncEngine"]