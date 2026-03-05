import os
from datetime import date, datetime
from typing import Sequence

import pandas as pd

from .config import CLICKHOUSE_DATABASE
from .connection import ensure_database
from .helpers import _map_type, _normalize_frame

_DEFAULT_INSERT_BATCH_SIZE = 100_000
_DEFAULT_INDEX_GRANULARITY = 4_096
_DEFAULT_MUTATIONS_SYNC = 1


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


_INSERT_BATCH_SIZE = _read_int_env(
    "CLICKHOUSE_INSERT_BATCH_SIZE",
    _DEFAULT_INSERT_BATCH_SIZE,
)
_INDEX_GRANULARITY = _read_int_env(
    "CLICKHOUSE_INDEX_GRANULARITY",
    _DEFAULT_INDEX_GRANULARITY,
    minimum=256,
)
_MUTATIONS_SYNC = _read_int_env(
    "CLICKHOUSE_MUTATIONS_SYNC",
    _DEFAULT_MUTATIONS_SYNC,
    minimum=0,
)
_ENABLE_WORDSTAT_INDEXES = _read_bool_env(
    "CLICKHOUSE_ENABLE_WORDSTAT_INDEXES",
    True,
)
_WORDSTAT_BLOOM_FALSE_POSITIVE_RATE = os.getenv(
    "CLICKHOUSE_WORDSTAT_BLOOM_FPR",
    "0.01",
).strip() or "0.01"


def _normalized_type(clickhouse_type: str) -> str:
    normalized = (clickhouse_type or "").strip().lower()
    while normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized[len("nullable("):-1].strip()
    while normalized.startswith("lowcardinality(") and normalized.endswith(")"):
        normalized = normalized[len("lowcardinality("):-1].strip()
    return normalized


def _is_date_like(clickhouse_type: str) -> bool:
    normalized = _normalized_type(clickhouse_type)
    return normalized.startswith("date") or normalized.startswith("datetime")


def _is_string_like(clickhouse_type: str) -> bool:
    normalized = _normalized_type(clickhouse_type)
    return normalized.startswith("string") or normalized.startswith("fixedstring")


def _pick_partition_column(
    columns: Sequence[str],
    column_types: dict[str, str],
) -> str | None:
    candidates = ("Date", "date", "dateTimeUTC", "dateTime")
    for candidate in candidates:
        if candidate in column_types and _is_date_like(column_types[candidate]):
            return candidate
    for col in columns:
        if "date" not in col.lower():
            continue
        if _is_date_like(column_types.get(col, "")):
            return col
    return None


def _partition_expression(column: str, clickhouse_type: str) -> str:
    normalized = (clickhouse_type or "").strip().lower()
    if normalized.startswith("nullable("):
        return (
            f"toYYYYMM(coalesce(toDate(`{column}`), toDate('1970-01-01')))"
        )
    return f"toYYYYMM(toDate(`{column}`))"


def _existing_skip_indexes(client, table: str) -> set[str]:
    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    try:
        result = client.query(
            "SELECT name "
            "FROM system.data_skipping_indices "
            "WHERE database = %(db)s AND table = %(table)s",
            parameters={
                "db": db,
                "table": table,
            },
        )
    except Exception:
        return set()
    return {str(row[0]) for row in (result.result_rows or []) if row}


def _ensure_wordstat_indexes(
    client,
    table: str,
    columns: dict[str, str],
    existing_indexes: set[str] | None = None,
) -> None:
    if not _ENABLE_WORDSTAT_INDEXES:
        return
    brand_type = columns.get("brand")
    phrase_type = columns.get("requestPhrase")
    if not brand_type or not phrase_type:
        return
    if not _is_string_like(brand_type) or not _is_string_like(phrase_type):
        return

    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    index_defs = (
        (
            "idx_brand_bf",
            "ifNull(`brand`, '')",
        ),
        (
            "idx_request_phrase_bf",
            "ifNull(`requestPhrase`, '')",
        ),
    )
    known_indexes = set(existing_indexes or _existing_skip_indexes(client, table))
    for index_name, expression in index_defs:
        if index_name in known_indexes:
            continue
        try:
            client.command(
                f"ALTER TABLE {db}.{table} "
                f"ADD INDEX {index_name} {expression} "
                f"TYPE bloom_filter({_WORDSTAT_BLOOM_FALSE_POSITIVE_RATE}) "
                "GRANULARITY 1"
            )
            known_indexes.add(index_name)
        except Exception:
            continue


def describe_columns(client, table: str) -> dict[str, str]:
    """Get column names and types for a table."""
    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    result = client.query(f"DESCRIBE TABLE {db}.{table}")
    return {row[0]: row[1] for row in result.result_rows}


def ensure_table(
    client,
    table: str,
    df: pd.DataFrame,
    order: Sequence[str] | None = None,
) -> None:
    """Create a table if it doesn't exist and align schema to the DataFrame."""
    ensure_database(client)
    normalized = _normalize_frame(df)
    col_types = {
        col: _map_type(col, normalized[col]) for col in normalized.columns
    }
    order_by = [
        col for col in (order or ("Date",)) if col in normalized.columns
    ]
    if not order_by and len(normalized.columns) > 0:
        order_by = [normalized.columns[0]]
    cols_clause = ", ".join(
        f"`{col}` {col_types[col]}"
        for col in normalized.columns
    )
    order_clause = (
        ", ".join(f"`{c}`" for c in order_by) if order_by else "tuple()"
    )
    partition_col = _pick_partition_column(normalized.columns, col_types)
    partition_clause = (
        f" PARTITION BY {_partition_expression(partition_col, col_types[partition_col])}"
        if partition_col
        else ""
    )
    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    client.command(
        f"CREATE TABLE IF NOT EXISTS {db}.{table} "
        f"({cols_clause}) ENGINE = MergeTree"
        f"{partition_clause} "
        f"ORDER BY ({order_clause}) "
        f"SETTINGS index_granularity = {_INDEX_GRANULARITY}"
    )
    existing = add_missing_columns(client, table, normalized)
    existing = upgrade_column_types(
        client,
        table,
        normalized,
        existing_columns=existing,
    )
    _ensure_wordstat_indexes(client, table, existing)


def add_missing_columns(
    client,
    table: str,
    df: pd.DataFrame,
    *,
    existing_columns: dict[str, str] | None = None,
) -> dict[str, str]:
    """Add DataFrame columns missing in table schema and return fresh schema."""
    existing = dict(existing_columns or describe_columns(client, table))
    normalized = _normalize_frame(df)
    changed = False
    for col in normalized.columns:
        if col in existing:
            continue
        col_type = _map_type(col, normalized[col])
        db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
        client.command(
            f"ALTER TABLE {db}.{table} "
            f"ADD COLUMN IF NOT EXISTS `{col}` {col_type}"
        )
        existing[col] = col_type
        changed = True
    if changed:
        return describe_columns(client, table)
    return existing


def upgrade_column_types(
    client,
    table: str,
    df: pd.DataFrame,
    *,
    existing_columns: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Upgrade existing columns from String → concrete types when the incoming
    DataFrame column has a more specific dtype.
    """
    existing = dict(existing_columns or describe_columns(client, table))
    normalized = _normalize_frame(df)
    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    changed = False

    for col in normalized.columns:
        if col not in existing:
            continue
        desired = _map_type(col, normalized[col])
        current = existing[col]
        if desired == current:
            continue

        current_norm = current.strip().lower()
        desired_norm = desired.strip().lower()

        if desired_norm.startswith("string"):
            continue

        is_string = (
            current_norm == "string" or current_norm == "nullable(string)"
        )
        if not is_string:
            continue

        try:
            client.command(
                f"ALTER TABLE {db}.{table} MODIFY COLUMN `{col}` {desired}"
            )
            changed = True
        except Exception:
            continue
    if changed:
        return describe_columns(client, table)
    return existing


def insert_dataframe(
    client,
    table: str,
    df: pd.DataFrame,
    order: Sequence[str] | None = None,
    *,
    ensure_schema: bool = True,
    batch_size: int | None = None,
) -> None:
    """Insert DataFrame into table with optional schema sync and chunked writes."""
    if df.empty:
        return
    normalized = _normalize_frame(df)
    if ensure_schema:
        ensure_table(client, table, normalized, order=order)
    prepared = normalized.where(pd.notna(normalized), None)
    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    chunk_size = batch_size or _INSERT_BATCH_SIZE
    if chunk_size <= 0 or len(prepared) <= chunk_size:
        client.insert_df(f"{db}.{table}", prepared)
        return
    for start in range(0, len(prepared), chunk_size):
        chunk = prepared.iloc[start:start + chunk_size]
        client.insert_df(f"{db}.{table}", chunk)


def delete_by_date_range(
    client,
    table: str,
    date_column: str,
    start: datetime | date,
    end: datetime | date,
) -> None:
    """Delete rows within a date range using configurable mutation sync."""
    ensure_database(client)
    start_dt = pd.to_datetime(start).normalize()
    end_dt_exclusive = (
        pd.to_datetime(end).normalize() + pd.Timedelta(days=1)
    )
    table_columns = describe_columns(client, table)
    date_type = table_columns.get(date_column, "")

    normalized_date_type = _normalized_type(date_type)
    is_plain_date = (
        normalized_date_type.startswith("date")
        and not normalized_date_type.startswith("datetime")
    )
    if is_plain_date:
        start_lit = f"toDate('{start_dt:%Y-%m-%d}')"
        end_lit = f"toDate('{end_dt_exclusive:%Y-%m-%d}')"
    else:
        start_lit = f"toDateTime('{start_dt:%Y-%m-%d %H:%M:%S}')"
        end_lit = f"toDateTime('{end_dt_exclusive:%Y-%m-%d %H:%M:%S}')"

    db = getattr(client, "database", None) or CLICKHOUSE_DATABASE
    client.command(
        f"ALTER TABLE {db}.{table} "
        f"DELETE WHERE `{date_column}` >= {start_lit} "
        f"AND `{date_column}` < {end_lit}",
        settings={"mutations_sync": _MUTATIONS_SYNC},
    )


__all__ = [
    "describe_columns",
    "ensure_table",
    "add_missing_columns",
    "upgrade_column_types",
    "insert_dataframe",
    "delete_by_date_range",
]