import re
from typing import Sequence

import pandas as pd


def detect_date_column(columns) -> str | None:
    """Detect a date/datetime column by common naming patterns."""
    for candidate in ("Date", "date", "dateTimeUTC", "dateTime"):
        if candidate in columns:
            return candidate
    for col in columns:
        if "date" in col.lower():
            return col
    return None


def detect_visits_column(columns) -> str | None:
    """Detect visits/sessions column by common naming patterns."""
    for candidate in ("visits", "Visits"):
        if candidate in columns:
            return candidate
    return None


def _as_nullable(value):
    """Convert pandas/None placeholders to real None for Nullable columns."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return value


_LOGIN_ALLOWED_RE = re.compile(r"^[A-Za-z0-9-]+$")


def sanitize_login(value: str | None) -> str:
    """
    Normalize and validate client logins:
    - replace underscores and dots with hyphens;
    - disallow special characters;
    - reject logins consisting only of digits.
    """
    if value is None:
        raise ValueError("login_required")
    candidate = str(value).strip()
    if not candidate:
        raise ValueError("login_required")
    candidate = candidate.replace("_", "-").replace(".", "-")
    if candidate.isdigit():
        raise ValueError("login_digits_only")
    if not _LOGIN_ALLOWED_RE.fullmatch(candidate):
        raise ValueError("login_invalid_chars")
    return candidate


def _compose_type(service_type: str | None, type_value: str | None) -> str | None:
    """Combine connector name and optional subtype into a single stored type value."""
    base = (service_type or "").strip()
    suffix = (type_value or "").strip()
    if base and suffix:
        return f"{base}:{suffix}"
    return base or suffix or None


def _split_type(raw: str | None) -> tuple[str | None, str | None]:
    """Split a stored type into (service, subtype)."""
    if raw is None:
        return None, None
    parts = str(raw).split(":", 1)
    if len(parts) == 2:
        return parts[0] or None, parts[1] or None
    return parts[0] or None, None


def _quote_nullable(value: str | None) -> str:
    """Quote a value for SQL, converting None to NULL."""
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_type_predicate(
    service_type: str | None,
    type_value: str | None,
    *,
    include_null: bool = False,
) -> str | None:
    """
    Build a WHERE clause fragment for the Accesses.type column that supports
    storing subtype in the form "<service>:<subtype>".
    """
    if service_type and (type_value is None or str(type_value).strip() == ""):
        predicate = f"(type = {_quote_nullable(service_type)} OR type LIKE '{service_type}:%')"
        if include_null:
            predicate = f"{predicate} OR type IS NULL"
        return f"({predicate})"

    composed = _compose_type(service_type, type_value)
    if composed:
        return f"type = {_quote_nullable(composed)}"
    if service_type:
        predicate = f"(type LIKE '{service_type}:%' OR type = {_quote_nullable(service_type)})"
        if include_null:
            predicate = f"({predicate} OR type IS NULL)"
        return predicate
    if include_null:
        return "type IS NULL"
    return None


def _map_type(name: str, series: pd.Series) -> str:
    """Map pandas Series dtype to ClickHouse column type."""
    nullable = False
    try:
        nullable = series.isnull().any()
    except Exception:
        nullable = False

    name_lower = name.lower()
    if name_lower == "date":
        base_type = "Date"
    elif pd.api.types.is_bool_dtype(series):
        base_type = "UInt8"
    elif pd.api.types.is_integer_dtype(series):
        base_type = "Int64"
    elif pd.api.types.is_float_dtype(series):
        base_type = "Float64"
    elif pd.api.types.is_datetime64_any_dtype(series):
        base_type = "DateTime"
    else:
        base_type = "String"

    if nullable and not base_type.startswith("Nullable("):
        return f"Nullable({base_type})"
    return base_type


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame types for ClickHouse compatibility."""
    out = df.copy()
    for col in out.columns:
        series = out[col]
        col_lower = col.lower()
        if pd.api.types.is_period_dtype(series):
            out[col] = series.dt.to_timestamp()
            series = out[col]
        if col_lower == "date":
            out[col] = pd.to_datetime(series, errors="coerce").dt.date
            continue
        if pd.api.types.is_datetime64_any_dtype(series):
            out[col] = (
                pd.to_datetime(series, errors="coerce").dt.tz_localize(None)
            )
            continue
        if pd.api.types.is_bool_dtype(series):
            out[col] = series.astype("uint8")
            continue
        if pd.api.types.is_timedelta64_dtype(series):
            out[col] = series.dt.total_seconds()
            continue
    return out


def _is_numeric_type(clickhouse_type: str) -> bool:
    """Return True if the ClickHouse type is numeric (int/uint/float/decimal)."""
    normalized = (clickhouse_type or "").strip().lower()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized[len("nullable("):-1].strip()
    return normalized.startswith(("int", "uint", "float", "decimal"))


def _get_column_names(result, default: Sequence[str] | None = None) -> list[str]:
    """Extract column names from query result with fallback options."""
    names = getattr(result, "column_names", None)
    if names:
        return list(names)
    if default:
        return list(default)
    if result.result_rows:
        return [f"col{i}" for i in range(len(result.result_rows[0]))]
    return []


__all__ = [
    "detect_date_column",
    "detect_visits_column",
    "sanitize_login",
    "_compose_type",
    "_split_type",
    "_quote_nullable",
    "_build_type_predicate",
    "_normalize_frame",
    "_map_type",
    "_is_numeric_type",
    "_get_column_names",
    "_as_nullable",
]