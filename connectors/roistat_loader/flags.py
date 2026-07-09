from __future__ import annotations

from typing import Mapping


DEFAULT_ROISTAT_FLAGS = {
    "analytics": True,
    "calls": True,
    "visits": False,
}
ROISTAT_FLAG_KEYS = tuple(DEFAULT_ROISTAT_FLAGS.keys())


def _to_bool(value) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def normalize_roistat_flags(flags: Mapping[str, object] | None) -> dict[str, bool]:
    source = flags or {}
    return {
        key: _to_bool(source.get(key, DEFAULT_ROISTAT_FLAGS[key]))
        for key in ROISTAT_FLAG_KEYS
    }


def parse_roistat_flags(payload: str | None) -> dict[str, bool]:
    flags = dict(DEFAULT_ROISTAT_FLAGS)
    if not payload:
        return flags

    try:
        parts = [part.strip() for part in str(payload).split(";") if part.strip()]
        parsed: dict[str, object] = {}
        for part in parts:
            if "=" not in part:
                continue
            key, raw_value = part.split("=", 1)
            key = key.strip().lower()
            if key not in DEFAULT_ROISTAT_FLAGS:
                continue
            parsed[key] = raw_value.strip()
        if not parsed:
            return flags
        return normalize_roistat_flags(parsed)
    except Exception:
        return flags


def serialize_roistat_payload(flags: Mapping[str, object] | None) -> str:
    normalized = normalize_roistat_flags(flags)
    return ";".join(
        f"{key}={1 if normalized[key] else 0}"
        for key in ROISTAT_FLAG_KEYS
    )


def serialize_roistat_type(flags: Mapping[str, object] | None) -> str:
    return f"roistat:{serialize_roistat_payload(flags)}"


def enabled_roistat_sections(flags: Mapping[str, object] | None) -> list[str]:
    normalized = normalize_roistat_flags(flags)
    return [key for key in ROISTAT_FLAG_KEYS if normalized[key]]


__all__ = [
    "DEFAULT_ROISTAT_FLAGS",
    "ROISTAT_FLAG_KEYS",
    "enabled_roistat_sections",
    "normalize_roistat_flags",
    "parse_roistat_flags",
    "serialize_roistat_payload",
    "serialize_roistat_type",
]
