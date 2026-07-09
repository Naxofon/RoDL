from __future__ import annotations

from typing import Any

from .flags import parse_roistat_flags


def roistat_client_from_access_row(row: dict[str, Any]) -> dict[str, Any] | None:
    site_id_raw = row.get("login")
    token = row.get("token")
    if not site_id_raw or not token:
        return None

    try:
        site_id = int(str(site_id_raw).strip())
    except (TypeError, ValueError):
        return None

    return {
        "site_id": site_id,
        "token": token,
        "account": (row.get("container") or "").strip(),
        "flags": parse_roistat_flags(row.get("subtype")),
        "type": row.get("type"),
    }


def collect_roistat_clients(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clients: list[dict[str, Any]] = []
    for row in rows:
        client = roistat_client_from_access_row(row)
        if client is not None:
            clients.append(client)
    return clients


__all__ = ["collect_roistat_clients", "roistat_client_from_access_row"]
