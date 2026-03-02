from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase

from .config import (
    DEFAULT_DB_PROFILE,
    DIRECT_TYPE_AGENCY_PARSED,
    DIRECT_TYPE_AGENCY_TOKEN,
    DIRECT_TYPE_NOT_AGENCY,
)
from .logging_utils import get_logger
from .uploader import YaStatUploader


async def refresh_agency_clients(access_db: AsyncDirectDatabase, *, profile: str = DEFAULT_DB_PROFILE):
    """
    Ensure Accesses contains up-to-date client logins for every agency token.
    """
    agency_rows = await access_db.fetch_direct_access_rows(
        type_values=[DIRECT_TYPE_AGENCY_TOKEN],
        include_null_type=True,
    )

    for row in agency_rows:
        token = row.get("token")
        if not token:
            get_logger().warning("Skip agency token with empty value (container=%s)", row.get("container"))
            continue
        container = row.get("container") or row.get("login") or "agency"
        uploader = YaStatUploader("", token, "", "", profile=profile)
        try:
            client_list = await uploader.get_client_list()
        except Exception as exc:
            get_logger().error(f"{container}: failed to fetch agency clients — {exc}")
            continue
        normalized = [
            norm for norm in (AsyncDirectDatabase._normalize_login(login) for login in client_list)
            if norm
        ]
        await access_db.upsert_access_records(
            normalized,
            token,
            container=container,
            type_value=DIRECT_TYPE_AGENCY_PARSED,
            replace=True,
        )


async def collect_direct_login_tokens(access_db: AsyncDirectDatabase, *, profile: str = DEFAULT_DB_PROFILE) -> dict[str, str]:
    """Return login→token mapping for Direct, refreshing agency tokens if present."""
    await refresh_agency_clients(access_db, profile=profile)
    rows = await access_db.fetch_direct_access_rows(include_null_type=True)
    login_tokens: dict[str, str] = {}
    priorities: dict[str, int] = {}
    subtype_priority = {
        DIRECT_TYPE_NOT_AGENCY: 2,
        DIRECT_TYPE_AGENCY_PARSED: 1,
        None: 0,
    }
    for row in rows:
        login = AsyncDirectDatabase._normalize_login(row.get("login"))
        token = row.get("token")
        service = row.get("service")
        subtype = row.get("subtype")
        if not login or not token:
            continue
        if service and service != "direct":
            continue
        if subtype not in (DIRECT_TYPE_AGENCY_PARSED, DIRECT_TYPE_NOT_AGENCY, None):
            continue

        new_prio = subtype_priority.get(subtype, 0)
        old_prio = priorities.get(login, -1)
        if new_prio >= old_prio:
            priorities[login] = new_prio
            login_tokens[login] = token
    return login_tokens


__all__ = ["refresh_agency_clients", "collect_direct_login_tokens"]
