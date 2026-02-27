import asyncio
from datetime import datetime, timedelta

import aiohttp

from prefect_loader.orchestration.clickhouse_utils import (
    CLICKHOUSE_DB_DIRECT_ANALYTICS,
    CLICKHOUSE_DB_DIRECT_LIGHT,
)
from .report_limits import GlobalRateLimiter, OfflineReportTracker, PerAdvertiserRateLimiter

SKIP_COST_CHECK_CLIENTS = {
    "lovol_russia",
    "changan_asmoto_ads",
}

ANALYTICS_FIELD_NAMES = [
    "Date",
    "Criterion",
    "AdFormat",
    "AdId",
    "CampaignType",
    "CampaignName",
    "AdGroupName",
    "Clicks",
    "Ctr",
    "Cost",
    "Impressions",
    "Conversions",
    "AvgImpressionPosition",
    "AvgPageviews",
    "AvgClickPosition",
    "Bounces",
    "Age",
    "Gender",
    "Device",
    "LocationOfPresenceName",
    "Sessions",
    "Slot",
    "Placement",
    "AdNetworkType",
    "AvgTrafficVolume",
]

ANALYTICS_COLUMN_TYPES = {
    "Criterion": str,
    "AdFormat": str,
    "AdId": int,
    "CampaignType": str,
    "CampaignName": str,
    "AdGroupName": str,
    "Clicks": int,
    "Ctr": float,
    "Cost": float,
    "Impressions": int,
    "Conversions": int,
    "AvgImpressionPosition": float,
    "AvgPageviews": float,
    "AvgClickPosition": float,
    "Bounces": int,
    "Age": str,
    "Gender": str,
    "Device": str,
    "LocationOfPresenceName": str,
    "Sessions": int,
    "Slot": str,
    "Placement": str,
    "AdNetworkType": str,
    "AvgTrafficVolume": float,
}

LIGHT_FIELDS = [
    "Date",
    "CampaignType",
    "CampaignName",
    "AdGroupName",
    "Clicks",
    "Conversions",
    "Cost",
    "Impressions",
    "Bounces",
    "Sessions",
    "Placement",
    "AdNetworkType",
]

LIGHT_COLUMN_TYPES = {
    "CampaignType": str,
    "CampaignName": str,
    "AdGroupName": str,
    "Clicks": int,
    "Conversions": int,
    "Cost": float,
    "Impressions": int,
    "AvgImpressionPosition": float,
    "Bounces": int,
    "Sessions": int,
    "Placement": str,
    "AdNetworkType": str,
    "AvgTrafficVolume": float,
}

DIRECT_TYPE_AGENCY_TOKEN = "agency_token"
DIRECT_TYPE_AGENCY_PARSED = "agency_parsed"
DIRECT_TYPE_NOT_AGENCY = "not_agency_token"

DIRECT_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_read=1200)
DIRECT_MAX_RETRIES = 5
DIRECT_BACKOFF_BASE = 2.0

_REPORT_QUEUE_SEM: asyncio.Semaphore | None = None
_REPORT_QUEUE_SEM_LOOP: asyncio.AbstractEventLoop | None = None


def get_report_queue_semaphore() -> asyncio.Semaphore:
    """Lazily create report queue semaphore in the current event loop, recreating if loop changed."""
    global _REPORT_QUEUE_SEM, _REPORT_QUEUE_SEM_LOOP

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _REPORT_QUEUE_SEM is None or _REPORT_QUEUE_SEM_LOOP is not current_loop:
        _REPORT_QUEUE_SEM = asyncio.Semaphore(5) if current_loop else None
        _REPORT_QUEUE_SEM_LOOP = current_loop

    return _REPORT_QUEUE_SEM

global_rate_limiter = GlobalRateLimiter(max_requests=19, window_seconds=10.0)
report_rate_limiter = PerAdvertiserRateLimiter(max_requests=20, window_seconds=10.0)
offline_report_tracker = OfflineReportTracker(max_inflight=5)

DATABASE_PROFILES = {
    "analytics": {
        "dsn": None,
        "database": CLICKHOUSE_DB_DIRECT_ANALYTICS,
        "columns": None,
        "compare_conversions": True,
        "field_names": ANALYTICS_FIELD_NAMES,
        "column_types": ANALYTICS_COLUMN_TYPES,
        "cost_tolerance": 500,
        "skip_cost_check_clients": set(SKIP_COST_CHECK_CLIENTS),
    },
    "light": {
        "dsn": None,
        "database": CLICKHOUSE_DB_DIRECT_LIGHT,
        "columns": LIGHT_FIELDS,
        "compare_conversions": True,
        "field_names": LIGHT_FIELDS,
        "column_types": LIGHT_COLUMN_TYPES,
        "cost_tolerance": 2,
        "skip_cost_check_clients": set(),
    },
}

DEFAULT_DB_PROFILE = "analytics"
VALID_DB_PROFILE_CHOICES = tuple(list(DATABASE_PROFILES.keys()) + ["both"])


def resolve_profiles(option: str) -> list[str]:
    """Expand profile selector into explicit profile names."""
    if option == "both":
        return list(DATABASE_PROFILES.keys())
    return [option]


def build_forced_refresh_window(days: int = 60) -> list[str]:
    """Generate a list of date strings covering the last *days* (inclusive)."""
    today = datetime.now().date()
    start = today - timedelta(days=days)
    return [
        (start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days + 1)
    ]


__all__ = [
    "ANALYTICS_FIELD_NAMES",
    "ANALYTICS_COLUMN_TYPES",
    "LIGHT_FIELDS",
    "LIGHT_COLUMN_TYPES",
    "DATABASE_PROFILES",
    "DEFAULT_DB_PROFILE",
    "VALID_DB_PROFILE_CHOICES",
    "DIRECT_TYPE_AGENCY_TOKEN",
    "DIRECT_TYPE_AGENCY_PARSED",
    "DIRECT_TYPE_NOT_AGENCY",
    "DIRECT_TIMEOUT",
    "DIRECT_MAX_RETRIES",
    "DIRECT_BACKOFF_BASE",
    "get_report_queue_semaphore",
    "global_rate_limiter",
    "report_rate_limiter",
    "offline_report_tracker",
    "resolve_profiles",
    "build_forced_refresh_window",
    "SKIP_COST_CHECK_CLIENTS",
]
