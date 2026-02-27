import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from .calltouch import calltouch_loader_flow, run_calltouch_all, run_calltouch_single
from .direct import (
    MAX_CONCURRENT_LOGINS,
    MAX_REPORTS_PER_LOGIN,
    detect_changes_for_client_task,
    direct_loader_flow,
    fetch_direct_range_task,
    get_direct_clients_task,
    run_direct_change_tracker,
    run_direct_change_tracker_single,
    run_direct_range,
    write_direct_range_task,
)
from .metrika import (
    MAX_PARALLEL_FETCH,
    fetch_metrika_range_task,
    metrika_loader_flow,
    plan_metrika_jobs_task,
    run_metrika_change_tracker,
    run_metrika_range,
    write_metrika_range_task,
)
from .vk import run_vk_all, vk_loader_flow

try:
    from .wordstat import wordstat_loader_flow
except ImportError:
    pass

try:
    from .custom_loader import custom_loader_flow
except ImportError:
    pass

__all__ = [
    "calltouch_loader_flow",
    "run_calltouch_all",
    "run_calltouch_single",
    "MAX_CONCURRENT_LOGINS",
    "MAX_REPORTS_PER_LOGIN",
    "detect_changes_for_client_task",
    "direct_loader_flow",
    "fetch_direct_range_task",
    "get_direct_clients_task",
    "run_direct_change_tracker",
    "run_direct_change_tracker_single",
    "run_direct_range",
    "write_direct_range_task",
    "MAX_PARALLEL_FETCH",
    "fetch_metrika_range_task",
    "metrika_loader_flow",
    "plan_metrika_jobs_task",
    "run_metrika_change_tracker",
    "run_metrika_range",
    "write_metrika_range_task",
    "run_vk_all",
    "vk_loader_flow",
]
