import sys
import logging
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if PROJECT_ROOT.as_posix() not in sys.path:
    sys.path.insert(0, PROJECT_ROOT.as_posix())

from orchestration.loader_registry import is_loader_enabled

logger = logging.getLogger(__name__)


from .callbacks import router_main
from .commands import router_command
from .metrika import router_metrika
from .direct import router_direct
from .calltouch import router_calltouch
from .vk import router_vk
from .admin_panel import router_admin_panel


__all__ = [
    "router_main",
    "router_command",
    "router_metrika",
    "router_direct",
    "router_calltouch",
    "router_vk",
    "router_admin_panel",
]


if is_loader_enabled("wordstat_loader"):
    try:
        from .wordstat import router_wordstat
        __all__.append("router_wordstat")
    except ImportError as e:
        logger.warning(f"wordstat_loader handler enabled but import failed: {e}")

if is_loader_enabled("custom_loader"):
    try:
        from .custom_loader import router_custom_loader
        __all__.append("router_custom_loader")
    except ImportError as e:
        logger.warning(f"custom_loader handler enabled but import failed: {e}")
