"""Core admin bot handlers.

Connector-specific handlers are loaded dynamically from connector plugins.
"""

from .callbacks import router_main
from .commands import router_command
from admin.handlers import router_admin_panel


__all__ = [
    "router_main",
    "router_command",
    "router_admin_panel",
]
