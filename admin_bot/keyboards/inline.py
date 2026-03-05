import sys
from pathlib import Path
from aiogram import types

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if PROJECT_ROOT.as_posix() not in sys.path:
    sys.path.insert(0, PROJECT_ROOT.as_posix())

from services.connector_plugins import get_enabled_connector_bot_plugins
from services.connector_plugins import plugin_has_alpha_upload_action
from admin.keyboards import get_kb_admin, get_kb_admin_backup, get_kb_reset_confirm


def get_kb_main(user_role: str | None = None):
    """Build main menu keyboard dynamically based on enabled loaders."""
    buttons = []
    alpha_mode = (user_role or "").strip() == "Alpha"

    for plugin in get_enabled_connector_bot_plugins():
        if alpha_mode and not plugin_has_alpha_upload_action(plugin):
            continue
        buttons.append([types.InlineKeyboardButton(text=plugin.display_name, callback_data=plugin.menu_callback)])

    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_back_main_menu():
    """Single back-to-main-menu button."""
    buttons = [
        [types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_cancel():
    """Cancel button for input states."""
    buttons = [
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_input")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
