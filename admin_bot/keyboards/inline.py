import sys
from pathlib import Path
from aiogram import types

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if PROJECT_ROOT.as_posix() not in sys.path:
    sys.path.insert(0, PROJECT_ROOT.as_posix())

from orchestration.loader_registry import is_loader_enabled, get_loader_config


def get_kb_main():
    """Build main menu keyboard dynamically based on enabled loaders."""
    buttons = []

    loader_buttons = [
        ("metrika_loader", "command_metrika_db_manager", "📊 Яндекс.Метрика"),
        ("direct_loader", "command_direct_db_manager", "💰 Яндекс.Директ"),
        ("calltouch_loader", "command_calltouch_db_manager", "📞 Calltouch"),
        ("vk_loader", "command_vk_db_manager", "🎯 VK Ads"),
    ]

    for loader_name, callback_data, default_text in loader_buttons:
        if is_loader_enabled(loader_name):
            config = get_loader_config(loader_name)
            display_name = config.get("display_name", default_text)
            buttons.append([types.InlineKeyboardButton(text=display_name, callback_data=callback_data)])

    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_admin():
    """Admin panel keyboard."""
    buttons = [
        [types.InlineKeyboardButton(text="👤 Добавить администратора", callback_data="add_admin")],
        [types.InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_alpha")],
        [types.InlineKeyboardButton(text="➖ Снять права", callback_data="remove_admin")],
        [types.InlineKeyboardButton(text="💾 Выгрузка Метрики", callback_data="admin_upload_all_metrika")],
        [types.InlineKeyboardButton(text="💾 Выгрузка Директа", callback_data="admin_upload_all_direct")],
        [types.InlineKeyboardButton(text="💾 Выгрузка Calltouch", callback_data="admin_upload_all_calltouch")],
        [types.InlineKeyboardButton(text="💾 Выгрузка VK Ads", callback_data="admin_upload_all_vk")],
        [types.InlineKeyboardButton(text="📦 Бэкап доступов", callback_data="admin_access_backup_menu")],
        [types.InlineKeyboardButton(text="🧹 Очистка старых клиентов", callback_data="admin_cleanup_stale")],
        [types.InlineKeyboardButton(text="🗑️ Полная очистка БД", callback_data="admin_reset_database")],
        [types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_admin_backup():
    buttons = [
        [
            types.InlineKeyboardButton(text="📤 Экспорт", callback_data="admin_access_backup_export"),
            types.InlineKeyboardButton(text="📥 Импорт", callback_data="admin_access_backup_import"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_access_backup_back"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_back_main_menu():
    """Single back-to-main-menu button."""
    buttons = [
        [types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_reset_confirm(service: str):
    """Get confirmation keyboard for database reset."""
    buttons = [
        [
            types.InlineKeyboardButton(text="✅ Да, удалить всё", callback_data=f"confirm_reset_{service}"),
            types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_reset"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_cancel():
    """Cancel button for input states."""
    buttons = [
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_input")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
