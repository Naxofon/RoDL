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

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_kb_metrika():
    """Metrika management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Метрики", callback_data="command_ym_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Агентский аккаунт", callback_data="command_add_ym_agent"),
            types.InlineKeyboardButton(text="➕ Клиентский аккаунт", callback_data="command_add_ym_client"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_ym_clients"),
            types.InlineKeyboardButton(text="🏢 Удалить агентство", callback_data="command_remove_ym_agency"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_ym_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_direct():
    """Direct management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Директа", callback_data="command_yd_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Агентский токен", callback_data="command_add_yd_agent"),
            types.InlineKeyboardButton(text="➕ Клиентский токен", callback_data="command_add_yd_client"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_yd_clients"),
            types.InlineKeyboardButton(text="🏢 Удалить агентство", callback_data="command_remove_yd_agency"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_yd_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


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
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


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
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_calltouch():
    """Calltouch management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Calltouch", callback_data="command_ct_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Добавить клиента", callback_data="command_add_ct_client"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_ct_clients"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_ct_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_vk():
    """VK Ads management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Агентства VK", callback_data="command_vk_agencies"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Добавить агентство", callback_data="command_add_vk_agency"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить агентство", callback_data="command_remove_vk_agency"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_vk_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_back_main_menu():
    """Single back-to-main-menu button."""
    buttons = [
        [types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_reset_confirm(service: str):
    """Get confirmation keyboard for database reset."""
    buttons = [
        [
            types.InlineKeyboardButton(text="✅ Да, удалить всё", callback_data=f"confirm_reset_{service}"),
            types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_reset"),
        ],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_kb_cancel():
    """Cancel button for input states."""
    buttons = [
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_input")],
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard