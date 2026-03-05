from aiogram import types
from services.connector_plugins import get_enabled_connector_admin_bot_plugins


def get_kb_admin():
    """Admin panel keyboard."""
    buttons = [
        [types.InlineKeyboardButton(text="👤 Добавить администратора", callback_data="add_admin")],
        [types.InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_alpha")],
        [types.InlineKeyboardButton(text="➖ Снять права", callback_data="remove_admin")],
    ]

    for plugin in get_enabled_connector_admin_bot_plugins():
        buttons.append(
            [types.InlineKeyboardButton(text=plugin.button_text, callback_data=plugin.button_callback)]
        )

    buttons.extend([
        [types.InlineKeyboardButton(text="📦 Бэкап доступов", callback_data="admin_access_backup_menu")],
        [types.InlineKeyboardButton(text="🧹 Очистка старых клиентов", callback_data="admin_cleanup_stale")],
        [types.InlineKeyboardButton(text="🗑️ Полная очистка БД", callback_data="admin_reset_database")],
        [types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")],
    ])
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


def get_kb_reset_confirm(service: str):
    """Get confirmation keyboard for database reset."""
    buttons = [
        [
            types.InlineKeyboardButton(text="✅ Да, удалить всё", callback_data=f"confirm_reset_{service}"),
            types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_reset"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
