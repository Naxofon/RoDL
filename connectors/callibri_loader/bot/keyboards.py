from aiogram import types


def get_kb_callibri():
    """callibri management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Сallibri", callback_data="command_callibri_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Добавить клиента", callback_data="command_add_callibri_client"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_callibri_clients"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_callibri_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_callibri_upload_only():
    """callibri keyboard for Alpha role (upload only)."""
    buttons = [
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_callibri_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
