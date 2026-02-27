from aiogram import types


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
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
