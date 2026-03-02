from aiogram import types


def get_kb_direct():
    """Direct management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Директа", callback_data="command_yd_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Агентский", callback_data="command_add_yd_agent"),
            types.InlineKeyboardButton(text="➕ Клиентский", callback_data="command_add_yd_client"),
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
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
