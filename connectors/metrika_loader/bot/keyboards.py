from aiogram import types


def get_kb_metrika():
    """Metrika management keyboard."""
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Метрики", callback_data="command_ym_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Клиентский", callback_data="command_add_ym_client"),
            types.InlineKeyboardButton(text="➕ Агентский", callback_data="command_add_ym_agent"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_ym_clients"),
            types.InlineKeyboardButton(text="🗑️ Удалить агентство", callback_data="command_remove_ym_agency"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_ym_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_metrika_upload_only():
    """Metrika keyboard for Alpha role (upload only)."""
    buttons = [
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_ym_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
