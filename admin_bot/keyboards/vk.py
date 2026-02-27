from aiogram import types


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
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
