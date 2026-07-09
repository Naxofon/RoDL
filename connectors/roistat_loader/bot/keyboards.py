from aiogram import types


def get_kb_roistat():
    buttons = [
        [
            types.InlineKeyboardButton(text="📋 Клиенты Roistat", callback_data="command_roistat_clients"),
        ],
        [
            types.InlineKeyboardButton(text="➕ Добавить клиента", callback_data="command_add_roistat_client"),
        ],
        [
            types.InlineKeyboardButton(text="🗑️ Удалить клиента", callback_data="command_remove_roistat_client"),
        ],
        [
            types.InlineKeyboardButton(text="⚙️ Настроить таблицы", callback_data="command_configure_roistat_tables"),
        ],
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_roistat_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_kb_roistat_upload_only():
    buttons = [
        [
            types.InlineKeyboardButton(text="💾 Выгрузка", callback_data="command_upload_roistat_data"),
        ],
        [
            types.InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu"),
        ],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def _flag_label(name: str, enabled: bool) -> str:
    icon = "✅" if enabled else "❌"
    title = {
        "analytics": "Analytics",
        "calls": "Calls",
        "visits": "Visits",
    }[name]
    return f"{icon} {title}"


def get_kb_roistat_flags(flags: dict[str, bool]):
    buttons = [
        [types.InlineKeyboardButton(text=_flag_label("analytics", flags["analytics"]), callback_data="roistat_toggle:analytics")],
        [types.InlineKeyboardButton(text=_flag_label("calls", flags["calls"]), callback_data="roistat_toggle:calls")],
        [types.InlineKeyboardButton(text=_flag_label("visits", flags["visits"]), callback_data="roistat_toggle:visits")],
        [types.InlineKeyboardButton(text="💾 Сохранить", callback_data="roistat_save_flags")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_input")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)
