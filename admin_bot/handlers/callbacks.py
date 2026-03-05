"""Main menu callback handlers."""
import logging

from aiogram import Router
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext

from keyboards.inline import get_kb_main
from admin.keyboards import get_kb_admin
from services.connector_plugins import build_keyboard_for_plugin, get_enabled_connector_bot_plugins
from services.user_roles import get_user_role

router_main = Router()
logger = logging.getLogger(__name__)

_PLUGIN_BY_CALLBACK = {
    plugin.menu_callback: plugin
    for plugin in get_enabled_connector_bot_plugins()
}
_PLUGIN_CALLBACKS = set(_PLUGIN_BY_CALLBACK.keys())


@router_main.callback_query(lambda c: c.data == "command_admin_db_manager")
async def handle_admin_db_manager(callback_query: CallbackQuery):
    """Show admin panel keyboard."""
    role = await get_user_role(callback_query.from_user.id)
    if role != "Admin":
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_admin())
    await callback_query.answer()


@router_main.callback_query(lambda c: c.data == "command_back_main_menu")
async def handle_back_main_menu(callback_query: CallbackQuery):
    """Return to main menu keyboard."""
    role = await get_user_role(callback_query.from_user.id)
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_main(user_role=role))
    await callback_query.answer()


@router_main.callback_query(lambda c: c.data == "cancel_input")
async def handle_cancel_input(callback_query: CallbackQuery, state: FSMContext):
    """Cancel any active input state and return to main menu."""
    await state.clear()
    role = await get_user_role(callback_query.from_user.id)
    await callback_query.message.answer(
        "❌ Операция отменена.",
        reply_markup=get_kb_main(user_role=role)
    )
    await callback_query.answer()


@router_main.callback_query(lambda c: (c.data or "") in _PLUGIN_CALLBACKS)
async def handle_connector_manager(callback_query: CallbackQuery):
    """Show connector-specific management keyboard from connector bot plugin."""
    callback_data = callback_query.data or ""
    plugin = _PLUGIN_BY_CALLBACK.get(callback_data)
    if plugin is None:
        await callback_query.answer()
        return

    role = await get_user_role(callback_query.from_user.id)
    alpha_mode = role == "Alpha"

    try:
        reply_markup = build_keyboard_for_plugin(plugin, alpha_upload_only=alpha_mode)
        if alpha_mode and not reply_markup.inline_keyboard:
            await callback_query.answer("Для роли Alpha в этом коннекторе нет доступной выгрузки.", show_alert=True)
            return
        await callback_query.message.edit_reply_markup(reply_markup=reply_markup)
    except Exception as exc:
        logger.warning("Failed to render bot keyboard for %s: %s", plugin.loader_name, exc)
        await callback_query.answer("Не удалось открыть меню коннектора", show_alert=True)
        return

    await callback_query.answer()
