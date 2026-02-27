"""Main menu callback handlers."""
from aiogram import Router
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext

from keyboards.inline import (
    get_kb_main,
    get_kb_metrika,
    get_kb_direct,
    get_kb_calltouch,
    get_kb_vk,
    get_kb_wordstat,
    get_kb_custom_loader,
    get_kb_admin,
)

router_main = Router()

@router_main.callback_query(lambda c: c.data == "command_metrika_db_manager")
async def handle_metrika_db_manager(callback_query: CallbackQuery):
    """Show Metrika management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_metrika())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_direct_db_manager")
async def handle_direct_db_manager(callback_query: CallbackQuery):
    """Show Direct management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_direct())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_calltouch_db_manager")
async def handle_calltouch_db_manager(callback_query: CallbackQuery):
    """Show Calltouch management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_calltouch())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_vk_db_manager")
async def handle_vk_db_manager(callback_query: CallbackQuery):
    """Show VK Ads management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_vk())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_wordstat_db_manager")
async def handle_wordstat_db_manager(callback_query: CallbackQuery):
    """Show Wordstat management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_wordstat())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_custom_loader_manager")
async def handle_custom_loader_manager(callback_query: CallbackQuery):
    """Show Custom Loader management keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_custom_loader())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_admin_db_manager")
async def handle_admin_db_manager(callback_query: CallbackQuery):
    """Show admin panel keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_admin())
    await callback_query.answer()

@router_main.callback_query(lambda c: c.data == "command_back_main_menu")
async def handle_back_main_menu(callback_query: CallbackQuery):
    """Return to main menu keyboard."""
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_main())
    await callback_query.answer()


@router_main.callback_query(lambda c: c.data == "cancel_input")
async def handle_cancel_input(callback_query: CallbackQuery, state: FSMContext):
    """Cancel any active input state and return to main menu."""
    await state.clear()
    await callback_query.message.answer(
        "❌ Операция отменена.",
        reply_markup=get_kb_main()
    )
    await callback_query.answer()
