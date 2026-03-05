import asyncio
import html
import logging
from io import StringIO

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from connectors.calltouch_loader.bot.config import (
    PREFECT_DEPLOYMENT_CALLTOUCH,
    PREFECT_DEPLOYMENT_CALLTOUCH_SINGLE,
)
from .keyboards import get_kb_calltouch, get_kb_calltouch_upload_only
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from services.user_roles import is_alpha_user
from prefect_loader.orchestration.clickhouse_utils import AsyncCalltouchDatabase

logger = logging.getLogger(__name__)

router_calltouch = Router()


class AddCalltouchClient(StatesGroup):
    waiting_for_input = State()


class RemoveCalltouchClient(StatesGroup):
    waiting_for_site_id = State()


class UpdateDataCalltouch(StatesGroup):
    waiting_for_site_id = State()
    waiting_for_tdelta = State()


async def _require_text(message: types.Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


async def _get_calltouch_menu(user_id: int):
    if await is_alpha_user(user_id):
        return get_kb_calltouch_upload_only()
    return get_kb_calltouch()


async def _deny_if_alpha(callback_query: types.CallbackQuery) -> bool:
    if not await is_alpha_user(callback_query.from_user.id):
        return False
    await callback_query.answer("Для роли Alpha доступна только выгрузка.", show_alert=True)
    return True


def sanitize_site_id(site_id_str: str) -> int:
    """Validate and convert site_id to integer."""
    site_id_str = site_id_str.strip()
    try:
        site_id = int(site_id_str)
        if site_id <= 0:
            raise ValueError("Site ID должен быть больше нуля")
        return site_id
    except ValueError:
        raise ValueError("Site ID должен быть положительным числом")


@router_calltouch.callback_query(lambda c: c.data == "command_ct_clients")
async def handle_ct_clients(callback_query: types.CallbackQuery):
    """List all Calltouch clients."""
    if await _deny_if_alpha(callback_query):
        return
    await callback_query.message.delete()
    await callback_query.answer()

    db = AsyncCalltouchDatabase()
    df = await db.get_calltouch_config_data()

    if df.empty:
        await callback_query.message.answer(
            "Список клиентов Calltouch пуст.",
            reply_markup=await _get_calltouch_menu(callback_query.from_user.id)
        )
        return

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    file = types.BufferedInputFile(
        csv_content.encode('utf-8'),
        filename="calltouch_clients.csv"
    )

    await callback_query.message.answer_document(
        document=file,
        caption=f"📋 Клиентов Calltouch: <b>{len(df)}</b>"
    )

    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=await _get_calltouch_menu(callback_query.from_user.id)
    )

@router_calltouch.callback_query(lambda c: c.data == "command_add_ct_client")
async def handle_add_ct_client(callback_query: types.CallbackQuery, state: FSMContext):
    """Start the add client flow."""
    if await _deny_if_alpha(callback_query):
        return
    await callback_query.answer()
    await state.set_state(AddCalltouchClient.waiting_for_input)
    await callback_query.message.answer(
        "📝 *Добавление клиента Calltouch*\n\n"
        "Отправьте данные клиента в формате:\n"
        "`site_id token [account_name]`\n\n"
        "Пример:\n"
        "`52718 your_api_token_here MyCompany`\n\n"
        "Название аккаунта указывать не обязательно.",
        parse_mode="Markdown"
    )


@router_calltouch.message(AddCalltouchClient.waiting_for_input)
async def process_add_ct_client(message: types.Message, state: FSMContext):
    """Process add client input."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        parts = text.strip().split()

        if len(parts) < 2:
            await message.answer(
                "❌ Неверный формат. Отправьте минимум: `site_id token`\n\n"
                "Пример: `52718 your_api_token_here MyCompany`",
                parse_mode="Markdown"
            )
            return

        site_id_str = parts[0]
        token = parts[1]
        account = " ".join(parts[2:]) if len(parts) > 2 else ""

        try:
            site_id = sanitize_site_id(site_id_str)
        except ValueError as e:
            await message.answer(f"❌ Некорректный Site ID: {e}")
            return

        if not token or len(token) < 10:
            await message.answer("❌ Похоже, токен слишком короткий. Проверьте и повторите.")
            return

        db = AsyncCalltouchDatabase()
        existing_df = await db.get_calltouch_config_data()
        if not existing_df.empty and (existing_df["site_id"] == site_id).any():
            existing_account = (
                existing_df.loc[existing_df["site_id"] == site_id, "account"]
                .iloc[0]
            )
            account_display = (
                f" (аккаунт: <code>{html.escape(str(existing_account))}</code>)"
                if existing_account
                else ""
            )
            await message.answer(
                "⚠️ Клиент с таким Site ID уже существует.\n\n"
                f"Site ID: <code>{site_id}</code>{account_display}",
                parse_mode="HTML",
                reply_markup=await _get_calltouch_menu(message.from_user.id),
            )
            return

        await db.add_client_to_calltouch_config(
            site_id=site_id,
            token=token,
            account=account
        )

        account_display = f" (<code>{html.escape(account)}</code>)" if account else ""
        await message.answer(
            f"✅ Клиент Calltouch добавлен.\n\n"
            f"Site ID: <code>{site_id}</code>{account_display}",
            parse_mode="HTML",
            reply_markup=await _get_calltouch_menu(message.from_user.id)
        )

    except Exception as e:
        logger.error(f"Error adding Calltouch client: {e}", exc_info=True)
        await message.answer(
            f"❌ Не удалось добавить клиента: {e}",
            reply_markup=await _get_calltouch_menu(message.from_user.id)
        )

    finally:
        await state.clear()


@router_calltouch.callback_query(lambda c: c.data == "command_remove_ct_clients")
async def handle_remove_ct_clients(callback_query: types.CallbackQuery, state: FSMContext):
    """Start the remove client flow."""
    if await _deny_if_alpha(callback_query):
        return
    await callback_query.answer()
    await state.set_state(RemoveCalltouchClient.waiting_for_site_id)
    await callback_query.message.answer(
        "🗑️ *Удаление клиента Calltouch*\n\n"
        "Отправьте Site ID для удаления:\n"
        "Пример: `52718`",
        parse_mode="Markdown"
    )


@router_calltouch.message(RemoveCalltouchClient.waiting_for_site_id)
async def process_remove_ct_client(message: types.Message, state: FSMContext):
    """Process remove client request."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        site_id_str = text.strip()
        try:
            site_id = sanitize_site_id(site_id_str)
        except ValueError as e:
            await message.answer(f"❌ Некорректный Site ID: {e}")
            return

        db = AsyncCalltouchDatabase()
        await db.delete_client_by_site_id(site_id)

        await message.answer(
            f"✅ Клиент Calltouch удалён.\n\n"
            f"Site ID: `{site_id}`",
            parse_mode="Markdown",
            reply_markup=await _get_calltouch_menu(message.from_user.id)
        )

    except Exception as e:
        logger.error(f"Error removing Calltouch client: {e}", exc_info=True)
        await message.answer(
            f"❌ Ошибка при удалении клиента: {e}",
            reply_markup=await _get_calltouch_menu(message.from_user.id)
        )

    finally:
        await state.clear()


@router_calltouch.callback_query(lambda c: c.data == "command_upload_ct_data")
async def handle_upload_ct_data(callback_query: types.CallbackQuery, state: FSMContext):
    """Start the update data flow."""
    await callback_query.answer()
    await state.set_state(UpdateDataCalltouch.waiting_for_site_id)
    await callback_query.message.answer(
        "🔄 *Выгрузка Calltouch*\n\n"
        "Укажите Site ID клиента для обновления.\n",
        parse_mode="Markdown"
    )


@router_calltouch.message(UpdateDataCalltouch.waiting_for_site_id)
async def process_upload_ct_site_id(message: types.Message, state: FSMContext):
    """Process site_id and ask for tdelta."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        site_id_input = text.strip().lower()

        try:
            site_id = sanitize_site_id(site_id_input)
            await state.update_data(site_id=site_id)
        except ValueError as e:
            await message.answer(f"❌ Некорректный Site ID: {e}")
            return

        await state.set_state(UpdateDataCalltouch.waiting_for_tdelta)
        await message.answer(
            "📅 За сколько дней выгружать данные?\n\n"
            "Отправьте число (по умолчанию 10):\n"
            "Пример: `10` — последние 10 дней",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error processing site_id: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()


@router_calltouch.message(UpdateDataCalltouch.waiting_for_tdelta)
async def process_upload_ct_tdelta(message: types.Message, state: FSMContext):
    """Process tdelta and trigger Prefect run."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        tdelta_str = text.strip()

        try:
            tdelta = int(tdelta_str)
            if tdelta <= 0 or tdelta > 365:
                await message.answer("❌ Укажите число от 1 до 365.")
                return
        except ValueError:
            await message.answer("❌ Укажите целое число.")
            return

        data = await state.get_data()
        site_id = data.get("site_id")
        if not site_id:
            await message.answer(
                "❌ Сначала укажите Site ID клиента.",
                reply_markup=await _get_calltouch_menu(message.from_user.id),
            )
            return

        parameters = {"tdelta": tdelta}
        tags = ["admin_bot", "single_upload"]
        parameters["site_id"] = site_id
        deployment_name = PREFECT_DEPLOYMENT_CALLTOUCH_SINGLE
        description = f"site_id={site_id}"

        try:
            run_id = await trigger_prefect_run(
                deployment_name=deployment_name,
                parameters=parameters,
                tags=tuple(tags),
            )

            description_safe = html.escape(description)
            run_id_safe = html.escape(str(run_id))
            await message.answer(
                "✅ Выгрузка Calltouch запущена!\n\n"
                f"Цель: {description_safe}\n"
                f"Глубина (дней): {tdelta}\n"
                f"Run ID: <code>{run_id_safe}</code>\n\n"
                "Следим за выполнением...",
                parse_mode="HTML",
                reply_markup=await _get_calltouch_menu(message.from_user.id)
            )

            asyncio.create_task(
                wait_for_prefect_flow_run(
                    run_id,
                    deployment_name=deployment_name,
                    notify_message=message,
                    poll_interval=20,
                    max_wait_seconds=21600,
                )
            )

        except Exception as e:
            logger.error(f"Error triggering Prefect run: {e}", exc_info=True)
            await message.answer(
                f"❌ Не удалось запустить выгрузку: {e}",
                reply_markup=await _get_calltouch_menu(message.from_user.id)
            )

    except Exception as e:
        logger.error(f"Error processing tdelta: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await _get_calltouch_menu(message.from_user.id))

    finally:
        await state.clear()
