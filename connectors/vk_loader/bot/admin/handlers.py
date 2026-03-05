import asyncio
import html

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from admin.keyboards import get_kb_admin
from database.user import UserDatabaseConnector
from config.settings import settings
from connectors.vk_loader.bot.config import PREFECT_DEPLOYMENT_VK
from keyboards.inline import get_kb_cancel
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from prefect_loader.connectors.vk_loader.access import get_vk_agencies
from prefect_loader.orchestration.clickhouse_utils import AsyncVkDatabase

router_vk_admin = Router()


class VkAdminUploadStates(StatesGroup):
    waiting_lookback = State()


async def _is_admin(user_id: int) -> bool:
    connector = UserDatabaseConnector(settings.DB_NAME)
    await connector.initialize()
    admin_ids = await connector.get_admin_user_ids()
    return str(user_id) in admin_ids


def _format_error(exc: Exception) -> str:
    return html.escape(str(exc))


async def _require_text(message: types.Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


def _parse_lookback(text: str) -> int:
    stripped = text.strip()
    days = int(stripped)
    if days <= 0:
        raise ValueError("non_positive")
    return days


async def _run_prefect_required(
    deployment_name: str,
    parameters: dict,
    notify_message: types.Message,
) -> bool:
    try:
        run_id = await trigger_prefect_run(
            deployment_name=deployment_name,
            parameters=parameters,
            tags=("admin_bot", "bulk_upload"),
        )
        asyncio.create_task(
            wait_for_prefect_flow_run(
                run_id,
                deployment_name=deployment_name,
                notify_message=notify_message,
            )
        )
        await notify_message.answer(
            f"▶️ Запуск создан ({deployment_name}).\nRun ID: <code>{run_id}</code>",
            reply_markup=get_kb_admin(),
        )
        return True
    except Exception as exc:
        await notify_message.answer(
            f"❌ Не удалось поставить задачу ({deployment_name}): {_format_error(exc)}.",
            reply_markup=get_kb_admin(),
        )
        return False


@router_vk_admin.callback_query(lambda c: c.data == "admin_upload_all_vk")
async def handle_upload_all_vk(callback_query: types.CallbackQuery, state: FSMContext):
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await callback_query.answer()
    await callback_query.message.answer(
        "📍 Админ-панель → Выгрузка VK Ads\n\n"
        "💬 <i>Выгрузка VK Ads для всех агентств</i>\n\n"
        "Укажите глубину в днях\n\n"
        "📌 <b>Допустимые значения:</b> 1-365 дней\n"
        "<b>Пример:</b> <code>10</code> (последние 10 дней)",
        reply_markup=get_kb_cancel(),
    )
    await state.set_state(VkAdminUploadStates.waiting_lookback)


@router_vk_admin.message(VkAdminUploadStates.waiting_lookback)
async def process_vk_lookback(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        lookback_days = _parse_lookback(text)

        db = AsyncVkDatabase()
        await db.init_db()
        try:
            agencies = await get_vk_agencies(db)
            agencies_count = len(agencies)
        finally:
            await db.close_engine()

        if agencies_count == 0:
            await message.answer(
                "⚠ Нет агентств VK для выгрузки. Добавьте агентство сначала.",
                reply_markup=get_kb_admin(),
            )
            await state.clear()
            return

        await message.answer(
            f"🔄 Запускаю выгрузку VK для {agencies_count} агентств за последние {lookback_days} дней.",
        )

        async def run():
            ran_via_prefect = await _run_prefect_required(
                PREFECT_DEPLOYMENT_VK,
                parameters={"lookback_days": lookback_days},
                notify_message=message,
            )
            if not ran_via_prefect:
                await message.answer(
                    "⏹ Запуск VK отменён (не удалось создать Prefect run).",
                    reply_markup=get_kb_admin(),
                )

        asyncio.create_task(run())
    except ValueError as exc:
        reason = str(exc)
        if reason == "non_positive":
            await message.answer("⚠ Число дней должно быть больше нуля.")
        else:
            await message.answer("⚠ Укажите количество дней, например <code>10</code>.")
        return
    except Exception as exc:
        await message.answer(f"❌ Ошибка запуска VK: {_format_error(exc)}.", reply_markup=get_kb_admin())
    finally:
        await state.clear()
