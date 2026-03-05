import asyncio
import html

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from admin.keyboards import get_kb_admin
from database.user import UserDatabaseConnector
from config.settings import settings
from connectors.calltouch_loader.bot.config import PREFECT_DEPLOYMENT_CALLTOUCH
from keyboards.inline import get_kb_cancel
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run

router_calltouch_admin = Router()


class CalltouchAdminUploadStates(StatesGroup):
    waiting_tdelta = State()


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


@router_calltouch_admin.callback_query(lambda c: c.data == "admin_upload_all_calltouch")
async def handle_upload_all_calltouch(callback_query: types.CallbackQuery, state: FSMContext):
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await callback_query.answer()
    await callback_query.message.answer(
        "📍 Админ-панель → Выгрузка Calltouch\n\n"
        "💬 <i>Выгрузка Calltouch для всех клиентов</i>\n\n"
        "Укажите глубину в днях\n\n"
        "📌 <b>Допустимые значения:</b> 1-365 дней\n"
        "<b>Пример:</b> <code>10</code> (последние 10 дней)",
        reply_markup=get_kb_cancel(),
    )
    await state.set_state(CalltouchAdminUploadStates.waiting_tdelta)


@router_calltouch_admin.message(CalltouchAdminUploadStates.waiting_tdelta)
async def process_calltouch_tdelta(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        tdelta = _parse_lookback(text)
        await message.answer(f"🔄 Запускаю выгрузку Calltouch за последние {tdelta} дней.")

        async def run():
            ran_via_prefect = await _run_prefect_required(
                PREFECT_DEPLOYMENT_CALLTOUCH,
                parameters={"tdelta": tdelta},
                notify_message=message,
            )
            if not ran_via_prefect:
                await message.answer(
                    "⏹ Запуск Calltouch отменён (не удалось создать Prefect run).",
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
        await message.answer(f"❌ Ошибка запуска Calltouch: {_format_error(exc)}.", reply_markup=get_kb_admin())
    finally:
        await state.clear()
