import asyncio
import html

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from admin.keyboards import get_kb_admin
from database.user import UserDatabaseConnector
from config.settings import settings
from connectors.direct_loader.bot.config import (
    PREFECT_DEPLOYMENT_DIRECT_ANALYTICS,
    PREFECT_DEPLOYMENT_DIRECT_LIGHT,
)
from keyboards.inline import get_kb_cancel
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from prefect_loader.connectors.direct_loader import DEFAULT_DB_PROFILE

router_direct_admin = Router()


class DirectAdminUploadStates(StatesGroup):
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


@router_direct_admin.callback_query(lambda c: c.data == "admin_upload_all_direct")
async def handle_upload_all_direct(callback_query: types.CallbackQuery):
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Аналитическая БД",
                    callback_data="admin_direct_all_profile_analytics",
                ),
                types.InlineKeyboardButton(
                    text="Легкая БД",
                    callback_data="admin_direct_all_profile_light",
                ),
            ]
        ]
    )
    await callback_query.message.answer(
        "💬 <i>Выгрузка Яндекс.Директ для всех клиентов</i>\n\n"
        "Выберите профиль базы данных для запуска трекера изменений:",
        reply_markup=keyboard,
    )
    await callback_query.answer()


@router_direct_admin.callback_query(
    lambda c: c.data in {"admin_direct_all_profile_analytics", "admin_direct_all_profile_light"}
)
async def process_direct_profile_selection(callback_query: types.CallbackQuery, state: FSMContext):
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    profile = DEFAULT_DB_PROFILE if callback_query.data.endswith("analytics") else "light"

    await callback_query.answer()
    await callback_query.message.answer(
        f"📍 Админ-панель → Выгрузка Директа ({profile})\n\n"
        f"💬 <i>Трекер изменений Яндекс.Директ для всех клиентов</i>\n"
        f"Профиль БД: <b>{profile}</b>\n\n"
        "Укажите глубину проверки в днях\n\n"
        "📌 <b>Допустимые значения:</b> 1-365 дней\n"
        "<b>Пример:</b> <code>100</code> (последние 100 дней)",
        reply_markup=get_kb_cancel(),
    )
    await state.update_data(profile=profile)
    await state.set_state(DirectAdminUploadStates.waiting_lookback)


@router_direct_admin.message(DirectAdminUploadStates.waiting_lookback)
async def process_direct_lookback(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        profile = data.get("profile")
        if profile not in {"analytics", "light"}:
            raise ValueError("missing_profile")

        text = await _require_text(message)
        if text is None:
            return
        lookback_days = _parse_lookback(text)
        await message.answer(
            f"🔄 Запускаю проверку изменений Яндекс.Директ за последние {lookback_days} дней (профиль: {profile}).",
        )

        async def run():
            deployment_name = (
                PREFECT_DEPLOYMENT_DIRECT_ANALYTICS if profile == "analytics"
                else PREFECT_DEPLOYMENT_DIRECT_LIGHT
            )
            ran_via_prefect = await _run_prefect_required(
                deployment_name,
                parameters={
                    "track_changes": True,
                    "lookback_days": lookback_days,
                    "profile": profile,
                },
                notify_message=message,
            )
            if not ran_via_prefect:
                await message.answer(
                    "⏹ Запуск отменён (не удалось создать Prefect run).",
                    reply_markup=get_kb_admin(),
                )

        asyncio.create_task(run())
    except ValueError as exc:
        reason = str(exc)
        if reason == "missing_profile":
            await message.answer("⚠ Сначала выберите профиль базы данных в админ-панели /a.")
        elif reason == "non_positive":
            await message.answer("⚠ Число дней должно быть больше нуля.")
        else:
            await message.answer("⚠ Укажите количество дней, например <code>100</code>.")
        return
    except Exception as exc:
        await message.answer(f"❌ Ошибка запуска выгрузки: {_format_error(exc)}.", reply_markup=get_kb_admin())
    finally:
        await state.clear()
