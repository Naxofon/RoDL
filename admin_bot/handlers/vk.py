import html
import pandas as pd

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from keyboards.inline import get_kb_vk, get_kb_cancel
from prefect_loader.orchestration.clickhouse_utils import AsyncVkDatabase
from prefect_loader.connectors.vk_loader.access import get_vk_agencies
from services.prefect_client import (
    trigger_prefect_run,
    wait_for_prefect_flow_run,
)
from config.settings import settings

PREFECT_DEPLOYMENT_VK = settings.PREFECT_DEPLOYMENT_VK
PREFECT_TAGS_VK = ("admin_bot", "vk_upload")

router_vk = Router()


class AddVkAgency(StatesGroup):
    waiting_for_input = State()


class RemoveVkAgency(StatesGroup):
    waiting_for_client_id = State()


_EMPTY_MARKERS = {"-", "none", "null", "nan", ""}


def _to_optional(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = text.strip()
    if normalized.lower() in _EMPTY_MARKERS:
        return None
    return normalized


def _format_error(exc: Exception) -> str:
    """Escape error text to avoid Telegram HTML parsing issues."""
    return html.escape(str(exc))


async def _require_text(message: types.Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


@router_vk.callback_query(lambda c: c.data == "command_vk_agencies")
async def handle_vk_agencies(callback_query: types.CallbackQuery):
    """List all VK agencies."""

    await callback_query.message.delete()

    db = AsyncVkDatabase()
    await db.init_db()

    try:
        agencies = await get_vk_agencies(db)

        if not agencies:
            await callback_query.message.answer("⚠ Агентства VK не найдены.")
            await callback_query.answer()
            return

        rows = []
        for client_id, info in agencies.items():
            rows.append({
                "client_id": client_id,
                "client_secret": info["client_secret"][:20] + "..." if len(info["client_secret"]) > 20 else info["client_secret"],
            })

        df = pd.DataFrame(rows, columns=["client_id", "client_secret"])
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        input_file = types.BufferedInputFile(
            file=csv_bytes,
            filename="vk_agencies.csv"
        )
        await callback_query.message.answer_document(
            input_file,
            caption=f"📋 Агентств VK: <b>{len(agencies)}</b>"
        )

        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_kb_vk()
        )
    except Exception as e:
        await callback_query.message.answer(
            f"❌ Ошибка при получении списка агентств: {_format_error(e)}",
            reply_markup=get_kb_vk()
        )
    finally:
        await db.close_engine()
        await callback_query.answer()


@router_vk.callback_query(lambda c: c.data == "command_add_vk_agency")
async def handle_add_vk_agency(callback_query: types.CallbackQuery, state: FSMContext):
    """Start add agency flow."""
    await callback_query.message.answer(
        "💬 <i>Добавление агентства VK Ads</i>\n\n"
        "Формат: <code>client_id client_secret</code>\n\n"
        "• <b>client_id</b> — идентификатор агентского приложения VK\n"
        "• <b>client_secret</b> — секретный ключ агентского приложения\n\n"
        "<b>Пример:</b>\n"
        "<code>123456 abc123def456</code>",
        parse_mode="HTML",
        reply_markup=get_kb_cancel()
    )
    await state.set_state(AddVkAgency.waiting_for_input)
    await callback_query.answer()


@router_vk.message(AddVkAgency.waiting_for_input)
async def process_add_vk_agency(message: types.Message, state: FSMContext):
    """Process add agency input."""
    db = AsyncVkDatabase()
    await db.init_db()

    try:
        text = await _require_text(message)
        if text is None:
            return
        parts = text.split()

        if len(parts) < 2:
            await message.answer(
                "⚠ <b>Ошибка:</b> Неверный формат.\n\n"
                f"Вы отправили: <code>{text}</code>\n\n"
                "Ожидается: <code>client_id client_secret</code>\n\n"
                "<b>Пример:</b>\n"
                "<code>123456 abc123def456</code>",
                parse_mode="HTML",
                reply_markup=get_kb_cancel()
            )
            return

        client_id = parts[0].strip()
        client_secret = parts[1].strip()
        provided_container = parts[2].strip() if len(parts) >= 3 else None
        container = None

        if not client_id or not client_secret:
            await message.answer(
                "⚠ <b>Ошибка:</b> client_id и client_secret не могут быть пустыми.\n\n"
                "Проверьте данные и попробуйте снова.",
                parse_mode="HTML",
                reply_markup=get_kb_cancel()
            )
            return

        await db.upsert_access_records(
            user_ids=[client_id],
            token=client_secret,
            container=container,
            type_value=None,
            replace=True,
        )

        await message.answer(
            f"✅ Агентство VK добавлено:\n"
            f"• Client ID: <code>{client_id}</code>\n"
            f"• Container: <b>не используется для VK"
            f"{f' (игнорировано: {html.escape(provided_container)})' if provided_container else ''}</b>\n\n"
            f"При следующей выгрузке система автоматически найдёт активных клиентов агентства.",
            reply_markup=get_kb_vk(),
            parse_mode="HTML"
        )

    except Exception as e:
        await message.answer(
            f"❌ Ошибка при добавлении агентства: {_format_error(e)}",
            reply_markup=get_kb_vk()
        )
    finally:
        await db.close_engine()
        await state.clear()


@router_vk.callback_query(lambda c: c.data == "command_remove_vk_agency")
async def handle_remove_vk_agency(callback_query: types.CallbackQuery, state: FSMContext):
    """Start remove agency flow."""
    await callback_query.message.answer(
        "💬 <i>Удаление агентства VK Ads</i>\n\n"
        "Введите <b>client_id</b> агентства для удаления:\n\n"
        "<b>Пример:</b> <code>123456</code>",
        parse_mode="HTML",
        reply_markup=get_kb_cancel()
    )
    await state.set_state(RemoveVkAgency.waiting_for_client_id)
    await callback_query.answer()


@router_vk.message(RemoveVkAgency.waiting_for_client_id)
async def process_remove_vk_agency(message: types.Message, state: FSMContext):
    """Process remove agency input."""
    db = AsyncVkDatabase()
    await db.init_db()

    try:
        text = await _require_text(message)
        if text is None:
            return
        client_id = text.strip()

        if not client_id:
            await message.answer(
                "⚠ <b>Ошибка:</b> client_id не может быть пустым.\n\n"
                "Отправьте client_id агентства для удаления.",
                parse_mode="HTML",
                reply_markup=get_kb_cancel()
            )
            return

        agencies = await get_vk_agencies(db)

        if client_id not in agencies:
            await message.answer(
                f"⚠ <b>Ошибка:</b> Агентство не найдено.\n\n"
                f"Client ID: <code>{client_id}</code>\n\n"
                f"Всего агентств в базе: <b>{len(agencies)}</b>",
                parse_mode="HTML",
                reply_markup=get_kb_cancel()
            )
            return

        await db.delete_access(
            user_id=client_id,
            container=None,
            type_value=None
        )

        await message.answer(
            f"✅ Агентство VK удалено: <code>{client_id}</code>\n\n"
            f"Данные клиентов этого агентства остались в базе, но новые выгрузки производиться не будут.",
            reply_markup=get_kb_vk(),
            parse_mode="HTML"
        )

    except Exception as e:
        await message.answer(
            f"❌ Ошибка при удалении агентства: {_format_error(e)}",
            reply_markup=get_kb_vk()
        )
    finally:
        await db.close_engine()
        await state.clear()


@router_vk.callback_query(lambda c: c.data == "command_upload_vk_data")
async def handle_upload_vk_data(callback_query: types.CallbackQuery):
    """Trigger VK data upload via Prefect."""
    await callback_query.answer("⏳ Запуск выгрузки VK...", show_alert=False)

    try:
        db = AsyncVkDatabase()
        await db.init_db()
        try:
            agencies = await get_vk_agencies(db)
            agencies_count = len(agencies)
        finally:
            await db.close_engine()

        if agencies_count == 0:
            await callback_query.message.answer(
                "⚠ Нет агентств VK для выгрузки. Добавьте агентство сначала.",
                reply_markup=get_kb_vk()
            )
            return

        await callback_query.message.answer(
            f"🔄 Запускаем выгрузку VK для {agencies_count} агентств...\n"
            f"Это может занять некоторое время."
        )

        run_id = await trigger_prefect_run(
            deployment_name=PREFECT_DEPLOYMENT_VK,
            parameters={"lookback_days": 10},
            tags=list(PREFECT_TAGS_VK)
        )

        if run_id:
            await callback_query.message.answer(
                f"✅ Выгрузка VK запущена\n"
                f"Run ID: <code>{run_id}</code>\n\n"
                f"Отслеживание выполнения...",
                reply_markup=get_kb_vk(),
                parse_mode="HTML"
            )

            success = await wait_for_prefect_flow_run(run_id, timeout=3600)

            if success:
                await callback_query.message.answer(
                    "✅ Выгрузка VK завершена успешно!",
                    reply_markup=get_kb_vk()
                )
            else:
                await callback_query.message.answer(
                    "⚠ Выгрузка VK завершилась с ошибками. Проверьте логи Prefect.",
                    reply_markup=get_kb_vk()
                )
        else:
            await callback_query.message.answer(
                "❌ Не удалось запустить выгрузку VK. Проверьте конфигурацию Prefect.",
                reply_markup=get_kb_vk()
            )

    except Exception as e:
        await callback_query.message.answer(
            f"❌ Ошибка при запуске выгрузки VK: {_format_error(e)}",
            reply_markup=get_kb_vk()
        )


__all__ = ["router_vk"]
