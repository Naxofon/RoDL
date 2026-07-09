import asyncio
import html
from io import StringIO

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from connectors.roistat_loader.bot.config import PREFECT_DEPLOYMENT_ROISTAT_SINGLE
from connectors.roistat_loader.bot.keyboards import (
    get_kb_roistat,
    get_kb_roistat_flags,
    get_kb_roistat_upload_only,
)
from connectors.roistat_loader.flags import DEFAULT_ROISTAT_FLAGS, normalize_roistat_flags
from prefect_loader.orchestration.clickhouse_utils import AsyncRoistatDatabase
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from services.user_roles import is_alpha_user

router_roistat = Router()


class AddRoistatClient(StatesGroup):
    waiting_for_input = State()


class RemoveRoistatClient(StatesGroup):
    waiting_for_site_id = State()


class ConfigureRoistatTables(StatesGroup):
    waiting_for_site_id = State()
    editing_flags = State()


class UpdateDataRoistat(StatesGroup):
    waiting_for_site_id = State()
    waiting_for_tdelta = State()


def sanitize_site_id(site_id_str: str) -> int:
    site_id_str = site_id_str.strip()
    try:
        site_id = int(site_id_str)
        if site_id <= 0:
            raise ValueError("Site ID должен быть больше нуля")
        return site_id
    except ValueError:
        raise ValueError("Site ID должен быть положительным числом")


def _format_error(exc: Exception) -> str:
    return html.escape(str(exc))


async def _require_text(message: types.Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


async def _get_roistat_menu(user_id: int):
    if await is_alpha_user(user_id):
        return get_kb_roistat_upload_only()
    return get_kb_roistat()


async def _deny_if_alpha(callback_query: types.CallbackQuery) -> bool:
    if not await is_alpha_user(callback_query.from_user.id):
        return False
    await callback_query.answer("Для роли Alpha доступна только выгрузка.", show_alert=True)
    return True


def _format_flags_text(site_id: int, flags: dict[str, bool], account: str | None = None) -> str:
    account_line = f"Аккаунт: <code>{html.escape(account)}</code>\n" if account else ""
    return (
        "⚙️ <b>Настройка таблиц Roistat</b>\n\n"
        f"Site ID: <code>{site_id}</code>\n"
        f"{account_line}"
        f"Analytics: <b>{'on' if flags['analytics'] else 'off'}</b>\n"
        f"Calls: <b>{'on' if flags['calls'] else 'off'}</b>\n"
        f"Visits: <b>{'on' if flags['visits'] else 'off'}</b>\n\n"
        "Переключите нужные таблицы и нажмите сохранить."
    )


@router_roistat.callback_query(lambda c: c.data == "command_roistat_clients")
async def handle_roistat_clients(callback_query: types.CallbackQuery):
    if await _deny_if_alpha(callback_query):
        return
    await callback_query.message.delete()
    await callback_query.answer()

    db = AsyncRoistatDatabase()
    df = await db.get_roistat_config_data()
    if df.empty:
        await callback_query.message.answer(
            "Список клиентов Roistat пуст.",
            reply_markup=await _get_roistat_menu(callback_query.from_user.id),
        )
        return

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    file = types.BufferedInputFile(buffer.getvalue().encode("utf-8"), filename="roistat_clients.csv")
    await callback_query.message.answer_document(
        document=file,
        caption=f"📋 Клиентов Roistat: <b>{len(df)}</b>",
    )
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=await _get_roistat_menu(callback_query.from_user.id),
    )


@router_roistat.callback_query(lambda c: c.data == "command_add_roistat_client")
async def handle_add_roistat_client(callback_query: types.CallbackQuery, state: FSMContext):
    if await _deny_if_alpha(callback_query):
        return
    await state.set_state(AddRoistatClient.waiting_for_input)
    await callback_query.answer()
    await callback_query.message.answer(
        "📝 <b>Добавление клиента Roistat</b>\n\n"
        "Отправьте данные в формате:\n"
        "<code>site_id token [account_name]</code>\n\n"
        "По умолчанию будут включены Analytics и Calls, Visits будет выключен."
    )


@router_roistat.message(AddRoistatClient.waiting_for_input)
async def process_add_roistat_client(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        parts = text.strip().split()
        if len(parts) < 2:
            await message.answer(
                "❌ Неверный формат. Используйте:\n"
                "<code>site_id token [account_name]</code>",
            )
            return

        site_id = sanitize_site_id(parts[0])
        token = parts[1].strip()
        account = " ".join(parts[2:]).strip() or None
        if not token:
            await message.answer("❌ Токен не должен быть пустым.")
            return

        db = AsyncRoistatDatabase()
        await db.add_or_update_client(
            site_id=site_id,
            token=token,
            account=account,
            flags=dict(DEFAULT_ROISTAT_FLAGS),
        )
        await message.answer(
            f"✅ Клиент Roistat сохранён.\n\n"
            f"Site ID: <code>{site_id}</code>",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
    except Exception as exc:
        await message.answer(
            f"❌ Не удалось сохранить клиента: {_format_error(exc)}",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
    finally:
        await state.clear()


@router_roistat.callback_query(lambda c: c.data == "command_remove_roistat_client")
async def handle_remove_roistat_client(callback_query: types.CallbackQuery, state: FSMContext):
    if await _deny_if_alpha(callback_query):
        return
    await state.set_state(RemoveRoistatClient.waiting_for_site_id)
    await callback_query.answer()
    await callback_query.message.answer(
        "🗑️ <b>Удаление клиента Roistat</b>\n\n"
        "Отправьте Site ID клиента для удаления.\n"
        "Исторические таблицы и данные в ClickHouse удалены не будут."
    )


@router_roistat.message(RemoveRoistatClient.waiting_for_site_id)
async def process_remove_roistat_client(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        site_id = sanitize_site_id(text.strip())
        db = AsyncRoistatDatabase()
        await db.delete_client_by_site_id(site_id)
        await message.answer(
            f"✅ Клиент Roistat удалён.\n\nSite ID: <code>{site_id}</code>",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
    except Exception as exc:
        await message.answer(
            f"❌ Ошибка при удалении клиента: {_format_error(exc)}",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
    finally:
        await state.clear()


@router_roistat.callback_query(lambda c: c.data == "command_configure_roistat_tables")
async def handle_configure_roistat_tables(callback_query: types.CallbackQuery, state: FSMContext):
    if await _deny_if_alpha(callback_query):
        return
    await state.set_state(ConfigureRoistatTables.waiting_for_site_id)
    await callback_query.answer()
    await callback_query.message.answer(
        "⚙️ <b>Настройка таблиц Roistat</b>\n\n"
        "Отправьте Site ID клиента."
    )


@router_roistat.message(ConfigureRoistatTables.waiting_for_site_id)
async def process_configure_roistat_site_id(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        site_id = sanitize_site_id(text.strip())
        db = AsyncRoistatDatabase()
        client = await db.get_client(site_id)
        if client is None:
            await message.answer("❌ Клиент с таким Site ID не найден.")
            return

        flags = normalize_roistat_flags(client.get("flags"))
        await state.update_data(
            site_id=site_id,
            account=client.get("account") or "",
            roistat_flags=flags,
        )
        await state.set_state(ConfigureRoistatTables.editing_flags)
        await message.answer(
            _format_flags_text(site_id, flags, client.get("account") or ""),
            reply_markup=get_kb_roistat_flags(flags),
        )
    except Exception as exc:
        await message.answer(
            f"❌ Ошибка настройки клиента: {_format_error(exc)}",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
        await state.clear()


@router_roistat.callback_query(lambda c: (c.data or "").startswith("roistat_toggle:"))
async def handle_roistat_toggle(callback_query: types.CallbackQuery, state: FSMContext):
    if await state.get_state() != ConfigureRoistatTables.editing_flags.state:
        await callback_query.answer()
        return

    _, flag_name = (callback_query.data or "").split(":", 1)
    data = await state.get_data()
    flags = normalize_roistat_flags(data.get("roistat_flags"))
    if flag_name not in flags:
        await callback_query.answer()
        return
    flags[flag_name] = not flags[flag_name]
    await state.update_data(roistat_flags=flags)
    await callback_query.message.edit_text(
        _format_flags_text(data["site_id"], flags, data.get("account") or ""),
        reply_markup=get_kb_roistat_flags(flags),
    )
    await callback_query.answer()


@router_roistat.callback_query(lambda c: c.data == "roistat_save_flags")
async def handle_roistat_save_flags(callback_query: types.CallbackQuery, state: FSMContext):
    if await state.get_state() != ConfigureRoistatTables.editing_flags.state:
        await callback_query.answer()
        return
    data = await state.get_data()
    site_id = int(data["site_id"])
    flags = normalize_roistat_flags(data.get("roistat_flags"))
    db = AsyncRoistatDatabase()
    try:
        await db.update_client_flags(site_id, flags)
        await callback_query.message.answer(
            f"✅ Настройки таблиц сохранены для Site ID <code>{site_id}</code>.",
            reply_markup=await _get_roistat_menu(callback_query.from_user.id),
        )
    except Exception as exc:
        await callback_query.message.answer(
            f"❌ Не удалось сохранить настройки: {_format_error(exc)}",
            reply_markup=await _get_roistat_menu(callback_query.from_user.id),
        )
    finally:
        await state.clear()
        await callback_query.answer()


@router_roistat.callback_query(lambda c: c.data == "command_upload_roistat_data")
async def handle_upload_roistat_data(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(UpdateDataRoistat.waiting_for_site_id)
    await callback_query.answer()
    await callback_query.message.answer(
        "🔄 <b>Выгрузка Roistat</b>\n\n"
        "Укажите Site ID клиента."
    )


@router_roistat.message(UpdateDataRoistat.waiting_for_site_id)
async def process_upload_roistat_site_id(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        site_id = sanitize_site_id(text.strip())
        db = AsyncRoistatDatabase()
        client = await db.get_client(site_id)
        if client is None:
            await message.answer("❌ Клиент с таким Site ID не найден.")
            return
        await state.update_data(site_id=site_id)
        await state.set_state(UpdateDataRoistat.waiting_for_tdelta)
        await message.answer(
            "📅 За сколько дней выгружать данные?\n\n"
            "Укажите число от 1 до 365.\n"
            "Сегодняшний день в выгрузку не входит."
        )
    except Exception as exc:
        await message.answer(f"❌ Ошибка: {_format_error(exc)}")
        await state.clear()


@router_roistat.message(UpdateDataRoistat.waiting_for_tdelta)
async def process_upload_roistat_tdelta(message: types.Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        try:
            tdelta = int(text.strip())
            if tdelta <= 0 or tdelta > 365:
                raise ValueError
        except ValueError:
            await message.answer("❌ Укажите целое число от 1 до 365.")
            return

        data = await state.get_data()
        site_id = data.get("site_id")
        if not site_id:
            await message.answer(
                "❌ Сначала укажите Site ID клиента.",
                reply_markup=await _get_roistat_menu(message.from_user.id),
            )
            return

        run_id = await trigger_prefect_run(
            deployment_name=PREFECT_DEPLOYMENT_ROISTAT_SINGLE,
            parameters={"site_id": int(site_id), "tdelta": tdelta},
            tags=("admin_bot", "single_upload"),
        )
        await message.answer(
            "✅ Выгрузка Roistat запущена.\n\n"
            f"Site ID: <code>{site_id}</code>\n"
            f"Глубина (дней): {tdelta}\n"
            f"Run ID: <code>{html.escape(str(run_id))}</code>",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
        asyncio.create_task(
            wait_for_prefect_flow_run(
                run_id,
                deployment_name=PREFECT_DEPLOYMENT_ROISTAT_SINGLE,
                notify_message=message,
                poll_interval=20,
                max_wait_seconds=21600,
            )
        )
    except Exception as exc:
        await message.answer(
            f"❌ Не удалось запустить выгрузку: {_format_error(exc)}",
            reply_markup=await _get_roistat_menu(message.from_user.id),
        )
    finally:
        await state.clear()
