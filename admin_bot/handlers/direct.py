import asyncio
import html
import pandas as pd

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from keyboards.inline import get_kb_main, get_kb_direct
from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase, sanitize_login
from prefect_loader.connectors.direct_loader import (
    DIRECT_TYPE_AGENCY_PARSED,
    DIRECT_TYPE_AGENCY_TOKEN,
    DIRECT_TYPE_NOT_AGENCY,
    DEFAULT_DB_PROFILE,
    refresh_agency_clients,
)
from services.prefect_client import (
    trigger_prefect_run,
    wait_for_prefect_flow_run,
)
from config.settings import settings

PREFECT_DEPLOYMENT_DIRECT_SINGLE = settings.PREFECT_DEPLOYMENT_DIRECT_SINGLE
PREFECT_DEPLOYMENT_DIRECT_SINGLE_ANALYTICS = settings.PREFECT_DEPLOYMENT_DIRECT_SINGLE_ANALYTICS
PREFECT_DEPLOYMENT_DIRECT_SINGLE_LIGHT = settings.PREFECT_DEPLOYMENT_DIRECT_SINGLE_LIGHT
PREFECT_TAGS_SINGLE = ("admin_bot", "single_upload")
DEFAULT_DEPLOYMENT_DIRECT_ANALYTICS = "direct-loader-clickhouse/direct-analytics-change"
DEFAULT_DEPLOYMENT_DIRECT_LIGHT = "direct-loader-clickhouse/direct-light-hourly"

router_direct = Router()

class AddDirectClient(StatesGroup):
    waiting_for_input = State()

class UpdateDataDirectClient(StatesGroup):
    waiting_for_profile = State()
    waiting_for_login_update = State()

class RemoveDirectClient(StatesGroup):
    waiting_for_login = State()

class RemoveDirectAgency(StatesGroup):
    waiting_for_login = State()

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

@router_direct.callback_query(lambda c: c.data == "command_yd_clients")
async def handle_yd_clients(callback_query: types.CallbackQuery):

    await callback_query.message.delete()

    db = AsyncDirectDatabase()
    await db.init_db()

    try:
        await refresh_agency_clients(db, profile=DEFAULT_DB_PROFILE)
        rows = [
            {
                "login": row.get("login") or "",
                "container": row.get("container") or "",
                "token": row.get("token") or "",
                "service": row.get("service") or "",
                "subtype": row.get("subtype") or "",
            }
            for row in await db.fetch_direct_access_rows(include_null_type=True)
            if not row.get("service") or row.get("service") == "direct"
        ]
        if not rows:
            await callback_query.message.answer("⚠ Клиенты не найдены.")
            return
        df = pd.DataFrame(rows, columns=["login", "container", "token", "service", "subtype"])
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        input_file = types.BufferedInputFile(
            file=csv_bytes,
            filename="direct_accesses.csv"
        )
        await callback_query.message.answer_document(
            input_file,
            caption=f"📋 Клиенты Директа: <b>{len(df)}</b>"
        )

        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_kb_direct()
        )
    except Exception as e:
        await callback_query.message.answer(f"❌ Ошибка при отправке списка клиентов: {_format_error(e)}")
    finally:
        await callback_query.answer()

def _set_add_state(state: FSMContext, *, add_type: str):
    return state.update_data(add_type=add_type)


@router_direct.callback_query(lambda c: c.data == "command_add_yd_agent")
async def handle_add_yd_agent(callback_query: types.CallbackQuery, state: FSMContext):
    await _set_add_state(state, add_type=DIRECT_TYPE_AGENCY_TOKEN)
    await callback_query.message.answer(
        "💬 <i>Добавление агентского токена (Яндекс.Директ)</i>\n\n"
        "Формат: <code>login token [container]</code>\n"
        "• login можно пропустить или указать <code>-</code>\n"
        "• container — необязательный тег для группировки токена\n"
        "• тип будет сохранен автоматически как <b>agency_token</b>\n"
        "Логин не должен содержать спецсимволов; '_' и '.' заменяются на '-', не допускаются только цифры."
    )
    await state.set_state(AddDirectClient.waiting_for_input)
    await callback_query.answer()


@router_direct.callback_query(lambda c: c.data == "command_add_yd_client")
async def handle_add_yd_client(callback_query: types.CallbackQuery, state: FSMContext):
    await _set_add_state(state, add_type=DIRECT_TYPE_NOT_AGENCY)
    await callback_query.message.answer(
        "💬 <i>Добавление клиентского токена (Яндекс.Директ)</i>\n\n"
        "Формат: <code>login token [container]</code>\n"
        "• login обязателен\n"
        "• container — необязательный тег\n"
        "• тип будет сохранен автоматически как <b>not_agency_token</b>\n"
        "Логин не должен содержать спецсимволов; '_' и '.' заменяются на '-', не допускаются только цифры."
    )
    await state.set_state(AddDirectClient.waiting_for_input)
    await callback_query.answer()

@router_direct.message(AddDirectClient.waiting_for_input)
async def process_add_yd_client(message: types.Message, state: FSMContext):
    db = AsyncDirectDatabase()
    await db.init_db()
    try:
        text = await _require_text(message)
        if text is None:
            return
        data = await state.get_data()
        preset_type = data.get("add_type")

        parts = text.split()
        if preset_type:
            if len(parts) < 2:
                raise ValueError("expected_login_token")
            raw_login = parts[0]
            token = parts[1]
            container = parts[2] if len(parts) >= 3 else ""
            token_type = preset_type
        else:
            parts = text.split(maxsplit=3)
            if len(parts) == 3:
                raw_login, token, token_type = parts
                container = ""
            elif len(parts) >= 4:
                raw_login, token, container, token_type = parts[:4]
            else:
                raise ValueError("expected_four_parts")
            token_type = token_type.strip()
            if not token_type:
                raise ValueError("expected_type")

        login_value = _to_optional(raw_login)
        container_value = _to_optional(container)

        sanitized_login = sanitize_login(login_value) if login_value else None
        normalized_login = AsyncDirectDatabase._normalize_identifier(sanitized_login) if sanitized_login else None
        normalized_container = AsyncDirectDatabase._normalize_identifier(container_value)
        token = token.strip()
        if not token:
            raise ValueError("expected_token")

        if token_type == DIRECT_TYPE_AGENCY_TOKEN:
            await db.upsert_access_records(
                [normalized_login],
                token,
                container=normalized_container,
                type_value=DIRECT_TYPE_AGENCY_TOKEN,
                replace=True,
            )
            await refresh_agency_clients(db, profile=DEFAULT_DB_PROFILE)
            parsed_rows = await db.fetch_direct_access_rows(
                container=normalized_container,
                type_values=[DIRECT_TYPE_AGENCY_PARSED],
                include_null_type=True,
            )
            await message.answer(
                f"✅ Агентский токен сохранен (container: {normalized_container}). "
                f"Клиентов обновлено: {len(parsed_rows)}.",
                reply_markup=get_kb_direct(),
            )
        elif token_type == DIRECT_TYPE_NOT_AGENCY:
            if not normalized_login:
                raise ValueError("login_required")
            await db.delete_access(normalized_login, container=normalized_container, type_value=DIRECT_TYPE_NOT_AGENCY)
            await db.upsert_access_records(
                [normalized_login],
                token,
                container=normalized_container,
                type_value=DIRECT_TYPE_NOT_AGENCY,
                replace=False,
            )
            await message.answer(
                f"✅ Клиент '{normalized_login}' добавлен как неагентский (container: {normalized_container}).",
                reply_markup=get_kb_direct(),
            )
        elif token_type == DIRECT_TYPE_AGENCY_PARSED:
            if not normalized_login:
                raise ValueError("login_required")
            await db.delete_access(normalized_login, container=normalized_container, type_value=DIRECT_TYPE_AGENCY_PARSED)
            await db.upsert_access_records(
                [normalized_login],
                token,
                container=normalized_container,
                type_value=DIRECT_TYPE_AGENCY_PARSED,
                replace=False,
            )
            await message.answer(
                f"✅ Логин '{normalized_login}' записан как агентский (container: {normalized_container}).",
                reply_markup=get_kb_direct(),
            )
        else:
            if not normalized_login:
                raise ValueError("login_required")
            await db.delete_access(normalized_login, container=normalized_container, type_value=token_type)
            await db.upsert_access_records(
                [normalized_login],
                token,
                container=normalized_container,
                type_value=token_type,
                replace=False,
            )
            await message.answer(
                f"✅ Доступ для '{normalized_login}' сохранён с типом '{token_type}' (container: {normalized_container}).",
                reply_markup=get_kb_direct(),
            )
    except ValueError as exc:
        reason = str(exc)
        if reason == "expected_four_parts":
            await message.answer(
                "⚠ Формат: <code>login token container type</code> (четыре значения через пробел).",
            )
        elif reason == "expected_login_token":
            await message.answer(
                "⚠ Формат: <code>login token [container]</code>. Укажите логин и токен через пробел.",
            )
        elif reason == "login_required":
            await message.answer("⚠ Для этого типа нужно указать логин (используйте <code>-</code> только для agency_token).")
        elif reason == "expected_type":
            await message.answer("⚠ Укажите тип доступа: agency_token, agency_parsed или not_agency_token.")
        elif reason == "expected_token":
            await message.answer("⚠ Токен не должен быть пустым.")
        elif reason == "login_invalid_chars":
            await message.answer("⚠ Логин может содержать только буквы/цифры и дефис. Символы '_' и '.' автоматически заменяются на '-'.")
        elif reason == "login_digits_only":
            await message.answer("⚠ Логин не может состоять только из цифр.")
        else:
            await message.answer("⚠ Ошибка ввода. Проверьте формат и попробуйте снова.")
    except Exception as e:
        await message.answer(f"❌ Ошибка добавления клиента: {_format_error(e)}")
    finally:
        await state.clear()

@router_direct.callback_query(lambda c: c.data == "command_remove_yd_clients")
async def handle_remove_yd_clients(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "💬 <i>Выбрано удаление клиента (Яндекс.Директ)</i>\n\n"
        "Отправьте логин клиента (не агентский), который нужно удалить."
    )
    await state.set_state(RemoveDirectClient.waiting_for_login)
    await callback_query.answer()

@router_direct.message(RemoveDirectClient.waiting_for_login)
async def process_remove_yd_client(message: types.Message, state: FSMContext):
    db = AsyncDirectDatabase()
    await db.init_db()
    text = await _require_text(message, reply_markup=get_kb_direct())
    if text is None:
        return
    login = text.strip()
    normalized_login = AsyncDirectDatabase._normalize_identifier(login)
    try:
        rows = await db.fetch_direct_access_rows(include_null_type=True)
        targets = [
            row for row in rows
            if (row.get("service") in (None, "direct"))
            and AsyncDirectDatabase._normalize_identifier(row.get("login")) == normalized_login
        ]
        if not targets:
            await message.answer(f"⚠ Записи для логина '{normalized_login}' не найдены.")
        else:
            for row in targets:
                await db.delete_access(
                    normalized_login,
                    container=row.get("container"),
                    type_value=row.get("subtype"),
                )
            await message.answer(
                f"✅ Удалено записей: {len(targets)} для '{normalized_login}'.",
                reply_markup=get_kb_direct(),
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка удаления: {_format_error(e)}")
    finally:
        await state.clear()


@router_direct.callback_query(lambda c: c.data == "command_remove_yd_agency")
async def handle_remove_yd_agency(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "💬 <i>Удаление агентского аккаунта (Яндекс.Директ)</i>\n\n"
        "Отправьте логин агентства, который нужно удалить."
    )
    await state.set_state(RemoveDirectAgency.waiting_for_login)
    await callback_query.answer()


@router_direct.message(RemoveDirectAgency.waiting_for_login)
async def process_remove_yd_agency(message: types.Message, state: FSMContext):
    db = AsyncDirectDatabase()
    await db.init_db()
    text = await _require_text(message, reply_markup=get_kb_direct())
    if text is None:
        return
    login = text.strip()
    normalized_login = AsyncDirectDatabase._normalize_identifier(login)
    try:
        rows = await db.fetch_direct_access_rows(include_null_type=True)
        targets = [
            row for row in rows
            if (row.get("service") in (None, "direct"))
            and row.get("subtype") in {DIRECT_TYPE_AGENCY_TOKEN, DIRECT_TYPE_AGENCY_PARSED}
            and AsyncDirectDatabase._normalize_identifier(row.get("login")) == normalized_login
        ]
        if not targets:
            await message.answer(f"⚠ Агентство с логином '{normalized_login}' не найдено.")
        else:
            for row in targets:
                await db.delete_access(
                    normalized_login,
                    container=row.get("container"),
                    type_value=row.get("subtype"),
                )
            await message.answer(
                f"✅ Удалено агентских записей: {len(targets)} для '{normalized_login}'.",
                reply_markup=get_kb_direct(),
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка удаления агентства: {_format_error(e)}")
    finally:
        await state.clear()


@router_direct.callback_query(lambda c: c.data == "command_upload_yd_data")
async def handle_update_yd_clients(callback_query: types.CallbackQuery, state: FSMContext):
    profile_keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Аналитическая БД", callback_data="direct_profile_analytics"),
                types.InlineKeyboardButton(text="Легкая БД", callback_data="direct_profile_light"),
            ]
        ]
    )
    await callback_query.message.answer(
        "💬 <i>Выбран запуск трекера изменений (Яндекс.Директ)</i>\n\n"
        "Выберите профиль базы данных:",
        reply_markup=profile_keyboard,
    )
    await state.set_state(UpdateDataDirectClient.waiting_for_profile)
    await callback_query.answer()

@router_direct.callback_query(lambda c: c.data in {"direct_profile_analytics", "direct_profile_light"})
async def process_direct_profile_choice(callback_query: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state != UpdateDataDirectClient.waiting_for_profile.state:
        await callback_query.answer()
        return

    profile = "analytics" if callback_query.data.endswith("analytics") else "light"
    await state.update_data(profile=profile)
    await state.set_state(UpdateDataDirectClient.waiting_for_login_update)
    await callback_query.message.answer(
        "Отправьте логин клиента, для которого нужно проверить изменения."
    )
    await callback_query.answer()

@router_direct.message(UpdateDataDirectClient.waiting_for_login_update)
async def process_update_client(message: types.Message, state: FSMContext):

    try:
        text = await _require_text(message)
        if text is None:
            return
        parts = text.split()
        if len(parts) != 1:
            raise ValueError("expected_single_login")

        raw_login = parts[0].strip()
        if not raw_login:
            raise ValueError("expected_single_login")

        data = await state.get_data()
        profile = data.get("profile")
        if profile not in {"analytics", "light"}:
            raise ValueError("missing_profile")

        normalized_login = AsyncDirectDatabase._normalize_identifier(raw_login)
        await message.answer(
            f"🔄 Запускаю обновление для '{raw_login}' (профиль: {profile})."
        )

        async def do_update():
            deployment_name = (
                PREFECT_DEPLOYMENT_DIRECT_SINGLE_ANALYTICS if profile == "analytics"
                else PREFECT_DEPLOYMENT_DIRECT_SINGLE_LIGHT
            )
            deployment_label = deployment_name or (
                DEFAULT_DEPLOYMENT_DIRECT_ANALYTICS if profile == "analytics"
                else DEFAULT_DEPLOYMENT_DIRECT_LIGHT
            )
            try:
                run_id = await trigger_prefect_run(
                    deployment_name=deployment_label,
                    parameters={
                        "track_changes": True,
                        "profile": profile,
                        "login": normalized_login,
                    },
                    tags=PREFECT_TAGS_SINGLE,
                )
                asyncio.create_task(
                    wait_for_prefect_flow_run(
                        run_id,
                        deployment_name=deployment_label,
                        notify_message=message,
                    )
                )
                await message.answer(
                    "▶️ Запуск создан"
                    f"({deployment_label}). Run ID: <code>{run_id}</code>",
                    reply_markup=get_kb_direct(),
                )
            except Exception as exc:
                await message.answer(
                    f"❌ Не удалось создать задачу ({deployment_label}): {_format_error(exc)}.",
                    reply_markup=get_kb_direct(),
                )

        asyncio.create_task(do_update())
    except ValueError as exc:
        if str(exc) == "missing_profile":
            await message.answer(
                "⚠ Сначала выберите профиль базы данных.",
                reply_markup=get_kb_direct(),
            )
        else:
            await message.answer(
                "⚠ Укажите только логин клиента (например, <code>example_login</code>).",
                reply_markup=get_kb_direct(),
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка проверки изменений: {_format_error(e)}")
    finally:
        await state.clear()


@router_direct.callback_query(lambda c: c.data == "command_back_main_menu")
async def handle_back_main_menu(callback_query: types.CallbackQuery):
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_main())
    await callback_query.answer()
