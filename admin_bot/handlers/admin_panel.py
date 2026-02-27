"""Admin panel handlers for user management and bulk uploads."""
import asyncio
import html
import io
from datetime import datetime

import pandas as pd
from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.user import UserDatabaseConnector
from keyboards.inline import get_kb_admin, get_kb_admin_backup, get_kb_cancel
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from config.settings import settings
from prefect_loader.connectors.direct_loader import DEFAULT_DB_PROFILE
from prefect_loader.orchestration.clickhouse_utils import (
    AsyncDirectDatabase,
    AsyncMetrikaDatabase,
    AsyncCalltouchDatabase,
    AsyncVkDatabase,
    ClickhouseDatabase,
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_ACCESS_PASSWORD,
)

try:
    from prefect_loader.orchestration.clickhouse_utils import AsyncWordstatDatabase
except ImportError:
    AsyncWordstatDatabase = None

try:
    from prefect_loader.orchestration.clickhouse_utils import AsyncCustomLoaderDatabase
except ImportError:
    AsyncCustomLoaderDatabase = None


router_admin_panel = Router()

class FormAddAdmin(StatesGroup):
    """FSM for adding new admin."""
    id = State()


class FormAddAlpha(StatesGroup):
    """FSM for adding new alpha user."""
    id = State()


class FormRemoveUser(StatesGroup):
    """FSM for removing admin/alpha user."""
    id = State()


class AdminUploadStates(StatesGroup):
    """FSM for bulk upload operations."""
    waiting_metrika_lookback = State()
    waiting_direct_lookback = State()
    waiting_calltouch_tdelta = State()
    waiting_vk_lookback = State()


class CleanupStates(StatesGroup):
    """FSM for database cleanup operations."""
    waiting_client_list = State()

class AccessBackupStates(StatesGroup):
    """FSM for Accesses backup upload."""
    waiting_backup_file = State()




PREFECT_TAGS_BULK = ("admin_bot", "bulk_upload")


async def _is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    connector = UserDatabaseConnector(settings.DB_NAME)
    await connector.initialize()
    admin_ids = await connector.get_admin_user_ids()
    return str(user_id) in admin_ids


def _format_error(exc: Exception) -> str:
    """Format exception for display."""
    return html.escape(str(exc))


async def _require_text(message: types.Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


def _parse_lookback(text: str) -> int:
    """Parse positive integer lookback days."""
    stripped = text.strip()
    days = int(stripped)
    if days <= 0:
        raise ValueError("non_positive")
    return days


def _clean_access_value(value: object) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"none", "null", "nan"}:
        return None
    return text


def _prepare_access_backup_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if df is None or df.empty:
        raise ValueError("empty_backup")

    normalized = df.copy()
    normalized.columns = [str(col).strip().lower() for col in normalized.columns]

    columns = set(normalized.columns)
    missing = []
    for required in ("login", "token"):
        if required not in columns:
            missing.append(required)
    if missing:
        raise ValueError("missing_columns:" + ",".join(missing))

    if "container" not in columns:
        normalized["container"] = None
    if "type" not in columns:
        normalized["type"] = None

    has_service = "service" in columns
    has_subtype = "subtype" in columns
    if has_service:
        normalized["service"] = normalized["service"].apply(_clean_access_value)
    if has_subtype:
        normalized["subtype"] = normalized["subtype"].apply(_clean_access_value)

    normalized["login"] = normalized["login"].apply(_clean_access_value)
    normalized["token"] = normalized["token"].apply(_clean_access_value)
    normalized["container"] = normalized["container"].apply(_clean_access_value)
    normalized["type"] = normalized["type"].apply(_clean_access_value)

    if has_service or has_subtype:
        def _compose(row: pd.Series) -> str | None:
            if row.get("type"):
                return row["type"]
            service = row.get("service")
            subtype = row.get("subtype")
            if service and subtype:
                return f"{service}:{subtype}"
            return service or subtype

        normalized["type"] = normalized.apply(_compose, axis=1)

    before = len(normalized)
    normalized = normalized.dropna(
        how="all",
        subset=["login", "token", "container", "type"],
    )
    dropped_empty = before - len(normalized)

    before = len(normalized)
    normalized = normalized[normalized["token"].notna()]
    dropped_no_token = before - len(normalized)

    normalized = normalized[["login", "token", "container", "type"]]
    normalized = normalized.drop_duplicates()

    stats = {
        "dropped_empty": dropped_empty,
        "dropped_no_token": dropped_no_token,
        "kept": len(normalized),
    }
    return normalized, stats


def _get_access_db() -> ClickhouseDatabase:
    return ClickhouseDatabase(
        database=CLICKHOUSE_ACCESS_DATABASE,
        user=CLICKHOUSE_ACCESS_USER,
        password=CLICKHOUSE_ACCESS_PASSWORD,
    )


async def _run_prefect_required(
    deployment_name: str,
    parameters: dict,
    notify_message: types.Message,
) -> bool:
    """
    Enqueue a Prefect run.
    Returns True on success, False on failure.
    """
    try:
        run_id = await trigger_prefect_run(
            deployment_name=deployment_name,
            parameters=parameters,
            tags=PREFECT_TAGS_BULK,
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


@router_admin_panel.callback_query(lambda c: c.data == "add_admin")
async def add_admin_start(query: types.CallbackQuery, state: FSMContext) -> None:
    """Start adding new admin."""
    await query.answer()

    status_msg = await query.message.answer("⏳ Загрузка списка администраторов...")

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_admin = await user_connector.get_administrators()

    await state.set_state(FormAddAdmin.id)

    message_lines = [
        "📍 Админ-панель → Добавление администратора\n",
        "💬 <b>Отправьте Telegram ID нового администратора</b>\n",
        "📋 <b>Текущие администраторы:</b>"
    ]

    if list_admin:
        for i, admin in enumerate(list_admin, 1):
            safe_name = html.escape(str(admin.get("name") or ""))
            message_lines.append(f'{i}. ID: <code>{admin["id"]}</code> | {safe_name}')
    else:
        message_lines.append("• <i>Нет администраторов</i>")

    message_lines.append("\n<b>Пример:</b> <code>123456789</code>")

    await status_msg.delete()
    await query.message.answer(
        text="\n".join(message_lines),
        reply_markup=get_kb_cancel()
    )


@router_admin_panel.message(FormAddAdmin.id)
async def process_add_admin(message: types.Message, state: FSMContext) -> None:
    """Process adding new admin."""
    text = await _require_text(message)
    if text is None:
        return
    await state.update_data(id=text)

    state_data = await state.get_data()
    user_id = state_data.get('id')

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_admins = await user_connector.get_admin_user_ids()

    if user_id not in list_admins:
        await user_connector.set_user_to_admin(user_id)
        await message.answer("✅ Администратор добавлен.", reply_markup=get_kb_admin())
    else:
        await message.answer("❌ Администратор уже в списке. Проверьте данные.", reply_markup=get_kb_admin())

    await state.clear()


@router_admin_panel.callback_query(lambda c: c.data == "add_alpha")
async def add_alpha_start(query: types.CallbackQuery, state: FSMContext) -> None:
    """Start adding new alpha user."""
    await query.answer()

    status_msg = await query.message.answer("⏳ Загрузка списка пользователей...")

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_alpha = await user_connector.get_alpha()

    await state.set_state(FormAddAlpha.id)

    message_lines = [
        "📍 Админ-панель → Добавление пользователя (Alpha)\n",
        "💬 <b>Отправьте Telegram ID нового пользователя</b>\n",
        "📋 <b>Текущие пользователи (Alpha):</b>"
    ]

    if list_alpha:
        for i, alpha in enumerate(list_alpha, 1):
            safe_name = html.escape(str(alpha.get("name") or ""))
            message_lines.append(f'{i}. ID: <code>{alpha["id"]}</code> | {safe_name}')
    else:
        message_lines.append("• <i>Нет пользователей Alpha</i>")

    message_lines.append("\n<b>Пример:</b> <code>123456789</code>")

    await status_msg.delete()
    await query.message.answer(
        text="\n".join(message_lines),
        reply_markup=get_kb_cancel()
    )


@router_admin_panel.message(FormAddAlpha.id)
async def process_add_alpha(message: types.Message, state: FSMContext) -> None:
    """Process adding new alpha user."""
    text = await _require_text(message)
    if text is None:
        return
    await state.update_data(id=text)

    state_data = await state.get_data()
    user_id = state_data.get('id')

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_alpha = await user_connector.get_alpha_user_ids()

    if user_id not in list_alpha:
        await user_connector.set_user_to_alpha(user_id)
        await message.answer("✅ Пользователь добавлен.", reply_markup=get_kb_admin())
    else:
        await message.answer("❌ Пользователь уже в списке. Проверьте данные.", reply_markup=get_kb_admin())

    await state.clear()


@router_admin_panel.callback_query(lambda c: c.data == "remove_admin")
async def remove_user_start(query: types.CallbackQuery, state: FSMContext) -> None:
    """Start removing user/admin."""
    await query.answer()

    status_msg = await query.message.answer("⏳ Загрузка списка пользователей...")

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_admin = await user_connector.get_administrators()
    list_alpha = await user_connector.get_alpha()

    await state.set_state(FormRemoveUser.id)

    message_lines = [
        "📍 Админ-панель → Снятие прав\n",
        "⚠️ <b>Снятие прав администратора/пользователя</b>\n",
        "💬 Отправьте Telegram ID пользователя для снятия прав\n",
        "👥 <b>Администраторы:</b>"
    ]

    if list_admin:
        for i, admin in enumerate(list_admin, 1):
            safe_name = html.escape(str(admin.get("name") or ""))
            message_lines.append(f'{i}. ID: <code>{admin["id"]}</code> | {safe_name}')
    else:
        message_lines.append("• <i>Нет администраторов</i>")

    message_lines.append("\n👤 <b>Пользователи (Alpha):</b>")

    if list_alpha:
        for i, alpha in enumerate(list_alpha, 1):
            safe_name = html.escape(str(alpha.get("name") or ""))
            message_lines.append(f'{i}. ID: <code>{alpha["id"]}</code> | {safe_name}')
    else:
        message_lines.append("• <i>Нет пользователей Alpha</i>")

    message_lines.append("\n<b>Пример:</b> <code>123456789</code>")

    await status_msg.delete()
    await query.message.answer(
        text="\n".join(message_lines),
        reply_markup=get_kb_cancel()
    )


@router_admin_panel.message(FormRemoveUser.id)
async def process_remove_user(message: types.Message, state: FSMContext) -> None:
    """Process removing user/admin."""
    text = await _require_text(message)
    if text is None:
        return
    await state.update_data(id=text)

    state_data = await state.get_data()
    user_id = state_data.get('id')

    user_connector = UserDatabaseConnector(settings.DB_NAME)
    await user_connector.initialize()

    list_admins = await user_connector.get_admin_user_ids()
    list_alpha = await user_connector.get_alpha_user_ids()

    if user_id not in list_admins + list_alpha:
        await message.answer("❌ Пользователь не найден. Проверьте данные", reply_markup=get_kb_admin())
    else:
        await user_connector.set_user_to_user(user_id)
        await message.answer("✅ Права пользователя сняты.", reply_markup=get_kb_admin())

    await state.clear()


@router_admin_panel.callback_query(lambda c: c.data == "admin_access_backup_menu")
async def handle_access_backup_menu(callback_query: types.CallbackQuery, state: FSMContext):
    """Open Accesses backup submenu."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await state.clear()
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_admin_backup())
    await callback_query.answer()


@router_admin_panel.callback_query(lambda c: c.data == "admin_access_backup_back")
async def handle_access_backup_back(callback_query: types.CallbackQuery, state: FSMContext):
    """Return from Accesses backup submenu."""
    await state.clear()
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_admin())
    await callback_query.answer()


@router_admin_panel.callback_query(lambda c: c.data == "admin_access_backup_export")
async def handle_access_backup_download(callback_query: types.CallbackQuery):
    """Download Accesses table backup."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await callback_query.message.delete()
    await callback_query.answer()

    status_msg = await callback_query.message.answer("⏳ Экспорт таблицы доступов...")

    try:
        access_db = _get_access_db()

        query = f"SELECT login, token, container, type FROM {access_db.database}.Accesses ORDER BY login, container"
        result = await access_db._query_with_retry(query)

        if not result.result_rows:
            await status_msg.delete()
            await callback_query.message.answer("⚠ Таблица доступов пуста.")
            return

        rows = []
        for login, token, container, type_value in result.result_rows:
            if type_value and ":" in type_value:
                service, subtype = type_value.split(":", 1)
            else:
                service = type_value
                subtype = None

            rows.append({
                "login": login,
                "token": token,
                "container": container,
                "type": type_value,
                "service": service,
                "subtype": subtype,
            })

        df = pd.DataFrame(rows)
        ordered = ["login", "token", "container", "type", "service", "subtype"]
        df = df[[col for col in ordered if col in df.columns]]
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        await status_msg.delete()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"accesses_backup_{timestamp}.csv"
        await callback_query.message.answer_document(
            types.BufferedInputFile(csv_bytes, filename=filename),
            caption=f"📦 Резервная копия таблицы доступов\n\n"
                    f"📊 Записей: <b>{len(df)}</b>\n"
                    f"⚠️ Файл содержит токены. Храните безопасно!",
        )

        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_kb_admin_backup()
        )
    except Exception as exc:
        if 'status_msg' in locals():
            await status_msg.delete()
        await callback_query.message.answer(
            f"❌ <b>Ошибка при создании бэкапа:</b>\n\n"
            f"<code>{_format_error(exc)}</code>",
        )


@router_admin_panel.callback_query(lambda c: c.data == "admin_access_backup_import")
async def handle_access_backup_upload(callback_query: types.CallbackQuery, state: FSMContext):
    """Start Accesses table restore."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await state.set_state(AccessBackupStates.waiting_backup_file)

    await callback_query.message.answer(
        "📍 Админ-панель → Бэкап доступов → Импорт\n\n"
        "💬 <i>Восстановление таблицы доступов</i>\n\n"
        "Отправьте CSV-файл с колонками: <code>login, token, container, type</code>\n\n"
        "📌 <b>Требования:</b>\n"
        "• Формат: CSV или TSV\n"
        "• Обязательные колонки: <code>login, token</code>\n"
        "• Опциональные: <code>container, type, service, subtype</code>\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b> Текущая таблица доступов будет полностью заменена!",
        reply_markup=get_kb_cancel(),
    )
    await callback_query.answer()


@router_admin_panel.message(AccessBackupStates.waiting_backup_file)
async def process_access_backup_upload(message: types.Message, state: FSMContext):
    """Process uploaded Accesses backup file."""
    if not await _is_admin(message.from_user.id):
        await message.answer("Недостаточно прав")
        await state.clear()
        return

    document = message.document
    if document is None:
        await message.answer(
            "⚠ <b>Ошибка:</b> Нужно отправить файл.\n\n"
            "Прикрепите CSV или TSV файл с бэкапом таблицы доступов.",
            reply_markup=get_kb_cancel()
        )
        return

    filename = (document.file_name or "").lower()
    if not (filename.endswith(".csv") or filename.endswith(".tsv")):
        await message.answer(
            "⚠ <b>Ошибка:</b> Неверный формат файла.\n\n"
            f"Вы отправили: <code>{document.file_name or 'unknown'}</code>\n"
            "Ожидается файл с расширением <code>.csv</code> или <code>.tsv</code>",
            reply_markup=get_kb_cancel()
        )
        return

    status_msg = await message.answer("⏳ Обработка файла...")

    buffer = io.BytesIO()
    await message.bot.download(document, destination=buffer)
    buffer.seek(0)

    try:
        sep = "\t" if filename.endswith(".tsv") else ","
        df = pd.read_csv(buffer, dtype=str, sep=sep)
        df_prepared, stats = _prepare_access_backup_frame(df)
        if df_prepared.empty:
            await status_msg.delete()
            await message.answer(
                "⚠ <b>Ошибка:</b> В файле не найдено данных для восстановления.\n\n"
                "Проверьте, что файл содержит корректные данные.",
                reply_markup=get_kb_cancel()
            )
            return

        access_db = _get_access_db()
        await access_db._prepare_accesses()
        await access_db.truncate_table("Accesses")
        await access_db.insert_dataframe("Accesses", df_prepared)

        await status_msg.delete()

        parts = [
            f"✅ <b>Таблица доступов восстановлена!</b>\n",
            f"📊 <b>Статистика:</b>",
            f"• Записей импортировано: <b>{stats['kept']}</b>",
        ]
        if stats["dropped_empty"]:
            parts.append(f"• Пустых строк пропущено: <b>{stats['dropped_empty']}</b>")
        if stats["dropped_no_token"]:
            parts.append(f"• Строк без токена пропущено: <b>{stats['dropped_no_token']}</b>")

        await message.answer("\n".join(parts), reply_markup=get_kb_admin())
    except ValueError as exc:
        if 'status_msg' in locals():
            await status_msg.delete()
        reason = str(exc)
        if reason.startswith("missing_columns:"):
            missing = reason.split(":", 1)[1]
            await message.answer(
                f"⚠ <b>Ошибка:</b> Отсутствуют обязательные колонки.\n\n"
                f"Не найдено: <code>{missing}</code>\n\n"
                "Обязательные колонки: <code>login, token</code>",
                reply_markup=get_kb_cancel()
            )
        elif reason == "empty_backup":
            await message.answer(
                "⚠ <b>Ошибка:</b> Файл пустой или не содержит данных.\n\n"
                "Убедитесь, что CSV-файл содержит данные.",
                reply_markup=get_kb_cancel()
            )
        else:
            await message.answer(
                f"⚠ <b>Ошибка обработки файла:</b>\n\n"
                f"<code>{_format_error(exc)}</code>",
                reply_markup=get_kb_cancel()
            )
    except Exception as exc:
        if 'status_msg' in locals():
            await status_msg.delete()
        await message.answer(
            f"❌ <b>Ошибка восстановления таблицы доступов:</b>\n\n"
            f"<code>{_format_error(exc)}</code>",
            reply_markup=get_kb_cancel()
        )
    finally:
        await state.clear()


@router_admin_panel.callback_query(lambda c: c.data == "admin_upload_all_metrika")
async def handle_upload_all_metrika(callback_query: types.CallbackQuery, state: FSMContext):
    """Start Metrika bulk upload."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await callback_query.answer()
    await callback_query.message.answer(
        "📍 Админ-панель → Выгрузка Метрики\n\n"
        "💬 <i>Проверка изменений Яндекс.Метрики для всех клиентов</i>\n\n"
        "Укажите глубину проверки в днях\n\n"
        "📌 <b>Допустимые значения:</b> 1-365 дней\n"
        "<b>Пример:</b> <code>100</code> (последние 100 дней)",
        reply_markup=get_kb_cancel(),
    )
    await state.set_state(AdminUploadStates.waiting_metrika_lookback)


@router_admin_panel.message(AdminUploadStates.waiting_metrika_lookback)
async def process_metrika_lookback(message: types.Message, state: FSMContext):
    """Process Metrika lookback days and trigger upload."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        lookback_days = _parse_lookback(text)
        await message.answer(
            f"🔄 Запускаю проверку изменений за последние {lookback_days} дней.",
        )

        async def run():
            ran_via_prefect = await _run_prefect_required(
                settings.PREFECT_DEPLOYMENT_METRIKA_ALL,
                parameters={"track_changes": True, "lookback_days": lookback_days},
                notify_message=message,
            )
            if not ran_via_prefect:
                await message.answer(
                    "⏹ Запуск Метрики отменён (не удалось создать Prefect run).",
                    reply_markup=get_kb_admin(),
                )

        asyncio.create_task(run())
    except ValueError as exc:
        reason = str(exc)
        if reason == "non_positive":
            await message.answer(
                "⚠ <b>Ошибка:</b> Число дней должно быть больше нуля.\n\n"
                "Допустимые значения: 1-365",
                reply_markup=get_kb_cancel()
            )
        else:
            await message.answer(
                "⚠ <b>Ошибка:</b> Неверный формат.\n\n"
                f"Вы отправили: <code>{text}</code>\n\n"
                "Ожидается целое число от 1 до 365.\n"
                "<b>Пример:</b> <code>100</code>",
                reply_markup=get_kb_cancel()
            )
        return
    except Exception as exc:
        await message.answer(f"❌ Ошибка запуска выгрузки: {_format_error(exc)}.", reply_markup=get_kb_admin())
    finally:
        await state.clear()


@router_admin_panel.callback_query(lambda c: c.data == "admin_upload_all_calltouch")
async def handle_upload_all_calltouch(callback_query: types.CallbackQuery, state: FSMContext):
    """Start Calltouch bulk upload for all clients."""
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
    await state.set_state(AdminUploadStates.waiting_calltouch_tdelta)


@router_admin_panel.message(AdminUploadStates.waiting_calltouch_tdelta)
async def process_calltouch_tdelta(message: types.Message, state: FSMContext):
    """Process Calltouch lookback days and trigger upload."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        tdelta = _parse_lookback(text)
        await message.answer(
            f"🔄 Запускаю выгрузку Calltouch за последние {tdelta} дней.",
        )

        async def run():
            ran_via_prefect = await _run_prefect_required(
                settings.PREFECT_DEPLOYMENT_CALLTOUCH,
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


@router_admin_panel.callback_query(lambda c: c.data == "admin_upload_all_vk")
async def handle_upload_all_vk(callback_query: types.CallbackQuery, state: FSMContext):
    """Start VK bulk upload for all agencies."""
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
    await state.set_state(AdminUploadStates.waiting_vk_lookback)


@router_admin_panel.message(AdminUploadStates.waiting_vk_lookback)
async def process_vk_lookback(message: types.Message, state: FSMContext):
    """Process VK lookback days and trigger upload."""
    try:
        text = await _require_text(message)
        if text is None:
            return
        lookback_days = _parse_lookback(text)

        db = AsyncVkDatabase()
        await db.init_db()
        try:
            from prefect_loader.connectors.vk_loader.access import get_vk_agencies
            agencies = await get_vk_agencies(db)
            agencies_count = len(agencies)
        finally:
            await db.close_engine()

        if agencies_count == 0:
            await message.answer(
                "⚠ Нет агентств VK для выгрузки. Добавьте агентство сначала.",
                reply_markup=get_kb_admin()
            )
            await state.clear()
            return

        await message.answer(
            f"🔄 Запускаю выгрузку VK для {agencies_count} агентств за последние {lookback_days} дней.",
        )

        async def run():
            deployment = settings.PREFECT_DEPLOYMENT_VK
            ran_via_prefect = await _run_prefect_required(
                deployment,
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


@router_admin_panel.callback_query(lambda c: c.data == "admin_upload_all_direct")
async def handle_upload_all_direct(callback_query: types.CallbackQuery):
    """Start Direct bulk upload - select profile."""
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


@router_admin_panel.callback_query(
    lambda c: c.data in {"admin_direct_all_profile_analytics", "admin_direct_all_profile_light"}
)
async def process_direct_profile_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """Process Direct profile selection."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    profile = (
        DEFAULT_DB_PROFILE
        if callback_query.data.endswith("analytics")
        else "light"
    )

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
    await state.set_state(AdminUploadStates.waiting_direct_lookback)


@router_admin_panel.message(AdminUploadStates.waiting_direct_lookback)
async def process_direct_lookback(message: types.Message, state: FSMContext):
    """Process Direct lookback days and trigger upload."""
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
                settings.PREFECT_DEPLOYMENT_DIRECT_ANALYTICS if profile == "analytics"
                else settings.PREFECT_DEPLOYMENT_DIRECT_LIGHT
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


@router_admin_panel.callback_query(lambda c: c.data == "admin_reset_database")
async def handle_reset_database(callback_query: types.CallbackQuery):
    """Show database selection for reset."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="🗑️ Метрика",
                    callback_data="reset_db_metrika",
                ),
                types.InlineKeyboardButton(
                    text="🗑️ Директ",
                    callback_data="reset_db_direct",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🗑️ Calltouch",
                    callback_data="reset_db_calltouch",
                ),
                types.InlineKeyboardButton(
                    text="🗑️ VK Ads",
                    callback_data="reset_db_vk",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🗑️ Wordstat",
                    callback_data="reset_db_wordstat",
                ),
                types.InlineKeyboardButton(
                    text="🗑️ Кастомный",
                    callback_data="reset_db_custom",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🗑️ Все БД",
                    callback_data="reset_db_all",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="⬅️ Главное меню",
                    callback_data="command_back_main_menu",
                ),
            ],
        ]
    )
    await callback_query.message.answer(
        "⚠️ <b>Полная очистка базы данных</b>\n\n"
        "Выберите базу данных для очистки всех данных из таблиц:",
        reply_markup=keyboard,
    )
    await callback_query.answer()


@router_admin_panel.callback_query(
    lambda c: c.data in {
        "reset_db_metrika",
        "reset_db_direct",
        "reset_db_calltouch",
        "reset_db_vk",
        "reset_db_wordstat",
        "reset_db_custom",
        "reset_db_all",
    }
)
async def confirm_reset_database(callback_query: types.CallbackQuery):
    """Ask for confirmation before resetting."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    service = callback_query.data.replace("reset_db_", "")
    service_names = {
        "metrika": "Яндекс.Метрика",
        "direct": "Яндекс.Директ",
        "calltouch": "Calltouch",
        "vk": "VK Ads",
        "wordstat": "Wordstat",
        "custom": "Кастомный загрузчик",
        "all": "ВСЕ базы данных (Яндекс.Метрика, Яндекс.Директ, Calltouch, VK Ads, Wordstat, Кастомный загрузчик)",
    }

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="🗑️ Очистить данные (TRUNCATE)",
                    callback_data=f"confirm_truncate_{service}",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="cancel_reset",
                ),
            ],
        ]
    )

    await callback_query.message.answer(
        f"⚠️ <b>Полная очистка БД: {service_names[service]}</b>\n\n"
        f"• Удаляет все данные\n"
        f"• Сохраняет структуру таблиц\n"
        f"⚠️ Действие <b>НЕОБРАТИМО</b>!",
        reply_markup=keyboard,
    )
    await callback_query.answer()


@router_admin_panel.callback_query(lambda c: c.data.startswith("confirm_truncate_"))
async def execute_reset_database(callback_query: types.CallbackQuery):
    """Execute the database reset."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    service = callback_query.data.replace("confirm_truncate_", "")

    await callback_query.answer()
    await callback_query.message.answer("🔄 Выполняется очистка таблиц...")

    try:
        results = {}

        if service in ("metrika", "all"):
            db = AsyncMetrikaDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["Метрика"] = result["data_db"]

        if service in ("direct", "all"):
            db = AsyncDirectDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["Директ"] = result["data_db"]

        if service in ("calltouch", "all"):
            db = AsyncCalltouchDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["Calltouch"] = result["data_db"]

        if service in ("vk", "all"):
            db = AsyncVkDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["VK Ads"] = result["data_db"]

        if service in ("wordstat", "all") and AsyncWordstatDatabase is not None:
            db = AsyncWordstatDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["Wordstat"] = result["data_db"]

        if service in ("custom", "all") and AsyncCustomLoaderDatabase is not None:
            db = AsyncCustomLoaderDatabase()
            await db.init_db()
            result = await db.reset_database()
            results["Кастомный"] = result["data_db"]

        message_lines = [f"✅ <b>Очистка БД завершена</b>\n"]
        for db_name, count in results.items():
            message_lines.append(f"• {db_name}: очищено таблиц: <b>{count}</b>")

        await callback_query.message.answer(
            "\n".join(message_lines),
            reply_markup=get_kb_admin(),
        )
    except Exception as exc:
        await callback_query.message.answer(
            f"❌ Ошибка при очистке базы данных: {_format_error(exc)}",
            reply_markup=get_kb_admin(),
        )


@router_admin_panel.callback_query(lambda c: c.data == "cancel_reset")
async def cancel_reset_database(callback_query: types.CallbackQuery):
    """Cancel database reset."""
    await callback_query.answer("Отменено", show_alert=True)
    await callback_query.message.answer(
        "❌ Очистка базы данных отменена.",
        reply_markup=get_kb_admin(),
    )


@router_admin_panel.callback_query(lambda c: c.data == "admin_cleanup_stale")
async def handle_cleanup_stale(callback_query: types.CallbackQuery):
    """Show database selection for cleanup."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="🧹 Метрика",
                    callback_data="cleanup_stale_metrika",
                ),
                types.InlineKeyboardButton(
                    text="🧹 Директ",
                    callback_data="cleanup_stale_direct",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🧹 Calltouch",
                    callback_data="cleanup_stale_calltouch",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🧹 VK Ads",
                    callback_data="cleanup_stale_vk",
                ),
                types.InlineKeyboardButton(
                    text="🧹 Все БД",
                    callback_data="cleanup_stale_all",
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="⬅️ Главное меню",
                    callback_data="command_back_main_menu",
                ),
            ],
        ]
    )
    await callback_query.message.answer(
        "🧹 <b>Очистка старых клиентов</b>\n\n"
        "Выберите базу данных для поиска клиентов без обновлений >30 дней:",
        reply_markup=keyboard,
    )
    await callback_query.answer()


@router_admin_panel.callback_query(
    lambda c: c.data in {
        "cleanup_stale_metrika",
        "cleanup_stale_direct",
        "cleanup_stale_calltouch",
        "cleanup_stale_vk",
        "cleanup_stale_all",
    }
)
async def show_stale_clients(callback_query: types.CallbackQuery, state: FSMContext):
    """Show list of stale clients and ask for confirmation."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    await callback_query.message.delete()

    service = callback_query.data.replace("cleanup_stale_", "")
    service_names = {
        "metrika": "Яндекс.Метрика",
        "direct": "Яндекс.Директ",
        "calltouch": "Calltouch",
        "vk": "VK Ads",
        "all": "Все базы данных",
    }

    await callback_query.answer()
    await callback_query.message.answer("🔍 Поиск старых клиентов...")

    try:
        all_stale_clients = []

        if service in ("metrika", "all"):
            db = AsyncMetrikaDatabase()
            await db.init_db()
            clients = await db.get_stale_clients(days_threshold=30)
            for client in clients:
                client['database'] = 'metrika'
            all_stale_clients.extend(clients)

        if service in ("direct", "all"):
            db = AsyncDirectDatabase()
            await db.init_db()
            clients = await db.get_stale_clients(days_threshold=30)
            for client in clients:
                client['database'] = 'direct'
            all_stale_clients.extend(clients)

        if service in ("calltouch", "all"):
            db = AsyncCalltouchDatabase()
            await db.init_db()
            clients = await db.get_stale_clients(days_threshold=30)
            for client in clients:
                client['database'] = 'calltouch'
            all_stale_clients.extend(clients)

        if service in ("vk", "all"):
            db = AsyncVkDatabase()
            await db.init_db()
            clients = await db.get_stale_clients(days_threshold=30)
            for client in clients:
                client['database'] = 'vk'
            all_stale_clients.extend(clients)

        stale_clients = all_stale_clients

        if not stale_clients:
            await callback_query.message.answer(
                f"✅ В базе <b>{service_names[service]}</b> нет старых клиентов (все обновлены в последние 30 дней).",
                reply_markup=get_kb_admin(),
            )
            return

        df = pd.DataFrame(stale_clients)

        if service == "all":
            df = df[['table_name', 'max_date', 'days_since_update', 'database']]
            df.columns = ["Клиент/Таблица", "Последняя дата данных", "Дней без обновлений", "База данных"]
        else:
            df = df[['table_name', 'max_date', 'days_since_update']]
            df.columns = ["Клиент/Таблица", "Последняя дата данных", "Дней без обновлений"]

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Старые клиенты')

            worksheet = writer.sheets['Старые клиенты']
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)

        excel_buffer.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stale_clients_{service}_{timestamp}.xlsx"

        if service == "all":
            caption = (
                f"📊 <b>Старые клиенты: {len(stale_clients)}</b>\n"
                f"База: <b>{service_names[service]}</b>\n\n"
                f"💬 <b>Отправьте список клиентов для удаления</b> через запятую или пробел с указанием базы:\n"
                f"Пример: <code>direct:client_1, metrika:m_21555, calltouch:ch_2, vk:123456</code>"
            )
        else:
            example_map = {
                "metrika": "m_21555",
                "direct": "client_1",
                "calltouch": "ch_2",
                "vk": "123456",
            }
            example_value = example_map.get(service, "client_1")
            caption = (
                f"📊 <b>Старые клиенты: {len(stale_clients)}</b>\n"
                f"База: <b>{service_names[service]}</b>\n\n"
                f"💬 <b>Отправьте список клиентов для удаления</b> через запятую или пробел:\n"
                f"Пример: <code>{example_value}</code>"
            )

        await callback_query.message.answer_document(
            types.BufferedInputFile(
                excel_buffer.read(),
                filename=filename
            ),
            caption=caption,
            reply_markup=get_kb_cancel()
        )

        await state.update_data(service=service, stale_clients=stale_clients)
        await state.set_state(CleanupStates.waiting_client_list)

    except Exception as exc:
        await callback_query.message.answer(
            f"❌ Ошибка при поиске старых клиентов: {_format_error(exc)}",
            reply_markup=get_kb_admin(),
        )


@router_admin_panel.message(CleanupStates.waiting_client_list)
async def process_cleanup_list(message: types.Message, state: FSMContext):
    """Process client list and delete specified tables."""
    try:
        data = await state.get_data()
        service = data.get("service")

        text = await _require_text(message)
        if text is None:
            return
        client_input = text.strip()
        entries = [e.strip() for e in client_input.replace(",", " ").split() if e.strip()]

        if not entries:
            await message.answer("⚠️ Список клиентов пуст. Попробуйте снова:")
            return

        clients_by_db = {
            'metrika': [],
            'direct': [],
            'calltouch': [],
            'vk': [],
        }

        if service == "all":
            for entry in entries:
                if ':' in entry:
                    db, client = entry.split(':', 1)
                    db = db.strip().lower()
                    client = client.strip()
                    if db in clients_by_db and client:
                        clients_by_db[db].append(client)
                    else:
                        await message.answer(
                            f"⚠️ Неверный формат: <code>{entry}</code>\n"
                            f"Используйте формат: <code>direct:client_1, metrika:m_21555, calltouch:ch_2, vk:123456</code>"
                        )
                        return
                else:
                    await message.answer(
                        f"⚠️ Для удаления из всех БД требуется указать базу данных: <code>database:client_name</code>\n"
                        f"Пример: <code>direct:{entry}, metrika:m_21555, calltouch:ch_2, vk:123456</code>"
                    )
                    return
        else:
            for entry in entries:
                if ':' in entry:
                    db, client = entry.split(':', 1)
                    db = db.strip().lower()
                    client = client.strip()
                    if db == service and client:
                        clients_by_db[service].append(client)
                    else:
                        await message.answer(
                            f"⚠️ База данных <code>{db}</code> не соответствует выбранной <code>{service}</code>: <code>{entry}</code>"
                        )
                        return
                else:
                    clients_by_db[service].append(entry)

        total_clients = sum(len(clients) for clients in clients_by_db.values())
        if total_clients == 0:
            await message.answer("⚠️ Не найдено клиентов для удаления. Попробуйте снова:")
            return

        preview_parts = []
        for db, clients in clients_by_db.items():
            if clients:
                preview_parts.append(f"{db}: {', '.join(clients[:3])}{'...' if len(clients) > 3 else ''}")

        await message.answer(
            f"🔄 Удаление {total_clients} клиент(ов):\n" + "\n".join(preview_parts)
        )

        total_deleted = 0
        results = {}

        for db_name, clients in clients_by_db.items():
            if not clients:
                continue

            if db_name == "metrika":
                db = AsyncMetrikaDatabase()
                await db.init_db()
                deleted = await db.cleanup_clients(clients)
                results["Метрика"] = deleted
                total_deleted += deleted
            elif db_name == "direct":
                db = AsyncDirectDatabase()
                await db.init_db()
                deleted = await db.cleanup_clients(clients)
                results["Директ"] = deleted
                total_deleted += deleted
            elif db_name == "calltouch":
                db = AsyncCalltouchDatabase()
                await db.init_db()
                deleted = await db.cleanup_clients(clients)
                results["Calltouch"] = deleted
                total_deleted += deleted
            elif db_name == "vk":
                db = AsyncVkDatabase()
                await db.init_db()
                deleted = await db.cleanup_clients(clients)
                results["VK Ads"] = deleted
                total_deleted += deleted

        message_lines = [
            f"✅ <b>Очистка завершена</b>\n",
            f"• Запрошено: <b>{total_clients}</b> клиентов",
            f"• Удалено: <b>{total_deleted}</b> таблиц\n"
        ]

        if len(results) > 1:
            message_lines.append("<b>По базам данных:</b>")
            for db_name, count in results.items():
                message_lines.append(f"  • {db_name}: {count} таблиц")

        await message.answer(
            "\n".join(message_lines),
            reply_markup=get_kb_admin(),
        )

    except Exception as exc:
        await message.answer(
            f"❌ Ошибка при очистке: {_format_error(exc)}",
            reply_markup=get_kb_admin(),
        )
    finally:
        await state.clear()
