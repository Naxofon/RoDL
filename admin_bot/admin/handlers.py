"""Admin panel handlers for user management and bulk uploads."""
import asyncio
import html
import importlib.util
import io
import logging
from datetime import datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.user import UserDatabaseConnector
from admin.keyboards import get_kb_admin, get_kb_admin_backup
from keyboards.inline import get_kb_cancel
from services.prefect_client import trigger_prefect_run, wait_for_prefect_flow_run
from config.settings import settings
from orchestration.loader_registry import get_all_loaders, get_loader_config, is_loader_enabled
from prefect_loader.orchestration.clickhouse_utils import (
    ClickhouseDatabase,
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_ACCESS_PASSWORD,
)

router_admin_panel = Router()
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MODULE_CACHE: dict[str, ModuleType] = {}

class FormAddAdmin(StatesGroup):
    """FSM for adding new admin."""
    id = State()


class FormAddAlpha(StatesGroup):
    """FSM for adding new alpha user."""
    id = State()


class FormRemoveUser(StatesGroup):
    """FSM for removing admin/alpha user."""
    id = State()


class CleanupStates(StatesGroup):
    """FSM for database cleanup operations."""
    waiting_client_list = State()

class AccessBackupStates(StatesGroup):
    """FSM for Accesses backup upload."""
    waiting_backup_file = State()




PREFECT_TAGS_BULK = ("admin_bot", "bulk_upload")


class _ConnectorAdminService:
    def __init__(self, loader_name: str, service_key: str, display_name: str, db_class: type):
        self.loader_name = loader_name
        self.service_key = service_key
        self.display_name = display_name
        self.db_class = db_class


def _fallback_display_name(loader_name: str) -> str:
    base_name = loader_name.removesuffix("_loader").replace("_", " ").strip()
    return base_name.title() if base_name else loader_name


def _load_module_from_path(module_path: Path, module_name: str) -> ModuleType:
    module_cache_key = f"{module_name}:{module_path.resolve()}"
    cached_module = _MODULE_CACHE.get(module_cache_key)
    if cached_module is not None:
        return cached_module

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec from file '{module_path}'.")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _MODULE_CACHE[module_cache_key] = module
    return module


def _resolve_connector_db_class(loader_name: str) -> type | None:
    module_path = PROJECT_ROOT / "connectors" / loader_name / "prefect" / "clickhouse_utils.py"
    if not module_path.exists():
        return None

    module = _load_module_from_path(module_path, f"_admin_connector_clickhouse_{loader_name}")
    candidates: list[str] = []

    explicit_exports = getattr(module, "__all__", None)
    if isinstance(explicit_exports, (list, tuple, set)):
        for export_name in explicit_exports:
            name = str(export_name)
            value = getattr(module, name, None)
            if isinstance(value, type) and name.startswith("Async") and name.endswith("Database"):
                candidates.append(name)

    if not candidates:
        for name, value in vars(module).items():
            if isinstance(value, type) and name.startswith("Async") and name.endswith("Database"):
                candidates.append(name)

    if not candidates:
        return None

    normalized_key = loader_name.replace("_loader", "").replace("_", "").lower()
    for name in candidates:
        if normalized_key and normalized_key in name.lower():
            return getattr(module, name)

    return getattr(module, candidates[0])


def _get_enabled_admin_services() -> dict[str, _ConnectorAdminService]:
    services: dict[str, _ConnectorAdminService] = {}

    for loader_name in get_all_loaders():
        if not is_loader_enabled(loader_name):
            continue

        try:
            db_class = _resolve_connector_db_class(loader_name)
        except Exception as exc:
            logger.warning("Failed to resolve DB class for loader '%s': %s", loader_name, exc)
            continue

        if db_class is None:
            logger.warning(
                "Loader '%s' is enabled but has no connector clickhouse DB class.",
                loader_name,
            )
            continue

        loader_cfg = get_loader_config(loader_name)
        display_name = str((loader_cfg or {}).get("display_name") or _fallback_display_name(loader_name)).strip()
        service_key = loader_name.removesuffix("_loader") or loader_name

        services[loader_name] = _ConnectorAdminService(
            loader_name=loader_name,
            service_key=service_key,
            display_name=display_name,
            db_class=db_class,
        )

    return services


def _get_services_with_method(method_name: str) -> dict[str, _ConnectorAdminService]:
    return {
        loader_name: service
        for loader_name, service in _get_enabled_admin_services().items()
        if callable(getattr(service.db_class, method_name, None))
    }


def _get_services_with_cleanup_support() -> dict[str, _ConnectorAdminService]:
    return {
        loader_name: service
        for loader_name, service in _get_enabled_admin_services().items()
        if callable(getattr(service.db_class, "get_stale_clients", None))
        and callable(getattr(service.db_class, "cleanup_clients", None))
    }


def _chunk_buttons(items: list[types.InlineKeyboardButton], size: int = 2) -> list[list[types.InlineKeyboardButton]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _extract_service_token(callback_data: str, modern_prefix: str, legacy_prefix: str) -> str | None:
    if callback_data.startswith(modern_prefix):
        return callback_data[len(modern_prefix) :]
    if callback_data.startswith(legacy_prefix):
        return callback_data[len(legacy_prefix) :]
    return None


def _service_aliases(service: _ConnectorAdminService) -> set[str]:
    aliases = {service.loader_name.lower(), service.service_key.lower()}
    aliases.add(service.loader_name.removesuffix("_loader").lower())
    return aliases


def _resolve_loader_by_token(
    token: str,
    services: dict[str, _ConnectorAdminService],
) -> str | None:
    normalized = token.strip().lower()
    if normalized == "all":
        return "all"
    if normalized in services:
        return normalized

    for loader_name, service in services.items():
        if normalized in _service_aliases(service):
            return loader_name
    return None


def _all_services_label(services: dict[str, _ConnectorAdminService]) -> str:
    if not services:
        return "нет доступных коннекторов"
    return ", ".join(service.display_name for service in services.values())


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




def _service_example_input(services: dict[str, _ConnectorAdminService], max_items: int = 4) -> str:
    aliases = [service.service_key for service in services.values()]
    if not aliases:
        return "database:client_1"
    return ", ".join(f"{alias}:client_1" for alias in aliases[:max_items])


def _service_alias_map(services: dict[str, _ConnectorAdminService]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for loader_name, service in services.items():
        for alias in _service_aliases(service):
            aliases[alias] = loader_name
    return aliases


async def _safe_close_db(db) -> None:
    close_method = getattr(db, "close_engine", None)
    if callable(close_method):
        try:
            await close_method()
        except Exception:
            pass


@router_admin_panel.callback_query(lambda c: c.data == "admin_reset_database")
async def handle_reset_database(callback_query: types.CallbackQuery):
    """Show database selection for reset."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    services = _get_services_with_method("reset_database")
    if not services:
        await callback_query.message.answer(
            "⚠️ Нет доступных коннекторов с поддержкой очистки БД.",
            reply_markup=get_kb_admin(),
        )
        await callback_query.answer()
        return

    service_buttons = [
        types.InlineKeyboardButton(
            text=f"🗑️ {service.display_name}",
            callback_data=f"reset_db:{loader_name}",
        )
        for loader_name, service in services.items()
    ]

    keyboard_rows = _chunk_buttons(service_buttons, size=2)
    keyboard_rows.append([
        types.InlineKeyboardButton(
            text="🗑️ Все БД",
            callback_data="reset_db:all",
        ),
    ])
    keyboard_rows.append([
        types.InlineKeyboardButton(
            text="⬅️ Главное меню",
            callback_data="command_back_main_menu",
        ),
    ])

    await callback_query.message.answer(
        "⚠️ <b>Полная очистка базы данных</b>\n\n"
        "Выберите базу данных для очистки всех данных из таблиц:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
    )
    await callback_query.answer()


@router_admin_panel.callback_query(
    lambda c: (c.data or "").startswith("reset_db:") or (c.data or "").startswith("reset_db_")
)
async def confirm_reset_database(callback_query: types.CallbackQuery):
    """Ask for confirmation before resetting."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    services = _get_services_with_method("reset_database")
    token = _extract_service_token(callback_query.data or "", "reset_db:", "reset_db_")
    if token is None:
        await callback_query.answer()
        return

    resolved = _resolve_loader_by_token(token, services)
    if resolved is None:
        await callback_query.answer("Неизвестный сервис", show_alert=True)
        return

    if resolved == "all":
        selected_label = f"ВСЕ базы данных ({_all_services_label(services)})"
    else:
        selected_label = services[resolved].display_name

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="🗑️ Очистить данные (TRUNCATE)",
                    callback_data=f"confirm_truncate:{resolved}",
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
        f"⚠️ <b>Полная очистка БД: {selected_label}</b>\n\n"
        "• Удаляет все данные\n"
        "• Сохраняет структуру таблиц\n"
        "⚠️ Действие <b>НЕОБРАТИМО</b>!",
        reply_markup=keyboard,
    )
    await callback_query.answer()


@router_admin_panel.callback_query(
    lambda c: (c.data or "").startswith("confirm_truncate:") or (c.data or "").startswith("confirm_truncate_")
)
async def execute_reset_database(callback_query: types.CallbackQuery):
    """Execute the database reset."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    services = _get_services_with_method("reset_database")
    token = _extract_service_token(callback_query.data or "", "confirm_truncate:", "confirm_truncate_")
    if token is None:
        await callback_query.answer()
        return

    resolved = _resolve_loader_by_token(token, services)
    if resolved is None:
        await callback_query.answer("Неизвестный сервис", show_alert=True)
        return

    selected_loaders = list(services.keys()) if resolved == "all" else [resolved]

    await callback_query.answer()
    await callback_query.message.answer("🔄 Выполняется очистка таблиц...")

    try:
        results: dict[str, int] = {}
        errors: list[str] = []

        for loader_name in selected_loaders:
            service = services[loader_name]
            db = service.db_class()
            try:
                init_method = getattr(db, "init_db", None)
                if callable(init_method):
                    await init_method()

                result = await db.reset_database()
                if isinstance(result, dict):
                    count = int(result.get("data_db") or 0)
                else:
                    try:
                        count = int(result)
                    except Exception:
                        count = 0
                results[service.display_name] = count
            except Exception as exc:
                errors.append(f"• {service.display_name}: {_format_error(exc)}")
            finally:
                await _safe_close_db(db)

        if not results and errors:
            await callback_query.message.answer(
                "❌ Ошибка при очистке базы данных:\n" + "\n".join(errors),
                reply_markup=get_kb_admin(),
            )
            return

        message_lines = ["✅ <b>Очистка БД завершена</b>\n"]
        for db_name, count in results.items():
            message_lines.append(f"• {db_name}: очищено таблиц: <b>{count}</b>")

        if errors:
            message_lines.append("\n⚠️ <b>Ошибки по отдельным базам:</b>")
            message_lines.extend(errors)

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

    services = _get_services_with_cleanup_support()
    if not services:
        await callback_query.message.answer(
            "⚠️ Нет доступных коннекторов с поддержкой очистки старых клиентов.",
            reply_markup=get_kb_admin(),
        )
        await callback_query.answer()
        return

    service_buttons = [
        types.InlineKeyboardButton(
            text=f"🧹 {service.display_name}",
            callback_data=f"cleanup_stale:{loader_name}",
        )
        for loader_name, service in services.items()
    ]

    keyboard_rows = _chunk_buttons(service_buttons, size=2)
    keyboard_rows.append([
        types.InlineKeyboardButton(
            text="🧹 Все БД",
            callback_data="cleanup_stale:all",
        ),
    ])
    keyboard_rows.append([
        types.InlineKeyboardButton(
            text="⬅️ Главное меню",
            callback_data="command_back_main_menu",
        ),
    ])

    await callback_query.message.answer(
        "🧹 <b>Очистка старых клиентов</b>\n\n"
        "Выберите базу данных для поиска клиентов без обновлений >30 дней:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
    )
    await callback_query.answer()


@router_admin_panel.callback_query(
    lambda c: (c.data or "").startswith("cleanup_stale:") or (c.data or "").startswith("cleanup_stale_")
)
async def show_stale_clients(callback_query: types.CallbackQuery, state: FSMContext):
    """Show list of stale clients and ask for confirmation."""
    if not await _is_admin(callback_query.from_user.id):
        await callback_query.answer("Недостаточно прав", show_alert=True)
        return

    services = _get_services_with_cleanup_support()
    token = _extract_service_token(callback_query.data or "", "cleanup_stale:", "cleanup_stale_")
    if token is None:
        await callback_query.answer()
        return

    resolved = _resolve_loader_by_token(token, services)
    if resolved is None:
        await callback_query.answer("Неизвестный сервис", show_alert=True)
        return

    selected_loaders = list(services.keys()) if resolved == "all" else [resolved]
    selected_label = "Все базы данных" if resolved == "all" else services[resolved].display_name

    await callback_query.message.delete()
    await callback_query.answer()
    await callback_query.message.answer("🔍 Поиск старых клиентов...")

    try:
        stale_clients: list[dict] = []

        for loader_name in selected_loaders:
            service = services[loader_name]
            db = service.db_class()
            try:
                init_method = getattr(db, "init_db", None)
                if callable(init_method):
                    await init_method()
                clients = await db.get_stale_clients(days_threshold=30)
            finally:
                await _safe_close_db(db)

            for client in clients or []:
                row = dict(client)
                row["database"] = service.service_key
                if "max_date" not in row and "min_date" in row:
                    row["max_date"] = row.get("min_date")
                stale_clients.append(row)

        if not stale_clients:
            await callback_query.message.answer(
                f"✅ В базе <b>{selected_label}</b> нет старых клиентов (все обновлены в последние 30 дней).",
                reply_markup=get_kb_admin(),
            )
            return

        df = pd.DataFrame(stale_clients)
        for column in ("table_name", "max_date", "days_since_update", "database"):
            if column not in df.columns:
                df[column] = ""

        if resolved == "all":
            df = df[["table_name", "max_date", "days_since_update", "database"]]
            df.columns = ["Клиент/Таблица", "Последняя дата данных", "Дней без обновлений", "База данных"]
        else:
            df = df[["table_name", "max_date", "days_since_update"]]
            df.columns = ["Клиент/Таблица", "Последняя дата данных", "Дней без обновлений"]

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Старые клиенты")
            worksheet = writer.sheets["Старые клиенты"]
            for idx, col in enumerate(df.columns):
                max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)

        excel_buffer.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stale_clients_{resolved}_{timestamp}.xlsx"
        examples = _service_example_input(services)

        if resolved == "all":
            caption = (
                f"📊 <b>Старые клиенты: {len(stale_clients)}</b>\n"
                f"База: <b>{selected_label}</b>\n\n"
                "💬 <b>Отправьте список клиентов для удаления</b> через запятую или пробел с указанием базы:\n"
                f"Пример: <code>{examples}</code>"
            )
        else:
            caption = (
                f"📊 <b>Старые клиенты: {len(stale_clients)}</b>\n"
                f"База: <b>{selected_label}</b>\n\n"
                "💬 <b>Отправьте список клиентов для удаления</b> через запятую или пробел:\n"
                "Пример: <code>client_1</code>"
            )

        await callback_query.message.answer_document(
            types.BufferedInputFile(excel_buffer.read(), filename=filename),
            caption=caption,
            reply_markup=get_kb_cancel(),
        )

        await state.update_data(service=resolved)
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
        selected_service = str(data.get("service") or "").strip().lower()

        services = _get_services_with_cleanup_support()
        if not services:
            await message.answer(
                "⚠️ Нет доступных коннекторов с поддержкой очистки старых клиентов.",
                reply_markup=get_kb_admin(),
            )
            return

        text = await _require_text(message)
        if text is None:
            return
        entries = [entry.strip() for entry in text.strip().replace(",", " ").split() if entry.strip()]

        if not entries:
            await message.answer("⚠️ Список клиентов пуст. Попробуйте снова:")
            return

        resolved_selected = _resolve_loader_by_token(selected_service, services)
        if resolved_selected is None:
            await message.answer(
                "⚠️ Выбранный сервис больше недоступен. Откройте раздел очистки заново.",
                reply_markup=get_kb_admin(),
            )
            return

        clients_by_loader: dict[str, list[str]] = {loader_name: [] for loader_name in services}
        aliases = _service_alias_map(services)
        examples = _service_example_input(services)

        if resolved_selected == "all":
            for entry in entries:
                if ":" not in entry:
                    await message.answer(
                        "⚠️ Для удаления из всех БД укажите формат <code>база:клиент</code>.\n"
                        f"Пример: <code>{examples}</code>"
                    )
                    return

                db_alias, client = entry.split(":", 1)
                target_loader = aliases.get(db_alias.strip().lower())
                client_name = client.strip()
                if not target_loader or not client_name:
                    await message.answer(
                        f"⚠️ Неверный формат: <code>{entry}</code>\n"
                        f"Пример: <code>{examples}</code>"
                    )
                    return
                clients_by_loader[target_loader].append(client_name)
        else:
            for entry in entries:
                if ":" in entry:
                    db_alias, client = entry.split(":", 1)
                    target_loader = aliases.get(db_alias.strip().lower())
                    client_name = client.strip()
                    if target_loader != resolved_selected or not client_name:
                        selected_label = services[resolved_selected].display_name
                        await message.answer(
                            f"⚠️ База данных в записи не соответствует выбранной <b>{selected_label}</b>: <code>{entry}</code>"
                        )
                        return
                    clients_by_loader[resolved_selected].append(client_name)
                else:
                    clients_by_loader[resolved_selected].append(entry)

        total_clients = sum(len(clients) for clients in clients_by_loader.values())
        if total_clients == 0:
            await message.answer("⚠️ Не найдено клиентов для удаления. Попробуйте снова:")
            return

        preview_parts = []
        for loader_name, clients in clients_by_loader.items():
            if not clients:
                continue
            preview_parts.append(
                f"{services[loader_name].service_key}: {', '.join(clients[:3])}{'...' if len(clients) > 3 else ''}"
            )

        await message.answer(f"🔄 Удаление {total_clients} клиент(ов):\n" + "\n".join(preview_parts))

        total_deleted = 0
        results: dict[str, int] = {}
        errors: list[str] = []

        for loader_name, clients in clients_by_loader.items():
            if not clients:
                continue

            service = services[loader_name]
            db = service.db_class()
            try:
                init_method = getattr(db, "init_db", None)
                if callable(init_method):
                    await init_method()

                deleted = await db.cleanup_clients(clients)
                deleted_count = int(deleted or 0)
                results[service.display_name] = deleted_count
                total_deleted += deleted_count
            except Exception as exc:
                errors.append(f"• {service.display_name}: {_format_error(exc)}")
            finally:
                await _safe_close_db(db)

        message_lines = [
            "✅ <b>Очистка завершена</b>\n",
            f"• Запрошено: <b>{total_clients}</b> клиентов",
            f"• Удалено: <b>{total_deleted}</b> таблиц\n",
        ]

        if results:
            message_lines.append("<b>По базам данных:</b>")
            for db_name, count in results.items():
                message_lines.append(f"• {db_name}: {count} таблиц")

        if errors:
            message_lines.append("\n⚠️ <b>Ошибки:</b>")
            message_lines.extend(errors)

        await message.answer("\n".join(message_lines), reply_markup=get_kb_admin())

    except Exception as exc:
        await message.answer(
            f"❌ Ошибка при очистке: {_format_error(exc)}",
            reply_markup=get_kb_admin(),
        )
    finally:
        await state.clear()
