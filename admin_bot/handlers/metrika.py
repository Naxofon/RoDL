import asyncio

from aiogram import Router, types
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from keyboards.inline import get_kb_main, get_kb_metrika, get_kb_cancel
from services.prefect_client import (
    trigger_prefect_run,
    wait_for_prefect_flow_run,
)
from config.settings import settings

from prefect_loader.orchestration.clickhouse_utils import AsyncMetrikaDatabase, sanitize_login

router_metrika = Router()

PREFECT_DEPLOYMENT_METRIKA_SINGLE = settings.PREFECT_DEPLOYMENT_METRIKA_SINGLE
PREFECT_DEPLOYMENT_METRIKA_ALL = settings.PREFECT_DEPLOYMENT_METRIKA_ALL
PREFECT_TAGS_SINGLE = ("admin_bot", "single_upload")
DEFAULT_DEPLOYMENT_METRIKA = "metrika-loader-clickhouse/metrika-loader-clickhouse"

class MetrikaStates(StatesGroup):
    add_client = State()
    remove_client = State()
    remove_agency = State()
    update_data = State()


async def _require_text(message: Message, *, reply_markup=None) -> str | None:
    if not message.text:
        await message.answer("⚠ Ожидается текстовое сообщение.", reply_markup=reply_markup)
        return None
    return message.text


@router_metrika.callback_query(lambda c: c.data == "command_ym_clients")
async def handle_ym_clients(callback_query: types.CallbackQuery):

    await callback_query.message.delete()

    db = AsyncMetrikaDatabase()
    df = await db.get_metrika_config_data()
    if df.empty:
        await callback_query.message.answer("⚠ Клиенты не найдены", reply_markup=get_kb_metrika())
    else:
        df_clients = df[['date', 'fact_login', 'counter_metric', 'token']]

        csv_bytes = df_clients.to_csv(index=False).encode('utf-8')

        input_file = types.BufferedInputFile(
            file=csv_bytes,
            filename="metrika_clients.csv"
        )

        await callback_query.message.answer_document(
            input_file,
            caption=f"📋 Клиенты Метрики: <b>{len(df_clients)}</b>"
        )

        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_kb_metrika()
        )
    await callback_query.answer()


async def _start_add_metrika_client(callback_query: CallbackQuery, state: FSMContext, account_type: str):
    account_label = "агентский" if account_type == "agency" else "клиентский"
    await state.update_data(account_type=account_type)
    await state.set_state(MetrikaStates.add_client)

    example = "yandex_login - token123" if account_type == "agency" else "yandex_login 123456 token123"

    await callback_query.message.answer(
        f"📍 Метрика → Добавление клиента ({account_label})\n\n"
        f"💬 <i>Добавление клиента (Яндекс.Метрика)</i>\n"
        f"Тип аккаунта: <b>{account_label}</b>\n\n"
        "Отправьте данные в формате:\n"
        "<code>yandex_login counter_id token</code>\n\n"
        "📌 <b>Особенности:</b>\n"
        f"• Для агентского: counter_id = <code>-</code>\n"
        f"• Агентский — для чтения списка избранных\n"
        f"• Клиентский — для выгрузки данных\n\n"
        f"<b>Пример:</b> <code>{example}</code>",
        reply_markup=get_kb_cancel()
    )
    await callback_query.answer()


@router_metrika.callback_query(lambda c: c.data == "command_add_ym_agent")
async def handle_add_ym_agent(callback_query: CallbackQuery, state: FSMContext):
    await _start_add_metrika_client(callback_query, state, account_type="agency")


@router_metrika.callback_query(lambda c: c.data == "command_add_ym_client")
async def handle_add_ym_client(callback_query: CallbackQuery, state: FSMContext):
    await _start_add_metrika_client(callback_query, state, account_type="client")

@router_metrika.message(MetrikaStates.add_client)
async def process_add_ym_client(message: Message, state: FSMContext):
    db = AsyncMetrikaDatabase()
    try:
        text = await _require_text(message)
        if text is None:
            return
        data = await state.get_data()
        account_type = data.get("account_type")
        if account_type not in ("agency", "client"):
            raise ValueError("type_missing")

        parts = text.split()
        if len(parts) != 3:
            raise ValueError("format")

        fact_login_raw, counter_metric_raw, token = parts
        sanitized_login = sanitize_login(fact_login_raw)
        if account_type == "agency" and counter_metric_raw.strip() in {"-", ""}:
            counter_metric = None
        else:
            counter_metric = int(counter_metric_raw)

        if account_type == "client":
            df_existing = await db.get_metrika_config_data()
            if not df_existing.empty and counter_metric in df_existing["counter_metric"].tolist():
                raise ValueError("duplicate_counter")

        await db.add_client_to_metrika_config(
            fact_login=sanitized_login,
            counter_metric=counter_metric,
            token=token,
            subtype=account_type,
        )
        await message.answer(
            f"✅ Клиент добавлен: {sanitized_login} / {counter_metric} ({account_type})",
            reply_markup=get_kb_metrika(),
        )
    except ValueError as ve:
        if str(ve) == "type":
            await message.answer(
                "⚠ Некорректный тип. Используйте <code>agency</code> или <code>client</code>.",
            )
        elif str(ve) == "login_required":
            await message.answer(
                "⚠ Укажите логин (yandex_login).",
            )
        elif str(ve) == "counter_required":
            await message.answer(
                "⚠ Для клиентского аккаунта укажите числовой counter_id.",
            )
        elif str(ve) == "type_missing":
            await message.answer(
                "⚠ Не выбран тип аккаунта. Используйте кнопки: «Агентский аккаунт» или «Клиентский аккаунт».",
            )
        elif str(ve) == "duplicate_counter":
            await message.answer(
                "⚠ Такой counter_id уже добавлен. Дубликаты не допускаются.",
            )
        elif str(ve) == "login_invalid_chars":
            await message.answer(
                "⚠ Логин может содержать только буквы/цифры и дефис. Символы '_' и '.' автоматически заменяются на '-'.",
            )
        elif str(ve) == "login_digits_only":
            await message.answer(
                "⚠ Логин не может состоять только из цифр.",
            )
        else:
            await message.answer(
                "⚠ Некорректный формат. Используйте (через пробелы):\n"
                "<code>yandex_login counter_id token</code>",
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка добавления клиента: {e}")
    finally:
        await state.clear()


@router_metrika.callback_query(lambda c: c.data == "command_remove_ym_clients")
async def handle_remove_ym_clients(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "📍 Метрика → Удаление клиента\n\n"
        "💬 <i>Удаление клиента (Яндекс.Метрика)</i>\n\n"
        "Отправьте ID счётчика для удаления клиента\n\n"
        "<b>Пример:</b> <code>123456</code>",
        reply_markup=get_kb_cancel()
    )
    await state.set_state(MetrikaStates.remove_client)
    await callback_query.answer()

@router_metrika.message(MetrikaStates.remove_client)
async def process_remove_ym_client(message: Message, state: FSMContext):
    db = AsyncMetrikaDatabase()
    try:
        text = await _require_text(message)
        if text is None:
            return
        counter_metric = int(text)
        await db.delete_client_by_counter_metric(counter_metric)
        await message.answer("✅ Клиент успешно удалён", reply_markup=get_kb_metrika())
    except ValueError:
        await message.answer("⚠ Некорректный формат. Используйте только цифры.")
    except Exception as e:
        await message.answer(f"❌ Ошибка удаления клиента: {e}")
    finally:
        await state.clear()


@router_metrika.callback_query(lambda c: c.data == "command_remove_ym_agency")
async def handle_remove_ym_agency(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "📍 Метрика → Удаление агентства\n\n"
        "💬 <i>Удаление агентского аккаунта (Яндекс.Метрика)</i>\n\n"
        "Отправьте логин Яндекс для агентства, который нужно удалить\n\n"
        "<b>Пример:</b> <code>agency_login</code>",
        reply_markup=get_kb_cancel()
    )
    await state.set_state(MetrikaStates.remove_agency)
    await callback_query.answer()


@router_metrika.message(MetrikaStates.remove_agency)
async def process_remove_ym_agency(message: Message, state: FSMContext):
    db = AsyncMetrikaDatabase()
    try:
        text = await _require_text(message)
        if text is None:
            return
        raw_login = text.strip()
        normalized_login = sanitize_login(raw_login)
        rows = await db._access_db.fetch_access_rows(service_type="metrika", include_null_type=True)
        targets = [
            row for row in rows
            if sanitize_login(row.get("login") or "") == normalized_login
        ]
        if not targets:
            await message.answer(f"⚠ Агентство с логином '{normalized_login}' не найдено.")
        else:
            for row in targets:
                await db._access_db.delete_access(
                    normalized_login,
                    service_type="metrika",
                    container=row.get("container"),
                    type_value=row.get("subtype"),
                )
            await message.answer(
                f"✅ Удалено агентских записей: {len(targets)} для '{normalized_login}'.",
                reply_markup=get_kb_metrika(),
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка удаления агентства: {e}")
    finally:
        await state.clear()


@router_metrika.callback_query(lambda c: c.data == "command_upload_ym_data")
async def handle_upload_ym_data(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "📍 Метрика → Выгрузка данных\n\n"
        "💬 <i>Запуск трекера изменений (Яндекс.Метрика)</i>\n\n"
        "Отправьте ID счётчика, который нужно обновить\n\n"
        "<b>Пример:</b> <code>123456</code>",
        reply_markup=get_kb_cancel()
    )
    await state.set_state(MetrikaStates.update_data)
    await callback_query.answer()

@router_metrika.message(MetrikaStates.update_data)
async def process_update_ym_data(message: Message, state: FSMContext):
    try:
        text = await _require_text(message)
        if text is None:
            return
        parts = text.split()
        if len(parts) != 1:
            raise ValueError("expected_single_counter")

        counter_metric = int(parts[0])
        deployment_name = PREFECT_DEPLOYMENT_METRIKA_SINGLE or PREFECT_DEPLOYMENT_METRIKA_ALL
        deployment_label = deployment_name or DEFAULT_DEPLOYMENT_METRIKA

        run_id = await trigger_prefect_run(
            deployment_name=deployment_label,
            parameters={
                "track_changes": True,
                "counter_id": counter_metric,
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
            reply_markup=get_kb_metrika(),
        )
    except ValueError:
        await message.answer(
            "⚠ Укажите только один ID счётчика (например, <code>123456</code>).",
            reply_markup=get_kb_metrika(),
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка запуска change tracker: {e}")
    finally:
        await state.clear()


@router_metrika.callback_query(lambda c: c.data == "command_back_main_menu")
async def handle_back_main_menu(callback_query: types.CallbackQuery):
    await callback_query.message.edit_reply_markup(reply_markup=get_kb_main())
    await callback_query.answer()
