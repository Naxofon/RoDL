import html

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command

from database.user import UserDatabaseConnector
from keyboards.inline import get_kb_main, get_kb_admin
from config.settings import settings


router_command = Router()

@router_command.message(Command("start"))
async def command_start_handler(message: types.Message, state: FSMContext) -> None:
    """Register new user or show main menu for authorized users."""
    db_connector = UserDatabaseConnector(settings.DB_NAME)
    await db_connector.initialize()

    user_id = message.from_user.id
    user_name = message.from_user.full_name

    existing_user = await db_connector.search_users_by_id(user_id)
    admin_user_ids = await db_connector.get_admin_user_ids()

    if not existing_user:
        await db_connector.add_user(user_id=user_id, name=user_name, role='User')
        user_role = 'User'
    else:
        _, _, user_role = existing_user

    await state.clear()

    if user_role == 'User':
        await message.answer(
            f"🤖 Привет, {html.escape(user_name)}!\n\n"
            "Заявка на доступ получена. Ожидайте подтверждения."
        )

        first_name = message.from_user.first_name or ""
        last_name = message.from_user.last_name or ""
        full_name = f"{first_name} {last_name}".strip()
        safe_name = html.escape(full_name or user_name)

        user_info = (
            "🤖 <b>Запрос доступа к боту</b>\n\n"
            f"Telegram ID: {message.from_user.id}\n"
            f"Имя: {safe_name}"
        )
        for ids in admin_user_ids:
            await message.bot.send_message(
                chat_id = int(ids),
                text = f'{user_info}',
            )

    elif user_role in ['Alpha', 'Admin']:
        await message.answer("🤖 Добро пожаловать!", reply_markup=get_kb_main())

    else:
        await message.answer("🤖 Авторизируйтесь через /start")


@router_command.message(Command("a"))
async def adm_client(message: types.Message, state: FSMContext) -> None:
    """Show admin panel for admins; notify admins of unauthorized access attempts."""
    db_connector = UserDatabaseConnector(settings.DB_NAME)
    await db_connector.initialize()

    admin_user_ids = await db_connector.get_admin_user_ids()
    user_id = str(message.from_user.id)

    if user_id in admin_user_ids:
        await state.clear()
        await message.answer(text="🤖 <i>Админ-панель</i>", reply_markup=get_kb_admin())
    else:
        first_name = message.from_user.first_name or ""
        last_name = message.from_user.last_name or ""
        full_name = f"{first_name} {last_name}".strip()
        safe_name = html.escape(full_name or message.from_user.full_name)

        user_info = (
            "🤖 <b>Запрос доступа в админ-панель</b>\n\n"
            f"Telegram ID: {message.from_user.id}\n"
            f"Имя: {safe_name}"
        )
        for ids in admin_user_ids:
            await message.bot.send_message(
                chat_id = int(ids),
                text = f'{user_info}',
            )


@router_command.message(Command("reg"))
async def register_admin_handler(message: types.Message, state: FSMContext) -> None:
    """Register calling user as admin using the configured registration code."""
    db_connector = UserDatabaseConnector(settings.DB_NAME)
    await db_connector.initialize()

    registration_code = settings.ADMIN_REGISTRATION_CODE

    if not registration_code:
        await message.answer("🤖 Регистрация администратора не настроена. Обратитесь к системному администратору.")
        return

    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    provided_code = parts[1].strip() if len(parts) > 1 else ""

    if not provided_code:
        await message.answer("🤖 Укажите код регистрации: <code>/reg КОД</code>")
        return

    if provided_code != registration_code:
        await message.answer("🤖 Неверный код регистрации.")
        return

    user_id = message.from_user.id
    user_name = message.from_user.full_name

    existing_user = await db_connector.search_users_by_id(user_id)

    if existing_user:
        await db_connector.set_user_to_admin(user_id)
    else:
        await db_connector.add_user(user_id=user_id, name=user_name, role='Admin')

    await state.clear()
    await message.answer(
        f"🤖 Успешная регистрация!\n\n{user_name}, вы зарегистрированы как администратор.\n\nИспользуйте /start для доступа к боту.",
        reply_markup=get_kb_main()
    )
