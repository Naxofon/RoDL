from config.settings import settings
from database.user import UserDatabaseConnector


_KNOWN_ROLES = {"User", "Alpha", "Admin"}


async def get_user_role(user_id: int) -> str:
    """Return user role from AdminUsers table; fallback to User."""
    connector = UserDatabaseConnector(settings.DB_NAME)
    await connector.initialize()
    row = await connector.search_users_by_id(user_id)
    if not row:
        return "User"

    role = str(row[2] or "").strip()
    return role if role in _KNOWN_ROLES else "User"


async def is_alpha_user(user_id: int) -> bool:
    return (await get_user_role(user_id)) == "Alpha"


async def is_admin_user(user_id: int) -> bool:
    return (await get_user_role(user_id)) == "Admin"
