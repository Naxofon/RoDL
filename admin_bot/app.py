import asyncio
import logging
import sys
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.as_posix() not in sys.path:
    sys.path.insert(0, PROJECT_ROOT.as_posix())

from orchestration.loader_registry import is_loader_enabled
from config.settings import settings
from handlers.commands import router_command
from handlers.callbacks import router_main
from handlers.admin_panel import router_admin_panel
from handlers.logs import check_logs


logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def startup_handler(bot: Bot):
    """Handle bot startup."""
    logger.info("Bot is starting...")
    scheduler.add_job(
        check_logs,
        'interval',
        seconds=settings.LOG_CHECK_INTERVAL,
        args=(bot,)
    )
    scheduler.start()
    logger.info("Log monitoring scheduler started")


async def shutdown_handler(dp: Dispatcher, bot: Bot):
    """Handle bot shutdown."""
    logger.info("Stopping scheduler...")
    scheduler.shutdown()
    await bot.session.close()


_CONNECTOR_ROUTERS = [
    ("metrika_loader", "handlers.metrika", "router_metrika"),
    ("direct_loader", "handlers.direct", "router_direct"),
    ("calltouch_loader", "handlers.calltouch", "router_calltouch"),
    ("vk_loader", "handlers.vk", "router_vk")
]

async def main() -> None:
    """Main bot entry point."""
    if not settings.BOT_TOKEN:
        raise RuntimeError("Set ADMIN_BOT_TOKEN (or TELEGRAM_BOT_TOKEN) in .env file")

    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    dp.include_router(router_command)
    dp.include_router(router_main)
    dp.include_router(router_admin_panel)

    for loader_name, module_path, router_attr in _CONNECTOR_ROUTERS:
        if not is_loader_enabled(loader_name):
            continue
        try:
            import importlib
            module = importlib.import_module(module_path)
            router = getattr(module, router_attr)
            dp.include_router(router)
            logger.info(f"{loader_name} router registered")
        except ImportError as e:
            logger.warning(f"{loader_name} enabled but router import failed: {e}")

    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp.startup.register(startup_handler)
    dp.shutdown.register(lambda: logger.info("Bot is shutting down..."))

    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot)
    finally:
        await shutdown_handler(dp, bot)


if __name__ == "__main__":
    asyncio.run(main())