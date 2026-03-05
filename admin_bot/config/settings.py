import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path, override=False)


class Settings:
    """Bot configuration settings."""

    BOT_TOKEN: str | None = os.getenv("ADMIN_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

    ADMIN_REGISTRATION_CODE: str = os.getenv("ADMIN_REGISTRATION_CODE", "")

    PREFECT_API_URL: str = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")

    DB_NAME: str = "user.db"

    LOG_CHECK_INTERVAL: int = 2100

settings = Settings()
