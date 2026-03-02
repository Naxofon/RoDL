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

    PREFECT_DEPLOYMENT_METRIKA_ALL: str = os.getenv(
        "PREFECT_DEPLOYMENT_METRIKA_ALL",
        "metrika-loader-clickhouse/metrika-loader-clickhouse"
    )
    PREFECT_DEPLOYMENT_METRIKA_SINGLE: str = os.getenv(
        "PREFECT_DEPLOYMENT_METRIKA_SINGLE",
        PREFECT_DEPLOYMENT_METRIKA_ALL
    )

    PREFECT_DEPLOYMENT_DIRECT_ALL: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_ALL",
        "direct-loader-clickhouse/direct-analytics-change"
    )
    PREFECT_DEPLOYMENT_DIRECT_SINGLE: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_SINGLE",
        ""
    )
    PREFECT_DEPLOYMENT_DIRECT_SINGLE_ANALYTICS: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_SINGLE_ANALYTICS",
        PREFECT_DEPLOYMENT_DIRECT_SINGLE or "direct-loader-clickhouse/direct-analytics-change"
    )
    PREFECT_DEPLOYMENT_DIRECT_SINGLE_LIGHT: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_SINGLE_LIGHT",
        PREFECT_DEPLOYMENT_DIRECT_SINGLE or "direct-loader-clickhouse/direct-light-hourly"
    )

    PREFECT_DEPLOYMENT_CALLTOUCH: str = os.getenv(
        "PREFECT_DEPLOYMENT_CALLTOUCH",
        "calltouch-loader-clickhouse/calltouch-loader-clickhouse"
    )
    PREFECT_DEPLOYMENT_CALLTOUCH_SINGLE: str = os.getenv(
        "PREFECT_DEPLOYMENT_CALLTOUCH_SINGLE",
        PREFECT_DEPLOYMENT_CALLTOUCH
    )
    PREFECT_DEPLOYMENT_VK: str = os.getenv(
        "PREFECT_DEPLOYMENT_VK",
        "vk-loader-clickhouse/vk-loader-daily"
    )
    PREFECT_DEPLOYMENT_DIRECT_ANALYTICS: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_ANALYTICS",
        "direct-loader-clickhouse/direct-analytics-change"
    )
    PREFECT_DEPLOYMENT_DIRECT_LIGHT: str = os.getenv(
        "PREFECT_DEPLOYMENT_DIRECT_LIGHT",
        "direct-loader-clickhouse/direct-light-hourly"
    )

    DB_NAME: str = "user.db"

    LOG_CHECK_INTERVAL: int = 2100

    WORDSTAT_LOGIN: str = os.getenv("WORDSTAT_LOGIN", "default")

settings = Settings()