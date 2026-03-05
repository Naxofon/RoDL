import logging
import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_db_config() -> dict[str, str]:
    """
    Load database names from loaders.yaml.

    Reads the top-level ``databases`` section and each loader's ``databases``
    sub-section from loaders.yaml, merging them into a single flat mapping.

    Precedence:
    1) Env ``LOADER_CONFIG_PATH`` → loaders.yaml
    2) ``config/loaders.yaml`` in project root
    """
    def _from_loaders_yaml(data: dict) -> dict[str, str]:
        result: dict[str, str] = {}
        top_dbs = data.get("databases")
        if isinstance(top_dbs, dict):
            result.update({str(k): str(v) for k, v in top_dbs.items() if v is not None})
        for loader_cfg in (data.get("loaders") or {}).values():
            if isinstance(loader_cfg, dict):
                loader_dbs = loader_cfg.get("databases")
                if isinstance(loader_dbs, dict):
                    result.update({str(k): str(v) for k, v in loader_dbs.items() if v is not None})
        return result

    candidates: list[Path] = []
    env_path = os.getenv("LOADER_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        candidates.append(p if p.is_absolute() else PROJECT_ROOT / p)
    candidates.append(PROJECT_ROOT / "config" / "loaders.yaml")

    for path in candidates:
        try:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            result = _from_loaders_yaml(data)
            if result:
                return result
        except Exception as exc:
            logging.warning("Failed to load DB config from %s: %s", path, exc)

    return {}


_DB_CONFIG = _load_db_config()


def _db_value(key: str, default: str) -> str:
    """Resolve database name from config, falling back to a static default."""
    if key in _DB_CONFIG and _DB_CONFIG[key]:
        return _DB_CONFIG[key]
    return default


def _bool_env(key: str, default: str = "false") -> bool:
    """Read an environment variable and interpret it as a boolean."""
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes"}


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123") or "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = _db_value("default", "loader")

CLICKHOUSE_DB_DIRECT_ANALYTICS = _db_value("direct_analytics", "loader_direct_analytics")
CLICKHOUSE_DB_DIRECT_LIGHT = _db_value("direct_light", "loader_direct_light")
CLICKHOUSE_DB_DIRECT = CLICKHOUSE_DB_DIRECT_ANALYTICS

CLICKHOUSE_DB_METRIKA = _db_value("metrika", "loader_metrika")
CLICKHOUSE_DB_CALLTOUCH = _db_value("calltouch", "loader_calltouch")
CLICKHOUSE_DB_VK = _db_value("vk", "loader_vk")
CLICKHOUSE_DB_WORDSTAT = _db_value("wordstat", "loader_wordstat")
CLICKHOUSE_DB_CUSTOM_LOADER = _db_value("custom_loader", "loader_custom")

CLICKHOUSE_ACCESS_DATABASE = _db_value("access", CLICKHOUSE_DATABASE)
CLICKHOUSE_ACCESS_USER = os.getenv("CLICKHOUSE_ACCESS_USER", CLICKHOUSE_USER)
CLICKHOUSE_ACCESS_PASSWORD = os.getenv("CLICKHOUSE_ACCESS_PASSWORD", CLICKHOUSE_PASSWORD)

CLICKHOUSE_ROOT_USER = (
    os.getenv("CLICKHOUSE_ROOT_USER")
    or CLICKHOUSE_ACCESS_USER
    or CLICKHOUSE_USER
)
CLICKHOUSE_ROOT_PASSWORD = (
    os.getenv("CLICKHOUSE_ROOT_PASSWORD")
    or CLICKHOUSE_ACCESS_PASSWORD
    or CLICKHOUSE_PASSWORD
)
CLICKHOUSE_SECURE = _bool_env("CLICKHOUSE_SECURE", "false")
CLICKHOUSE_POOL_SIZE = int(os.getenv("CLICKHOUSE_POOL_SIZE", "30") or "30")
CLICKHOUSE_POOL_BLOCK = _bool_env("CLICKHOUSE_POOL_BLOCK", "true")


__all__ = [
    "PROJECT_ROOT",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_DB_DIRECT_ANALYTICS",
    "CLICKHOUSE_DB_DIRECT_LIGHT",
    "CLICKHOUSE_DB_DIRECT",
    "CLICKHOUSE_DB_METRIKA",
    "CLICKHOUSE_DB_CALLTOUCH",
    "CLICKHOUSE_DB_VK",
    "CLICKHOUSE_DB_WORDSTAT",
    "CLICKHOUSE_DB_CUSTOM_LOADER",
    "CLICKHOUSE_ACCESS_DATABASE",
    "CLICKHOUSE_ACCESS_USER",
    "CLICKHOUSE_ACCESS_PASSWORD",
    "CLICKHOUSE_ROOT_USER",
    "CLICKHOUSE_ROOT_PASSWORD",
    "CLICKHOUSE_SECURE",
    "CLICKHOUSE_POOL_SIZE",
    "CLICKHOUSE_POOL_BLOCK",
]
