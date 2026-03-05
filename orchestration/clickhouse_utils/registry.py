import logging
import ast
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from orchestration.loader_registry import get_all_loaders, is_loader_enabled

from .config import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_DB_CALLTOUCH,
    CLICKHOUSE_DB_DIRECT,
    CLICKHOUSE_DB_DIRECT_ANALYTICS,
    CLICKHOUSE_DB_DIRECT_LIGHT,
    CLICKHOUSE_DB_METRIKA,
    CLICKHOUSE_DB_VK,
    CLICKHOUSE_DB_WORDSTAT,
    CLICKHOUSE_DB_CUSTOM_LOADER,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_POOL_BLOCK,
    CLICKHOUSE_POOL_SIZE,
    CLICKHOUSE_ROOT_PASSWORD,
    CLICKHOUSE_ROOT_USER,
    CLICKHOUSE_SECURE,
    CLICKHOUSE_USER,
    PROJECT_ROOT,
)
from .connection import ensure_database, get_client
from .helpers import detect_date_column, detect_visits_column, sanitize_login
from .database import ClickhouseDatabase
from .schema import (
    add_missing_columns,
    delete_by_date_range,
    ensure_table,
    insert_dataframe,
    upgrade_column_types,
    describe_columns,
)

logger = logging.getLogger(__name__)


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
    "get_client",
    "ensure_database",
    "detect_date_column",
    "detect_visits_column",
    "sanitize_login",
    "describe_columns",
    "ensure_table",
    "add_missing_columns",
    "upgrade_column_types",
    "insert_dataframe",
    "delete_by_date_range",
    "ClickhouseDatabase",
]


def _load_module_from_path(loader_name: str, module_path: str) -> ModuleType:
    dynamic_module_name = f"_orchestration_clickhouse_utils_{loader_name}"
    cached_module = sys.modules.get(dynamic_module_name)
    if cached_module is not None:
        return cached_module

    spec = importlib.util.spec_from_file_location(dynamic_module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from '{module_path}'.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[dynamic_module_name] = module
    spec.loader.exec_module(module)
    return module


def _collect_symbol_names(module_path: str, module: ModuleType) -> list[str]:
    explicit_exports = getattr(module, "__all__", None)
    if isinstance(explicit_exports, (list, tuple, set)):
        candidates = [str(name) for name in explicit_exports if str(name)]
    else:
        candidates = []
        tree = ast.parse(Path(module_path).read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                if node.name.startswith("Async") and node.name.endswith("Database"):
                    candidates.append(node.name)

    unique_names: list[str] = []
    for name in candidates:
        if name.startswith("_") or name in unique_names:
            continue
        if not hasattr(module, name):
            logger.warning("ClickHouse symbol '%s' declared but not found in %s", name, module_path)
            continue
        unique_names.append(name)
    return unique_names


def _register_symbol(name: str, value, loader_name: str) -> None:
    existing = globals().get(name)
    if existing is not None and existing is not value:
        logger.warning(
            "Skipping duplicate clickhouse symbol '%s' from '%s' (already registered).",
            name,
            loader_name,
        )
        return

    globals()[name] = value
    if name not in __all__:
        __all__.append(name)


def _load_enabled_connector_clickhouse_utils() -> None:
    for loader_name in get_all_loaders():
        if not is_loader_enabled(loader_name):
            continue

        module_path = PROJECT_ROOT / "connectors" / loader_name / "prefect" / "clickhouse_utils.py"
        if not module_path.exists():
            logger.warning(
                "Loader '%s' is enabled, but clickhouse utils module is missing: %s",
                loader_name,
                module_path,
            )
            continue

        try:
            module = _load_module_from_path(loader_name, module_path.as_posix())
            for symbol_name in _collect_symbol_names(module_path.as_posix(), module):
                _register_symbol(symbol_name, getattr(module, symbol_name), loader_name)
        except Exception as exc:
            logger.warning("Failed to load clickhouse utils for '%s': %s", loader_name, exc)


_load_enabled_connector_clickhouse_utils()
