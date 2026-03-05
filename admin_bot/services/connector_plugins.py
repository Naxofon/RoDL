import importlib
import importlib.util
import logging
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from orchestration.loader_registry import get_all_loaders, get_loader_config, is_loader_enabled

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ConnectorBotPlugin:
    loader_name: str
    display_name: str
    menu_callback: str
    router_ref: str
    keyboard_ref: str
    alpha_upload_callback: str | None = None


@dataclass(frozen=True)
class ConnectorAdminBotPlugin:
    loader_name: str
    button_text: str
    button_callback: str
    router_ref: str


def _fallback_display_name(loader_name: str) -> str:
    base_name = loader_name.removesuffix("_loader").replace("_", " ").strip()
    return base_name.title() if base_name else loader_name


def _default_plugin_ref(loader_name: str) -> str:
    return f"connectors/{loader_name}/bot/plugin.py:plugin"


def _load_module_from_file(module_path: Path):
    resolved_path = module_path.resolve()
    module_name = f"_bot_plugin_{abs(hash(str(resolved_path)))}"

    existing_module = sys.modules.get(module_name)
    if existing_module is not None:
        return existing_module

    spec = importlib.util.spec_from_file_location(module_name, resolved_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec from file '{resolved_path}'.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _import_ref(import_ref: str) -> Any:
    module_name, sep, attr_name = import_ref.partition(":")
    if not sep or not module_name or not attr_name:
        raise ValueError(f"Invalid import ref '{import_ref}'. Expected '<module>:<attr>'.")

    module_name = module_name.strip()
    attr_name = attr_name.strip()
    if not attr_name:
        raise ValueError(f"Invalid import ref '{import_ref}'. Attribute is empty.")

    path_candidate = Path(module_name)
    if path_candidate.suffix == ".py" or "/" in module_name:
        module_path = path_candidate if path_candidate.is_absolute() else (PROJECT_ROOT / path_candidate)
        if not module_path.exists():
            raise FileNotFoundError(f"Module file '{module_path}' does not exist.")
        module = _load_module_from_file(module_path)
    else:
        module = importlib.import_module(module_name)

    try:
        return getattr(module, attr_name)
    except AttributeError as exc:
        raise ImportError(f"Attribute '{attr_name}' not found in module '{module_name}'.") from exc


def _build_plugin(loader_name: str, loader_cfg: dict[str, Any], raw_plugin_cfg: Any) -> ConnectorBotPlugin:
    plugin_cfg = raw_plugin_cfg() if callable(raw_plugin_cfg) else raw_plugin_cfg
    if not isinstance(plugin_cfg, dict):
        raise TypeError(
            f"Bot plugin for '{loader_name}' must be a dict or callable returning dict, got {type(plugin_cfg).__name__}."
        )

    bot_cfg = loader_cfg.get("bot")
    if bot_cfg is None:
        bot_cfg = {}
    if not isinstance(bot_cfg, dict):
        raise TypeError(f"'bot' section for '{loader_name}' must be a dict.")

    menu_callback = str(bot_cfg.get("menu_callback") or plugin_cfg.get("menu_callback") or "").strip()
    router_ref = str(bot_cfg.get("router") or plugin_cfg.get("router") or "").strip()
    keyboard_ref = str(bot_cfg.get("keyboard") or plugin_cfg.get("keyboard") or "").strip()
    display_name = str(
        loader_cfg.get("display_name")
        or bot_cfg.get("display_name")
        or plugin_cfg.get("display_name")
        or _fallback_display_name(loader_name)
    ).strip()

    if not menu_callback:
        raise ValueError(f"Bot plugin for '{loader_name}' must define non-empty 'menu_callback'.")
    if not router_ref:
        raise ValueError(f"Bot plugin for '{loader_name}' must define non-empty 'router'.")
    if not keyboard_ref:
        raise ValueError(f"Bot plugin for '{loader_name}' must define non-empty 'keyboard'.")

    alpha_upload_callback = str(bot_cfg.get("alpha_upload_callback") or plugin_cfg.get("alpha_upload_callback") or "").strip()

    return ConnectorBotPlugin(
        loader_name=loader_name,
        display_name=display_name,
        menu_callback=menu_callback,
        router_ref=router_ref,
        keyboard_ref=keyboard_ref,
        alpha_upload_callback=alpha_upload_callback or None,
    )


def _build_admin_plugin(
    loader_name: str,
    loader_cfg: dict[str, Any],
    raw_plugin_cfg: Any,
) -> ConnectorAdminBotPlugin | None:
    plugin_cfg = raw_plugin_cfg() if callable(raw_plugin_cfg) else raw_plugin_cfg
    if not isinstance(plugin_cfg, dict):
        raise TypeError(
            f"Bot plugin for '{loader_name}' must be a dict or callable returning dict, got {type(plugin_cfg).__name__}."
        )

    bot_cfg = loader_cfg.get("bot")
    if bot_cfg is None:
        bot_cfg = {}
    if not isinstance(bot_cfg, dict):
        raise TypeError(f"'bot' section for '{loader_name}' must be a dict.")

    plugin_admin_cfg = plugin_cfg.get("admin")
    if plugin_admin_cfg is None:
        plugin_admin_cfg = {}
    if not isinstance(plugin_admin_cfg, dict):
        raise TypeError(f"'admin' section in plugin for '{loader_name}' must be a dict.")

    bot_admin_cfg = bot_cfg.get("admin")
    if bot_admin_cfg is None:
        bot_admin_cfg = {}
    if not isinstance(bot_admin_cfg, dict):
        raise TypeError(f"'bot.admin' section for '{loader_name}' must be a dict.")

    merged_admin_cfg = {**plugin_admin_cfg, **bot_admin_cfg}

    router_ref = str(merged_admin_cfg.get("router") or "").strip()
    button_text = str(merged_admin_cfg.get("button_text") or "").strip()
    button_callback = str(merged_admin_cfg.get("button_callback") or "").strip()

    if not router_ref and not button_text and not button_callback:
        return None

    if not router_ref:
        raise ValueError(f"Admin bot plugin for '{loader_name}' must define non-empty 'admin.router'.")
    if not button_text:
        raise ValueError(f"Admin bot plugin for '{loader_name}' must define non-empty 'admin.button_text'.")
    if not button_callback:
        raise ValueError(f"Admin bot plugin for '{loader_name}' must define non-empty 'admin.button_callback'.")

    return ConnectorAdminBotPlugin(
        loader_name=loader_name,
        button_text=button_text,
        button_callback=button_callback,
        router_ref=router_ref,
    )


@lru_cache(maxsize=1)
def get_enabled_connector_bot_plugins() -> tuple[ConnectorBotPlugin, ...]:
    plugins: list[ConnectorBotPlugin] = []
    callbacks_seen: set[str] = set()

    for loader_name in get_all_loaders():
        if not is_loader_enabled(loader_name):
            continue

        loader_cfg = get_loader_config(loader_name)
        if not isinstance(loader_cfg, dict):
            logger.warning("Loader config for '%s' is invalid and will be skipped.", loader_name)
            continue

        bot_cfg = loader_cfg.get("bot")
        if bot_cfg is not None and not isinstance(bot_cfg, dict):
            logger.warning("Loader '%s' has invalid 'bot' section and will be skipped.", loader_name)
            continue

        plugin_ref = str((bot_cfg or {}).get("plugin") or _default_plugin_ref(loader_name)).strip()
        try:
            raw_plugin_cfg = _import_ref(plugin_ref)
            plugin = _build_plugin(loader_name, loader_cfg, raw_plugin_cfg)
        except Exception as exc:
            logger.warning("Failed to load bot plugin for '%s' (%s): %s", loader_name, plugin_ref, exc)
            continue

        if plugin.menu_callback in callbacks_seen:
            logger.warning(
                "Duplicate bot menu callback '%s' (loader '%s'). Plugin skipped.",
                plugin.menu_callback,
                loader_name,
            )
            continue

        callbacks_seen.add(plugin.menu_callback)
        plugins.append(plugin)

    return tuple(plugins)


@lru_cache(maxsize=1)
def get_enabled_connector_admin_bot_plugins() -> tuple[ConnectorAdminBotPlugin, ...]:
    plugins: list[ConnectorAdminBotPlugin] = []
    callbacks_seen: set[str] = set()

    for loader_name in get_all_loaders():
        if not is_loader_enabled(loader_name):
            continue

        loader_cfg = get_loader_config(loader_name)
        if not isinstance(loader_cfg, dict):
            logger.warning("Loader config for '%s' is invalid and will be skipped.", loader_name)
            continue

        bot_cfg = loader_cfg.get("bot")
        if bot_cfg is not None and not isinstance(bot_cfg, dict):
            logger.warning("Loader '%s' has invalid 'bot' section and will be skipped.", loader_name)
            continue

        plugin_ref = str((bot_cfg or {}).get("plugin") or _default_plugin_ref(loader_name)).strip()
        try:
            raw_plugin_cfg = _import_ref(plugin_ref)
            plugin = _build_admin_plugin(loader_name, loader_cfg, raw_plugin_cfg)
        except Exception as exc:
            logger.warning("Failed to load admin bot plugin for '%s' (%s): %s", loader_name, plugin_ref, exc)
            continue

        if plugin is None:
            continue

        if plugin.button_callback in callbacks_seen:
            logger.warning(
                "Duplicate admin bot callback '%s' (loader '%s'). Plugin skipped.",
                plugin.button_callback,
                loader_name,
            )
            continue

        callbacks_seen.add(plugin.button_callback)
        plugins.append(plugin)

    return tuple(plugins)


def load_router_for_plugin(plugin: ConnectorBotPlugin) -> Any:
    return _import_ref(plugin.router_ref)


def _extract_alpha_rows(
    markup: InlineKeyboardMarkup,
    *,
    alpha_upload_callback: str,
) -> list[list[InlineKeyboardButton]]:
    upload_rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        upload_buttons = [
            button
            for button in row
            if (button.callback_data or "") == alpha_upload_callback
        ]
        if upload_buttons:
            upload_rows.append(upload_buttons)
    return upload_rows


def plugin_has_alpha_upload_action(plugin: ConnectorBotPlugin) -> bool:
    if not plugin.alpha_upload_callback:
        return False

    try:
        keyboard_factory = _import_ref(plugin.keyboard_ref)
    except Exception:
        return False

    if not callable(keyboard_factory):
        return False

    try:
        markup = keyboard_factory()
    except Exception:
        return False

    if not isinstance(markup, InlineKeyboardMarkup):
        return False

    return bool(_extract_alpha_rows(markup, alpha_upload_callback=plugin.alpha_upload_callback))


def build_keyboard_for_plugin(plugin: ConnectorBotPlugin, *, alpha_upload_only: bool = False) -> Any:
    keyboard_factory = _import_ref(plugin.keyboard_ref)
    if not callable(keyboard_factory):
        raise TypeError(
            f"Keyboard reference '{plugin.keyboard_ref}' for '{plugin.loader_name}' must resolve to a callable."
        )
    markup = keyboard_factory()
    if not alpha_upload_only:
        return markup

    if not isinstance(markup, InlineKeyboardMarkup):
        raise TypeError(
            f"Keyboard reference '{plugin.keyboard_ref}' for '{plugin.loader_name}' must return InlineKeyboardMarkup."
        )

    if not plugin.alpha_upload_callback:
        return InlineKeyboardMarkup(inline_keyboard=[])

    upload_rows = _extract_alpha_rows(markup, alpha_upload_callback=plugin.alpha_upload_callback)
    if not upload_rows:
        return InlineKeyboardMarkup(inline_keyboard=[])

    upload_rows.append(
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="command_back_main_menu")]
    )
    return InlineKeyboardMarkup(inline_keyboard=upload_rows)


def load_admin_router_for_plugin(plugin: ConnectorAdminBotPlugin) -> Any:
    return _import_ref(plugin.router_ref)
