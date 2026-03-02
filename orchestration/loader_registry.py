import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

logger = logging.getLogger(__name__)

_loader_config_cache: Optional[Dict[str, Any]] = None


def _get_config_path() -> Path:
    """
    Get the path to the loader configuration file.

    Returns:
        Path to loaders.yaml, respecting LOADER_CONFIG_PATH env var if set.
    """
    project_root = Path(__file__).parent.parent

    config_path_env = os.getenv("LOADER_CONFIG_PATH")
    if config_path_env:
        p = Path(config_path_env)
        return p if p.is_absolute() else project_root / p

    return project_root / "config" / "loaders.yaml"


def _load_config() -> Dict[str, Any]:
    """
    Load the loader configuration from YAML file.

    Returns:
        Dictionary with loader configuration.
        If config is missing or invalid, returns an empty loaders config.
    """
    global _loader_config_cache

    if _loader_config_cache is not None:
        return _loader_config_cache

    config_path = _get_config_path()

    if not config_path.exists():
        logger.error(
            "Loader config not found at %s. All loaders will be treated as disabled.",
            config_path,
        )
        _loader_config_cache = {"loaders": {}}
        return _loader_config_cache

    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)

        if not isinstance(config, dict) or "loaders" not in config or not isinstance(config.get("loaders"), dict):
            logger.error(
                "Invalid loader config at %s. All loaders will be treated as disabled.",
                config_path,
            )
            config = {"loaders": {}}

        _loader_config_cache = config
        logger.info("Loaded loader configuration from %s", config_path)
        return _loader_config_cache

    except Exception as exc:
        logger.error(
            "Error loading loader config from %s: %s. All loaders will be treated as disabled.",
            config_path,
            exc,
        )
        _loader_config_cache = {"loaders": {}}
        return _loader_config_cache


def is_loader_enabled(loader_name: str) -> bool:
    """
    Check if a loader is enabled in the configuration.

    Args:
        loader_name: Name of the loader (e.g., "wordstat_loader")

    Returns:
        True if loader is enabled, False otherwise.
        Defaults to False if loader is not found in config.
    """
    config = _load_config()
    loaders = config.get("loaders", {})
    loader_config = loaders.get(loader_name, {})

    return bool(loader_config.get("enabled", False))


def get_loader_config(loader_name: str) -> Dict[str, Any]:
    """
    Get the full configuration for a specific loader.

    Args:
        loader_name: Name of the loader (e.g., "wordstat_loader")

    Returns:
        Dictionary with loader configuration, or empty dict if not found.
    """
    config = _load_config()
    loaders = config.get("loaders", {})
    return loaders.get(loader_name, {})


def get_enabled_loaders() -> list[str]:
    """
    Get list of all enabled loader names.

    Returns:
        List of loader names that are currently enabled.
    """
    config = _load_config()
    loaders = config.get("loaders", {})

    return [
        loader_name
        for loader_name, loader_config in loaders.items()
        if bool(loader_config.get("enabled", False))
    ]


def get_all_loaders() -> list[str]:
    """
    Get list of all configured loader names.

    Returns:
        List of loader names from config/loaders.yaml (or LOADER_CONFIG_PATH).
    """
    config = _load_config()
    loaders = config.get("loaders", {})
    return list(loaders.keys())


def reload_config() -> None:
    """
    Force reload of the configuration from disk.
    Useful for testing or if config file changes at runtime.
    """
    global _loader_config_cache
    _loader_config_cache = None
    _load_config()
