import ast
import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType

from orchestration.loader_registry import get_all_loaders, is_loader_enabled

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

__all__: list[str] = []


def _is_exportable_flow_symbol(name: str) -> bool:
    if name.startswith("MAX_"):
        return True
    if name.endswith("_flow") or name.endswith("_task"):
        return True
    return (
        name.startswith("run_")
        or name.startswith("fetch_")
        or name.startswith("write_")
        or name.startswith("plan_")
        or name.startswith("detect_")
        or name.startswith("get_")
    )


def _load_module_from_path(loader_name: str, module_path: Path) -> ModuleType:
    module_name = f"_orchestration_flow_{loader_name}"
    cached_module = sys.modules.get(module_name)
    if cached_module is not None:
        return cached_module

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from '{module_path}'.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _collect_symbol_names(module_path: Path, module: ModuleType) -> list[str]:
    explicit_exports = getattr(module, "__all__", None)
    if isinstance(explicit_exports, (list, tuple, set)):
        candidates = [str(name) for name in explicit_exports if str(name)]
    else:
        candidates = []
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_exportable_flow_symbol(node.name):
                candidates.append(node.name)
                continue
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and _is_exportable_flow_symbol(target.id):
                        candidates.append(target.id)
                continue
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if _is_exportable_flow_symbol(node.target.id):
                    candidates.append(node.target.id)

    unique_names: list[str] = []
    for name in candidates:
        if name.startswith("_") or name in unique_names:
            continue
        if not hasattr(module, name):
            logger.warning("Flow symbol '%s' declared but not found in %s", name, module_path)
            continue
        unique_names.append(name)
    return unique_names


def _register_symbol(name: str, value, loader_name: str) -> None:
    existing = globals().get(name)
    if existing is not None and existing is not value:
        logger.warning(
            "Skipping duplicate flow symbol '%s' from '%s' (already registered).",
            name,
            loader_name,
        )
        return

    globals()[name] = value
    if name not in __all__:
        __all__.append(name)


def _load_enabled_connector_flows() -> None:
    for loader_name in get_all_loaders():
        if not is_loader_enabled(loader_name):
            continue

        module_path = PROJECT_ROOT / "connectors" / loader_name / "prefect" / "flows.py"
        if not module_path.exists():
            logger.warning(
                "Loader '%s' is enabled, but flow module is missing: %s",
                loader_name,
                module_path,
            )
            continue

        try:
            module = _load_module_from_path(loader_name, module_path)
            for symbol_name in _collect_symbol_names(module_path, module):
                _register_symbol(symbol_name, getattr(module, symbol_name), loader_name)
        except Exception as exc:
            logger.warning("Failed to load flow module for '%s': %s", loader_name, exc)


_load_enabled_connector_flows()
