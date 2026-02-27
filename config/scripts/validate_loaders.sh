#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "════════════════════════════════════════════════════════════"
echo "🔍 Validating Loader Configuration"
echo "════════════════════════════════════════════════════════════"

VALIDATE_PROJECT_ROOT="${PROJECT_ROOT}" python3 - <<'PYVALIDATE'
import sys
import os
from pathlib import Path

# Use project root passed from the calling script
project_root = os.environ.get("VALIDATE_PROJECT_ROOT", os.getcwd())
sys.path.insert(0, project_root)

try:
    from orchestration.loader_registry import (
        is_loader_enabled,
        get_loader_config,
        _load_config,
    )

    config = _load_config()
    all_loaders = sorted(config.get("loaders", {}).keys())
    enabled_loaders = [n for n in all_loaders if is_loader_enabled(n)]

    print(f"\n✓ Loader configuration loaded successfully")
    print(f"\n📦 Enabled loaders ({len(enabled_loaders)}):")

    for loader_name in all_loaders:
        enabled = is_loader_enabled(loader_name)
        cfg = get_loader_config(loader_name)
        display_name = cfg.get("display_name", loader_name)

        if enabled:
            print(f"   ✓ {loader_name:20s} - {display_name}")
        else:
            print(f"   ✗ {loader_name:20s} - DISABLED")

    print(f"\n💡 Configuration file: {os.getenv('LOADER_CONFIG_PATH', 'config/loaders.yaml')}")

    sys.exit(0)

except Exception as e:
    print(f"\n⚠️  Warning: Could not validate loader configuration: {e}")
    print("   Continuing with startup anyway...")
    sys.exit(0)  # Don't fail startup on validation errors

PYVALIDATE

echo "════════════════════════════════════════════════════════════"
echo ""
