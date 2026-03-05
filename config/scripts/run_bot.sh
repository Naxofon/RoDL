#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[bot] Validating loader configuration..."
VALIDATE_LOADERS_QUIET=1 bash "${SCRIPT_DIR}/validate_loaders.sh"

echo "[bot] Starting admin bot..."
exec python admin_bot/app.py
