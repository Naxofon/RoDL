#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[bootstrap] Validating loader configuration..."
bash "${SCRIPT_DIR}/validate_loaders.sh"

echo "[bootstrap] Waiting for Prefect API at ${PREFECT_API_URL:-unset}..."
python - <<'PY'
import os
import sys
import time
import urllib.error
import urllib.request

api = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
deadline = time.time() + 120
url = f"{api.rstrip('/')}/health"

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if 200 <= resp.status < 300:
                sys.exit(0)
    except Exception:
        time.sleep(2)

print("Timed out waiting for Prefect API", file=sys.stderr)
sys.exit(1)
PY

POOL_NAME="${PREFECT_WORK_POOL:-default-agent-pool}"

if prefect work-pool inspect "${POOL_NAME}" >/dev/null 2>&1; then
  echo "[bootstrap] Work pool '${POOL_NAME}' already exists."
else
  echo "[bootstrap] Creating work pool '${POOL_NAME}'..."
  prefect work-pool create "${POOL_NAME}" --type process
fi

echo "[bootstrap] Deploying flows from connector prefect manifests..."
mapfile -t DEPLOYMENT_ITEMS < <(python3 - <<'PY'
from pathlib import Path
import sys

try:
    import yaml
except Exception as exc:
    print(f"[bootstrap] ERROR: failed to import PyYAML: {exc}", file=sys.stderr)
    sys.exit(1)

enabled_loaders: set[str] | None = None
try:
    from orchestration.loader_registry import get_enabled_loaders
    enabled_loaders = set(get_enabled_loaders())
except Exception as exc:
    print(
        f"[bootstrap] WARN: failed to read enabled loaders from config; "
        f"falling back to all connector manifests ({exc}).",
        file=sys.stderr,
    )

if enabled_loaders is None:
    manifest_files = sorted(Path("connectors").glob("*/prefect/prefect.yaml"))
else:
    manifest_files = []
    for loader_name in sorted(enabled_loaders):
        manifest_path = Path("connectors") / loader_name / "prefect" / "prefect.yaml"
        if manifest_path.exists():
            manifest_files.append(manifest_path)
        else:
            print(
                f"[bootstrap] Skipping '{loader_name}': prefect manifest not found at '{manifest_path}'.",
                file=sys.stderr,
            )

if not manifest_files:
    print("[bootstrap] No connector prefect manifests found for deployment.", file=sys.stderr)
    sys.exit(0)

seen_names: set[str] = set()
for manifest in manifest_files:
    try:
        config = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        print(f"[bootstrap] ERROR: failed to parse {manifest}: {exc}", file=sys.stderr)
        continue

    deployments = config.get("deployments") or []
    if not isinstance(deployments, list):
        print(f"[bootstrap] ERROR: 'deployments' in {manifest} must be a list", file=sys.stderr)
        continue

    for dep in deployments:
        if not isinstance(dep, dict):
            continue
        name = str(dep.get("name") or "").strip()
        entrypoint = str(dep.get("entrypoint") or "").strip()
        if not name or not entrypoint:
            continue
        if name in seen_names:
            continue
        entry_path = entrypoint.split(":", 1)[0].strip()
        if not entry_path:
            continue
        if not Path(entry_path).exists():
            print(
                f"[bootstrap] Skipping deployment '{name}' from '{manifest}': "
                f"entrypoint file '{entry_path}' not found.",
                file=sys.stderr,
            )
            continue
        seen_names.add(name)
        print(f"{manifest}|{name}")
PY
)

if [[ ${#DEPLOYMENT_ITEMS[@]} -eq 0 ]]; then
  echo "[bootstrap] No deployable flows found in connector manifests."
else
  for item in "${DEPLOYMENT_ITEMS[@]}"; do
    manifest="${item%%|*}"
    deployment="${item#*|}"
    if [[ -z "${manifest}" || -z "${deployment}" ]]; then
      continue
    fi
    prefect --no-prompt deploy --prefect-file "${manifest}" --name "${deployment}"
  done
fi

echo "[bootstrap] Done."
