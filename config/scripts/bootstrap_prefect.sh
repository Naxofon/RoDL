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

echo "[bootstrap] Deploying flows from orchestration/prefect.yaml..."
for deployment in direct-analytics-change direct-light-hourly direct-light-3h metrika-loader-clickhouse calltouch-loader-clickhouse vk-loader-daily wordstat-loader-clickhouse custom-loader-clickhouse; do
  prefect --no-prompt deploy --prefect-file orchestration/prefect.yaml --name "${deployment}"
done

echo "[bootstrap] Done."
