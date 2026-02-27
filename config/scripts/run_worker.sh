#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[worker] Validating loader configuration..."
bash "${SCRIPT_DIR}/validate_loaders.sh"

API_URL="${PREFECT_API_URL:-http://prefect-server:4200/api}"
POOL="${PREFECT_WORK_POOL:-default-agent-pool}"
RESTART_DELAY="${PREFECT_WORKER_RESTART_DELAY:-10}"
WORKER_NAME="${PREFECT_WORKER_NAME:-$(hostname -s || hostname || echo worker)}"

wait_for_api() {
  while true; do
    if python - "$API_URL" <<'PY'
import sys
import urllib.request

url = sys.argv[1].rstrip("/") + "/health"
try:
    with urllib.request.urlopen(url, timeout=5) as resp:
        if 200 <= resp.status < 300:
            sys.exit(0)
except Exception:
    pass
sys.exit(1)
PY
    then
      return 0
    fi
    echo "[worker] Prefect API not reachable at ${API_URL}, retrying in 5s..."
    sleep 5
  done
}

while true; do
  wait_for_api
  echo "[worker] Starting Prefect worker '${WORKER_NAME}' for pool '${POOL}'..."
  set +e
  prefect worker start --pool "${POOL}" --name "${WORKER_NAME}"
  exit_code=$?
  set -e
  echo "[worker] Worker exited with code ${exit_code}, restarting in ${RESTART_DELAY}s..."
  sleep "${RESTART_DELAY}"
done
