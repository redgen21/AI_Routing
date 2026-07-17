#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-0.0.0.0}"
PORT="${2:-8065}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-10}"
OUT_LOG="common_vrp_api.out.log"
ERR_LOG="common_vrp_api.err.log"

cd "$(dirname "$0")"

if [ -x ".venv/bin/python" ]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="python3"
fi

echo "[watch] Common VRP API watchdog started for ${HOST}:${PORT} (interval: ${CHECK_INTERVAL_SECONDS}s)"

while true; do
  if ! pgrep -f "sr_common_vrp_api_server.py.*--port ${PORT}" >/dev/null 2>&1; then
    echo "[watch] Common VRP API not running on port ${PORT}. Starting..."
    nohup "${PYTHON_BIN}" sr_common_vrp_api_server.py --host "${HOST}" --port "${PORT}" > "${OUT_LOG}" 2> "${ERR_LOG}" &
    sleep 2
    if pgrep -f "sr_common_vrp_api_server.py.*--port ${PORT}" >/dev/null 2>&1; then
      echo "[watch] Common VRP API started successfully."
    else
      echo "[watch] Failed to start Common VRP API. Check ${OUT_LOG} / ${ERR_LOG}."
    fi
  fi
  sleep "${CHECK_INTERVAL_SECONDS}"
done
