#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-0.0.0.0}"
PORT="${2:-8055}"
OUT_LOG="smart_routing_api.out.log"
ERR_LOG="smart_routing_api.err.log"

cd "$(dirname "$0")"

if [ -x ".venv/bin/python" ]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="python3"
fi

echo "[restart] stopping existing Smart Routing API processes on port ${PORT}..."
if pgrep -f "sr_vrp_api_server.py.*--port ${PORT}" >/dev/null 2>&1; then
  pkill -f "sr_vrp_api_server.py.*--port ${PORT}" || true
  sleep 2
fi

if pgrep -f "sr_vrp_api_server.py.*--port ${PORT}" >/dev/null 2>&1; then
  echo "[restart] force killing remaining processes..."
  pkill -9 -f "sr_vrp_api_server.py.*--port ${PORT}" || true
  sleep 1
fi

echo "[restart] starting Smart Routing API on ${HOST}:${PORT}..."
nohup "${PYTHON_BIN}" sr_vrp_api_server.py --host "${HOST}" --port "${PORT}" > "${OUT_LOG}" 2> "${ERR_LOG}" &
sleep 2

if pgrep -f "sr_vrp_api_server.py.*--port ${PORT}" >/dev/null 2>&1; then
  echo "[restart] Smart Routing API started successfully."
  echo "[restart] python: ${PYTHON_BIN}"
  echo "[restart] process:"
  pgrep -af "sr_vrp_api_server.py.*--port ${PORT}"
  echo "[restart] logs:"
  echo "  out: ${OUT_LOG}"
  echo "  err: ${ERR_LOG}"
else
  echo "[restart] failed to start Smart Routing API."
  echo "[restart] check logs:"
  echo "  tail -n 200 ${OUT_LOG}"
  echo "  tail -n 200 ${ERR_LOG}"
  exit 1
fi
