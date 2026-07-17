#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8501}"
OUT_LOG="common_vrp_client_server.out.log"
ERR_LOG="common_vrp_client_server.err.log"

cd "$(dirname "$0")"

if [ -x ".venv/bin/streamlit" ]; then
  STREAMLIT_BIN=".venv/bin/streamlit"
else
  STREAMLIT_BIN="streamlit"
fi

echo "[restart] stopping existing server-backed Common VRP Client processes on port ${PORT}..."
if pgrep -f "streamlit run sr_common_vrp_client_server.py.*--server.port ${PORT}" >/dev/null 2>&1; then
  pkill -f "streamlit run sr_common_vrp_client_server.py.*--server.port ${PORT}" || true
  sleep 2
fi

echo "[restart] starting server-backed Common VRP Client on port ${PORT}..."
nohup "${STREAMLIT_BIN}" run sr_common_vrp_client_server.py \
  --server.port "${PORT}" \
  --server.address 0.0.0.0 \
  > "${OUT_LOG}" 2> "${ERR_LOG}" &
sleep 2

if pgrep -f "streamlit run sr_common_vrp_client_server.py.*--server.port ${PORT}" >/dev/null 2>&1; then
  echo "[restart] server-backed Common VRP Client started successfully."
  pgrep -af "streamlit run sr_common_vrp_client_server.py.*--server.port ${PORT}"
  echo "[restart] logs:"
  echo "  out: ${OUT_LOG}"
  echo "  err: ${ERR_LOG}"
else
  echo "[restart] failed to start server-backed Common VRP Client."
  echo "[restart] check logs:"
  echo "  tail -n 200 ${OUT_LOG}"
  echo "  tail -n 200 ${ERR_LOG}"
  exit 1
fi
