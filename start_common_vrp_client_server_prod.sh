#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export NA_DATA_CATALOG_PATH="${NA_DATA_CATALOG_PATH:-/home/csda/AI_Routing/shared/config/data_catalog.production.json}"
CONFIG="${COMMON_VRP_CONFIG_PATH:-${ROOT}/config_common_vrp.json}"
[ -f "${CONFIG}" ] || CONFIG="${ROOT}/config/common_vrp.prod.json"
PORT="${1:-8501}"
LOG_DIR="${ROOT}/log/common_vrp/prod"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"

cd "${ROOT}"
mkdir -p "${LOG_DIR}"
select_python
select_streamlit
"${PYTHON_BIN}" verify_deployment.py \
  --config "${CONFIG}" --expected-environment production
"${PYTHON_BIN}" sr_common_vrp_api_server.py --config "${CONFIG}" \
  --host 0.0.0.0 --port 8065 --expected-environment production --check-config

stop_matching_processes "streamlit run sr_common_vrp_client_server.py.*--server.port ${PORT}"
assert_tcp_port_free "${PORT}"

COMMON_VRP_CONFIG_PATH="${CONFIG}" nohup "${STREAMLIT_BIN}" run sr_common_vrp_client_server.py \
  --server.address 0.0.0.0 --server.port "${PORT}" \
  > "${LOG_DIR}/client_server.out.log" 2> "${LOG_DIR}/client_server.err.log" &
PID=$!
if wait_for_http_status "http://127.0.0.1:${PORT}/_stcore/health" 200 30 "${PID}"; then
  echo "Production Common VRP client server is ready on port ${PORT} (pid ${PID})."
else
  kill "${PID}" >/dev/null 2>&1 || true
  echo "Production client server failed readiness; see ${LOG_DIR}/client_server.err.log" >&2
  exit 1
fi
