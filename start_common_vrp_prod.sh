#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export NA_DATA_CATALOG_PATH="${NA_DATA_CATALOG_PATH:-/home/csda/AI_Routing/shared/config/data_catalog.production.json}"
CONFIG="${COMMON_VRP_CONFIG_PATH:-${ROOT}/config_common_vrp.json}"
[ -f "${CONFIG}" ] || CONFIG="${ROOT}/config/common_vrp.prod.json"
HOST="${1:-0.0.0.0}"
PORT="${2:-8065}"
LOG_DIR="${ROOT}/log/common_vrp/prod"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"

cd "${ROOT}"
mkdir -p "${LOG_DIR}"
select_python

"${PYTHON_BIN}" verify_deployment.py \
  --config "${CONFIG}" --expected-environment production

"${PYTHON_BIN}" sr_common_vrp_api_server.py --config "${CONFIG}" \
  --host "${HOST}" --port "${PORT}" --expected-environment production --check-config

stop_matching_processes \
  "sr_common_vrp_api_server.py.*--port ${PORT}" \
  "services/api/run_common_vrp_api.py.*--port ${PORT}" \
  "services/api/sr_common_vrp_api_server.py.*--port ${PORT}" \
  "(^|/)sr_common_vrp_api_server.py.*--port ${PORT}"
assert_tcp_port_free "${PORT}"

nohup "${PYTHON_BIN}" sr_common_vrp_api_server.py \
  --config "${CONFIG}" --host "${HOST}" --port "${PORT}" --expected-environment production \
  > "${LOG_DIR}/api.out.log" 2> "${LOG_DIR}/api.err.log" &
PID=$!
if wait_for_http_status "http://127.0.0.1:${PORT}/api/v1/common/contexts" 200 30 "${PID}"; then
  echo "Production Common VRP API is ready on ${HOST}:${PORT} (pid ${PID})."
else
  kill "${PID}" >/dev/null 2>&1 || true
  echo "Production Common VRP API failed readiness; see ${LOG_DIR}/api.err.log" >&2
  exit 1
fi
