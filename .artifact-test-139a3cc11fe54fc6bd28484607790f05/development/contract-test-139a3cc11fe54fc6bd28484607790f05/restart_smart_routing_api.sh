#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ENVIRONMENT="${ROUTING_ENVIRONMENT:-}"
if [ -z "${ENVIRONMENT}" ]; then
  case "$(basename "${ROOT}")" in
    development) ENVIRONMENT="development" ;;
    production) ENVIRONMENT="production" ;;
    *)
      if [ -f "${ROOT}/config_common_vrp.dev.json" ] && [ ! -f "${ROOT}/config_common_vrp.json" ]; then
        ENVIRONMENT="development"
      else
        ENVIRONMENT="production"
      fi
      ;;
  esac
fi
case "${ENVIRONMENT}" in
  development)
    export NA_DATA_CATALOG_PATH="${NA_DATA_CATALOG_PATH:-/home/csda/AI_Routing/shared/config/data_catalog.development.json}"
    export VRP_JOB_ROOT="${VRP_JOB_ROOT:-/home/csda/AI_Routing/state/development/vrp_api_jobs}"
    CONFIG="${COMMON_VRP_CONFIG_PATH:-${ROOT}/config_common_vrp.dev.json}"
    [ -f "${CONFIG}" ] || CONFIG="${ROOT}/config/common_vrp.dev.json"
    DEFAULT_PORT=8056
    LOG_ENV=dev
    ;;
  production)
    export NA_DATA_CATALOG_PATH="${NA_DATA_CATALOG_PATH:-/home/csda/AI_Routing/shared/config/data_catalog.production.json}"
    export VRP_JOB_ROOT="${VRP_JOB_ROOT:-/home/csda/AI_Routing/state/production/vrp_api_jobs}"
    CONFIG="${COMMON_VRP_CONFIG_PATH:-${ROOT}/config_common_vrp.json}"
    [ -f "${CONFIG}" ] || CONFIG="${ROOT}/config/common_vrp.prod.json"
    DEFAULT_PORT=8055
    LOG_ENV=prod
    ;;
  *)
    echo "ROUTING_ENVIRONMENT must be development or production: ${ENVIRONMENT}" >&2
    exit 1
    ;;
esac

HOST="${1:-0.0.0.0}"
PORT="${2:-${DEFAULT_PORT}}"
OUT_LOG="${ROOT}/log/smart_routing/${LOG_ENV}/api.out.log"
ERR_LOG="${ROOT}/log/smart_routing/${LOG_ENV}/api.err.log"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"

cd "${ROOT}"
mkdir -p "$(dirname "${OUT_LOG}")"
select_python

"${PYTHON_BIN}" verify_deployment.py \
  --config "${CONFIG}" --expected-environment "${ENVIRONMENT}"

echo "[restart] stopping existing Smart Routing API processes on port ${PORT}..."
stop_matching_processes \
  "sr_vrp_api_server.py.*--port ${PORT}" \
  "services/api/sr_vrp_api_server.py.*--port ${PORT}" \
  "(^|/)sr_vrp_api_server.py.*--port ${PORT}"
assert_tcp_port_free "${PORT}"

echo "[restart] starting Smart Routing API on ${HOST}:${PORT}..."
nohup "${PYTHON_BIN}" sr_vrp_api_server.py --host "${HOST}" --port "${PORT}" > "${OUT_LOG}" 2> "${ERR_LOG}" &
PID=$!

if wait_for_http_status "http://127.0.0.1:${PORT}/api/v1/routing/health" 200 30 "${PID}"; then
  echo "[restart] Smart Routing API is ready."
  echo "[restart] python: ${PYTHON_BIN}"
  echo "[restart] process: ${PID}"
  echo "[restart] logs:"
  echo "  out: ${OUT_LOG}"
  echo "  err: ${ERR_LOG}"
else
  kill "${PID}" >/dev/null 2>&1 || true
  echo "[restart] failed Smart Routing API readiness."
  echo "[restart] check logs:"
  echo "  tail -n 200 ${OUT_LOG}"
  echo "  tail -n 200 ${ERR_LOG}"
  exit 1
fi
