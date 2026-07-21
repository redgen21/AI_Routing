#!/usr/bin/env bash
set -euo pipefail

CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-10}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ENVIRONMENT="${ROUTING_ENVIRONMENT:-}"
if [ -z "${ENVIRONMENT}" ]; then
  case "$(basename "${ROOT}")" in
    development) ENVIRONMENT="development" ;;
    *) ENVIRONMENT="production" ;;
  esac
fi
if [ "${ENVIRONMENT}" = "development" ]; then
  DEFAULT_PORT=8056
  LOG_ENV=dev
else
  DEFAULT_PORT=8055
  LOG_ENV=prod
fi
HOST="${1:-0.0.0.0}"
PORT="${2:-${DEFAULT_PORT}}"
OUT_LOG="${ROOT}/log/smart_routing/${LOG_ENV}/api.out.log"
ERR_LOG="${ROOT}/log/smart_routing/${LOG_ENV}/api.err.log"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"

cd "${ROOT}"
select_python

echo "[watch] Smart Routing API watchdog started for ${HOST}:${PORT} (interval: ${CHECK_INTERVAL_SECONDS}s)"

while true; do
  if ! wait_for_http_status "http://127.0.0.1:${PORT}/api/v1/routing/health" 200 1; then
    echo "[watch] Smart Routing API is unhealthy. Restarting..."
    "${SCRIPT_DIR}/restart_smart_routing_api.sh" "${HOST}" "${PORT}" || \
      echo "[watch] Restart failed. Check ${OUT_LOG} / ${ERR_LOG}."
  fi
  sleep "${CHECK_INTERVAL_SECONDS}"
done
