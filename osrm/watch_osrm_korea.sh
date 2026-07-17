#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-15}"
CONTAINER="osrm-korea"
PORT="5000"
HEALTHCHECK_URL="http://127.0.0.1:${PORT}/nearest/v1/driving/126.9780,37.5665"

cd "${BASE_DIR}"

echo "[watch] OSRM Korea watchdog started (container: ${CONTAINER}, port: ${PORT}, interval: ${CHECK_INTERVAL_SECONDS}s)"

while true; do
  container_running="false"
  docker_ps_output="$(docker ps --format '{{.Names}}' 2>/dev/null || true)"
  if printf '%s\n' "${docker_ps_output}" | grep -Fx "${CONTAINER}" >/dev/null 2>&1; then
    container_running="true"
  fi

  if [ "${container_running}" != "true" ] || ! curl -fsS "${HEALTHCHECK_URL}" >/dev/null 2>&1; then
    echo "[watch] OSRM Korea unavailable. Starting/restarting..."
    ./run_osrm_korea.sh || true
  fi

  sleep "${CHECK_INTERVAL_SECONDS}"
done
