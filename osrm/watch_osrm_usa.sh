#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-15}"

# Format: "<dir_name>|<display_name>|<host_port>|<healthcheck_lonlat>"
CITY_ENTRIES=(
  "socal|LA|5001|-118.2437,34.0522"
  "georgia|Atlanta|5002|-84.3880,33.7490"
)

cd "${BASE_DIR}"

echo "[watch] OSRM USA watchdog started (interval: ${CHECK_INTERVAL_SECONDS}s)"

while true; do
  needs_restart="false"
  docker_ps_output="$(docker ps --format '{{.Names}}' 2>/dev/null || true)"

  for entry in "${CITY_ENTRIES[@]}"; do
    IFS='|' read -r dir_name display_name host_port healthcheck_lonlat <<< "${entry}"
    container_name="osrm-${dir_name}"
    healthcheck_url="http://127.0.0.1:${host_port}/nearest/v1/driving/${healthcheck_lonlat}"

    if ! printf '%s\n' "${docker_ps_output}" | grep -Fx "${container_name}" >/dev/null 2>&1; then
      echo "[watch] ${display_name} container is not running."
      needs_restart="true"
      break
    fi
    if ! curl -fsS "${healthcheck_url}" >/dev/null 2>&1; then
      echo "[watch] ${display_name} healthcheck failed."
      needs_restart="true"
      break
    fi
  done

  if [ "${needs_restart}" = "true" ]; then
    echo "[watch] Restarting USA OSRM regions..."
    ./run_osrm_usa.sh || true
  fi

  sleep "${CHECK_INTERVAL_SECONDS}"
done
