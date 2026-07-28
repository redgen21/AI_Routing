#!/usr/bin/env bash
# OSRM USA server restart for one or more configured regions

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend:latest}"
LOG_MAX_SIZE="${OSRM_DOCKER_LOG_MAX_SIZE:-100m}"
LOG_MAX_FILE="${OSRM_DOCKER_LOG_MAX_FILE:-3}"
REQUIRED_SUFFIXES=(
  partition
  cells
  cell_metrics
  cnbg
  datasource_names
  ebg
  ebg_nodes
  edges
  fileIndex
  geometry
  icd
  mldgr
  names
  properties
  ramIndex
  tld
  tls
  turn_duration_penalties
  turn_weight_penalties
)

# Format: "<dir_name>|<display_name>|<host_port>|<healthcheck_lonlat>"
CITY_ENTRIES=(
  "socal|LA|5001|-118.2437,34.0522"
  "georgia|Atlanta|5002|-84.3880,33.7490"
  "northeast|Northeast|5006|-74.1724,40.7357"
  "san_diego|San Diego|5008|-117.1611,32.7157"
  "dc_metro|Washington DC|5009|-77.0369,38.9072"
)

SELECTED_REGIONS=("$@")

matches_selected_region() {
  local dir_name="$1"
  local display_name="$2"
  local selected
  if [ "${#SELECTED_REGIONS[@]}" -eq 0 ]; then
    return 0
  fi
  for selected in "${SELECTED_REGIONS[@]}"; do
    selected="$(echo "${selected}" | tr '[:upper:]' '[:lower:]')"
    if [ "${selected}" = "all" ] || [ "${selected}" = "$(echo "${dir_name}" | tr '[:upper:]' '[:lower:]')" ]; then
      return 0
    fi
    if echo "${display_name}" | tr '[:upper:]' '[:lower:]' | grep -q "${selected}"; then
      return 0
    fi
  done
  return 1
}

wait_for_osrm() {
  local container_name="$1"
  local healthcheck_url="$2"
  local display_name="$3"

  echo "=== waiting for ${display_name} (${container_name}) ==="
  for _ in {1..30}; do
    if curl -fsS "${healthcheck_url}" >/dev/null 2>&1; then
      echo "${display_name} OK"
      return 0
    fi
    sleep 2
  done

  echo "${display_name} failed healthcheck"
  docker logs "${container_name}" || true
  return 1
}

echo "=== USA OSRM server restart ==="

for entry in "${CITY_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name host_port _ <<< "${entry}"
  if ! matches_selected_region "${dir_name}" "${display_name}"; then
    continue
  fi
  osrm_base="${OSRM_STORAGE_ROOT}/${dir_name}/${dir_name}-latest.osrm"
  container_name="osrm-${dir_name}"

  for suffix in "${REQUIRED_SUFFIXES[@]}"; do
    artifact_path="${osrm_base}.${suffix}"
    if [ ! -f "${artifact_path}" ]; then
      echo "Missing required artifact for ${display_name}: ${artifact_path}"
      echo "Run ./install_osrm_usa.sh first."
      exit 1
    fi
  done

  docker stop "${container_name}" 2>/dev/null || true
  docker rm -f "${container_name}" 2>/dev/null || true

  echo "=== starting ${display_name} on port ${host_port} ==="
  docker run -d \
    --name "${container_name}" \
    --restart unless-stopped \
    --log-driver json-file \
    --log-opt "max-size=${LOG_MAX_SIZE}" \
    --log-opt "max-file=${LOG_MAX_FILE}" \
    -p "${host_port}:5000" \
    -v "${OSRM_STORAGE_ROOT}:/data:ro" \
    "${OSRM_IMAGE}" \
    osrm-routed --algorithm mld \
    "/data/${dir_name}/${dir_name}-latest.osrm"
done

for entry in "${CITY_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name host_port healthcheck_lonlat <<< "${entry}"
  if ! matches_selected_region "${dir_name}" "${display_name}"; then
    continue
  fi
  wait_for_osrm \
    "osrm-${dir_name}" \
    "http://127.0.0.1:${host_port}/nearest/v1/driving/${healthcheck_lonlat}" \
    "${display_name}"
done

echo ""
echo "=== USA OSRM servers are ready ==="
for entry in "${CITY_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name host_port _ <<< "${entry}"
  if ! matches_selected_region "${dir_name}" "${display_name}"; then
    continue
  fi
  echo "${display_name} -> http://20.51.244.68:${host_port}"
done
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
