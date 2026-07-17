#!/usr/bin/env bash
# Start OSRM MLD servers for Bangkok, Jakarta, and Kuala Lumpur.

set -euo pipefail

DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend}"
PUBLIC_HOST="${OSRM_PUBLIC_HOST:-20.51.244.68}"
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

# Format: "<dir_name>|<container_name>|<display_name>|<host_port>|<healthcheck_lonlat>"
REGION_ENTRIES=(
  "thailand|osrm-thailand|Thailand|5003|100.5018,13.7563"
  "indonesia|osrm-indonesia|Indonesia|5004|106.8456,-6.2088"
  "malaysia-singapore-brunei|osrm-malaysia|Malaysia|5005|101.6869,3.1390"
)

wait_for_osrm() {
  local container_name="$1"
  local healthcheck_url="$2"
  local display_name="$3"

  echo "=== waiting for ${display_name} (${container_name}) ==="
  for _ in {1..60}; do
    if curl -fsS "${healthcheck_url}" | grep -q '"code":"Ok"'; then
      echo "${display_name} OK"
      return 0
    fi
    sleep 2
  done

  echo "${display_name} failed healthcheck"
  docker logs "${container_name}" || true
  return 1
}

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but was not found."
  exit 1
}
docker info >/dev/null 2>&1 || {
  echo "Docker daemon is not available."
  exit 1
}
if [ ! -d "${ASIA_STORAGE_ROOT}" ]; then
  echo "Asia storage root does not exist: ${ASIA_STORAGE_ROOT}"
  echo "Run ./install_asia_routing_stack.sh first."
  exit 1
fi

echo "=== Asia OSRM server restart ==="
for entry in "${REGION_ENTRIES[@]}"; do
  IFS='|' read -r dir_name container_name display_name host_port _ <<< "${entry}"
  osrm_base="${ASIA_STORAGE_ROOT}/${dir_name}/${dir_name}-latest.osrm"

  for suffix in "${REQUIRED_SUFFIXES[@]}"; do
    artifact_path="${osrm_base}.${suffix}"
    if [ ! -f "${artifact_path}" ]; then
      echo "Missing required artifact for ${display_name}: ${artifact_path}"
      echo "Run ./install_osrm_asia.sh first."
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
    -v "${ASIA_STORAGE_ROOT}:/data:ro" \
    "${OSRM_IMAGE}" \
    osrm-routed --algorithm mld \
    "/data/${dir_name}/${dir_name}-latest.osrm"
done

for entry in "${REGION_ENTRIES[@]}"; do
  IFS='|' read -r _ container_name display_name host_port healthcheck_lonlat <<< "${entry}"
  wait_for_osrm \
    "${container_name}" \
    "http://127.0.0.1:${host_port}/nearest/v1/driving/${healthcheck_lonlat}" \
    "${display_name}"
done

echo ""
echo "=== Asia OSRM servers are ready ==="
for entry in "${REGION_ENTRIES[@]}"; do
  IFS='|' read -r _ _ display_name host_port _ <<< "${entry}"
  echo "${display_name} -> http://${PUBLIC_HOST}:${host_port}"
done
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
