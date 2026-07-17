#!/usr/bin/env bash
# Download and build OSRM MLD data for Bangkok, Jakarta, and Kuala Lumpur.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"
PROFILE="${SCRIPT_DIR}/profiles/custom_car_asia.lua"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend}"
FORCE_DOWNLOAD="${FORCE_DOWNLOAD:-false}"
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

# Format: "<dir_name>|<display_name>|<download_url>"
REGION_ENTRIES=(
  "thailand|Bangkok|https://download.geofabrik.de/asia/thailand-latest.osm.pbf"
  "indonesia|Jakarta|https://download.geofabrik.de/asia/indonesia-latest.osm.pbf"
  "malaysia-singapore-brunei|Kuala Lumpur|https://download.geofabrik.de/asia/malaysia-singapore-brunei-latest.osm.pbf"
)

download_file() {
  local url="$1"
  local output_path="$2"
  local tmp_path="${output_path}.tmp"

  rm -f "${tmp_path}"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --retry-delay 5 "${url}" -o "${tmp_path}"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "${tmp_path}" "${url}"
  else
    echo "Neither curl nor wget is available."
    exit 1
  fi
  mv -f "${tmp_path}" "${output_path}"
}

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but was not found."
  exit 1
}
docker info >/dev/null 2>&1 || {
  echo "Docker daemon is not available."
  exit 1
}
if [ ! -f "${PROFILE}" ]; then
  echo "Missing Asia Lua profile: ${PROFILE}"
  exit 1
fi
mkdir -p "${ASIA_STORAGE_ROOT}"
if [ ! -w "${ASIA_STORAGE_ROOT}" ]; then
  echo "Asia storage root is not writable: ${ASIA_STORAGE_ROOT}"
  exit 1
fi

echo "=== Asia OSRM build start ==="
echo "Image: ${OSRM_IMAGE}"
echo "Storage root: ${ASIA_STORAGE_ROOT}"
df -h "${ASIA_STORAGE_ROOT}" || true
docker pull "${OSRM_IMAGE}"

for entry in "${REGION_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name download_url <<< "${entry}"
  region_dir="${ASIA_STORAGE_ROOT}/${dir_name}"
  pbf_path="${region_dir}/${dir_name}-latest.osm.pbf"
  osrm_path="/data/${dir_name}/${dir_name}-latest.osrm"
  osrm_base="${region_dir}/${dir_name}-latest.osrm"

  mkdir -p "${region_dir}"
  if [ "${FORCE_DOWNLOAD}" = "true" ] || [ ! -s "${pbf_path}" ]; then
    echo "=== ${display_name}: downloading OSM PBF ==="
    echo "${download_url}"
    download_file "${download_url}" "${pbf_path}"
  else
    echo "=== ${display_name}: using existing PBF ${pbf_path} ==="
  fi

  echo "=== ${display_name}: clearing existing OSRM artifacts ==="
  find "${region_dir}" -maxdepth 1 -type f -name "${dir_name}-latest.osrm*" -delete

  echo "=== ${display_name}: extract ==="
  docker run --rm \
    --log-driver json-file \
    --log-opt "max-size=${LOG_MAX_SIZE}" \
    --log-opt "max-file=${LOG_MAX_FILE}" \
    -v "${ASIA_STORAGE_ROOT}:/data" \
    -v "${SCRIPT_DIR}/profiles:/profiles:ro" \
    "${OSRM_IMAGE}" \
    osrm-extract \
    -p "/profiles/custom_car_asia.lua" \
    "/data/${dir_name}/${dir_name}-latest.osm.pbf"

  echo "=== ${display_name}: partition ==="
  docker run --rm \
    -v "${ASIA_STORAGE_ROOT}:/data" \
    "${OSRM_IMAGE}" \
    osrm-partition "${osrm_path}"

  echo "=== ${display_name}: customize ==="
  docker run --rm \
    -v "${ASIA_STORAGE_ROOT}:/data" \
    "${OSRM_IMAGE}" \
    osrm-customize "${osrm_path}"

  echo "=== ${display_name}: verifying artifacts ==="
  for suffix in "${REQUIRED_SUFFIXES[@]}"; do
    artifact_path="${osrm_base}.${suffix}"
    if [ ! -f "${artifact_path}" ]; then
      echo "Missing required artifact: ${artifact_path}"
      exit 1
    fi
  done
done

echo "Asia OSRM build completed for ${#REGION_ENTRIES[@]} region(s)."
