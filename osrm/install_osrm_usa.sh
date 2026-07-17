#!/usr/bin/env bash
# OSRM USA build for one or more configured regions

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE="${BASE_DIR}/profiles/custom_car.lua"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
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

# Format: "<dir_name>|<display_name>"
CITY_ENTRIES=(
  "socal|LA"
  "georgia|Atlanta"
  "northeast|Northeast"
  "san_diego|San Diego"
  "dc_metro|Washington DC"
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
    if [ "${selected}" = "all" ]; then
      return 0
    fi
    if [ "${selected}" = "$(echo "${dir_name}" | tr '[:upper:]' '[:lower:]')" ]; then
      return 0
    fi
    if echo "${display_name}" | tr '[:upper:]' '[:lower:]' | grep -q "${selected}"; then
      return 0
    fi
  done

  return 1
}

echo "=== USA OSRM build start ==="
if [ "${#SELECTED_REGIONS[@]}" -gt 0 ]; then
  echo "Selected region filter: ${SELECTED_REGIONS[*]}"
fi

if [ ! -f "${PROFILE}" ]; then
  echo "Missing Lua profile: ${PROFILE}"
  exit 1
fi

build_count=0
for entry in "${CITY_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name <<< "${entry}"
  if ! matches_selected_region "${dir_name}" "${display_name}"; then
    continue
  fi

  build_count=$((build_count + 1))
  city_dir="${OSRM_STORAGE_ROOT}/${dir_name}"
  pbf_path="${city_dir}/${dir_name}-latest.osm.pbf"
  osrm_path="/data/${dir_name}/${dir_name}-latest.osrm"
  osrm_base="${city_dir}/${dir_name}-latest.osrm"

  if [ ! -f "${pbf_path}" ]; then
    echo "Missing ${display_name} PBF file: ${pbf_path}"
    exit 1
  fi

  echo "=== ${display_name}: clearing existing OSRM artifacts ==="
  rm -f "${osrm_base}"*

  echo "=== ${display_name}: extract ==="
  docker run --rm \
    --log-driver json-file \
    --log-opt "max-size=${LOG_MAX_SIZE}" \
    --log-opt "max-file=${LOG_MAX_FILE}" \
    -v "${OSRM_STORAGE_ROOT}:/data" \
    -v "${BASE_DIR}/profiles:/profiles:ro" \
    ghcr.io/project-osrm/osrm-backend \
    osrm-extract \
    -p "/profiles/custom_car.lua" \
    "/data/${dir_name}/${dir_name}-latest.osm.pbf"

  echo "=== ${display_name}: partition ==="
  docker run --rm \
    --log-driver json-file \
    --log-opt "max-size=${LOG_MAX_SIZE}" \
    --log-opt "max-file=${LOG_MAX_FILE}" \
    -v "${OSRM_STORAGE_ROOT}:/data" \
    ghcr.io/project-osrm/osrm-backend \
    osrm-partition \
    "${osrm_path}"

  echo "=== ${display_name}: customize ==="
  docker run --rm \
    --log-driver json-file \
    --log-opt "max-size=${LOG_MAX_SIZE}" \
    --log-opt "max-file=${LOG_MAX_FILE}" \
    -v "${OSRM_STORAGE_ROOT}:/data" \
    ghcr.io/project-osrm/osrm-backend \
    osrm-customize \
    "${osrm_path}"

  echo "=== ${display_name}: verifying artifacts ==="
  for suffix in "${REQUIRED_SUFFIXES[@]}"; do
    artifact_path="${osrm_base}.${suffix}"
    if [ ! -f "${artifact_path}" ]; then
      echo "Missing required artifact: ${artifact_path}"
      exit 1
    fi
  done
done

if [ "${build_count}" -eq 0 ]; then
  echo "No USA OSRM region matched filter: ${SELECTED_REGIONS[*]}"
  exit 1
fi

echo "USA build completed for ${build_count} region(s)."
