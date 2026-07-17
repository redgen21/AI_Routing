#!/usr/bin/env bash
# Merge the Asia country extracts already downloaded for OSRM into one PBF for Nominatim.

set -euo pipefail

DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"
OUTPUT_DIR="${ASIA_STORAGE_ROOT}/nominatim"
OUTPUT_PBF="${OUTPUT_DIR}/southeast-asia-latest.osm.pbf"
TMP_PBF="${OUTPUT_PBF}.tmp"
OSMIUM_DOCKER_IMAGE="${OSMIUM_DOCKER_IMAGE:-debian:bookworm-slim}"
FORCE_MERGE="${FORCE_MERGE:-false}"
INPUT_PBFS=(
  "${ASIA_STORAGE_ROOT}/thailand/thailand-latest.osm.pbf"
  "${ASIA_STORAGE_ROOT}/indonesia/indonesia-latest.osm.pbf"
  "${ASIA_STORAGE_ROOT}/malaysia-singapore-brunei/malaysia-singapore-brunei-latest.osm.pbf"
)

for input_pbf in "${INPUT_PBFS[@]}"; do
  if [ ! -s "${input_pbf}" ]; then
    echo "Missing input PBF: ${input_pbf}"
    echo "Run ./install_osrm_asia.sh first."
    exit 1
  fi
done

needs_merge="${FORCE_MERGE}"
if [ ! -s "${OUTPUT_PBF}" ]; then
  needs_merge="true"
fi
if [ "${needs_merge}" != "true" ]; then
  for input_pbf in "${INPUT_PBFS[@]}"; do
    if [ "${input_pbf}" -nt "${OUTPUT_PBF}" ]; then
      needs_merge="true"
      break
    fi
  done
fi

if [ "${needs_merge}" != "true" ]; then
  echo "Merged Nominatim PBF is current: ${OUTPUT_PBF}"
  exit 0
fi

mkdir -p "${OUTPUT_DIR}"
rm -f "${TMP_PBF}"

echo "=== Merging Asia PBF files for Nominatim ==="
if command -v osmium >/dev/null 2>&1; then
  osmium merge \
    "${INPUT_PBFS[@]}" \
    -o "${TMP_PBF}" \
    -f pbf \
    --overwrite
else
  command -v docker >/dev/null 2>&1 || {
    echo "Either osmium-tool or Docker is required to merge PBF files."
    exit 1
  }
  docker info >/dev/null 2>&1 || {
    echo "Docker daemon is not available."
    exit 1
  }
  docker run --rm \
    -v "${ASIA_STORAGE_ROOT}:/data" \
    "${OSMIUM_DOCKER_IMAGE}" \
    sh -c '
      set -eu
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends osmium-tool ca-certificates
      rm -rf /var/lib/apt/lists/*
      osmium merge \
        /data/thailand/thailand-latest.osm.pbf \
        /data/indonesia/indonesia-latest.osm.pbf \
        /data/malaysia-singapore-brunei/malaysia-singapore-brunei-latest.osm.pbf \
        -o /data/nominatim/southeast-asia-latest.osm.pbf.tmp \
        -f pbf \
        --overwrite
    '
fi

if [ ! -s "${TMP_PBF}" ]; then
  echo "Merged PBF was not created: ${TMP_PBF}"
  exit 1
fi
mv -f "${TMP_PBF}" "${OUTPUT_PBF}"

echo "Merged PBF ready: ${OUTPUT_PBF}"
ls -lh "${OUTPUT_PBF}"
