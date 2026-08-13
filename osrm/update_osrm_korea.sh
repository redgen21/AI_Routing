#!/usr/bin/env bash
# Download latest South Korea PBF and rebuild OSRM artifacts

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend:latest}"
DATA_DIR="${OSRM_STORAGE_ROOT}/south-korea"
PBF_PATH="${DATA_DIR}/south-korea-latest.osm.pbf"
DOWNLOAD_URL="${DOWNLOAD_URL:-https://download.geofabrik.de/asia/south-korea-latest.osm.pbf}"
REQUIRED_SUFFIXES=(
  "" partition cells cell_metrics cnbg datasource_names ebg ebg_nodes edges
  fileIndex geometry icd mldgr names properties ramIndex tld tls
  turn_duration_penalties turn_weight_penalties
)

mkdir -p "${DATA_DIR}"
cd "${BASE_DIR}"
docker pull "${OSRM_IMAGE}"
IMAGE_ID="$(docker image inspect "${OSRM_IMAGE}" --format '{{.Id}}')"

echo "=== Updating Korea OSM PBF ==="
echo "Downloading: ${DOWNLOAD_URL}"

tmp_path="${PBF_PATH}.tmp"
rm -f "${tmp_path}"

if command -v curl >/dev/null 2>&1; then
  curl -fL "${DOWNLOAD_URL}" -o "${tmp_path}"
elif command -v wget >/dev/null 2>&1; then
  wget -O "${tmp_path}" "${DOWNLOAD_URL}"
else
  echo "Neither curl nor wget is available."
  exit 1
fi

artifacts_complete=true
for suffix in "${REQUIRED_SUFFIXES[@]}"; do
  if [ ! -s "${DATA_DIR}/south-korea-latest.osrm.${suffix}" ]; then
    artifacts_complete=false
    break
  fi
done
if [ -f "${PBF_PATH}" ] && cmp -s "${tmp_path}" "${PBF_PATH}" && [ "${artifacts_complete}" = "true" ]; then
  if [ -f "${DATA_DIR}/.osrm-image-id" ] && [ "$(cat "${DATA_DIR}/.osrm-image-id")" = "${IMAGE_ID}" ]; then
    rm -f "${tmp_path}"
    echo "Unchanged, artifacts complete, and image unchanged; build/restart skipped."
    exit 0
  fi
fi
mv -f "${tmp_path}" "${PBF_PATH}"
echo "Changed: ${PBF_PATH}"

echo "=== Rebuilding Korea OSRM artifacts ==="
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT}" bash "${BASE_DIR}/install_osrm_korea.sh"
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT}" bash "${BASE_DIR}/run_osrm_korea.sh"

echo "Korea OSRM update completed."
