#!/usr/bin/env bash
# Download latest South Korea PBF and rebuild OSRM artifacts

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
DATA_DIR="${OSRM_STORAGE_ROOT}/south-korea"
PBF_PATH="${DATA_DIR}/south-korea-latest.osm.pbf"
DOWNLOAD_URL="${DOWNLOAD_URL:-https://download.geofabrik.de/asia/south-korea-latest.osm.pbf}"

mkdir -p "${DATA_DIR}"
cd "${BASE_DIR}"

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

mv -f "${tmp_path}" "${PBF_PATH}"
echo "Saved: ${PBF_PATH}"

echo "=== Rebuilding Korea OSRM artifacts ==="
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT}" bash "${BASE_DIR}/install_osrm_korea.sh"

echo "Korea OSRM update completed."
