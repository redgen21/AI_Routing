#!/usr/bin/env bash
# Copy existing Korea and USA OSRM data to /data without rebuilding.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LEGACY_OSRM_ROOT="${LEGACY_OSRM_ROOT:-${SCRIPT_DIR}}"
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-/data/ai-routing/osrm}"
CLEAN_LEGACY_OSRM_DATA="${CLEAN_LEGACY_OSRM_DATA:-false}"
REGIONS=(south-korea socal georgia)

command -v rsync >/dev/null 2>&1 || {
  echo "rsync is required: sudo apt-get install -y rsync"
  exit 1
}

case "$(readlink -m "${OSRM_STORAGE_ROOT}")" in
  /data|/data/*) ;;
  *)
    echo "Refusing destination outside /data: ${OSRM_STORAGE_ROOT}"
    exit 1
    ;;
esac

mkdir -p "${OSRM_STORAGE_ROOT}"
test -w "${OSRM_STORAGE_ROOT}" || {
  echo "Destination is not writable: ${OSRM_STORAGE_ROOT}"
  exit 1
}

echo "Source:      ${LEGACY_OSRM_ROOT}"
echo "Destination: ${OSRM_STORAGE_ROOT}"
df -h "${OSRM_STORAGE_ROOT}"

for region in "${REGIONS[@]}"; do
  source_dir="${LEGACY_OSRM_ROOT}/${region}"
  target_dir="${OSRM_STORAGE_ROOT}/${region}"

  if [ ! -d "${source_dir}" ]; then
    echo "Missing source directory: ${source_dir}"
    exit 1
  fi

  unreadable_files="$(find "${source_dir}" -type f ! -readable -print)"
  if [ -n "${unreadable_files}" ]; then
    echo "Unreadable source files were found:"
    echo "${unreadable_files}"
    echo "Fix ownership, then run this script again:"
    echo "  sudo chown -R \$(id -un):\$(id -gn) \"${source_dir}\""
    exit 1
  fi

  echo "=== Copying ${region} ==="
  mkdir -p "${target_dir}"
  rsync -aH --info=progress2 "${source_dir}/" "${target_dir}/"

  pending_changes="$(rsync -aHn --delete --itemize-changes "${source_dir}/" "${target_dir}/")"
  if [ -n "${pending_changes}" ]; then
    echo "Copy verification failed for ${region}:"
    echo "${pending_changes}"
    exit 1
  fi
done

echo "=== Migration copy verified ==="
du -sh "${OSRM_STORAGE_ROOT}"/{south-korea,socal,georgia}

if [ "${CLEAN_LEGACY_OSRM_DATA}" = "true" ]; then
  for region in "${REGIONS[@]}"; do
    source_dir="$(readlink -f "${LEGACY_OSRM_ROOT}/${region}")"
    case "${source_dir}" in
      "${SCRIPT_DIR}"/south-korea|"${SCRIPT_DIR}"/socal|"${SCRIPT_DIR}"/georgia)
        rm -rf -- "${source_dir}"
        ;;
      *)
        echo "Refusing to remove unexpected source: ${source_dir}"
        exit 1
        ;;
    esac
  done
  echo "Legacy OSRM data removed."
else
  echo ""
  echo "Original data was retained. Restart and verify services, then clean it with:"
  echo "  CLEAN_LEGACY_OSRM_DATA=true bash ${SCRIPT_DIR}/migrate_osrm_data_to_data.sh"
fi
