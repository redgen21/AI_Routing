#!/usr/bin/env bash
# Download latest USA region PBF files and rebuild OSRM artifacts

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"

# Format: "<dir_name>|<display_name>|<download_url>"
CITY_ENTRIES=(
  "socal|LA|https://download.geofabrik.de/north-america/us/california-latest.osm.pbf"
  "georgia|Atlanta|https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf"
  "northeast|Northeast|https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf"
  "san_diego|San Diego|https://download.geofabrik.de/north-america/us/california-latest.osm.pbf"
  "dc_metro|Washington DC|https://download.geofabrik.de/north-america/us/district-of-columbia-latest.osm.pbf"
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

cd "${BASE_DIR}"
if [ "${#SELECTED_REGIONS[@]}" -gt 0 ]; then
  echo "Selected region filter: ${SELECTED_REGIONS[*]}"
fi

update_count=0
for entry in "${CITY_ENTRIES[@]}"; do
  IFS='|' read -r dir_name display_name download_url <<< "${entry}"
  if ! matches_selected_region "${dir_name}" "${display_name}"; then
    continue
  fi

  update_count=$((update_count + 1))
  city_dir="${OSRM_STORAGE_ROOT}/${dir_name}"
  pbf_path="${city_dir}/${dir_name}-latest.osm.pbf"
  tmp_path="${pbf_path}.tmp"

  mkdir -p "${city_dir}"

  echo "=== Updating ${display_name} OSM PBF ==="
  echo "Downloading: ${download_url}"
  rm -f "${tmp_path}"

  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --retry-delay 10 "${download_url}" -o "${tmp_path}"
  elif command -v wget >/dev/null 2>&1; then
    wget --tries=3 --waitretry=10 -O "${tmp_path}" "${download_url}"
  else
    echo "Neither curl nor wget is available."
    exit 1
  fi

  downloaded_size="$(wc -c < "${tmp_path}")"
  if [ "${downloaded_size}" -lt 1048576 ]; then
    echo "Downloaded file is too small to be a valid PBF: ${tmp_path} (${downloaded_size} bytes)"
    head -c 200 "${tmp_path}" || true
    echo ""
    exit 1
  fi
  if head -c 256 "${tmp_path}" | grep -qiE '<html|<!doctype'; then
    echo "Downloaded file looks like HTML, not a PBF: ${tmp_path}"
    head -c 200 "${tmp_path}" || true
    echo ""
    exit 1
  fi

  mv -f "${tmp_path}" "${pbf_path}"
  echo "Saved: ${pbf_path}"
done

if [ "${update_count}" -eq 0 ]; then
  echo "No USA OSRM region matched filter: ${SELECTED_REGIONS[*]}"
  exit 1
fi

echo "=== Rebuilding USA OSRM artifacts ==="
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT}" bash "${BASE_DIR}/install_osrm_usa.sh" "${SELECTED_REGIONS[@]}"

echo "USA OSRM update completed."
