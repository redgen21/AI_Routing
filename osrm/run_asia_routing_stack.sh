#!/usr/bin/env bash
# Start Asia OSRM and Nominatim services together.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"

echo "Asia stack storage: ${ASIA_STORAGE_ROOT}"
echo "=== Starting Asia OSRM services ==="
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT}" bash "${BASE_DIR}/run_osrm_asia.sh"

echo "=== Starting Nominatim Asia ==="
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT}" bash "${BASE_DIR}/run_nominatim_asia.sh"

echo ""
echo "Asia routing/geocoding stack is ready."
echo "Thailand OSRM      -> http://127.0.0.1:5003"
echo "Indonesia OSRM      -> http://127.0.0.1:5004"
echo "Malaysia OSRM -> http://127.0.0.1:5005"
echo "Nominatim Asia    -> http://127.0.0.1:${NOMINATIM_PORT:-8080}"
