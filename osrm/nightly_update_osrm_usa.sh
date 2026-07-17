#!/usr/bin/env bash
# Nightly USA OSRM refresh: download latest PBFs, rebuild, restart containers

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "${BASE_DIR}"

echo "[nightly] USA OSRM update started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
bash "${BASE_DIR}/update_osrm_usa.sh"
bash "${BASE_DIR}/run_osrm_usa.sh"
echo "[nightly] USA OSRM update finished: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
