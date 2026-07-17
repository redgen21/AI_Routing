#!/usr/bin/env bash
# Nightly Korea OSRM refresh: download latest PBF, rebuild, restart container

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "${BASE_DIR}"

echo "[nightly] Korea OSRM update started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
bash "${BASE_DIR}/update_osrm_korea.sh"
bash "${BASE_DIR}/run_osrm_korea.sh"
echo "[nightly] Korea OSRM update finished: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
