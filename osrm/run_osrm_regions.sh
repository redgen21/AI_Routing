#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "${BASE_DIR}"

echo "[start] Starting OSRM Korea..."
./run_osrm_korea.sh

echo "[start] Starting OSRM USA..."
./run_osrm_usa.sh

echo "[start] OSRM Korea + USA startup finished."
