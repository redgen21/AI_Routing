#!/usr/bin/env bash
# Build and start a persistent Nominatim database for Thailand, Indonesia, and Malaysia.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"
NOMINATIM_IMAGE="${NOMINATIM_IMAGE:-mediagis/nominatim:5.3}"
NOMINATIM_CONTAINER="${NOMINATIM_CONTAINER:-nominatim-asia}"
NOMINATIM_DB_DIR="${NOMINATIM_DB_DIR:-${ASIA_STORAGE_ROOT}/nominatim-db}"
NOMINATIM_FLATNODE_DIR="${NOMINATIM_FLATNODE_DIR:-${ASIA_STORAGE_ROOT}/nominatim-flatnode}"
NOMINATIM_PORT="${NOMINATIM_PORT:-8080}"
NOMINATIM_PASSWORD="${NOMINATIM_PASSWORD:-change-this-nominatim-password}"
NOMINATIM_THREADS="${NOMINATIM_THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
NOMINATIM_SHM_SIZE="${NOMINATIM_SHM_SIZE:-4g}"
NOMINATIM_IMPORT_TIMEOUT_SECONDS="${NOMINATIM_IMPORT_TIMEOUT_SECONDS:-86400}"
REBUILD_NOMINATIM="${REBUILD_NOMINATIM:-false}"
PBF_PATH="${ASIA_STORAGE_ROOT}/nominatim/southeast-asia-latest.osm.pbf"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but was not found."
  exit 1
}
docker info >/dev/null 2>&1 || {
  echo "Docker daemon is not available."
  exit 1
}

mkdir -p "${ASIA_STORAGE_ROOT}" "${NOMINATIM_DB_DIR}" "${NOMINATIM_FLATNODE_DIR}"
if [ ! -w "${ASIA_STORAGE_ROOT}" ]; then
  echo "Asia storage root is not writable: ${ASIA_STORAGE_ROOT}"
  exit 1
fi
echo "Storage root: ${ASIA_STORAGE_ROOT}"
df -h "${ASIA_STORAGE_ROOT}" || true

ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT}" bash "${SCRIPT_DIR}/prepare_nominatim_asia_pbf.sh"

if [ "${REBUILD_NOMINATIM}" = "true" ]; then
  if [ "${ALLOW_NOMINATIM_DATA_DELETE:-false}" != "true" ]; then
    echo "REBUILD_NOMINATIM=true requires ALLOW_NOMINATIM_DATA_DELETE=true."
    exit 1
  fi
  echo "=== Removing existing Nominatim Asia container and database data ==="
  docker rm -f "${NOMINATIM_CONTAINER}" 2>/dev/null || true
  case "${NOMINATIM_DB_DIR}" in
    "${ASIA_STORAGE_ROOT}"/*) find "${NOMINATIM_DB_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + ;;
    *) echo "Refusing to clear unexpected DB path: ${NOMINATIM_DB_DIR}"; exit 1 ;;
  esac
  case "${NOMINATIM_FLATNODE_DIR}" in
    "${ASIA_STORAGE_ROOT}"/*) find "${NOMINATIM_FLATNODE_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + ;;
    *) echo "Refusing to clear unexpected flatnode path: ${NOMINATIM_FLATNODE_DIR}"; exit 1 ;;
  esac
fi

docker pull "${NOMINATIM_IMAGE}"

if docker ps -a --format '{{.Names}}' | grep -Fx "${NOMINATIM_CONTAINER}" >/dev/null 2>&1; then
  echo "=== Starting existing ${NOMINATIM_CONTAINER} container ==="
  docker start "${NOMINATIM_CONTAINER}" >/dev/null
else
  echo "=== Creating Nominatim Asia container ==="
  echo "Initial import may take several hours."
  docker run -d \
    --name "${NOMINATIM_CONTAINER}" \
    --restart unless-stopped \
    --shm-size="${NOMINATIM_SHM_SIZE}" \
    -p "${NOMINATIM_PORT}:8080" \
    -e PBF_PATH=/nominatim/data/southeast-asia-latest.osm.pbf \
    -e IMPORT_STYLE=address \
    -e IMPORT_WIKIPEDIA=false \
    -e IMPORT_SECONDARY_WIKIPEDIA=false \
    -e UPDATE_MODE=none \
    -e FREEZE=true \
    -e THREADS="${NOMINATIM_THREADS}" \
    -e GUNICORN_WORKERS=4 \
    -e NOMINATIM_PASSWORD="${NOMINATIM_PASSWORD}" \
    -e POSTGRES_SHARED_BUFFERS="${POSTGRES_SHARED_BUFFERS:-2GB}" \
    -e POSTGRES_MAINTENANCE_WORK_MEM="${POSTGRES_MAINTENANCE_WORK_MEM:-2GB}" \
    -e POSTGRES_AUTOVACUUM_WORK_MEM="${POSTGRES_AUTOVACUUM_WORK_MEM:-1GB}" \
    -e POSTGRES_WORK_MEM="${POSTGRES_WORK_MEM:-50MB}" \
    -e POSTGRES_EFFECTIVE_CACHE_SIZE="${POSTGRES_EFFECTIVE_CACHE_SIZE:-8GB}" \
    -v "${ASIA_STORAGE_ROOT}/nominatim:/nominatim/data" \
    -v "${NOMINATIM_DB_DIR}:/var/lib/postgresql/16/main" \
    -v "${NOMINATIM_FLATNODE_DIR}:/nominatim/flatnode" \
    "${NOMINATIM_IMAGE}"
fi

echo "=== Waiting for Nominatim Asia import/API ==="
started_at="$(date +%s)"
while true; do
  if curl -fsS "http://127.0.0.1:${NOMINATIM_PORT}/status" 2>/dev/null | grep -q "OK"; then
    echo "Nominatim Asia API is ready."
    break
  fi
  if ! docker ps --format '{{.Names}}' | grep -Fx "${NOMINATIM_CONTAINER}" >/dev/null 2>&1; then
    echo "Nominatim container stopped unexpectedly."
    docker logs --tail 200 "${NOMINATIM_CONTAINER}" || true
    exit 1
  fi
  now="$(date +%s)"
  elapsed="$((now - started_at))"
  if [ "${elapsed}" -ge "${NOMINATIM_IMPORT_TIMEOUT_SECONDS}" ]; then
    echo "Timed out waiting for Nominatim after ${elapsed}s."
    echo "The container was left running. Check progress with:"
    echo "  docker logs -f ${NOMINATIM_CONTAINER}"
    exit 1
  fi
  if [ $((elapsed % 60)) -lt 5 ]; then
    echo "Nominatim import still running (${elapsed}s elapsed)..."
  fi
  sleep 5
done

curl -fsS \
  "http://127.0.0.1:${NOMINATIM_PORT}/search?q=Bangkok&countrycodes=th&format=jsonv2&limit=1" \
  | grep -q '"lat"' || {
    echo "Nominatim API is healthy, but the Bangkok search test failed."
    exit 1
  }

echo "Nominatim Asia -> http://127.0.0.1:${NOMINATIM_PORT}"
