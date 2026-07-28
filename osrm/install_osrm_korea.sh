#!/bin/bash
# OSRM Korea build (custom Lua 적용)

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE="${BASE_DIR}/profiles/custom_car.lua"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend:latest}"
DATA_DIR="${OSRM_STORAGE_ROOT}/south-korea"
LOG_MAX_SIZE="${OSRM_DOCKER_LOG_MAX_SIZE:-100m}"
LOG_MAX_FILE="${OSRM_DOCKER_LOG_MAX_FILE:-3}"

PBF="${DATA_DIR}/south-korea-latest.osm.pbf"
OSRM="/data/south-korea/south-korea-latest.osrm"

echo "=== Korea OSRM build 시작 ==="

if [ ! -f "$PROFILE" ]; then
  echo "❌ Lua 파일 없음"
  exit 1
fi

if [ ! -f "$PBF" ]; then
  echo "❌ PBF 파일 없음"
  exit 1
fi

echo "=== 기존 OSRM 파일 삭제 ==="
rm -f "${DATA_DIR}/south-korea-latest.osrm"*

echo "=== 1) extract ==="
docker run --rm \
  --log-driver json-file \
  --log-opt "max-size=${LOG_MAX_SIZE}" \
  --log-opt "max-file=${LOG_MAX_FILE}" \
  -v "${OSRM_STORAGE_ROOT}:/data" \
  -v "${BASE_DIR}/profiles:/profiles:ro" \
  "${OSRM_IMAGE}" \
  osrm-extract \
  -p "/profiles/custom_car.lua" \
  "/data/south-korea/south-korea-latest.osm.pbf"

echo "=== 2) partition ==="
docker run --rm \
  --log-driver json-file \
  --log-opt "max-size=${LOG_MAX_SIZE}" \
  --log-opt "max-file=${LOG_MAX_FILE}" \
  -v "${OSRM_STORAGE_ROOT}:/data" \
  "${OSRM_IMAGE}" \
  osrm-partition "$OSRM"

echo "=== 3) customize ==="
docker run --rm \
  --log-driver json-file \
  --log-opt "max-size=${LOG_MAX_SIZE}" \
  --log-opt "max-file=${LOG_MAX_FILE}" \
  -v "${OSRM_STORAGE_ROOT}:/data" \
  "${OSRM_IMAGE}" \
  osrm-customize "$OSRM"

echo "=== 결과 파일 확인 ==="

required_files=(
  "${DATA_DIR}/south-korea-latest.osrm"
  "${DATA_DIR}/south-korea-latest.osrm.partition"
  "${DATA_DIR}/south-korea-latest.osrm.cells"
  "${DATA_DIR}/south-korea-latest.osrm.cell_metrics"
  "${DATA_DIR}/south-korea-latest.osrm.cnbg"
  "${DATA_DIR}/south-korea-latest.osrm.datasource_names"
  "${DATA_DIR}/south-korea-latest.osrm.ebg"
  "${DATA_DIR}/south-korea-latest.osrm.ebg_nodes"
  "${DATA_DIR}/south-korea-latest.osrm.edges"
  "${DATA_DIR}/south-korea-latest.osrm.fileIndex"
  "${DATA_DIR}/south-korea-latest.osrm.geometry"
  "${DATA_DIR}/south-korea-latest.osrm.icd"
  "${DATA_DIR}/south-korea-latest.osrm.mldgr"
  "${DATA_DIR}/south-korea-latest.osrm.names"
  "${DATA_DIR}/south-korea-latest.osrm.properties"
  "${DATA_DIR}/south-korea-latest.osrm.ramIndex"
  "${DATA_DIR}/south-korea-latest.osrm.tld"
  "${DATA_DIR}/south-korea-latest.osrm.tls"
  "${DATA_DIR}/south-korea-latest.osrm.turn_duration_penalties"
  "${DATA_DIR}/south-korea-latest.osrm.turn_weight_penalties"
)

for f in "${required_files[@]}"; do
  if [ ! -f "$f" ]; then
    echo "❌ 필수 파일 누락: $f"
    exit 1
  fi
done

docker image inspect "${OSRM_IMAGE}" --format '{{.Id}}' > "${DATA_DIR}/.osrm-image-id"

echo "✅ Korea build 완료"
