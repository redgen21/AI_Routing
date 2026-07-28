#!/bin/bash
# OSRM Korea server restart

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_OSRM_STORAGE_ROOT="/data/ai-routing/osrm"
if [ -d /mnt/data/ai-routing/osrm ]; then
  DEFAULT_OSRM_STORAGE_ROOT="/mnt/data/ai-routing/osrm"
fi
OSRM_STORAGE_ROOT="${OSRM_STORAGE_ROOT:-${DEFAULT_OSRM_STORAGE_ROOT}}"
OSRM_IMAGE="${OSRM_IMAGE:-ghcr.io/project-osrm/osrm-backend:latest}"
CONTAINER="osrm-korea"
PORT="5000"
LOG_MAX_SIZE="${OSRM_DOCKER_LOG_MAX_SIZE:-100m}"
LOG_MAX_FILE="${OSRM_DOCKER_LOG_MAX_FILE:-3}"

echo "=== Korea 서버 재시작 ==="

docker stop "$CONTAINER" 2>/dev/null || true
docker rm -f "$CONTAINER" 2>/dev/null || true

echo "=== 서버 시작 ==="

docker run -d \
  --name "$CONTAINER" \
  --restart unless-stopped \
  --log-driver json-file \
  --log-opt "max-size=${LOG_MAX_SIZE}" \
  --log-opt "max-file=${LOG_MAX_FILE}" \
  -p "${PORT}:5000" \
  -v "${OSRM_STORAGE_ROOT}:/data:ro" \
  "${OSRM_IMAGE}" \
  osrm-routed --algorithm mld \
  "/data/south-korea/south-korea-latest.osrm"

echo "=== 준비 대기 ==="
for i in {1..30}; do
  if curl -fsS "http://127.0.0.1:${PORT}/nearest/v1/driving/126.9780,37.5665" > /dev/null 2>&1; then
    echo "✅ Korea OK"
    echo "http://20.51.244.68:${PORT}"
    exit 0
  fi
  sleep 2
done

echo "❌ 서버 응답 없음"
docker logs "$CONTAINER" || true
exit 1
