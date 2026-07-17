#!/usr/bin/env bash
# Start the previously imported persistent Nominatim Asia container.

set -euo pipefail

NOMINATIM_CONTAINER="${NOMINATIM_CONTAINER:-nominatim-asia}"
NOMINATIM_PORT="${NOMINATIM_PORT:-8080}"
WAIT_SECONDS="${NOMINATIM_START_WAIT_SECONDS:-180}"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but was not found."
  exit 1
}
if ! docker ps -a --format '{{.Names}}' | grep -Fx "${NOMINATIM_CONTAINER}" >/dev/null 2>&1; then
  echo "Nominatim Asia is not installed."
  echo "Run ./install_nominatim_asia.sh first."
  exit 1
fi

docker start "${NOMINATIM_CONTAINER}" >/dev/null

for _ in $(seq 1 "$((WAIT_SECONDS / 3))"); do
  if curl -fsS "http://127.0.0.1:${NOMINATIM_PORT}/status" 2>/dev/null | grep -q "OK"; then
    echo "Nominatim Asia -> http://127.0.0.1:${NOMINATIM_PORT}"
    exit 0
  fi
  sleep 3
done

echo "Nominatim Asia failed healthcheck."
docker logs --tail 100 "${NOMINATIM_CONTAINER}" || true
exit 1
