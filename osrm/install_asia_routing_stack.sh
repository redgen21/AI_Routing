#!/usr/bin/env bash
# One-command installation for the Asia OSRM routing stack.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_ASIA_STORAGE_ROOT="/data/ai-routing/asia-stack"
if [ -d /mnt/data ]; then
  DEFAULT_ASIA_STORAGE_ROOT="/mnt/data/ai-routing/asia-stack"
fi
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT:-${DEFAULT_ASIA_STORAGE_ROOT}}"
MIN_FREE_GB="${MIN_FREE_GB:-80}"
ALLOW_DOCKER_ROOT_OUTSIDE_DATA="${ALLOW_DOCKER_ROOT_OUTSIDE_DATA:-false}"

mkdir -p "${ASIA_STORAGE_ROOT}"
test -w "${ASIA_STORAGE_ROOT}" || {
  echo "Storage path is not writable: ${ASIA_STORAGE_ROOT}"
  exit 1
}
echo "Asia stack storage: ${ASIA_STORAGE_ROOT}"
df -h "${ASIA_STORAGE_ROOT}" || true

DOCKER_ROOT="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
if [ -z "${DOCKER_ROOT}" ]; then
  echo "Docker is not running or is not accessible."
  exit 1
fi
case "$(readlink -f "${DOCKER_ROOT}")" in
  /data|/data/*|/mnt/data|/mnt/data/*) ;;
  *)
    if [ "${ALLOW_DOCKER_ROOT_OUTSIDE_DATA}" != "true" ]; then
      echo "Docker data-root is outside /data: ${DOCKER_ROOT}"
      echo "Move it first. For Snap Docker:"
      echo "  sudo bash ${BASE_DIR}/configure_snap_docker_data_root.sh"
      exit 1
    fi
    echo "WARNING: Docker data-root is outside /data: ${DOCKER_ROOT}"
    ;;
esac

AVAILABLE_KB="$(df -Pk "${ASIA_STORAGE_ROOT}" | awk 'NR == 2 {print $4}')"
REQUIRED_KB=$((MIN_FREE_GB * 1024 * 1024))
if [ "${AVAILABLE_KB}" -lt "${REQUIRED_KB}" ]; then
  echo "At least ${MIN_FREE_GB} GiB free is required under ${ASIA_STORAGE_ROOT}."
  exit 1
fi

echo "=== Download and build Asia OSRM regions ==="
ASIA_STORAGE_ROOT="${ASIA_STORAGE_ROOT}" bash "${BASE_DIR}/install_osrm_asia.sh"

echo ""
echo "Asia OSRM installation completed."
echo "Install Nominatim separately with: ${BASE_DIR}/install_nominatim_asia.sh"
echo "Start all services with: ${BASE_DIR}/run_asia_routing_stack.sh"
