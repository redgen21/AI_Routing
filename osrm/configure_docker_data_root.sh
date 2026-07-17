#!/usr/bin/env bash
# Move Docker's data-root from the root filesystem to /data.
# Run once with sudo before installing the Asia stack.

set -euo pipefail

TARGET_ROOT="${DOCKER_DATA_ROOT:-/data/docker}"
CURRENT_ROOT="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)"
CURRENT_ROOT="$(readlink -f "${CURRENT_ROOT}")"
TARGET_ROOT="$(readlink -m "${TARGET_ROOT}")"
DAEMON_JSON="/etc/docker/daemon.json"
BACKUP_JSON="/etc/docker/daemon.json.before-data-root"
CLEAN_OLD_DOCKER_ROOT="${CLEAN_OLD_DOCKER_ROOT:-false}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this script with sudo:"
  echo "  sudo bash $0"
  exit 1
fi
command -v docker >/dev/null 2>&1 || {
  echo "Docker is not installed."
  exit 1
}
command -v rsync >/dev/null 2>&1 || {
  echo "rsync is required. Install it first: apt-get install -y rsync"
  exit 1
}
case "${TARGET_ROOT}" in
  /data/*) ;;
  *) echo "Refusing Docker data-root outside /data: ${TARGET_ROOT}"; exit 1 ;;
esac
if [ "${CURRENT_ROOT}" = "${TARGET_ROOT}" ]; then
  echo "Docker already uses ${TARGET_ROOT}."
  if [ "${CLEAN_OLD_DOCKER_ROOT}" = "true" ] &&
     [ -d /var/lib/docker ] &&
     [ "$(readlink -f /var/lib/docker)" != "${TARGET_ROOT}" ]; then
    echo "Removing retained files from /var/lib/docker."
    find /var/lib/docker -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
  exit 0
fi

mkdir -p "${TARGET_ROOT}"
echo "Current Docker root: ${CURRENT_ROOT}"
echo "Target Docker root:  ${TARGET_ROOT}"
df -h "${CURRENT_ROOT}" "${TARGET_ROOT}" || true

echo "=== Stopping Docker ==="
systemctl stop docker docker.socket containerd 2>/dev/null || true

echo "=== Copying Docker data to ${TARGET_ROOT} ==="
rsync -aHAXx --numeric-ids --info=progress2 "${CURRENT_ROOT}/" "${TARGET_ROOT}/"

if [ -f "${DAEMON_JSON}" ] && [ ! -f "${BACKUP_JSON}" ]; then
  cp -a "${DAEMON_JSON}" "${BACKUP_JSON}"
fi

python3 - "${DAEMON_JSON}" "${TARGET_ROOT}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
if path.exists() and path.read_text(encoding="utf-8").strip():
    config = json.loads(path.read_text(encoding="utf-8"))
else:
    config = {}
config["data-root"] = target
path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
PY

echo "=== Starting Docker with the new data-root ==="
systemctl start containerd docker
sleep 3

NEW_ROOT="$(docker info --format '{{.DockerRootDir}}')"
if [ "${NEW_ROOT}" != "${TARGET_ROOT}" ]; then
  echo "Docker data-root verification failed: ${NEW_ROOT}"
  echo "Restore ${BACKUP_JSON} if needed."
  exit 1
fi

echo "Docker now uses ${NEW_ROOT}."
docker ps >/dev/null

if [ "${CLEAN_OLD_DOCKER_ROOT}" = "true" ]; then
  case "${CURRENT_ROOT}" in
    /var/lib/docker)
      echo "=== Removing old Docker data after successful verification ==="
      find "${CURRENT_ROOT}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
      ;;
    *)
      echo "Refusing to clean unexpected old Docker root: ${CURRENT_ROOT}"
      exit 1
      ;;
  esac
else
  echo ""
  echo "Old Docker data remains at ${CURRENT_ROOT}."
  echo "After verifying containers, free root space with:"
  echo "  sudo CLEAN_OLD_DOCKER_ROOT=true bash $0"
fi

df -h / "${TARGET_ROOT}" || true
