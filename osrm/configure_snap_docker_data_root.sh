#!/usr/bin/env bash
# Move Snap Docker's data-root to the /data disk exposed through /mnt/data.

set -euo pipefail

TARGET_ROOT="${DOCKER_DATA_ROOT:-/mnt/data/docker}"
CURRENT_ROOT="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
CLEAN_OLD_DOCKER_ROOT="${CLEAN_OLD_DOCKER_ROOT:-false}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this script with sudo:"
  echo "  sudo bash $0"
  exit 1
fi
command -v snap >/dev/null 2>&1 || {
  echo "snap is not installed."
  exit 1
}
command -v rsync >/dev/null 2>&1 || {
  echo "rsync is required: apt-get install -y rsync"
  exit 1
}
snap list docker >/dev/null 2>&1 || {
  echo "The Docker snap is not installed."
  exit 1
}
case "$(readlink -m "${TARGET_ROOT}")" in
  /mnt/data|/mnt/data/*) ;;
  *)
    echo "Refusing Snap Docker data-root outside /mnt/data: ${TARGET_ROOT}"
    exit 1
    ;;
esac
mountpoint -q /mnt/data || {
  echo "/mnt/data is not mounted. Restore the /data bind mount first."
  exit 1
}

CURRENT_ROOT="$(readlink -f "${CURRENT_ROOT:-/var/snap/docker/common/var-lib-docker}")"
TARGET_ROOT="$(readlink -m "${TARGET_ROOT}")"

if [ "${CURRENT_ROOT}" = "${TARGET_ROOT}" ]; then
  echo "Snap Docker already uses ${TARGET_ROOT}."
  if [ "${CLEAN_OLD_DOCKER_ROOT}" = "true" ] &&
     [ -d /var/snap/docker/common/var-lib-docker ]; then
    echo "Removing retained files from the old Snap Docker data-root."
    find /var/snap/docker/common/var-lib-docker \
      -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
  exit 0
fi

case "${CURRENT_ROOT}" in
  /var/snap/docker/common/var-lib-docker) ;;
  *)
    echo "Unexpected current Snap Docker data-root: ${CURRENT_ROOT}"
    exit 1
    ;;
esac

snap connect docker:removable-media
mkdir -p "${TARGET_ROOT}"

echo "Current Docker root: ${CURRENT_ROOT}"
echo "Target Docker root:  ${TARGET_ROOT}"
df -h / "${TARGET_ROOT}"

echo "=== Stopping Snap Docker ==="
snap stop docker

echo "=== Copying Docker images, containers, and metadata ==="
rsync -aHAXx --numeric-ids --info=progress2 "${CURRENT_ROOT}/" "${TARGET_ROOT}/"

echo "=== Configuring Snap Docker data-root ==="
snap set docker data-root="${TARGET_ROOT}"
snap start docker
sleep 5

NEW_ROOT="$(docker info --format '{{.DockerRootDir}}')"
if [ "$(readlink -f "${NEW_ROOT}")" != "${TARGET_ROOT}" ]; then
  echo "Docker data-root verification failed: ${NEW_ROOT}"
  exit 1
fi

docker ps >/dev/null
echo "Snap Docker now uses ${NEW_ROOT}."

if [ "${CLEAN_OLD_DOCKER_ROOT}" = "true" ]; then
  echo "Removing old Snap Docker data after successful verification."
  find "${CURRENT_ROOT}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
else
  echo ""
  echo "Old Docker data remains at ${CURRENT_ROOT}."
  echo "After verifying all containers, clean it with:"
  echo "  sudo CLEAN_OLD_DOCKER_ROOT=true bash $0"
fi

df -h / "${TARGET_ROOT}"
