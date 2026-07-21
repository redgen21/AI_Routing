#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLL_SECONDS="${POLL_SECONDS:-20}"
ROOT="${SCRIPT_DIR}"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"
select_python

while true; do
  if ! wait_for_http_status "http://127.0.0.1:8065/api/v1/common/contexts" 200 1; then
    "${SCRIPT_DIR}/start_common_vrp_prod.sh"
  fi
  sleep "${POLL_SECONDS}"
done
