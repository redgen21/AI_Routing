#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export NA_DATA_CATALOG_PATH="${NA_DATA_CATALOG_PATH:-/home/csda/AI_Routing/shared/config/data_catalog.development.json}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${COMMON_VRP_CONFIG_PATH:-${ROOT}/config_common_vrp.dev.json}"
[ -f "${CONFIG}" ] || CONFIG="${ROOT}/config/common_vrp.dev.json"

# shellcheck source=runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"
select_python
cd "${ROOT}"

"${PYTHON_BIN}" verify_deployment.py \
  --config "${CONFIG}" --expected-environment development

exec "${PYTHON_BIN}" sr_common_vrp_api_server.py \
  --config "${CONFIG}" --host 0.0.0.0 --port 8066 \
  --expected-environment development --bootstrap-only
