#!/usr/bin/env bash
# Read-only Production vs Development Home-routing input comparison.
# The postgres OS account cannot traverse /home/csda, so this script opens the
# SQL file as csda and pipes it into psql instead of passing psql a -f path.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_ROOT="${AI_ROUTING_ROOT:-/home/csda/AI_Routing}"
PROD_CONFIG="${VRP_PROD_CONFIG:-}"
SUBSIDIARY="${VRP_COMPARE_SUBSIDIARY:-LGEAI}"
CITY="${VRP_COMPARE_CITY:-Atlanta, GA}"
PROMISE_DATE="${VRP_COMPARE_PROMISE_DATE:-20260821}"

if [[ -z "${PROD_CONFIG}" ]]; then
  for candidate in \
    "${RUNTIME_ROOT}/config/common_vrp.prod.json" \
    "${RUNTIME_ROOT}/common_vrp.prod.json" \
    "${RUNTIME_ROOT}/production/config_common_vrp.prod.json" \
    "${RUNTIME_ROOT}/production/common_vrp.prod.json"; do
    if [[ -r "${candidate}" ]]; then PROD_CONFIG="${candidate}"; break; fi
  done
fi
if [[ -z "${PROD_CONFIG}" ]]; then
  PROD_CONFIG="$(find "${RUNTIME_ROOT}" -maxdepth 3 -type f \( -name 'common_vrp.prod.json' -o -name 'config_common_vrp.prod.json' \) -readable -print -quit)"
fi
if [[ -z "${PROD_CONFIG}" || ! -r "${PROD_CONFIG}" ]]; then
  echo "No readable Production Common VRP config was found below ${RUNTIME_ROOT}." >&2
  echo "Set VRP_PROD_CONFIG to its absolute path if it is stored elsewhere." >&2
  exit 1
fi

VRP_PROD_DSN="$(python3 - "${PROD_CONFIG}" <<'PY'
import json
import sys

database = json.load(open(sys.argv[1], encoding="utf-8")).get("database") or {}
required = ("host", "dbname", "user", "password")
missing = [name for name in required if not database.get(name)]
if missing:
    raise SystemExit("Production database config is missing: " + ", ".join(missing))

def quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

parts = [f"{name}={quote(database[name])}" for name in ("host", "dbname", "user", "password")]
if database.get("port"):
    parts.append(f"port={quote(database['port'])}")
print(" ".join(parts))
PY
)"

sudo -u postgres sh -c 'cd /tmp && exec psql -d vrp_db_dev -v source_dsn="$1" -v subsidiary="$2" -v city="$3" -v promise_date="$4"' -- \
  "${VRP_PROD_DSN}" "${SUBSIDIARY}" "${CITY}" "${PROMISE_DATE}" \
  < "${SCRIPT_DIR}/compare_prod_dev_home_routing_inputs.sql"

unset VRP_PROD_DSN
