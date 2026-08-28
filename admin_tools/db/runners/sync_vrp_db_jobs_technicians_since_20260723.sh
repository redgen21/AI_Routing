#!/usr/bin/env bash
# Incremental Production (vrp_db) -> Development (vrp_db_dev) input sync.
#
# Production credentials are read from the server's Production config. Development
# is accessed through the local PostgreSQL OS account (peer auth), so no database
# password or manually exported DSN is required.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_ROOT="${AI_ROUTING_ROOT:-/home/csda/AI_Routing}"
PROD_CONFIG="${VRP_PROD_CONFIG:-}"

if [[ -z "${PROD_CONFIG}" ]]; then
  for candidate in \
    "${RUNTIME_ROOT}/config/common_vrp.prod.json" \
    "${RUNTIME_ROOT}/common_vrp.prod.json" \
    "${RUNTIME_ROOT}/production/config_common_vrp.prod.json" \
    "${RUNTIME_ROOT}/production/common_vrp.prod.json"; do
    if [[ -r "${candidate}" ]]; then
      PROD_CONFIG="${candidate}"
      break
    fi
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

# This only parses JSON configuration; the data copy itself is the SQL file below.
VRP_PROD_DSN="$(python3 - "${PROD_CONFIG}" <<'PY'
import json
import sys

config_path = sys.argv[1]
database = json.load(open(config_path, encoding="utf-8")).get("database") or {}
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

# Open the SQL file as csda, then pipe it to psql as postgres.  The postgres
# account cannot traverse /home/csda, so it must not receive this file via -f.
sudo -u postgres sh -c 'cd /tmp && exec psql -d vrp_db_dev -v source_dsn="$1"' -- "$VRP_PROD_DSN" \
  < "${SCRIPT_DIR}/sync_vrp_db_jobs_technicians_since_20260723.sql"

unset VRP_PROD_DSN
