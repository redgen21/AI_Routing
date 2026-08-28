#!/usr/bin/env bash
set -euo pipefail

# Manual, development-only fallback for the Region Plan Schema v2 reconciler.
# This script never accepts a database name and never touches production.

DB_NAME="vrp_db_dev"
ADMIN_RELEASE_ROOT="${1:-/home/csda/AI_Routing/admin_tools}"
RUN_ROOT="/tmp/region-plan-schema-v2-$(date -u +%Y%m%dT%H%M%SZ)"

if [[ -z "${ADMIN_RELEASE_ROOT}" ]]; then
  ADMIN_RELEASE_ROOT="$(
    find "${ADMIN_RELEASE_ROOT}" -mindepth 1 -maxdepth 1 -type d -name 'admin-*' \
      -printf '%f\n' | sort | tail -n 1
  )"
  [[ -n "${ADMIN_RELEASE_ROOT}" ]] || {
    echo "No deployed Admin Tools release was found." >&2
    exit 1
  }
  ADMIN_RELEASE_ROOT="${ADMIN_RELEASE_ROOT}/${ADMIN_RELEASE_ROOT}"
fi

ADMIN_RELEASE_ROOT="$(readlink -f -- "${ADMIN_RELEASE_ROOT}")"

BASE_SQL="${ADMIN_RELEASE_ROOT}/admin_tools/db/migrations/V001__atlanta_6area_region_plan.sql"
V2_SQL="${ADMIN_RELEASE_ROOT}/admin_tools/db/region_plan_schema_v2.sql"
for file in "${BASE_SQL}" "${V2_SQL}"; do
  [[ -f "${file}" ]] || {
    echo "Required Schema v2 SQL is missing: ${file}" >&2
    exit 1
  }
done

ACTUAL_DB="$(sudo -u postgres psql -XAt -d "${DB_NAME}" -c 'select current_database()')"
[[ "${ACTUAL_DB}" == "${DB_NAME}" ]] || {
  echo "Database target mismatch: ${ACTUAL_DB}" >&2
  exit 1
}

echo "Target database: ${ACTUAL_DB}"
echo "Admin release: ${ADMIN_RELEASE_ROOT}"
echo "Schema checksums:"
sha256sum "${BASE_SQL}" "${V2_SQL}"

sudo install -d -o postgres -g postgres -m 0700 "${RUN_ROOT}"
sudo install -o postgres -g postgres -m 0600 "${BASE_SQL}" "${RUN_ROOT}/V001.sql"
sudo install -o postgres -g postgres -m 0600 "${V2_SQL}" "${RUN_ROOT}/schema_v2.sql"

cd /tmp
sudo -u postgres psql -X -v ON_ERROR_STOP=1 --single-transaction \
  -d "${DB_NAME}" \
  -f "${RUN_ROOT}/V001.sql" \
  -f "${RUN_ROOT}/schema_v2.sql"

sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d "${DB_NAME}" <<'SQL'
select
  current_database() as database,
  exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='common_region_plan'
       and column_name='verified_content_sha256'
  ) as verified_content_sha256,
  exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='common_region_plan'
       and column_name='verified_at'
  ) as verified_at,
  exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='common_region_plan'
       and column_name='verified_by'
  ) as verified_by,
  exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='common_region_plan_region'
       and column_name='required_center_type'
  ) as required_center_type,
  exists (
    select 1 from pg_constraint
     where conname='common_region_plan_verified_content_sha256_v2_check'
  ) as verification_constraint,
  has_table_privilege(
    'vrp_agent','public.common_region_plan','SELECT,INSERT,UPDATE'
  ) as vrp_agent_plan_access;
SQL

echo "Schema v2 reconciliation completed. Staged SQL remains at ${RUN_ROOT} for audit."
