# Versioned database migrations

This directory is reserved for ordered, immutable schema migrations.

The legacy Common VRP runtime schema remains defined by
`smart_routing/common_vrp_db.py`. The additive Atlanta six-area plan repository
is owned here by `V001__atlanta_6area_region_plan.sql`; it deliberately does not
alter or replace `common_region_master` or the existing Atlanta three-area data.

When migrations are introduced, each migration must have a version, checksum,
idempotency or explicit one-time semantics, development evidence, production
backup/rollback instructions, and an entry in a schema migration history table.

Migration filenames use `VNNN__lowercase_description.sql`. `manifest.json` is
the executable allowlist and each migration also has a review sidecar manifest.
`admin_tools.db.release_backend` validates statement types, forbidden
primitives, checksum, typed target confirmation, advisory lock, timeout,
transaction, and history semantics. The region-plan CLI exposes only the fixed
V001 preview/install workflow and permits schema installation in Development.

V001 through V004 remain historical compatibility migrations. Operators do not
run them individually when installing Region Plan Schema v2. The single common
reconciler is `admin_tools.db.region_plan_schema_backend`; its additive SQL is
`admin_tools/db/region_plan_schema_v2.sql`. It reconciles fresh and historical
databases transactionally and grants `vrp_agent` only `SELECT`, `INSERT`, and
`UPDATE` access required by candidate and runtime lifecycle operations. It does
not grant `DELETE`, `TRUNCATE`, ownership, or access to legacy master tables.
