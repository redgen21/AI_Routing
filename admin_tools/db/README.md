# Common VRP database administration

This package contains explicit, offline database administration commands. It is
separate from `services/`, which contains server runtime adapters.

## Layout

- `runners/reset_common_vrp_data.py`: clears scoped transactional jobs,
  requests, and results while retaining master data.
- `seeds/build_la_bucket_vrp_inputs.py`: builds LA bucket input files and, only
  with `--update-db`, refreshes the related master data.
- `seeds/import_asia_technician_centroids.py`: prepares Asia technician,
  capability, and region masters and writes only with `--apply`.
- `common_vrp.py`: release-local config, read-only schema preflight, master
  upsert, and default-seed repository used only by these offline commands.
- `data_catalog.py` and `heavy_repair.py`: resolve the explicit shared-data
  catalog and its heavy-repair lookup without application-runtime imports.
- `guard.py`: requires an exact environment/database pairing
  (`development`/`vrp_db_dev` or `production`/`vrp_db`) and blocks production
  writes unless they are explicitly confirmed.
- `migrations/`: reserved for future ordered, versioned migrations.

Admin Tools never create, alter, or repair the runtime schema. Before any
reset or seed/import write, it validates the required table columns and primary
keys through a read-only catalog query and fails closed on a legacy mismatch.
Reviewed migrations are solely responsible for schema initialization and
ordered production changes.

### LA candidate inputs

`build_la_bucket_vrp_inputs` treats cataloged reviewed and seed region files as
immutable inputs. It never promotes a reviewed plan into a seed file. Generated
region candidates are written only below an explicit `--output-root` (the
legacy `--output-dir` spelling is accepted) or a timestamped catalog
`region_candidates_dir` path. The output root is rejected when it is inside a
reviewed or seed input directory.

Every generated run writes `la_bucket_input_lineage.json`. It records the
source profile/service paths and SHA-256 checksums and, for each region
candidate, the selected source path, SHA-256 checksum, derived candidate path, and the
`candidate_only_no_reviewed_to_seed_promotion` lifecycle marker. A technician
area mapping is not inferred from a local draft workbook; it requires a
separately reviewed, cataloged input before it can be used.

## Safety

All commands default to `config/common_vrp.dev.json`. A production write requires
both the production config and `--confirm-production`. Commands are dry-run or
file-generation only unless their explicit write flag is present.

```powershell
# Generate LA inputs only; no database write
py -m admin_tools.db.seeds.build_la_bucket_vrp_inputs

# Development DB update
py -m admin_tools.db.seeds.build_la_bucket_vrp_inputs --update-db

# Production DB update, only after backup and deployment approval
py -m admin_tools.db.seeds.build_la_bucket_vrp_inputs `
  --config config/common_vrp.prod.json --update-db --confirm-production

# Inspect reset counts only; no rows are deleted
py -m admin_tools.db.runners.reset_common_vrp_data
```

These tools are not required by the API, UI, or solver at runtime. Package and
deploy them as a separately approved administrative artifact when server-side DB
maintenance is required.

On the server, select the shared data release explicitly with an absolute
catalog path. This prevents a production admin command from silently resolving
LA region/profile files from the application checkout:

```bash
cd /home/csda/AI_Routing/admin_tools_release
python -m admin_tools.db.seeds.build_la_bucket_vrp_inputs \
  --data-catalog /home/csda/AI_Routing/shared/config/data_catalog.json \
  --config /home/csda/AI_Routing/production/config/common_vrp.prod.json \
  --update-db --confirm-production
```

## Release console backend

`release_backend.py` exposes typed dataclasses and a fail-closed API for a future
deployment console:

- `MigrationSpec`, `MigrationPlan`, `MigrationPreview`, `MigrationResult`,
  `SelectPreview`;
- `DatabaseReleaseBackend.plan()`, `.preview_migration()`, `.apply()`, and
  `.preview_select()`;
- `AdminCommandSpec`, `PreparedAdminCommand`, and `prepare_admin_command()`.

Only `MigrationSpec` entries supplied by the reviewed release allowlist can run.
The migration ID and filename must follow `VNNN__description.sql`, and the file
must match its registered SHA-256 checksum. Apply uses a statement timeout,
transaction advisory lock, one transaction, typed environment/database
confirmation, and `admin_schema_migration_history`. Rollback instructions and an
optional rollback migration ID are stored with the applied checksum.

A statement failure rolls back the whole migration and does not create a
successful history row, so the same immutable version can be retried. If an
operator or external release controller has recorded a `failed` history row,
the backend additionally requires `retry_failed=True`; an existing `success`
row is idempotently reported as `already_applied` and never rerun.

The backend has no default DB connector and never starts subprocesses. A caller
must inject a connection factory, while seed/import selection only returns a
fixed argv specification. Free-form migration execution and multi-statement SQL
text boxes are intentionally unsupported. Preview accepts one read-only SELECT
and enforces a 1–500 row limit.

```powershell
# Development verification only; dirty artifacts are marked non-promotable
pwsh -File services/deploy/build_admin_tools_package.ps1 `
  -Version 2026.07.19-test -AllowDirtySource

# Approved admin release; requires a clean committed checkout
pwsh -File services/deploy/build_admin_tools_package.ps1 -Version 2026.07.19
```

Install the approved ZIP under
`/home/csda/AI_Routing/admin_tools/releases/<version>`. Do not merge it into
`production/` or `development/`, and do not register it with systemd or cron.
Create a release-local `.venv` and install its `requirements.txt` before use.
Run commands manually with the matching application config path. Production
commands additionally require a backup, approval, and `--confirm-production`.

The admin release contains no live data or secret config. Copy
`config/data_catalog.admin.template.json` to the server-only
`/home/csda/AI_Routing/catalogs/north_america.shared.json` and update active
artifact versions when needed. Start the tool from its versioned release
directory and pass the application environment root explicitly:

```bash
ADMIN_RELEASE=/home/csda/AI_Routing/admin_tools/releases/2026.07.19
DEV_ROOT=/home/csda/AI_Routing/development

cd "$ADMIN_RELEASE"
.venv/bin/python -m admin_tools.db.seeds.build_la_bucket_vrp_inputs \
  --runtime-root "$DEV_ROOT" \
  --config "$DEV_ROOT/config_common_vrp.dev.json" \
  --data-catalog /home/csda/AI_Routing/catalogs/north_america.shared.json \
  --service-file /home/csda/AI_Routing/shared/north_america/processed/service/<version>/Service_geocoded.csv \
  --output-dir /home/csda/AI_Routing/state/development/admin/la_bucket \
  --update-db
```

For production, substitute the production root, catalog, state path, and config;
create a DB backup first and add `--confirm-production`. The package is not
registered with systemd or cron.
