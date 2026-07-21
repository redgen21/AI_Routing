# Runtime and release operations

`services/` contains Python service adapters, DB utilities, packaging code, and
tests. It is not a separate production copy of `smart_routing/`.

Stable executable scripts and service entrypoints live at the repository root,
matching the server contract. Both environments run the same reviewed code; they
are isolated by directory, config, database, ports, logs, and runtime data.

## Server layout

```text
/home/csda/AI_Routing/
  production/               # production runtime code only
    config_common_vrp.json  # server-only secret config
    config/config.json      # server-only geocoding/routing config
    sr_common_vrp_api_server.py
    sr_common_vrp_client_server.py
    smart_routing/
    services/api/
  development/              # development runtime code only
    config_common_vrp.dev.json  # server-only development config
    config/config.json          # development-only provider/routing config
    ...same runtime source with the development overlay...
  shared/                   # one authoritative read-only reference-data copy
  state/
    production/             # production-only jobs/cache/uploads/results
    development/            # development-only jobs/cache/uploads/results

/home/osrm/                 # separately installed and operated OSRM graphs/scripts
```

Create the two application roots once, using the account configured in the
systemd units (currently `csda`):

```bash
sudo install -d -o csda -g csda /home/csda/AI_Routing/production
sudo install -d -o csda -g csda /home/csda/AI_Routing/development
```

Use `/home/csda/AI_Routing` only as the parent directory. Back up the legacy
root-level runtime files, then hydrate and start `production/` and `development/`
independently; do not keep a third runnable copy directly in the parent.

| Environment | Root | Common API | Client | Smart API | Database | Logs |
| --- | --- | ---: | ---: | ---: | --- | --- |
| Production | `/home/csda/AI_Routing/production` | 8065 | 8501 | 8055 | `vrp_db` | `log/common_vrp/prod` |
| Development | `/home/csda/AI_Routing/development` | 8066 | 8503 | 8056 | `vrp_db_dev` | `log/common_vrp/dev` |

Production and development must not share writable config, DB, logs, job archive,
cache, upload, or runtime-result folders. OSRM is an external shared service under
`/home/osrm`; changing its graph/profile requires a separate rollout.

## Root commands

These scripts are manual validation/recovery entrypoints. After systemd is
installed, normal start/stop/restart operations use `systemctl`; systemd starts
Python/Streamlit directly and does not call the shell wrappers.

```bash
# Development
./bootstrap_common_vrp_dev.sh
./start_common_vrp_dev.sh
./start_common_vrp_client_server_dev.sh

# Production
./start_common_vrp_prod.sh
./start_common_vrp_client_server_prod.sh
./restart_common_vrp_client_server.sh
```

```bash
# Normal production restart
sudo systemctl restart common-vrp common-vrp-client smart-routing

# Normal development restart
sudo systemctl restart common-vrp-dev common-vrp-client-dev smart-routing-dev
```

The scripts prefer root server configs, then fall back to repository-local configs
for workstation development. Startup validates the selected environment, DB name,
port, config, and hydrated data before replacing a process.

Production bootstrap is never automatic. It requires a DB backup, change approval,
and the explicit `--confirm-production-bootstrap` CLI option.

## Build environment-specific artifacts

The repository root is the development workspace. In particular,
`smart_routing/` and `services/` are edited and tested only there. A successful
build copies the approved runtime subset into a separate generated deployment
tree:

```text
repository root/
  smart_routing/                 # development source
  services/                      # development source and build/test tools
  deployment/                    # generated; never edit directly
    development/<version>/
      smart_routing/
      services/api/
      ...development runtime overlay...
    production/<version>/
      smart_routing/
      services/api/
      ...production runtime overlay...
```

Always rebuild a deployment tree from the development source after tests. Never
copy changes back from `deployment/` to the development workspace.

```powershell
# Development verification may use a dirty tree only when explicitly authorized.
pwsh -File services/deploy/build_deploy_package.ps1 `
  -Environment development -Version 2026.07.18 -AllowDirtySource

# Production must be a clean committed checkout.
pwsh -File services/deploy/build_deploy_package.ps1 `
  -Environment production -Version 2026.07.18
```

The build writes the expanded deployment copy and ZIP under
`deployment/<environment>/`. Upload
`deployment/development/ai-routing-runtime-development-<version>.zip` to
`/home/csda/AI_Routing/development`, or
`deployment/production/ai-routing-runtime-production-<version>.zip` to
`/home/csda/AI_Routing/production`.

Extract into the matching root and create a separate virtual environment in
each directory:

```bash
cd /home/csda/AI_Routing/production
unzip ai-routing-runtime-production-<version>.zip
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

cd /home/csda/AI_Routing/development
unzip ai-routing-runtime-development-<version>.zip
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

After extraction on Linux, restore execution permission for root shell scripts:

```bash
chmod +x /home/csda/AI_Routing/production/*.sh
chmod +x /home/csda/AI_Routing/development/*.sh
```

The server-runtime package includes only:

- the Common API, Common server UI, Smart API, and deployment verifier root entrypoints;
- the complete `smart_routing/` runtime source;
- the minimal `services/api/` adapter subset;
- root Linux recovery scripts for the selected environment;
- only the selected environment's systemd units and Common config template;
- the general config template, data-catalog pointer, requirements, and SHA-256 manifest.

It excludes `admin_tools`, `services/deploy`, `services/tests`, root analysis/UI
apps, Windows launchers, `tools`, `tests`, `osrm`, prompts used only by offline
preprocessing, documentation unless explicitly requested, server secrets, and all
data/runtime files.

Database administration commands are promoted separately with
`services/deploy/build_admin_tools_package.ps1`; see `admin_tools/db/README.md`.

Create server configs from the packaged templates; never upload workstation secret
files unchanged:

```bash
# production directory
cp config/common_vrp.prod.template.json config_common_vrp.json
cp config/config.template.json config/config.json

# development directory
cp config/common_vrp.dev.template.json config_common_vrp.dev.json
cp config/config.template.json config/config.json
```

Then inject environment-specific DB credentials, provider secrets, URLs, and ports.

## Files supplied separately on the server

Before bootstrap/start, hydrate the exact paths selected by
`config/data_catalog.json`, especially:

- `data/north_america/processed/service/.../Service_*_geocoded.csv`
- `data/north_america/processed/profile/.../*_production.xlsx`
- `data/north_america/reference/client/All_In_One_Master.xlsx` for the Common client job UI
- `data/north_america/reference/geospatial/tl_2024_us_zcta520.zip` for map geometry
- either `260310/production_input/atlanta_heavy_repair_lookup.csv` or
  `data/Notification_Symptom_mapping_20241120_3depth.xlsx`

Do not duplicate the complete `data/` or `260310/` trees in both application
roots. Hydrate approved immutable reference leaves from the shared store as
read-only paths. Keep jobs, technician uploads, caches, reports, and outputs in
environment-specific state. Never share the complete `data` or `260310` directory
with one writable symlink because both currently contain legacy writable paths.

Run the hydration/config gate before bootstrap or manual startup:

```bash
.venv/bin/python verify_deployment.py \
  --config config_common_vrp.json --expected-environment production
```

## systemd and health

Install the matching units from `systemd/`:

- production: `common-vrp.service`, `common-vrp-client.service`,
  `smart-routing.service`
- development: `common-vrp-dev.service`, `common-vrp-client-dev.service`,
  `smart-routing-dev.service`

OSRM units and scripts are not included in the application runtime artifact.
Install and operate them separately under `/home/osrm`.

```bash
sudo cp /home/csda/AI_Routing/production/systemd/common-vrp.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/production/systemd/common-vrp-client.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/production/systemd/smart-routing.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/development/systemd/common-vrp-dev.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/development/systemd/common-vrp-client-dev.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/development/systemd/smart-routing-dev.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Health endpoints:

- production: 8065 `/api/v1/common/contexts`, 8501 `/_stcore/health`,
  8055 `/api/v1/routing/health`
- development: 8066 `/api/v1/common/contexts`, 8503 `/_stcore/health`,
  8056 `/api/v1/routing/health`

## Release and rollback

Production release requires a clean artifact, manifest/hash review, DB and current
artifact/config/unit backup, migration rehearsal, data hydration verification,
and health checks. Rollback restores the previous artifact, server config, units,
and DB backup when schema compatibility requires it; bootstrap is not rollback.
