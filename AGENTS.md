# VRP Routing Project Agent Rules

## Main orchestration

The main agent owns request triage, task decomposition, selection of one primary
owner, cross-domain handoffs, conflict resolution, QA closure, and the final answer.

Do not assign the same implementation file to multiple agents concurrently.
Parallel work is limited to independent files or read-only investigation.
Use only the specialists materially affected by the request; the larger registry
does not mean every task should use every agent.

### Data access before delegation

- The main orchestration agent must not read full CSV/XLSX contents before
  delegation.
- The main orchestration agent may inspect only filenames, file sizes,
  extensions, headers, and row counts for triage.
- CSV/XLSX content inspection, profiling, transformation, validation, and writing
  must be delegated to `terra_routing_data`.
- Large files must be sampled or profiled through a bounded tool or script.
- If the main orchestration agent reads data directly as an exception, the
  reason and scope must be recorded.

## Agent routing

### `luna_worker`

Use for repository exploration, file/function lookup, logs, mechanical edits,
documentation, static checks, and running existing tests. It does not make data,
spatial, territory, solver, API, architecture, or release policy decisions.

### `terra_routing_data`

Use for source ingestion, canonical schemas, row quality, lineage, profiles,
data catalogs, rejects, DB migrations, seeds, backfills, and environment-explicit
data writes. It owns source meaning but not spatial truth or routing policy.

### `terra_geospatial`

Use for coordinate order/CRS, geocoding spatial quality, ZIP/ZCTA polygons,
point-in-polygon, centroids, adjacency, barriers, spatial indexes, and map-ready
geometry. It does not choose regions, solver objectives, or UI behavior.

### `terra_clustering_allocation`

Use for historical demand aggregation, DMS/DMS2 territory policy, geographic
clustering, postal-to-region assignment, region-count sweeps, capacity/radius/
balance analysis, candidate evaluation, and reviewed-plan evidence. It does not
perform a specific day's technician assignment or write approved plans directly.

### `terra_vrp_solver`

Use for OR-Tools/VROOM modeling, technician eligibility, fixed jobs, slots,
service/work/travel time, time windows, hard/soft constraints, objectives,
assignment, infeasibility, unassigned reasons, and visit sequencing.

### `terra_osrm_engine`

Use for OSRM Route/Table/Nearest semantics, profiles, map compatibility, matrix
shape/direction/units, chunking, snapping, fallbacks, cache keys, and engine
performance. Process deployment and graph rollout belong to `terra_platform`.

### `terra_routing_api`

Use for HTTP handlers, request validation, routing job state, idempotency,
transactions, cancel/retry, runtime repositories, API health/metrics, and
interfaces among DB, solver, OSRM, and clients.

### `terra_routing_ui`

Use for Streamlit/web screens, uploads, filters, maps, layers, KPIs, progress,
errors, downloads, accessibility, privacy presentation, and UI/result consistency.
The UI may manage masters only through explicit API workflows; it must not write
the DB directly or invoke solver internals for routing execution.

### `terra_routing_architecture`

Use for cross-layer boundaries, versioned contracts, logical database design,
dependency direction, security/privacy, failure semantics, reliability,
integration, concurrency, and ownership decisions. It reviews contracts rather
than taking over each layer's implementation.

### `terra_platform`

Use for dev/prod isolation, configuration, secrets, dependencies, packaging,
systemd/container/process lifecycle, health, observability, OSRM service rollout,
backup/restore, read-only SFTP inventory, exact runtime allowlists, artifact manifests,
deployment, promotion, and rollback. Remote upload/delete/restart/config changes require
explicit user authorization; inventory collection alone does not authorize mutation.

### `qa_routing_reviewer`

Use as the independent read-only gate after meaningful code, data-contract,
region, solver, matrix, API, UI, config, schema, migration, or release changes.
Critical and High findings must be resolved before completion is reported.

## Routing tie-break

When ownership is ambiguous, route by the decision being made:

- source meaning or data quality -> `terra_routing_data`
- coordinates, geometry, or spatial truth -> `terra_geospatial`
- territory/region policy -> `terra_clustering_allocation`
- road matrix truth -> `terra_osrm_engine`
- technician assignment objective or constraint -> `terra_vrp_solver`
- HTTP, transaction, or job state -> `terra_routing_api`
- presentation or user interaction -> `terra_routing_ui`
- cross-boundary contract -> `terra_routing_architecture`
- runtime or release -> `terra_platform`
- evidence collection only -> `luna_worker`
- independent verdict -> `qa_routing_reviewer`

## Primary code ownership

Ownership is primary, not shared. A secondary agent reviews through a handoff and
does not edit the primary owner's file concurrently.

- `tools/preprocess/`, `tools/data/`, `smart_routing/service_preprocess.py`,
  `profile_sync.py`, `data_catalog.py`, migrations and data seed tools
  -> `terra_routing_data`
- provider geocoders, CRS/geometry utilities, spatial parts of
  `smart_routing/area_map*.py`, postal barrier tools -> `terra_geospatial`
- `smart_routing/region_design.py`, `region_sweep.py`, candidate builders
  -> `terra_clustering_allocation`
- `smart_routing/osrm_routing.py`, matrix adapters and OSRM profile semantics
  -> `terra_osrm_engine`
- `smart_routing/production_assign_atlanta.py`,
  `production_assign_atlanta_osrm.py`, `production_assign*_vrp.py`,
  `vrp_mode_*.py`, solver evaluation
  -> `terra_vrp_solver`
- `services/api/`, `smart_routing/*api_server.py`, `common_vrp_runtime.py`,
  runtime portions of `common_vrp_db.py` -> `terra_routing_api`
- root `sr_*client*.py`, `sr_area_map.py`, `sr_production_map.py`
  -> `terra_routing_ui`
- root service execution scripts, `services/deploy/`, `systemd/`, config templates
  -> `terra_platform`
- shared contracts and architecture documents -> `terra_routing_architecture`

`routing_compare.py`, `common_vrp_db.py`, and `area_map.py` cross boundaries.
The main agent must name one primary owner for each change before implementation.
For `production_assign_atlanta_osrm.py`, `terra_vrp_solver` is primary for
assignment behavior and `terra_osrm_engine` reviews matrix/route semantics.

## Required handoffs

1. Data -> Geospatial: canonical coordinates/postals, rejects, lineage, and quality.
2. Geospatial -> Clustering: spatial feature table, CRS, polygons, adjacency/barriers.
3. Clustering -> Solver: immutable plan ID, postal map, centroids, policy, metrics.
4. OSRM -> Solver: normalized km/min matrix plus map/profile/cache/fallback metadata.
5. Architecture -> API/UI: frozen request/response/error/auth/version contract.
6. API -> UI: endpoints, state enum, idempotency, polling, and download contract.
7. Data -> Platform: migration, seed, backfill, hydration, and rollback requirements.
8. Solver -> Platform: approved routing-policy config keys, units, defaults, validation,
   and backward-compatibility requirements; Platform owns template/environment edits.
9. Architecture/API/OSRM -> Platform: runtime, health, security, and rollout needs.
10. Platform -> QA: clean artifact, environment impact, health, and rollback evidence.
11. Every meaningful boundary change -> `qa_routing_reviewer` final gate.

## Mandatory workflows

### Data or geocoding change

1. `terra_routing_data` owns schema, row accounting, lineage, and writes.
2. `terra_geospatial` reviews when coordinates, provider spatial accuracy, or
   postal/polygon mapping changes.
3. Affected clustering/solver consumers review the contract.
4. QA verifies quality, idempotency, and regression.

### Region/DMS/DMS2 change

1. Data and geospatial inputs may be inspected in parallel.
2. `terra_clustering_allocation` creates the versioned candidate and policy.
3. `terra_vrp_solver` evaluates it through the public routing contract.
4. Promotion requires coverage, no overlap/empty region, routing evidence,
   immutable plan ID, approval metadata, and checksum protection.
5. QA reviews coverage, fairness, route quality, and stability.

### Solver change

1. `luna_worker` locates the execution flow only when needed.
2. `terra_vrp_solver` implements objectives and constraints.
3. `terra_osrm_engine` validates matrix assumptions when affected.
4. `terra_routing_api` reviews payload/result compatibility when affected.
5. QA reviews feasibility, correctness, quality, and performance.

### OSRM change

1. `terra_osrm_engine` owns API/profile/matrix behavior.
2. `terra_vrp_solver` checks unit, direction, fallback, and route impact.
3. `terra_platform` acts only when server/profile/graph rollout changes.
4. QA reviews matrix correctness, fallback, cache, performance, and rollback.

### API/UI change

1. `terra_routing_architecture` freezes a material shared contract.
2. API and UI agents may implement in parallel only in different files.
3. UI values must trace to API/solver source fields.
4. QA compares state, errors, unassigned work, and displayed KPIs with results.

### End-to-end change

Use the affected subset in dependency order:

`data -> geospatial -> clustering -> OSRM -> solver -> API -> UI -> platform -> QA`

Skip unaffected stages; do not spawn agents merely to satisfy the sequence.

### Deployment or server operation change

1. `terra_platform` owns server inventory, exact runtime allowlist, packaging,
   environment impact, rollout, health, and rollback.
2. SFTP inventory is read-only. Upload, delete, move, chmod, service restart,
   systemd installation, remote config edit, and DB/data mutation each require
   explicit user authorization for that action.
3. Development may use a marked dirty-source verification artifact; production
   requires a clean checkout and must not use `-AllowDirtySource`.
4. Code runtime, admin tools, and server data are separate artifacts. Secrets,
   raw/local data, tests, caches, logs, and reports are excluded from code ZIPs.
5. Generated staging must match the allowlist exactly, pass entrypoint/dynamic-mode
   smoke imports, contain a no-BOM manifest with valid hashes, and pass secret scan.
6. `qa_routing_reviewer` independently approves the artifact and rollback evidence.

## Non-negotiable rules

- Do not hide unassigned jobs or reasons.
- Do not reassign fixed jobs without explicit policy.
- Unavailable and exception technicians receive no new work.
- State distance, time, slot, coordinate, and service-time units.
- Verify OSRM longitude/latitude order and matrix direction.
- Make DMS/DMS2 overlap, priority, and fallback explicit.
- Track input, region policy, matrix, solver setting, and result versions.
- Candidate, reviewed, seed, and runtime artifacts are distinct lifecycle stages.
- Do not modify the same module from multiple agents concurrently.
- Preserve user changes and unrelated dirty-worktree files.

## Definition of done

- Data: row accounting, schema/null/duplicate checks, lineage, rejects, idempotency.
- Geospatial: CRS/order/bounds, spatial coverage, fallback provenance, geometry version.
- Region: full non-overlap coverage, no empty region, radius/capacity/balance, route score.
- OSRM: matrix shape/units/direction, versioned profile/map/cache, fallback/error metrics.
- Solver: assigned once or explicit unassigned, hard constraints, baselines, runtime.
- API: validation, state/idempotency/transaction/failure compatibility and tests.
- UI: API traceability, visible errors/unassigned/units, KPI and map consistency.
- Architecture: dependency/contract/failure/security compatibility and migration path.
- Platform: clean artifact, isolation, startup health, migration/hydration, rollback.
- QA: independent reproduction, regression checks, missing tests, and residual risk.

## Project scope

These agents apply only to this VRP routing repository. Do not use them for
unrelated projects such as quant trading or SPM.
