# North America routing data

This is the canonical data root for the North America routing pipeline.

- `raw`: immutable source snapshots
- `processed`: normalized/geocoded service and production profile artifacts
- `processed/technicians`: address-free technician map view for server upload
- `planning/regions/candidates`: unapproved clustering outputs
- `reviewed/regions`: approved maps used by map review and promotion
- `db_input/regions`: DB-ready region seed files
- `db_input/technicians`: technician home/region inputs loaded into the DB
- `db_input/lookups`: DB bootstrap lookup tables
- `reference`: shared client, geospatial, and symptom reference files
- `runtime/<environment>`: uploaded jobs, technicians, and routing runs
- `reports`: generated analysis outputs
- `catalog`: migration and lineage manifests
- `archive/legacy_layout`: inactive verified copies from the previous canonical layout

Active files are selected only by `config/data_catalog.json`. The old `260310`
paths are compatibility sources during migration and are not the active catalog.

Raw and processed artifacts contain large/PII-bearing data and are supplied outside
Git. Reviewed postal maps, DB region seeds, and their migration manifest are kept
with the repository so a clean deployment has the active region contracts.

Server code packages never contain data. Build the upload-only data layout with:

```powershell
powershell -File services/deploy/build_server_data_package.ps1 -Version <version> -AcknowledgeSensitiveData
```

Upload the generated folder contents to `/home/csda/AI_Routing/`. Shared inputs
go to `shared/north_america`; environment-specific catalogs select writable state
under `state/development` and `state/production`.
