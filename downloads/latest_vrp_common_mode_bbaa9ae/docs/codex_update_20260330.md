# Codex Update Note 2026-03-30

## Why This File Exists

`UPDATED_BY_CODEX.md` is currently stored in a legacy encoding, so it could not be safely patched in-place during this update.

This note records the 2026-03-30 production-routing changes in UTF-8.

## Recorded Changes

- Added a dedicated chunk runner for standard line assignment rebuilds:
  - `sr_production_atlanta_assign_soft_line_chunks.py`
- Reworked Atlanta production assignment growth logic so new jobs are attached to the closest anchor among:
  - engineer home/start point
  - all already assigned stops
- Kept post-assignment worst-route move logic disabled.
- Added production map safeguards so partially rebuilt output CSVs do not crash the app during background regeneration.
- Kept production comparison modes separated into:
  - Actual Routes
  - Line Assign
  - Line Assign (Actual Attendance)
  - OSRM Assign
  - OSRM Assign (Actual Attendance)
- Added merged 320-ZIP Atlanta comparison mapping:
  - `atlanta_fixed_region_zip_3_manual320.csv`
  - manual fill rule:
    - `ATL West -> Region 1`
    - `ATL East -> Region 2`
    - `ATL South -> Region 3`
- Updated the Atlanta region comparison app so the left map uses the merged 320-ZIP mapping.
