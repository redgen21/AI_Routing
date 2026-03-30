# Atlanta Production Routing Update 2026-03-30

## Scope

This document supplements the existing North America routing design and reflects the current Atlanta production-routing logic as of 2026-03-30.

## Current Production Modes

The production map compares four routing modes.

- `Actual Routes`
- `Line Assign`
- `Line Assign (Actual Attendance)`
- `OSRM Assign`
- `OSRM Assign (Actual Attendance)`

Interpretation:

- `Line` modes score assignment proximity with straight-line distance.
- `OSRM` modes score assignment proximity with road-network travel metrics.
- `Actual Attendance` modes restrict the working pool to the engineers who actually worked on the date, with replacements allowed only up to the real headcount gap.

## Staffing Model

Atlanta production routing still assumes:

- `3` base regions
- `15 DMS`
- `2 DMS2`

But dispatch no longer treats region ownership as a hard wall.

- Base region ownership is retained as a preference.
- Cross-region assignment is allowed with a penalty.
- The goal is to let nearby engineers naturally absorb work across region borders when that produces a better operational route.

## Repairability Rules

- `MAJOR DEALER` and `REGIONAL DEALER` jobs are excluded.
- Normal job service time: `45 min`
- Heavy repair service time: `100 min`
- `TV` jobs are `DMS2 only`
- `REF` heavy repair is allowed only when `AREA_PRODUCT_FLAG = Y`

## Start Point

- DMS routes start from geocoded home coordinates.
- DMS2 starts from home coordinates when available.
- If DMS2 home coordinates are unavailable, the fallback anchor is the designated region center.

## Assignment Logic

### Previous Problem

Earlier production assignment tended to grow routes using only the engineer's last assigned stop.

That made the provisional assignment order behave too much like a visit sequence, which is not appropriate for North America batch routing.

### Current Rule

Assignment now uses a `seed + grow` model.

1. Build the active engineer pool for the date.
2. Seed each active engineer with an initial nearby job when feasible.
3. For every remaining job, evaluate candidate engineers using:
   - repairability constraints
   - DMS / DMS2 eligibility
   - attendance limit, if enabled
   - soft-region penalty
   - current workload
   - proximity to the engineer's existing assigned work

Important change:

- Grow scoring is no longer based only on the engineer's current last stop.
- A new job is scored against the closest anchor among:
  - the engineer's start point
  - every already assigned customer point

This means the system now tries to assign "near existing cluster" work rather than "near the current chain tail" work.

## Final Route Order

Assignment order is not treated as final visit order.

After assignment is complete:

- the route is rebuilt
- final stop sequence is generated separately
- displayed distance, duration, and schedule use OSRM route construction

This matches the North America requirement:

- group nearby work first
- decide the fastest practical route afterward

## Rebalance Logic

The previous post-processing idea of:

- finding the engineer with the longest route
- moving one job at a time to another engineer

is currently disabled.

Reason:

- straight-line-based post moves often made real road routes worse
- OSRM-based local improvement was too slow for full multi-day rebuilds

Current production routing therefore ends at:

- seed assignment
- grow assignment
- final route ordering

## Actual Attendance Variant

The actual-attendance-limited version is intended to answer a different question from the base simulation.

It asks:

- if we keep the real daily attendance constraint,
- can the redesigned routing still be executed,
- and how much work remains feasible or infeasible?

Rules:

- prefer the actually worked engineers of that day
- if a planned engineer is missing, replacement engineers can be used only up to the real attendance gap
- total working headcount should align with real daily attendance, not with the full modeled pool

## ZIP Region Definition

Two Atlanta ZIP-region mappings are maintained.

### Production Routing ZIP Set

- `260310/production_input/atlanta_fixed_region_zip_3.csv`

This is the original production routing region file.

### Merged 320-ZIP Comparison Set

- `260310/production_input/atlanta_fixed_region_zip_3_manual320.csv`

Rules:

- keep the current visible ZIP region assignments as-is
- fill only the remaining ZIPs from the manual `ATL Three Markets.xlsx` file
- manual bucket mapping:
  - `ATL West -> Region 1`
  - `ATL East -> Region 2`
  - `ATL South -> Region 3`

## Operational Notes

- `sr_production_map.py` now defaults to `Actual Routes`
- actual-route schedule building runs only for the selected date
- the map now contains guard logic so partially rebuilt CSV outputs do not crash the app while background rebuilds are still running
