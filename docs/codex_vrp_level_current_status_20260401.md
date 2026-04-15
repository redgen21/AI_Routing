# VRP-Level Current Status

Date: 2026-04-01

## Scope

- Performance validation scope is limited to `2026-01-01` through `2026-01-12`.
- Available service dates inside that range are:
  - `2026-01-02`
  - `2026-01-03`
  - `2026-01-05`
  - `2026-01-06`
  - `2026-01-07`
  - `2026-01-08`
  - `2026-01-09`
  - `2026-01-12`
- Dates outside this range must not be used for the VRP-level performance comparison.

## Current VRP-Level Algorithm

Current `vrp_level` mode in [production_assign_atlanta_osrm.py](c:/Python/북미 라우팅/smart_routing/production_assign_atlanta_osrm.py) is:

1. Apply micro-cluster preference hints per region.
2. Build a cluster-aware OSRM seed assignment with `base._assign_day(...)`.
3. Run a first travel-focused local improvement pass.
4. Run a second travel-focused refinement pass.
5. Run a balance-focused local improvement pass.
6. Build final summary and schedule using OSRM route metrics.

The active path is effectively:

- `Cluster-aware seed`
- `Travel improvement`
- `Travel refinement`
- `Balance improvement`

This is no longer the earlier `Savings + 2-opt + SA` path described in older Claude documents.

## Work Completed In This Round

### 1. Fixed feasibility violations in OSRM seed/grow assignment

Files:

- [production_assign_atlanta_osrm.py](c:/Python/북미 라우팅/smart_routing/production_assign_atlanta_osrm.py)

Changes:

- Added candidate filtering so `_matrix_seed_assign_jobs()` only assigns a job to engineers returned by `base._candidate_engineers(...)`.
- Added the same feasibility filtering to `_matrix_grow_assign_jobs()`.
- Added a reusable candidate-code map helper to avoid selecting invalid engineers during matrix-based assignment.

Reason:

- Before this fix, the OSRM seed/grow logic could assign infeasible work, especially TV jobs, because it optimized by distance without enforcing eligibility.

### 2. Fixed active-day fallback behavior in `_assign_day()`

Files:

- [production_assign_atlanta.py](c:/Python/북미 라우팅/smart_routing/production_assign_atlanta.py)

Changes:

- Changed leftover-job fallback from region-limited engineer pools to the full active day engineer pool.

Reason:

- A real failure existed on `2026-01-03`: there was one active engineer on the day, but that engineer was off-region relative to the job, so the previous region-scoped fallback produced `0` assignments.
- After the fix, `vrp_level` correctly assigns that job and matches VRP exactly for that day.

### 3. Changed the balance objective to use actual work-time spread

Files:

- [production_assign_atlanta.py](c:/Python/북미 라우팅/smart_routing/production_assign_atlanta.py)

Changes:

- Updated `_assignment_objective(...)` so the local-search comparison uses `total_work_min` standard deviation directly, instead of relying only on weighted job-count spread.
- Current objective ordering:
  - `travel_first`: overflow count -> total travel -> work std -> max work -> weighted job std
  - `balance_first`: overflow count -> work std -> max work -> total travel -> weighted job std

Reason:

- The target is VRP-like behavior.
- VRP comparison is based on route distance, work spread, and max work. The heuristic objective needed to align more closely with those same metrics.

### 4. Added regression coverage for the new failures

Files:

- [sr_test_vrp_level_unit_fixed.py](c:/Python/북미 라우팅/sr_test_vrp_level_unit_fixed.py)
- [sr_test_vrp_level_unit.py](c:/Python/북미 라우팅/sr_test_vrp_level_unit.py)

Added tests:

- `savings_prefers_near_engineer`
- `savings_respects_feasibility`
- `matrix_assigners_respect_feasibility`
- `assign_day_global_fallback`
- `vrp_level_pipeline_shape`

Current result:

- `7/7` tests passed during this round.

## Validation Notes

### Confirmed correctness improvement

`2026-01-03` benchmark after the fallback fix:

- `VRP-Level Engineers`: `1`
- `VRP Engineers`: `1`
- `Travel Distance`: `40.43 km` vs `40.43 km`
- `Work Std Dev`: `0.00` vs `0.00`
- `Max Work`: `84.97 min` vs `84.97 min`

This day now matches the VRP baseline exactly.

### Current performance gap still remaining

`2026-01-12` benchmark after the latest fixes:

- `VRP-Level Travel Distance`: `1109.29 km`
- `VRP Travel Distance`: `911.95 km`
- `Travel Gap`: `21.64%`
- `VRP-Level Work Std Dev`: `24.07`
- `VRP Work Std Dev`: `13.91`
- `Work Std Gap`: `72.99%`
- `VRP-Level Max Work`: `368.80 min`
- `VRP Max Work`: `327.89 min`
- `Max Work Gap`: `12.48%`

Interpretation:

- Correctness is improved.
- The heuristic is still not close enough to VRP performance on heavy days.
- The main remaining problem is optimization quality, not assignment validity.

## Important Status

- Benchmarking must remain restricted to `2026-01-01` through `2026-01-12`.
- Older documents that still describe the mode as `Savings + 2-opt + Simulated Annealing` are outdated relative to the current active implementation.
- Current work is in a valid intermediate state:
  - correctness regressions fixed
  - regression tests added
  - performance tuning still incomplete

## Next Tuning Direction

The next improvements should focus on the heavy-day gap inside the allowed range, especially `2026-01-02` and `2026-01-12`:

1. Stronger inter-engineer move and swap search.
2. Better route-cost-aware reassignment during local search.
3. More targeted reduction of total-work spread after travel optimization.
4. Repeat comparison only on dates between `2026-01-01` and `2026-01-12`.
