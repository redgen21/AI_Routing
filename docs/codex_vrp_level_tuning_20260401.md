# VRP-Level Tuning Update

Date: 2026-04-01

## Summary

This update changed the live `vrp_level` path from the earlier Savings-centered flow to a stronger cluster-based hybrid flow.

Current `vrp_level` pipeline:

1. cluster-aware OSRM seed
2. travel improvement pass
3. travel refinement pass
4. balance improvement pass

The change was driven by measured performance on `2026-01-12`.

## Why it changed

The earlier Savings-based `vrp_level` path was materially worse than both `cluster_iteration` and the OR-Tools VRP baseline.

Measured before the hybrid retune on `2026-01-12`:
- travel distance was far above VRP
- work-time spread was also too high

The cluster-based hybrid path performed better in quick search experiments, so it replaced the previous live `vrp_level` branch.

## Current benchmark

Benchmark date: `2026-01-12`

| Metric | VRP-Level | VRP | Gap |
|---|---:|---:|---:|
| Engineers | 13 | 13 | 0.00% |
| Travel Distance (km) | 1063.55 | 911.95 | 16.62% |
| Work Std Dev (min) | 23.08 | 13.91 | 65.89% |
| Max Work (min) | 345.66 | 327.89 | 5.42% |
| Overflow 480 | 0 | 0 | 0 |

This is better than the earlier broken `vrp_level` run, but it is still not close enough to call VRP-equivalent.

## Key code changes

- `smart_routing/production_assign_atlanta_osrm.py`
  - fixed the remaining Savings engineer-ranking bug
  - replaced the live `vrp_level` path with the cluster-seed hybrid flow
- `smart_routing/production_assign_atlanta_vrp.py`
  - fixed OR-Tools vehicle-domain handling so the VRP baseline can run
- `sr_test_vrp_level_unit.py`
  - now delegates to the ASCII-safe regression suite
- `sr_test_vrp_level_unit_fixed.py`
  - now includes deterministic behavioral checks
- `sr_test_vrp_level_mode.py`
  - now describes the actual live pipeline more accurately

## Automation added

### `sr_benchmark_vrp_level_vs_vrp.py`

Purpose:
- runs the `vrp_level` vs `vrp` benchmark for a target date
- can write a Markdown benchmark report

Current benchmark report output:
- `docs/vrp_level_benchmark_20260112.md`

### `sr_watch_vrp_feedback.py`

Purpose:
- watches markdown files in repo root, `docs/`, and `.claude/`
- when a markdown file changes, it reruns:
  - `git diff --stat`
  - `python sr_test_vrp_level_unit.py`
  - `python sr_benchmark_vrp_level_vs_vrp.py --date 2026-01-12`
- writes the current validation status to:
  - `docs/vrp_feedback_watch_status.md`

Important limitation:
- this watcher can automatically detect and re-validate changes
- it cannot autonomously rewrite code without an active model session

## Next tuning direction

The largest remaining gap is still work spread relative to VRP, while travel is also still too high.

The next promising areas are:

1. swap-based local search between engineers, not only single-job moves
2. objective tuning that uses real work-time spread directly during improvement
3. tighter region-aware move generation so balance improvements do not pay too much travel cost
