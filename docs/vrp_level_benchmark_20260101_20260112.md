# VRP-Level Benchmark 2026-01-01 to 2026-01-12

## Scope

- Validation window is restricted to `2026-01-01` through `2026-01-12`.
- Available service dates in this run: 2026-01-02, 2026-01-03, 2026-01-05, 2026-01-06, 2026-01-07, 2026-01-08, 2026-01-09, 2026-01-12

## Summary

| Metric | Average Gap | Worst Gap | Worst Date |
|---|---:|---:|---|
| Travel Distance (km) | 3.64% | 16.85% | 2026-01-07 |
| Work Std Dev (min) | 77.72% | 149.82% | 2026-01-09 |
| Max Work (min) | 12.84% | 27.95% | 2026-01-09 |

## Per-Date Results

| Date | VRP-Level km | VRP km | Travel Gap | VRP-Level Std | VRP Std | Std Gap | VRP-Level Max | VRP Max | Max Gap | Overflow Delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-01-02 | 577.67 | 531.55 | 8.68% | 16.81 | 12.48 | 34.68% | 277.20 | 256.79 | 7.95% | 0 |
| 2026-01-03 | 28.59 | 28.59 | 0.00% | 0.00 | 0.00 | 0.00% | 79.31 | 79.31 | 0.00% | 0 |
| 2026-01-05 | 628.70 | 550.77 | 14.15% | 34.83 | 21.44 | 62.40% | 312.64 | 258.66 | 20.87% | 0 |
| 2026-01-06 | 446.55 | 477.34 | -6.45% | 30.60 | 12.62 | 142.41% | 287.18 | 265.69 | 8.09% | 0 |
| 2026-01-07 | 472.98 | 404.76 | 16.85% | 37.93 | 18.43 | 105.86% | 292.95 | 258.10 | 13.50% | 0 |
| 2026-01-08 | 589.46 | 566.82 | 3.99% | 48.83 | 21.04 | 132.10% | 268.67 | 232.81 | 15.40% | 0 |
| 2026-01-09 | 231.79 | 253.35 | -8.51% | 43.76 | 17.52 | 149.82% | 194.03 | 151.65 | 27.95% | 0 |
| 2026-01-12 | 622.40 | 619.75 | 0.43% | 20.78 | 21.99 | -5.51% | 333.57 | 306.17 | 8.95% | 0 |

## Notes

- `vrp_level` uses the current OSRM heuristic pipeline.
- `vrp` uses the OR-Tools baseline solver.
- This report should be treated as the shared Codex/Claude benchmark artifact for the allowed January window.
