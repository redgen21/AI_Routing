# CSI/SITS Benchmark 2026-01-12

## Metrics

| Metric | CSI | SITS | VRP | CSI Gap | SITS Gap |
|---|---:|---:|---:|---:|---:|
| Engineers | 13 | 13 | 13 | 0.00% | 0.00% |
| Travel Distance (km) | 1018.18 | 948.44 | 911.95 | 11.65% | 4.00% |
| Work Std Dev (min) | 119.55 | 92.16 | 13.91 | 759.28% | 562.41% |
| Max Work (min) | 459.84 | 453.42 | 327.89 | 40.24% | 38.28% |
| Overflow 480 | 0 | 0 | 0 | 0 | 0 |

## Notes

- `csi` uses cluster + Hungarian queueing with global insertion.
- `sits` uses the same insertion pipeline plus targeted swap.
- `vrp` uses the OR-Tools baseline solver.
