# CSI/Hybrid/SITS Benchmark 2026-01-12

## Metrics

| Metric | CSI | Hybrid | SITS | VRP | CSI Gap | Hybrid Gap | SITS Gap |
|---|---:|---:|---:|---:|---:|---:|---:|
| Engineers | 13 | 13 | 13 | 13 | 0.00% | 0.00% | 0.00% |
| Travel Distance (km) | 1016.50 | 1010.12 | 1060.49 | 911.95 | 11.46% | 10.76% | 16.29% |
| Work Std Dev (min) | 42.19 | 21.44 | 15.88 | 13.91 | 203.22% | 54.12% | 14.12% |
| Max Work (min) | 398.52 | 342.06 | 342.06 | 327.89 | 21.54% | 4.32% | 4.32% |
| Overflow 480 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## Notes

- `csi` uses non-OR-Tools global insertion with `max_weight=1.9` and `std_weight=2.7`.
- `hybrid` uses `csi` insertion, then capped relocation with `span_weight=1.3`, `travel_budget=+3%`, and `15` passes.
- `sits` starts from `csi` and applies relocation with `span_weight=2.0` for up to `20` passes.
- `vrp` uses the OR-Tools baseline solver with the default span coefficient.
