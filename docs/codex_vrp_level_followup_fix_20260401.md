# VRP-Level Follow-up Fix Note

Date: 2026-04-01

## What was fixed

### 1. Savings seed engineer ranking

The remaining correctness bug was in the engineer ranking inside the Savings seed.

The previous implementation:
- checked pair feasibility across both jobs
- but still ranked candidate engineers by the old savings value

That ranking could still prefer a farther engineer over a nearer engineer for the same pair.

The fix now does this instead:
- compute the best individual start cost for each job
- compute the best feasible shared-engineer pair route cost
- keep a pair only when the shared assignment has positive gain over individual assignment
- choose the engineer with the lowest real pair route cost

This removes the far-engineer bias that was still reproducible after the first round of fixes.

### 2. Unit test entrypoint

`sr_test_vrp_level_unit.py` now delegates to the ASCII-safe regression suite in `sr_test_vrp_level_unit_fixed.py`.

This removes the CP949 console failure from the original unit script.

### 3. Regression tests

The fixed unit test now validates real behavior instead of only checking source strings.

Added coverage:
- near engineer is preferred over far engineer for the same feasible pair
- mixed feasibility is respected for non-TV and TV jobs
- the current `vrp_level` branch still uses:
  - iterative improvement
  - local rebalance

### 4. Integration test labels

`sr_test_vrp_level_mode.py` now describes the actual pipeline:
- Savings
- Iteration
- Local Rebalance

It no longer claims to run 2-opt and simulated annealing.

## Verification run

Commands run after the fix:

```powershell
python -m py_compile smart_routing\production_assign_atlanta_osrm.py sr_test_vrp_level_mode.py sr_test_vrp_level_unit.py sr_test_vrp_level_unit_fixed.py
python sr_test_vrp_level_unit_fixed.py
python sr_test_vrp_level_unit.py
```

Targeted regression also confirmed that the near engineer is now selected in the previous failing reproduction.

## Remaining note

The large narrative documents created earlier still describe the older "2-opt + simulated annealing" story.

After this fix, the live `vrp_level` path is better described as:

1. Savings-style pair seed
2. Iterative improvement
3. Local rebalance
