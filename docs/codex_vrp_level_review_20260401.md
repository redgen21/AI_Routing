# VRP-Level Mode Review by Codex

Date: 2026-04-01

## Scope

Reviewed documents:
- `docs/VRP_LEVEL_MODE_IMPLEMENTATION.md`
- `IMPLEMENTATION_SUMMARY.md`
- `VRP_LEVEL_QUICK_START.md`

Reviewed implementation:
- `smart_routing/production_assign_atlanta_osrm.py`
- `sr_test_vrp_level_mode.py`
- `sr_test_vrp_level_unit.py`

This review focuses on algorithm correctness, not style.

## Conclusion

The documented design is reasonable:

1. Savings for initial assignment
2. 2-opt for local improvement
3. Simulated annealing for escaping local minima

However, the current implementation should not be treated as validated or production-safe yet.

There are correctness issues in the Savings stage, the 2-opt stage is not functioning correctly, and the optimization stages do not fully synchronize assignment metadata after swaps.

Because of those issues, the current VRP-Level results are not reliable enough to support the performance claims written in the documents.

## Main Findings

### 1. Savings assignment is selecting engineers incorrectly

Relevant code:
- `smart_routing/production_assign_atlanta_osrm.py:487`
- `smart_routing/production_assign_atlanta_osrm.py:506`

Problems:
- Engineer feasibility is checked against `job1` only, not both jobs in the pair.
- Savings is sorted in descending order across engineers.
- That formula makes a far-away engineer look better than a near engineer when comparing the same job pair.

Observed effect from a small reproduction:
- A far DMS engineer was chosen over a near DMS2 engineer.
- A TV job was also assigned to the wrong engineer type.

This is a correctness bug, not a tuning issue.

### 2. 2-opt route improvement is currently broken

Relevant code:
- `smart_routing/production_assign_atlanta_osrm.py:628`
- `smart_routing/production_assign_atlanta_osrm.py:631`
- `smart_routing/production_assign_atlanta_osrm.py:636`

Problems:
- The code calls `.tolist()` on a Python list and raises `AttributeError`.
- Even if that typo is fixed, the current DataFrame assignment pattern does not actually reorder the route rows in a meaningful way.

Observed effect from a small reproduction:
- `_two_opt_improve_routes()` failed with:
  - `AttributeError: 'list' object has no attribute 'tolist'`

So the documented second stage is not working as described.

### 3. Swap-based optimization does not update assignment metadata consistently

Relevant code:
- `smart_routing/production_assign_atlanta_osrm.py:571`
- `smart_routing/production_assign_atlanta_osrm.py:573`
- `smart_routing/production_assign_atlanta_osrm.py:674`
- `smart_routing/production_assign_atlanta_osrm.py:675`
- `smart_routing/production_assign_atlanta_osrm.py:745`
- `smart_routing/production_assign_atlanta_osrm.py:746`
- `smart_routing/production_assign_atlanta.py:1473`
- `smart_routing/production_assign_atlanta.py:1475`

Problems:
- 2-opt cross swaps and simulated annealing update `assigned_sm_code`.
- They do not also update:
  - `assigned_sm_name`
  - `assigned_center_type`
  - `home_start_longitude`
  - `home_start_latitude`

Why this matters:
- Output CSV rows can point to one engineer code and another engineer name.
- Schedule generation uses the stored home start coordinates from the first row in each group.
- That means the routing metrics can be computed from the wrong home location after a swap.

Observed effect from a small reproduction:
- `assigned_sm_code` changed from A to B, but `assigned_sm_name` still stayed as Engineer A.

This can silently corrupt route summaries.

### 4. Test readiness is weaker than the documents imply

Relevant code:
- `sr_test_vrp_level_unit.py:88`
- `sr_test_vrp_level_mode.py`

Problems:
- The unit test script failed immediately in the default Windows console because of Unicode box-drawing characters.
- The integration test did not complete within the review timeout window.

Observed effect:
- `sr_test_vrp_level_unit.py` failed with `UnicodeEncodeError` under CP949.
- `sr_test_vrp_level_mode.py` timed out during execution in review.

That does not prove the algorithm is wrong by itself, but it does mean the current validation story is incomplete.

## Assessment of the Algorithm Idea

The idea itself is good.

A staged approach of:
- initial construction
- local improvement
- metaheuristic escape

is a valid direction for a lightweight VRP-style solver without OR-Tools.

The issue is not the high-level design.
The issue is that the current implementation does not faithfully realize the intended algorithm yet.

So my view is:

- design direction: good
- implementation completeness: partial
- algorithm correctness: not yet established
- performance claims: should be treated as provisional until the bugs above are fixed

## Recommended Fix Order

### Priority 1

Fix Savings assignment so that:
- both jobs are feasibility-checked against the candidate engineer
- engineer ranking uses an objective that prefers lower real cost, not larger raw savings across engineers

### Priority 2

Rewrite 2-opt so that:
- route order is represented explicitly
- reorder operations actually change route sequence
- the function has deterministic unit tests

### Priority 3

After any reassignment or swap, synchronize all assignment metadata:
- engineer code
- engineer name
- center type
- home start coordinates

### Priority 4

Add short deterministic tests that verify:
- TV jobs never land on invalid engineers
- 2-opt reduces route distance on a known crossing example
- SA swaps preserve metadata consistency

## Final Opinion

The current VRP-Level mode should be described as an experimental implementation, not a completed and verified algorithm.

The documents are useful as design notes, but the implementation still has correctness bugs that can change assignment validity and route metrics.

After the core bugs are fixed and a few deterministic tests are added, it can become a solid approach.
