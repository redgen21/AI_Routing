# North America Routing Design Update 2026-04-01

## Scope

This document summarizes the routing design as of 2026-04-01 and focuses on:

- the current Atlanta production and live-routing architecture
- actual-attendance-limited dispatch behavior
- the routing / search methods that have been implemented or tested
- the detailed strengths and weaknesses of each method

This document supplements:

- [na_routing_design_20260318.md](/d:/python/북미%20라우팅/docs/na_routing_design_20260318.md)
- [na_routing_design_20260330_production_update.md](/d:/python/북미%20라우팅/docs/na_routing_design_20260330_production_update.md)

## Current Operating Assumptions

### Service scope

- Atlanta only
- DMS only
- DMS2 disabled
- TV jobs removed from service data

### Daily dispatch basis

- The production-style simulations and live routing both focus on `Actual Attendance`
- Dispatchable engineer count should match the real daily working headcount
- If a real working engineer is not in the current roster, that person must be replaced by a current-roster engineer
- The system must not assign work to a non-roster engineer

### Date basis

Two date models now exist in the codebase.

Production simulation:

- generally uses the preprocessed `service_date` / `service_date_key`

Live routing:

- now uses `PROMISE_DATE` first
- falls back to `PROMISE_TIMESTAMP`
- only falls back to `REPAIR_END_DATE_YYYYMMDD` if promise-date fields are unavailable

This matches the live dispatch use case, because production routing is intended for future promised work rather than completed historical work.

## Current System Split

### Offline / simulation app

Main file:

- [sr_production_map.py](/d:/python/북미%20라우팅/sr_production_map.py)

Purpose:

- compare routing strategies on prepared Atlanta datasets
- inspect route maps, schedules, and engineer summaries
- compare selected dates without querying live systems

### Live / operational app

Main file:

- [sr_live_atlanta_routing.py](/d:/python/북미%20라우팅/sr_live_atlanta_routing.py)

Purpose:

- choose a start date and end date in Streamlit
- query service data directly from BigQuery
- reuse cached geocodes
- build Atlanta-ready inputs in memory
- run routing immediately
- show route maps and per-engineer schedules
- allow CSV download

Supporting modules:

- [bigquery_runtime.py](/d:/python/북미%20라우팅/smart_routing/bigquery_runtime.py)
- [live_atlanta_runtime.py](/d:/python/북미%20라우팅/smart_routing/live_atlanta_runtime.py)

## Live Data Retrieval Design

### Query source

The live app reads SQL from:

- [select_data.sql](/d:/python/북미%20라우팅/smart_routing/select_data.sql)

Authentication is read from Streamlit secrets.

### Query execution flow

1. User selects `Start Date` and `End Date`
2. SQL date ranges are rendered dynamically
3. BigQuery query runs
4. Result is returned as a DataFrame

### Geocode merge flow

The queried data does not rely on a fresh full geocode pass each time.

Instead:

1. Existing Census and Google geocode caches are loaded
2. Matching address keys are reused
3. Only failed / uncached addresses go to fallback geocoders

### Runtime Atlanta preparation flow

1. Normalize service columns
2. Deduplicate by `GSFS_RECEIPT_NO`
3. Filter to Atlanta
4. Remove excluded center types
5. Merge region ZIP mapping
6. Apply heavy-repair enrichment
7. Reuse fixed engineer-region and home-geocode inputs

## Current Routing Modes

The codebase now has several routing modes. Not all are meant for permanent production use, but all are relevant for design comparison.

### 1. Actual Routes

Purpose:

- show what actually happened historically
- no simulated reassignment

Behavior:

- routes are built from actual service records by actual engineer code

Use:

- baseline comparison

### 2. Iteration Assign (Actual Attendance)

Type:

- straight-line distance
- actual-attendance-limited
- iterative improvement

Purpose:

- fast simulation baseline
- lower-cost routing comparison

Main idea:

- build an initial assignment
- repeatedly move jobs when the objective improves

### 3. Iteration OSRM Assign (Actual Attendance)

Type:

- OSRM matrix-based assignment
- actual-attendance-limited
- iterative improvement

Purpose:

- current main realistic heuristic
- best current production-style operating mode

### 4. LNS Assign (Actual Attendance, 3 Days)

Type:

- current OSRM iteration / local-improvement path
- exported under a dedicated comparison label

Purpose:

- stand-in for a local-search-style heuristic comparison
- compare against VRP on a limited 3-day slice

### 5. VRP Assign (Actual Attendance, 3 Days)

Type:

- OR-Tools multi-vehicle VRP
- actual-attendance-limited
- 3 selected dates only

Purpose:

- closer to true route optimization
- benchmark current heuristic quality

## Detailed Design by Search Method

This section explains each search / assignment method in more detail.

### Method A. Sequence Assignment

Core idea:

- Build a single route-like global customer order first
- Split that sequence into contiguous chunks
- Give each chunk to an engineer

Implementation history:

- inspired by earlier Korea routing notebooks
- implemented in Actual Attendance variants for testing

Detailed flow:

1. Build a date-level customer list
2. Create a nearest-neighbor sequence over all customers
3. Divide the sequence by weighted workload
4. Assign each block to one engineer
5. Build final route order per engineer

Advantages:

- very fast
- easy to understand
- naturally produces balanced job blocks
- reduces some early seed-order randomness

Weaknesses:

- weak engineer-home awareness
- global order can cut across natural personal territories
- not robust under strong skill constraints
- can still create awkward personal route boundaries

Recommended use:

- fast baseline only
- not recommended as final production logic

### Method B. Anchor-Based Greedy Assignment

Core idea:

- assign each new job to the engineer whose current anchor set is closest

Anchor set means:

- home / start point
- plus all already assigned customer points

Detailed flow:

1. Build active engineer pool
2. Seed engineers with initial jobs
3. For each remaining job, evaluate candidate engineers
4. Score using:
   - nearest anchor distance
   - workload penalties
   - region penalties
   - 480-minute overflow penalties
5. Assign greedily
6. Build final route order afterward

Advantages:

- much better than tail-only growth
- simple to maintain
- allows soft-region support
- works with skill constraints

Weaknesses:

- still greedy
- nearest-anchor cost is only a local approximation
- does not evaluate full route insertion cost
- can still produce visually tangled routes

Recommended use:

- acceptable as a simple baseline
- not ideal as final optimized logic

### Method C. Iterative Improvement / Local Search

Core idea:

- initial assignment is provisional
- continue moving jobs if the total solution improves

Detailed flow:

1. Build initial assignment
2. Evaluate objective
3. Try one-job moves between engineers
4. Accept only if the move improves the objective
5. Repeat until no improvement

Current objective emphasis:

- reduce `max total_work`
- reduce weighted-job imbalance
- reduce travel burden

Advantages:

- reduces early assignment luck
- improves over pure greedy assignment
- flexible under operational constraints
- practical for existing codebase

Weaknesses:

- still heuristic, not globally optimal
- quality depends on move operators
- can be slower than sequence-based approaches
- if the objective is incomplete, route quality can still look wrong

Recommended use:

- strong practical default
- good stepping stone toward LNS

### Method D. OSRM Matrix-Based Iteration

Core idea:

- same iterative assignment philosophy
- but candidate costs are based on OSRM road-network matrices

Detailed flow:

1. For seed and grow assignment, build OSRM distance/duration matrices
2. Score candidate engineers using matrix values
3. Apply iterative reassignment
4. Build final route and schedule with OSRM

Important note:

- current implementation uses matrix/table in the assignment stage
- it no longer relies on repeated pair-distance calls for the main grow loop

Advantages:

- road-network-aware
- much more realistic than straight-line assignment
- better practical dispatch quality
- strongest currently operational heuristic

Weaknesses:

- slower than straight-line modes
- still not a full route-insertion solver
- assignment can still look globally suboptimal
- route quality can suffer when the assignment heuristic locks in a bad local structure

Recommended use:

- current best production-style heuristic
- recommended for live routing today

### Method E. LNS-Style Heuristic Comparison

Current role:

- the 3-day `LNS` comparison output currently uses the OSRM iterative local-improvement path as its practical stand-in

Design goal:

- move toward a true Large Neighborhood Search structure

Future full LNS shape would be:

1. start from an initial feasible solution
2. destroy part of the solution
3. rebuild the removed portion
4. run local improvements
5. repeat until time limit or no improvement

Advantages:

- usually better than simple greedy assignment
- highly flexible with operational constraints
- good tradeoff between quality and runtime
- easier to evolve than a fully exact solver

Weaknesses:

- requires careful operator design
- no guarantee of optimality
- runtime quality depends heavily on destroy / repair strategy

Recommended use:

- best long-term practical direction for operational routing

### Method F. VRP Solver

Current implementation:

- OR-Tools-based multi-vehicle VRP for the 3-day comparison slice

Detailed flow:

1. Deduplicate jobs by receipt
2. Build engineer start nodes and job nodes
3. Build OSRM matrix between starts and jobs
4. Create one vehicle per engineer
5. Restrict vehicles by skill eligibility
6. Add time dimension with 480-minute limit
7. Solve with OR-Tools using:
   - `PATH_CHEAPEST_ARC`
   - `GUIDED_LOCAL_SEARCH`
8. Convert solved order into schedule and route outputs

Advantages:

- much closer to true route optimization
- assignment and route order are solved together
- naturally models route sequence cost
- useful benchmark against heuristics

Weaknesses:

- more complex to maintain
- harder to inject custom business penalties cleanly
- can become slow on larger day ranges
- current version is intentionally limited to a 3-day comparison slice

Recommended use:

- benchmark and decision support
- not yet the default live engine

## Comparison Summary

### Sequence Assignment

Pros:

- fastest
- easy to explain
- balanced by construction

Cons:

- weakest realism
- poor home-awareness
- not suitable as final production dispatch

### Straight-Line Iteration

Pros:

- cheap to run
- operationally flexible
- good baseline

Cons:

- road-network blind
- route quality can be misleading

### OSRM Iteration

Pros:

- best currently operational method
- realistic travel costs
- flexible with real-world constraints

Cons:

- still heuristic
- slower than line-based methods
- can still create tangled-looking routes

### LNS Direction

Pros:

- strongest long-term practical path
- can materially improve current heuristic quality

Cons:

- not fully built yet as a separate full operator framework

### VRP Solver

Pros:

- closest to true optimization
- best benchmark method

Cons:

- more complex
- more expensive to run
- harder to operationalize directly without simplification

## Recommended Position as of 2026-04-01

### For live operations now

Use:

- `OSRM iteration`
- `Actual Attendance`
- `PROMISE_DATE`-based retrieval

Reason:

- best balance of realism, maintainability, and runtime

### For design comparison

Compare:

- `LNS / OSRM iteration`
- `VRP`

on small selected day slices

Reason:

- this gives a realistic answer to:
  - how far the current heuristic is from a solver-based route plan
  - whether the extra optimization quality is operationally worth the extra complexity

### For next improvement step

Recommended next step:

- upgrade OSRM iteration from nearest-anchor scoring to route insertion cost

Reason:

- this is the highest-value improvement that still preserves the current code structure

## Files Relevant to This Design

### Core assignment

- [production_assign_atlanta.py](/d:/python/북미%20라우팅/smart_routing/production_assign_atlanta.py)
- [production_assign_atlanta_osrm.py](/d:/python/북미%20라우팅/smart_routing/production_assign_atlanta_osrm.py)
- [production_assign_atlanta_vrp.py](/d:/python/북미%20라우팅/smart_routing/production_assign_atlanta_vrp.py)

### Apps

- [sr_production_map.py](/d:/python/북미%20라우팅/sr_production_map.py)
- [sr_live_atlanta_routing.py](/d:/python/북미%20라우팅/sr_live_atlanta_routing.py)

### Data retrieval and runtime prep

- [select_data.sql](/d:/python/북미%20라우팅/smart_routing/select_data.sql)
- [bigquery_runtime.py](/d:/python/북미%20라우팅/smart_routing/bigquery_runtime.py)
- [live_atlanta_runtime.py](/d:/python/북미%20라우팅/smart_routing/live_atlanta_runtime.py)

### Comparison runner

- [sr_production_atlanta_compare_lns_vrp_3days.py](/d:/python/북미%20라우팅/sr_production_atlanta_compare_lns_vrp_3days.py)
