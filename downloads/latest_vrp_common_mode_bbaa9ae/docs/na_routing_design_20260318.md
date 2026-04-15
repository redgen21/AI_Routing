# North America Routing Design 2026-03-18

## Goal

North America routing is not a Korean-style sequential timetable assignment problem.

- Korea: receipt order matters because one slot is fixed once assigned.
- North America: a full day of receipts is collected first, then batch routing decides the execution order.

The primary optimization target is not only travel distance reduction.
The main target is to estimate how many effective SMs are needed after integrating nearby regions, while also keeping travel distance and assignment balance under control.

## Phase 1: Integrated Region Design

Inputs:

- geocoded daily service data
- updated Zip Coverage
- updated Slot

Process:

1. Aggregate service volume by `STRATEGIC_CITY_NAME + POSTAL_CODE`.
2. Estimate city-level effective daily capacity from updated Slot.
3. Compute required SM count by city.
4. Divide the city into integrated operating regions using weighted geographic clustering on postal centroids.
5. For each integrated region, calculate:
   - service count
   - postal count
   - required SM count
   - service count per required SM

Current prototype rule:

- active center types: `DMS`, `DMS2`
- target integrated region size: `5 SM`
- required SM count in a region:
  `ceil(region_service_count / city_avg_slot_capacity)`
- region construction method:
  - aggregate daily service volume by `STRATEGIC_CITY_NAME + POSTAL_CODE`
  - compute a postal centroid from mean service latitude and longitude
- estimate city-level required SM count from total services and average slot capacity
- estimate city-level required SM count from average daily service volume using `REPAIR_END_DATE_YYYYMMDD`
- convert that into an integrated region count using the target `SM per region`
  - run weighted geographic clustering on postal centroids using `service_count` as the weight
  - assign ZIPs with a soft objective that combines:
    - geographic distance to region center
    - service-count balance penalty against the target service volume per region
    - region-radius penalty when a ZIP would expand the region too far
  - keep one seed ZIP in every region so the requested region count is preserved
  - dissolve assigned ZIP polygons by `region_id`

Tracked metrics:

- `service_gap_pct`: how far each region is from the target service volume
- `max_radius_km`: maximum ZIP-to-center distance inside the region
- `avg_radius_km`: average ZIP-to-center distance inside the region

Current daily-demand assumption:

- the source service file spans multiple dates
- region design must not treat the whole file as one-day demand
- city and postal demand are computed on a daily basis from `REPAIR_END_DATE_YYYYMMDD`
- current `service_count` in region-design outputs means average daily service count
- current operational assumption for SM throughput is `5 services per SM per day`
- `avg_slot_capacity` is still retained as a reference metric, but `required_sm_count` can be computed using `effective_service_per_sm`

Outputs:

- city summary
- integrated region summary
- postal-to-region assignment
- service-to-region assignment
- integrated region map layer for visual comparison against current AREA coverage

## Phase 2: Routing Inside Each Integrated Region

After integrated regions are fixed:

1. Restrict candidate SMs to the SM pool inside the same integrated region.
2. Ignore product priority and timetable ordering priority.
3. Assign each service to the nearest available SM.
4. Sequence visits inside each SM route to reduce travel distance.
5. Evaluate:
   - distance per SM
   - assigned count per SM
   - assignment balance

## Comparison Prototype

Current comparison prototype:

- current scenario:
  - actual daily `SVC_ENGINEER_CODE` assignments from service data
- integrated scenario:
  - region-based daily regrouping
  - daily SM count estimated by `ceil(daily_region_jobs / effective_service_per_sm)`
- route metric:
- current prototype uses straight-line haversine distance with nearest-neighbor open routing
- OSRM mode is prepared, but local North America OSRM ports were not fully available during this run
- region design can now switch between:
  - `balanced`: distance + service-balance + radius penalty
  - `weighted_kmeans`: the initial simpler weighted k-means approach
- routing comparison now supports `city_osrm_else_haversine`
  - Korea uses `http://20.51.244.68:5000`
  - `Los Angeles, CA` uses `http://20.51.244.68:5001`
  - `Atlanta, GA` uses `http://20.51.244.68:5002`
  - other cities fall back to haversine
- The current OSRM comparison implementation uses:
  - haversine distance for fast candidate assignment inside a region/day
  - OSRM `route` on the final ordered stop list for final road distance and travel time
- Constrained assignment now supports:
  - `service_time_per_job_min`
  - `max_work_min_per_sm_day`
  - `max_travel_min_per_sm_day`
  - `max_travel_km_per_sm_day`
- When no existing SM/day route can accept a new job within the limits, an additional SM/day route is opened automatically.

Current result for `Atlanta, GA` and `Los Angeles, CA`:

- deployed SM count decreases
- jobs per SM increases
- but daily distance and duration increase too much
- under the latest `weighted_kmeans` region design, the increase is smaller than the previous balanced run, but it is still material

Interpretation:

- the current integrated region design is still too coarse geographically
- before claiming staffing efficiency, region count must also be constrained by geographic span or max radius

## Candidate Region Count Sweep

For candidate-count sweep, the project now evaluates fixed region-count options per city.

- Atlanta candidates: `2, 3, 4, 5`
- Los Angeles candidates: `3, 4, 5, 6`

Each candidate is compared against the current operation using:

- average deployed SM count
- average jobs per SM
- jobs-per-SM standard deviation
- average distance per SM
- average duration per SM
- `p95` and `max` of daily total workload per SM
- `480 min` overflow count and overflow ratio

A heuristic `balance_score` is also calculated to rank candidates.
Lower score is better.
It penalizes:

- distance-per-SM increase
- duration-per-SM increase
- jobs-per-SM standard deviation increase
- jobs-per-SM increase

and gives partial credit for deployed-SM reduction.

## Current Assumptions

- `1 receipt = 1 service job`
- `REPAIR_RECEIPT_TIMESTAMP` order is not used for batch optimization priority
- all products are repairable by all active SMs
- updated Zip Coverage and Slot are already synchronized against service activity

## Initial Deliverables

- `smart_routing/region_design.py`
- `sr_region_design.py`
- `260310/input/region_design_postal_*.csv`
- `260310/input/region_design_service_*.csv`
- `260310/output/region_design_city_summary_*.csv`
- `260310/output/region_design_region_summary_*.csv`

## Map Explorer

- `sr_area_map.py` now uses a single-map explorer layout with left-side filters rather than a stacked current/new map view.
- Filter controls:
  - date: `ALL` or a specific service day
  - city: `Atlanta, GA` or `Los Angeles, CA`
  - region type: existing area or integrated region
  - `AREA NAME`: existing `AREA_NAME` or generated `Region 1..N`
  - assigned SM code
- Existing-region view uses the synchronized primary-area ZIP coverage.
- Integrated-region view uses the latest best candidate counts from `region_count_sweep_summary_*.csv`.
  - current best counts: Atlanta `5`, Los Angeles `6`
- Integrated daily SM assignments are rebuilt from the constrained routing assignment logic so the map can show:
  - assigned service points
  - synthetic assigned SM codes such as `R01_SM01`
  - route line for the selected SM
- Route drawing uses OSRM only when a selected city has an available OSRM endpoint and falls back to haversine ordering otherwise.
- Existing-area map filtering now resolves the current ZIP layer area key correctly (primary_area_name), so selecting an AREA_NAME in existing-region mode no longer fails.
- Route explorer sidebar now shows total service count and an SM-level service-count table for the currently selected date/area scope, plus the selected-SM count.
- Route explorer now renders numbered customer markers when a specific date is selected. Marker numbers follow the displayed SM route order.
- Route explorer sidebar summary now shows visible service count, assigned SM count, average route distance, and average route duration for the current filter scope instead of repeated date/area/SM counts.
- The route explorer no longer uses a simple ±‚¡∏¡ˆø™/Ω≈±‘¡ˆø™ toggle. It now supports city-specific candidate-count options from the latest sweep summary, such as ±‚¡∏¡ˆø™, Ω≈±‘¡ˆø™2, Ω≈±‘¡ˆø™3, etc., and displays a candidate comparison table for the selected city.
- Added sr_export_daily_stats.py to export date-level routing statistics into a multi-sheet Excel workbook. Sheets are split by city and region option (±‚¡∏¡ˆø™, Ω≈±‘¡ˆø™N).
- Daily statistics workbook sheets now append a second table listing, for each date, the SM code with the maximum total workload and the associated workload metrics.
- Routing statistics are now aligned with the map route logic. Group distance/time metrics use the ordered-route calculation (	able for stop ordering + oute for final path) instead of the previous 	rip-based metric.
- The daily statistics workbook now also includes one city-level overall summary sheet per city (¿¸√º≈Î∞Ë) comparing ±‚¡∏¡ˆø™ and each Ω≈±‘¡ˆø™N candidate in a single table.
- Existing-region map markers now expose service-center buckets (DMS, DMS2, ASC). In existing-region mode, ASC numbered markers are rendered with a thicker border, and the sidebar service-count summary includes DMS/DMS2/ASC counts.
- Existing-region map data now preserves all center types from the service file. ASC counts in the sidebar and popups are based on real service rows; only integrated-region calculations remain limited to DMS/DMS2.
- Region clustering, integrated assignment, candidate sweep statistics, and exported daily statistics now use the full service workload including ASC. The previous DMS/DMS2-only workload assumption has been removed from these analytics.
- Numbered customer markers now render ASC jobs with a black border so outsourced-service stops stand out from DMS/DMS2 stops on the map.
- Existing-region-only ASC styling is now enforced in the map explorer. Integrated-region numbered markers no longer inherit the ASC black border from original service rows.
- Integrated-region routing no longer uses the previous greedy per-job assignment. It now follows a ZÍ±¥-Ï£ºÎßê-style day-batch approach: each region/day is clustered as a whole, and the minimum SM count is increased until every assigned SM route satisfies the 480-minute workload cap and optional travel constraints.
- The route explorer integrated-region view was updated to reuse the same day-batch minimum-SM assignment logic so displayed Ïã†Í∑úÏßÄÏó≠ assignments match the analytics engine.
- Full sweep/stat export regeneration after this algorithm change is still pending because Atlanta/LA candidate runs exceeded the current execution time limit.
- The latest daily statistics workbook now reflects the new day-batch minimum-SM routing logic. Because a full two-city export exceeded the execution limit, Atlanta and Los Angeles candidate sheets were regenerated separately and merged into the final workbook.
- The route explorer UI is now fully labeled in English. Sidebar filters, service summary captions, candidate-region comparison labels, and map metric cards were translated from Korean to English.
- Updated the North America batch-routing starting workload assumption from 5 jobs per SM to 4 jobs per SM. The minimum-SM search for integrated regions now starts from ceil(region-day jobs / 4).
- Added a persistent route-explorer cache layer and a route-geometry cache for ordered routes. Atlanta and Los Angeles current/new-region options were prewarmed so the map explorer can open with cached area data and cached date-level route payloads.

- 2026-03-21: Current-region map display now classifies SM/day groups with only one assigned job as ASC for consistent map styling and sidebar service-count summaries.

- 2026-03-21: New Region map summaries now classify integrated assignments as DMS by default and ASC for single-job SM/day groups, replacing original service center-type labels in integrated views.

- 2026-03-22: New Region batch assignment now post-processes single-job clusters by attempting reassignment to the nearest same-region DMS cluster with <=3 jobs if the merged route remains within 480 minutes; unresolved singletons remain ASC. Route explorer cache version bumped to rebuild cached map data.

- 2026-03-22: Sidebar 'Service Count by SM' table now includes per-SM route distance for the selected date and area filters.

- 2026-03-22: New Region singleton reassignment now considers same-region DMS clusters with <=4 jobs and selects the candidate that minimizes merged route distance while remaining within 480 minutes.

- 2026-03-23: Current Region map now shows all Zip Coverage ZIP polygons with geometry; ZIPs with no service are shaded gray, and coverage ZIPs missing ZCTA geometry are listed in the sidebar.

- 2026-03-23: Current Region map now supplements coverage ZIPs without polygon geometry using point markers when service coordinates exist; the sidebar list includes area, service_count, and whether a map point is available.

- 2026-03-23: Current Region map now supplements coverage ZIPs without polygon geometry using point markers when service coordinates exist; the sidebar list includes area, service_count, and whether a map point is available.

## More Realistic Simulation Plan (2026-03-26)

The next simulation step should move from pure batch region assignment to an operations-style daily dispatch model.

### Current Simplifications To Improve
- A day is treated as a flat workload bucket with fixed `60 min` per job.
- Integrated-region assignment is based on region/day clustering rather than a true nearest-stop dispatch loop.
- ASC handling is currently inferred from singleton clusters and simple reassignment rules.
- Travel limits are applied at cluster/route level, but daily dispatch does not yet simulate incremental assignment decisions.

### Recommended Phase 1 Realistic Dispatch Model
For each `city -> region_count -> region -> service_date`:
- Collect all jobs for that region/day.
- Start with `N = ceil(job_count / 4)` DMS resources.
- Dispatch jobs in a batch-nearest style similar to the Korea weekend Z-job concept, but without timetable slots.
- Each DMS resource has:
  - assigned job list
  - current last-stop position
  - estimated route distance/time
  - total workload = route duration + job_count * service_time_per_job_min
- Base DMS target is `4 jobs`, but a DMS can absorb a 5th job when the resulting workload remains within limits.
- Hard rule: if assigning a job to a DMS would push the resource beyond `480 min`, that assignment is rejected.
- Soft overflow rule: after primary DMS assignment, leftover jobs can be tested against nearby DMS routes; if merged workload would exceed `480 min * 1.10`, the job must be treated as ASC.

### Recommended DMS / ASC Logic
- New-region simulation should treat the initial `N` resources as DMS.
- Jobs left unassigned after the DMS absorption step are counted as ASC overflow.
- Existing-region analytics should preserve actual `SVC_CENTER_TYPE`, but integrated-region analytics should report:
  - DMS jobs absorbed by direct resources
  - ASC jobs that could not be absorbed within workload limits

### Recommended Output Metrics
Per city / candidate region count / date:
- service_count
- dms_service_count
- asc_service_count
- assigned_dms_count
- avg_jobs_per_dms
- avg_distance_per_dms_km
- avg_duration_per_dms_min
- p95_total_work_min
- max_total_work_min
- overflow_480_ratio
- asc_overflow_ratio

### Recommended Implementation Order
1. Replace region/day batch clustering assignment with nearest-dispatch DMS assignment.
2. Keep region boundaries fixed while validating the new dispatch behavior.
3. Recompute candidate-count sweep metrics using the new dispatch engine.
4. Update the map explorer so New Region views reflect DMS absorption and ASC overflow explicitly.

## Atlanta Production Routing Spec (2026-03-27)

A new production-routing track starts here. Previous clustering/cache logic must be preserved for comparison, but Atlanta operational dispatch will use a new deterministic setup.

### Fixed Atlanta Inputs
- City: `Atlanta, GA`
- New region count: `3`
- Resources: `15 DMS + 2 DMS2`
- Baseline DMS allocation: `5 DMS` per new region
- DMS2 allocation: assign the `2` DMS2 resources to the new regions with the highest workload / service demand
- Daily slot capacity: normalize all resources to `8`

### Region / Engineer Mapping
New regions should keep continuity with original territories:
- cluster ZIPs into 3 new regions
- assign each original engineer to the new region that contains the largest overlap of the engineer's original ZIP set
- this keeps original-area proximity aligned with the new 3-region design

### Start Point
The start point for each engineer comes from sheet `4. Address` of `Top 10_DMS_DMS2_Profile_20260317.xlsx`.
- geocode `Home Street Address + City + State + Zip`
- use the resulting coordinate as the route start point
- daily distance/time is computed from home start point to the assigned first stop and through the route

### Service Time Rules
- normal job: `45 min`
- heavy repair job: `100 min`
- heavy repair detection:
  - use `data/Notification_Symptom_mapping_20241120_3depth.xlsx`
  - match `SERVICE_PRODUCT_GROUP_CODE`, `SERVICE_PRODUCT_CODE`, and `RECEIPT_DETAIL_SYMPTOM_CODE == SYMP_CODE_THREE`
- `AREA_PRODUCT_FLAG = N` means the engineer cannot take REF heavy-repair jobs

### Dispatch Goal
Dispatch should follow a Korea weekend-Z-job style nearest-assignment concept, adapted for NA daily batch routing.
- all jobs for the day are known in advance
- no timetable slot ordering priority is required
- assign jobs to engineers within the same new region
- choose assignments so that `service_time + travel_time` remains as balanced as possible across engineers
- primary objective: balanced workload
- secondary objective: shorter travel distance/time

### Capacity / Overflow Policy
- do not use ASC overflow at this stage; first simulate whether `15 DMS + 2 DMS2` can absorb all workload
- compute overflow ratio / workload exceed ratio instead of offloading to ASC
- report how often engineers exceed `480 min`

### Schedule Output
After routing, generate engineer-level schedules.
- derive stop order from routed sequence
- include travel time between stops
- include service duration per stop (`45` or `100` min)
- include a lunch break of approximately `60 min` during the midday window
- exact lunch-window rule still needs to be fixed before implementation

## Atlanta Production Routing Spec v2 (2026-03-27)

This section supersedes the earlier Atlanta production-routing draft and defines the execution-ready rules for the new production-routing track. Existing clustering, explorer, and cache logic must remain preserved for comparison, but the production-routing implementation will be separated into new modules and caches.

### Scope
- City: `Atlanta, GA`
- Fixed new-region count: `3`
- The new-region ZIP allocation will be saved as a fixed ZIP-to-region file and reused for production simulation.
- Previous clustering outputs remain as reference only.

### Resource Pool
- DMS resources: `15`
- DMS2 resources: `2`
- Baseline DMS region allocation: `5 DMS` per region
- DMS2 allocation rule:
  - place the two DMS2 resources into the regions with the highest workload priority
  - workload priority order:
    1. total service-time demand
    2. heavy-repair count
    3. total service count
- Daily capacity baseline: normalize all engineer slots to `8`

### Original Engineer To New Region Mapping
Each existing engineer is attached to the new region that preserves the largest portion of the engineer's original territory.
- build the engineer's original ZIP set from sheet `1. Zip Coverage`
- compute overlap count against each of the three new-region ZIP sets
- assign the engineer to the region with the maximum overlap
- tie-breakers:
  1. larger total service-time demand in the candidate region
  2. larger ZIP-count overlap ratio
  3. alphabetical region id

### Start Point
Use engineer home addresses from sheet `4. Address`.
- geocode `Home Street Address + City + State + Zip`
- save the geocoded result into an updated address workbook / table so it is not recomputed each run
- route distance/time must start from the engineer home coordinate
- for DMS2 resources, if a stable home base is not used, use the center point of the assigned region as the start point

### Service Inclusion / Exclusion
- Exclude all jobs where `SVC_CENTER_TYPE` is `MAJOR DEALER` or `REGIONAL DEALER`
- Remaining jobs are candidates for direct assignment simulation
- At this stage, do not offload overflow to ASC; measure overflow only

### Service-Time Rules
- Normal repair: `45 min`
- Heavy repair: `100 min`
- Heavy repair detection:
  - source file: `data/Notification_Symptom_mapping_20241120_3depth.xlsx`
  - sheet: `3depth Í∏∞Ï§Ä Ï§ëÏàòÎ¶¨ Ï¶ùÏÉÅ`
  - match keys:
    - `SERVICE_PRODUCT_GROUP_CODE`
    - `SERVICE_PRODUCT_CODE`
    - `RECEIPT_DETAIL_SYMPTOM_CODE == SYMP_CODE_THREE`

### Capability Rules
- TV jobs:
  - `SERVICE_PRODUCT_GROUP_CODE == TV`
  - must be assigned to DMS2 only
  - DMS2 receives TV jobs first, then can absorb other jobs after TV demand is covered
- REF heavy repair jobs:
  - if heavy repair is detected and `AREA_PRODUCT_FLAG = N`, the engineer is not eligible
  - only `AREA_PRODUCT_FLAG = Y` engineers can receive that job
- For other products, use the simplified assumption that direct resources can repair them

### Daily Batch Assignment Logic
K-means is no longer the core routing logic. Dispatch should follow the Korea `Z-job weekend` style nearest-assignment concept, adapted to NA constraints.

For each `service_date -> region`:
1. gather all jobs in that region/day
2. build the eligible engineer pool for that region
3. filter by capability rules
4. assign jobs in a batch-nearest style with these priorities:
   - same region only
   - capability satisfied
   - lowest current `service_time + travel_time`
   - if tied, shorter incremental travel distance/time
5. objective:
   - primary: balance total workload
   - secondary: reduce travel distance/time

Total workload is defined as:
- `travel_time + service_time`

### 480-Minute Policy
- No ASC overflow assignment in this production-routing phase
- Try to assign all eligible workload to the 17 direct resources
- After assignment, measure:
  - 480-minute exceed ratio
  - exceed minutes by engineer
  - exceed pattern by region / date

### Schedule Generation
After routing, build engineer-level daily schedules.
Each schedule must include:
- home departure
- travel time between stops
- visit start time
- visit end time
- service duration (`45` / `100`)
- lunch break
- final end time

Lunch rule:
- use a `12:00~13:00` target with `¬±30 min` flexibility
- operational interpretation: place an approximately `60 min` lunch break within the `11:30~13:30` window
- lunch placement should avoid splitting a service visit

### Required Production Outputs
- fixed Atlanta 3-region ZIP file
- engineer-to-region assignment file
- engineer home geocode file
- heavy-repair lookup table
- filtered daily service file (excluding Major Dealer / Regional Dealer)
- engineer-day assignment result file
- engineer-day route summary file
- engineer-day schedule file
- region-day workload summary file

### UI / Explorer Requirements
- preserve the existing explorer and caches untouched
- create a new production-routing explorer / cache path
- allow engineer selection in the new production view
- show engineer schedule in the UI
- include planned visit time in map popups

## Production Routing Update (2026-03-27)
- Atlanta production prep is now implemented in `smart_routing/production_atlanta.py`.
- Fixed ZIP-to-region output for Atlanta 3 regions is generated and saved.
- DMS 15 engineers are assigned to the 3 new regions by ZIP-overlap with an exact 5/5/5 split.
- DMS2 is no longer fixed to a single region. Both DMS2 engineers are stored as floating TV-first resources.
- Home addresses from `4. Address` are geocoded and saved into the production workbook and CSV outputs.
- Production prep outputs are written to `260310/production_input` and `260310/production_output`.
