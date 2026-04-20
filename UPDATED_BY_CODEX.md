# UPDATED BY CODEX

## 2026-03-10

### 반영 내용

- 문서 관리 규칙을 프로젝트 운영 장치로 확정
- `docs` 폴더 기반 설계 문서 관리 방식을 정의
- 루트의 Codex 변경 이력 문서 유지 규칙을 정의
- 북미 라우팅 프로젝트의 현재 구조, 운영 장치, 데이터 기준에 대한 설계 초안을 기록

### 생성 파일

- `docs/프로젝트_설계서.md`
- `UPDATED_BY_CODEX.md`

### 메모

- 현재 문서는 최종본이 아니라 구조 정리용 초안으로 작성
- 이후 구조 변경, 설계 변경, 코드 생성 내역을 계속 추적 갱신

## 2026-03-16

### 반영 내용

- 미국 주소 지오코딩 기본 방식을 `US Census Geocoder`로 확정
- 하루 최대 10,000건의 신규 주소만 변환하고 이후 실행에서는 캐시를 재사용하는 구조 설계
- 이미 변환한 주소 재사용을 위한 캐시 파일 구조 반영
- 서비스 파일과 지오코드 결과를 병합하는 CLI 스크립트 추가

### 생성 파일

- `smart_routing/__init__.py`
- `smart_routing/census_geocoder.py`
- `sr_geocode.py`

### 수정 파일

- `docs/프로젝트_설계서.md`
- `UPDATED_BY_CODEX.md`

### 메모

- 현재 구현은 미국 주소만 대상으로 동작
- 입력 서비스 파일은 CSV와 Excel을 모두 지원
- 결과는 캐시 누적 후 `input` 폴더의 병합 CSV로 저장
- Census 응답 제한을 피하기 위해 대량 요청을 배치로 분할
- 지오코드 실행 전 실제 미해결 주소 기준으로 요청 건수 계산을 보정
- Census 응답 좌표 필드 파싱 오류 수정
- Census 미해결 주소에 한해 Google Geocoding API fallback 추가
- 지오코드 설정과 Google API 경로를 루트 `config.json`에서 읽도록 변경
- Google API 시도 이력 파일을 추가하고 과도한 재시도를 막는 보호 로직 반영
- Google API 일별 시도 건수 집계와 10,000건 초과 방지 로직 추가
- Google `REQUEST_DENIED` 응답이 발생해도 전체 실행이 중단되지 않도록 처리
- Google 호출 중 SSL/네트워크 오류가 발생해도 전체 실행이 중단되지 않도록 처리
- 주소 전처리를 추가해 `ADDRESS_LINE1_INFO`에 중복 포함된 city/state/postal/country를 제거
- 과거 Google 실패 이력을 1회성으로 무시하고 재시도할 수 있는 옵션 추가
- 서비스 병합 시 정제된 주소 기준을 우선 사용하도록 조정해 중복 주소의 좌표 일관성을 보정
- 최종 병합 결과에서 좌표 미생성 행의 `source`를 `failed`로 표기하도록 수정
- 실행 로그에서 Census 단계 결과와 최종 병합 결과를 분리해 출력하도록 개선

## 2026-03-18

### 추가 반영 내용

- 지도 통계 집계 시 `AREA_NAME` 문자열 정규화 로직 추가
- 지역명의 줄바꿈, 앞뒤 공백, 중복 공백 때문에 `area_km2=0`처럼 보이던 경계 문제를 보정
- `STRATEGIC_CITY_NAME`, `SVC_ENGINEER_CODE`, `SVC_CENTER_TYPE` 등 주요 텍스트 컬럼도 같은 방식으로 정규화
- 지도 관련 보정 메모를 `docs/area_map_notes_20260318.md`에 기록
- 지도 전처리 결과를 `data/cache/area_map` 아래 pickle 캐시로 저장하도록 추가
- 원본 프로필 파일, ZCTA ZIP 파일, geocoded 서비스 파일의 경로와 수정 시각이 같으면 캐시를 재사용하도록 구성
- ZIP/AREA/context geometry를 분리 캐시로 저장하도록 추가
- Streamlit에서 `city + area + service point 표시 여부` 기준의 지도 HTML 캐시 추가
- 지도 색상 체계를 고정 팔레트에서 지역 수에 맞는 동적 색상 생성 방식으로 변경
- 지도 필터에 `Show Service Points` 체크박스 추가
- ZIP/AREA tooltip 값을 `Slot Sum`에서 `POSTAL_CODE` 기준 실제 서비스 건수(`service_count`)로 변경
- 서비스 건수 컬럼 추가에 맞춰 area map 캐시 버전 갱신
- ZIP 경계선은 얇은 중립선으로, AREA 레이어만 강조되도록 시각화 조정
- AREA 이름 hover 강조를 추가하고 ZIP 경계와 우편번호 경계가 더 잘 구분되도록 보정
- 지역용 area geometry를 ZIP 조각 전체가 아니라 대표 `AREA_NAME` 기준 집계 영역으로 재구성
- LA 화면에서 마우스 hover 겹침과 popup 충돌 문제를 완화
- `smart_routing/profile_sync.py`와 `sr_update_profile_from_service.py` 추가
- `DMS/DMS2` 서비스 데이터 기준으로 `Zip Coverage`와 `Slot` 업데이트 CSV 생성 로직 추가

### 생성 파일

- `260310/input/Zip_Coverage_updated_Service_202603181109.csv`
- `260310/input/Slot_updated_Service_202603181109.csv`
- `260310/output/unmatched_service_sm_Service_202603181109.csv`
- `260310/output/profile_sync_summary_Service_202603181109.csv`
- `260310/output/region_design_city_summary_Service_202603181109_geocoded.csv`
- `260310/output/region_design_region_summary_Service_202603181109_geocoded.csv`
- `260310/input/region_design_postal_Service_202603181109_geocoded.csv`
- `260310/input/region_design_service_Service_202603181109_geocoded.csv`

### 메모

- profile sync의 `Zip Coverage` 업데이트 시 중복 ZIP이 여러 지역에 동시에 반영되지 않도록 1개 지역만 선택하는 규칙 추가
- 업데이트 결과 검증 시 `POSTAL_CODE + STRATEGIC_CITY_NAME + SVC_CENTER_TYPE` 기준으로 `AREA_NAME`이 1개만 매핑되도록 확인
- 북미 라우팅 1차 통합 권역 설계 모듈 `smart_routing/region_design.py`와 실행 스크립트 `sr_region_design.py` 추가
- 권역 설계는 `DMS/DMS2` 서비스만을 대상으로 하고, 표시된 Slot 용량 기준 필요 SM 수를 계산하도록 구성
- 우편번호 중심 좌표와 서비스 건수 가중치를 기반으로 통합 권역을 설계

## 2026-03-18

### 반영 내용

- 미국 Census ZCTA 공식 ZIP 경계 파일 다운로드
- `Atlanta, GA` 전용 ZIP/AREA 검토용 지도 데이터 로직 추가
- Streamlit 기반 지도 검토 UI 추가
- ZIP 레이어와 AREA dissolve 레이어를 함께 표시하도록 구현

### 생성 파일

- `smart_routing/area_map.py`
- `sr_area_map.py`

### 수정 파일

- `docs/프로젝트_설계서.md`
- `UPDATED_BY_CODEX.md`

### 메모

- 미국 ZIP 경계는 `data/geo/tl_2024_us_zcta520.zip`을 기준으로 사용
- Atlanta 데이터에는 하나의 ZIP이 여러 AREA/SM에 중복 배정되는 사례가 많음을 확인
- 지도 범위를 Atlanta 전용에서 미국 전체 또는 도시 선택 구조로 확장
- 지도 팝업에 `AREA_NAME`, `SVC_ENGINEER_CODE`, `SVC_CENTER_TYPE`를 함께 표시
- geocoded 서비스 좌표를 지도 오버레이로 추가
- `AREA_NAME` 기준 색상 구분과 미매핑 ZIP 강조 배경을 추가
- 지도 화면을 좌우 분할 레이아웃으로 개편하고 통계 정보(우편번호 수, 서비스 수, 면적)를 함께 표시
## 2026-03-18

### Added

- Integrated region comparison view in `sr_area_map.py`
- Secondary Folium map for redesigned regions stacked below the current coverage map
- Left-panel integrated region stats table sourced from `region_design_postal_*.csv`

### Notes

- Current coverage and redesigned integrated regions can now be reviewed on one screen.
- Integrated region polygons are built from the latest postal-to-region assignment file under `260310/input`.
- Region design logic is documented in `docs/na_routing_design_20260318.md`.
- Replaced deprecated GeoPandas dissolve usage from `unary_union` to `union_all()` in `sr_area_map.py`.
- Replaced the initial simple weighted k-means region design with a soft-balanced region assignment that considers:
  - service-count balance against target load per region
  - region radius expansion penalty
  - weighted geographic distance
- Added region metrics `service_gap_pct`, `max_radius_km`, and `avg_radius_km` to the generated outputs and the map UI.
- Changed region-demand estimation from whole-period cumulative service volume to daily demand based on `REPAIR_END_DATE_YYYYMMDD`.
- Region-design `service_count` outputs now represent average daily service volume rather than total multi-day volume.
- Added `effective_service_per_sm` control to region design and recalculated the latest outputs using `5 services per SM per day`.
- Added routing comparison modules:
  - `smart_routing/osrm_routing.py`
  - `smart_routing/routing_compare.py`
  - `sr_compare_routing.py`
- Added city-filtered routing comparison for `Atlanta, GA` and `Los Angeles, CA`.
- Current comparison run used haversine distance because local North America OSRM ports were not fully available.
- Added region-design algorithm switching between `balanced` and `weighted_kmeans`.
- Regenerated the latest region-design outputs with `weighted_kmeans` under the daily-demand and `5 services per SM` assumptions.
- Re-ran routing comparison for `Atlanta, GA` and `Los Angeles, CA` using the latest `weighted_kmeans` region assignments.
- Updated routing config to use:
- Korea `http://20.51.244.68:5000`
- Los Angeles `http://20.51.244.68:5001`
- Atlanta `http://20.51.244.68:5002`
- Added `city_osrm_else_haversine` mode so only configured cities use OSRM and other cities use haversine.
- Operational check on 2026-03-19:
- `http://20.51.244.68:5000` Korea route calls also reset the connection during direct test requests
  - `http://20.51.244.68:5001` and `:5002` route calls did not complete successfully
  - both endpoints reset the connection after roughly 30 seconds during direct test requests
- After server recovery on 2026-03-19, OSRM route endpoints for Korea, Los Angeles, and Atlanta returned normal responses again.
- Routing comparison was rerun using real OSRM road distance/time with `table + nearest-neighbor + route` for `Atlanta, GA` and `Los Angeles, CA`.
- Added fixed candidate-count sweep:
  - Atlanta `2,3,4,5`
  - Los Angeles `3,4,5,6`
- Added:
  - `smart_routing/region_sweep.py`
  - `sr_region_sweep.py`
- Generated candidate comparison outputs and selected the best candidate per city using a heuristic balance score.
- Extended candidate sweep outputs with outlier metrics:
  - `p95_total_work_min`
  - `max_total_work_min`
  - `overflow_480_count`
  - `overflow_480_ratio`
- Changed constrained assignment to use haversine distance during clustering/assignment and OSRM only for final route distance/time evaluation.
- Rebuilt `sr_area_map.py` into a single-map route explorer with left-side filters:
  - date
  - city
  - region type
  - area name
  - assigned SM code
- Added route-explorer data builders in `smart_routing/area_map.py`:
  - current-area service assignment view
  - integrated-region service assignment view for the latest best candidate count per city
- Added public ordered-route geometry support to `smart_routing/osrm_routing.py` so the selected SM route can be drawn on the map.
- The map now supports:
  - existing-region polygons
  - integrated-region polygons based on best candidate counts
  - service points filtered by date/area/assigned SM
  - route polyline for the selected assigned SM
- Fixed `area_map.load_service_points()` to derive `service_date` from `REPAIR_END_DATE_YYYYMMDD` so integrated assignment logic can run from the cached geocoded service file.
- Fixed sr_area_map.py existing-area filtering so AREA_NAME selection uses primary_area_name on the current ZIP layer instead of failing with KeyError: 'AREA_NAME'.
- Updated sr_area_map.py sidebar counts to show total service count, filtered date/area/selected-SM counts, and an SM-by-SM service count table for the current date/area filter.
- Updated sr_area_map.py so when a date is selected, customer points are rendered as numbered markers using the selected route stop order; when no date is selected, clustered point markers are kept.
- Changed sr_area_map.py so numbered customer markers are shown only in 신규 지역 mode; 기존 지역 keeps regular clustered point markers even when a date is selected.
- Adjusted sr_area_map.py marker behavior again: cluster/count-style markers are shown only when date=ALL; once a specific date is selected, both 기존 지역 and 신규 지역 use route-order numbered markers.
- Replaced redundant sidebar count captions in sr_area_map.py with summary metrics: visible service count, assigned SM count, average route distance, and average route duration for the current selection scope.
- Reworked sr_area_map.py to use region-count options instead of a binary 기존/신규 toggle. The sidebar now offers 기존지역 plus city-specific options such as 신규지역2, 신규지역3, etc., and the map/statistics update for the selected candidate count.
- Added candidate-count summary support to `smart_routing/area_map.py`, including loading city-specific sweep options and rebuilding integrated map data for an explicitly selected region count.
- Added daily statistics Excel export: smart_routing/export_daily_stats.py and sr_export_daily_stats.py. The export writes one sheet per city and region option (기존지역 and 신규지역N) with date-level routing statistics.
- Extended daily-stats Excel export so each sheet now appends a date-level summary of the SM code with the maximum total workload (max_total_work_sm_code) and its workload metrics.
- Changed routing statistics calculation to match the map route logic: group distance/time metrics now use OSRMTripClient.build_ordered_route() instead of get_trip(). Regenerated region-count sweep outputs and the daily-stats Excel workbook with the route-based metric definition.
- Extended the daily statistics workbook with city-level overall summary sheets. Each city now has an additional 전체통계 sheet comparing 기존지역 and 신규지역N candidates in one table.
- Updated sr_area_map.py so existing-region markers distinguish direct and outsourced service: DMS, DMS2, and ASC are shown in popups, ASC numbered markers use a thicker border in 기존지역 mode, and the sidebar service-count line now includes DMS/DMS2/ASC counts.
- Fixed existing-region explorer data to keep all SVC_CENTER_TYPE values. Existing-region sidebar counts and marker popups now include real ASC counts instead of always showing zero; the DMS/DMS2-only filter remains applied only to integrated-region calculations.
- Changed region design, integrated assignment, region-count sweep, and exported daily statistics to use all service jobs (including ASC) instead of filtering to only DMS/DMS2. Regenerated region design outputs, sweep summaries, and the daily statistics workbook on the full service workload.
- Updated sr_area_map.py numbered marker styling so ASC jobs use a black border instead of a white border. This makes outsourced-service stops easier to distinguish on the map.
- Fixed sr_area_map.py so ASC black-border numbered markers are applied only in Current Region mode. New Region markers now use the standard white border even when the original service row came from ASC.
- Replaced the integrated-region assignment engine with a day-batch region/day routing model. For each region-day, the code now searches for the minimum SM count that satisfies the 480-minute workload limit (plus optional travel limits), instead of using the previous greedy sequential assignment.
- Updated smart_routing/area_map.py to use the same day-batch minimum-SM assignment logic as smart_routing/routing_compare.py so map explorer integrated-region assignments stay aligned with the analytics logic.
- Verified the modified modules compile successfully, but full Atlanta/LA sweep regeneration exceeded the current execution time limit and still needs a long run to refresh output CSV/XLSX artifacts.
- Regenerated the daily statistics Excel workbook with the new day-batch minimum-SM routing logic by building Atlanta and Los Angeles candidate sheets separately and merging them into the final workbook at 260310/output/daily_stats_by_city_region_Service_202603181109_geocoded.xlsx.
- Translated sr_area_map.py UI labels to English, including sidebar filters, summary captions, candidate-region table headers, and top metric cards.
- Changed the integrated-routing starting headcount rule from jobs/5 to jobs/4 by updating routing.effective_service_per_sm to 4 in config.json. New Region batch assignment, region design, and downstream exports will now start minimum-SM search from ceil(job_count / 4).
- Added persistent route-explorer disk cache by city and region option, plus persistent ordered-route cache files for build_ordered_route(). Prewarmed Atlanta and Los Angeles for current region and all New Region options so sr_area_map.py can load map data and date-level routes without rebuilding them on first open.

- 2026-03-21: Updated current-region map counting rule so single-job SM/day assignments are counted as ASC in sidebar counts, popups, and marker styling.

- 2026-03-21: Updated New Region center-type counting to classify integrated assignments as DMS by default and ASC when an assigned SM/day has only one job; sidebar summary now shows DMS/ASC for New Region.

- 2026-03-21: Applied black border marker styling to ASC jobs in both Current Region and New Region map views.

- 2026-03-21: Added DMS/DMS2/ASC breakdown to Assigned SM Count in the map sidebar, using the same center-bucket rules as service counts.

- 2026-03-21: Sidebar average distance/duration now exclude zero-route SM/day groups (for example single-job assignments with no travel).

- 2026-03-22: Added post-processing for New Region batch assignment to try absorbing single-job SM/day clusters into the nearest same-region DMS cluster with 3 or fewer jobs when the merged route stays within 480 minutes; otherwise the singleton remains ASC. Bumped route-explorer cache version to rebuild map caches.

- 2026-03-22: Added route distance (distance_km) to the sidebar 'Service Count by SM' table for date-filtered views.

- 2026-03-22: Updated New Region singleton reassignment rule to consider candidate DMS clusters with up to 4 jobs and choose the candidate that minimizes merged route distance (within 480 minutes), rather than nearest-by-point distance. Bumped route-explorer cache version to v6.

- 2026-03-23: Updated Current Region map to render all Zip Coverage ZIPs with geometry, shading no-service ZIPs in gray and listing coverage ZIPs without geometry in the sidebar.

- 2026-03-23: Added point-based map fallback for coverage ZIPs without polygon geometry when service coordinates exist, and expanded the sidebar list with area, service_count, and has_point columns.

- 2026-03-23: Added point-based map fallback for coverage ZIPs without polygon geometry when service coordinates exist, and expanded the sidebar list with area, service_count, and has_point columns.

- 2026-03-26: Added a Phase 1 realistic-dispatch simulation design to docs: region/day nearest-dispatch DMS assignment with 4-job starting capacity, 480-minute hard cap, and ASC overflow handling for unabsorbed jobs.

- 2026-03-26: Hardened area-map cache loading to reject malformed cached service/explorer data, select the latest valid geocoded service file by required columns, derive service_date from alternate date columns when needed, and skip preloading oversized route JSONL caches to avoid MemoryError.

- 2026-03-26: Restored 260310/input/Service_202603181109_geocoded.csv by re-merging the original Service_202603181109.csv with the saved geocode caches (no new geocoding calls).

- 2026-03-26: Cleared stale area_map and route_explorer caches after restoring the correct geocoded service file so the map app can rebuild from valid inputs.

- 2026-03-27: Added Atlanta production-routing spec: 3 fixed new regions, 15 DMS + 2 DMS2, original ZIP-overlap-based engineer-to-region mapping, home-address start-point geocoding, 45/100 minute service durations, REF heavy-repair capability constraint, and engineer-level schedule generation with lunch break.

- 2026-03-27: Added Atlanta production-routing spec v2 with fixed 3-region execution rules, ZIP-overlap engineer mapping, home-address start geocoding, MAJOR/REGIONAL DEALER exclusion, TV-to-DMS2-only rule, REF heavy-repair eligibility rule, 45/100 minute service times, 480-minute analysis policy, and schedule-generation requirements.
- 2026-03-27: Added `smart_routing/production_atlanta.py` and `sr_production_atlanta_prep.py` for Atlanta production-routing prep.
  - Generates fixed Atlanta 3-region ZIP mapping.
  - Assigns 15 DMS engineers to regions by ZIP overlap with a strict 5/5/5 split.
  - Stores 2 DMS2 engineers as floating resources instead of fixed-region placement.
  - Geocodes engineer home addresses and writes a production workbook copy.
  - Writes production input/output CSV files under `260310/production_input` and `260310/production_output`.
- 2026-03-28: Added `sr_production_map.py` as a separate Streamlit UI for Atlanta production-routing inputs.
  - Reads fixed 3-region ZIPs, engineer-region assignments, home geocodes, and enriched service data.
  - Displays production regions, service points, and engineer home markers without modifying the existing `sr_area_map.py`.
- 2026-03-28: Updated `sr_production_map.py` so date selection builds engineer-level routes from home start points.
  - Engineer selection now filters service counts and route display to the selected engineer.
  - Date-selected views show route lines and numbered stop markers instead of only aggregate region counts.
- 2026-03-28: Added Atlanta production assignment engine and connected `sr_production_map.py` to assignment outputs.
  - New files: `smart_routing/production_assign_atlanta.py`, `sr_production_atlanta_assign.py`.
  - Production map now prefers `atlanta_assignment_result.csv`, `atlanta_engineer_day_summary.csv`, and `atlanta_schedule.csv` when present.
  - Date-selected views show assigned engineer routes, per-stop visit times, and engineer workload summaries from the new assignment output.
- 2026-03-28: Reworked `smart_routing/production_assign_atlanta.py` from workload-first assignment to a seed-and-grow nearest expansion model.
  - Per region/day, active engineers are selected first.
  - Engineers receive seed jobs from their nearest start points, then nearby jobs are grown from the current route frontier.
  - This prioritizes local route cohesion before workload penalties.

## 2026-03-30

### Added

- Confirmed UPDATED_BY_CODEX.md is maintained as the canonical running work log in UTF-8.
- Added sr_production_atlanta_assign_soft_line_chunks.py so standard line assignment can be rebuilt in resumable chunks, matching the existing actual-attendance and OSRM chunk runners.
- Added Atlanta production design supplement:
  - docs/na_routing_design_20260330_production_update.md
- Added UTF-8 note file for the March 30 production updates:
  - docs/codex_update_20260330.md
- Added merged 320-ZIP Atlanta region mapping output:
  - 260310/production_input/atlanta_fixed_region_zip_3_manual320.csv
  - 260310/production_output/atlanta_fixed_region_zip_3_manual320_summary.csv

### Updated

- Refined Atlanta production assignment growth logic in smart_routing/production_assign_atlanta.py.
  - New jobs are no longer attached using only the current last stop.
  - New jobs are scored against the closest anchor among the engineer home/start point and all already assigned stops.
  - This reduces order bias and matches the North America batch-routing requirement better.
- Kept the post-assignment longest-route move/rebalance logic disabled.
- Updated sr_production_map.py:
  - Actual Routes is now the default assignment mode.
  - Actual-route schedule building runs only for the selected date instead of all dates at app startup.
  - Added guard logic so partially rebuilt summary/schedule CSVs do not crash the app during background regeneration.
  - Added safer date/region/engineer filtering when summary files are missing expected columns mid-rebuild.
  - Replaced remaining deprecated geometry centroid path to use union_all() instead of unary_union.
- Updated sr_atl_region_compare.py so the left map uses the merged 320-ZIP region definition.

### Region Merge Rule

- Keep the current visible-region assignment for the geometry-visible ZIPs.
- Fill only the remaining manual ZIPs from ATL Three Markets.xlsx.
- Manual bucket mapping:
  - ATL West -> Region 1
  - ATL East -> Region 2
  - ATL South -> Region 3

### Production Routing Status

- Production comparison modes now operate as:
  - Actual Routes
  - Line Assign
  - Line Assign (Actual Attendance)
  - OSRM Assign
  - OSRM Assign (Actual Attendance)
- Background rebuild scripts were relaunched using `py.exe` because the alternate `python.exe` on the machine did not have the `requests` dependency required by the routing modules.
## 2026-03-31
- Updated the Actual Attendance limit logic so when the current roster is smaller than the real worked headcount, the flow no longer adds synthetic engineers and instead substitutes only within the current roster.
- Restarted Line/OSRM Actual Attendance rebuilds, and completed the OSRM Actual Attendance final output in current-roster-only mode.
- Verified that `atlanta_assignment_result_osrm_actual_attendance.csv` contains zero non-roster `assigned_sm_code` values.
- Added two Actual Attendance simulation modes: `Sequence Assign (Actual Attendance)` and `Iteration Assign (Actual Attendance)`.
- The Sequence method builds a nearest-neighbor global customer order first, then splits it into contiguous weighted chunks for engineer assignment.
- The Iteration method starts from an initial assignment and repeatedly applies one-job moves to improve the objective (`max total_work`, weighted job std, total travel).
- Updated `sr_production_map.py` to hide the legacy general Assign mode, focus on Actual-only modes, and load the new simulation output files.
- Added background rebuild scripts: `sr_production_atlanta_assign_actual_attendance_sequence_chunks.py`, `sr_production_atlanta_assign_actual_attendance_iteration_chunks.py`.
- Added two OSRM Actual Attendance modes: `Sequence OSRM Assign (Actual Attendance)` and `Iteration OSRM Assign (Actual Attendance)`.
- Added `assignment_strategy` to `production_assign_atlanta_osrm.py` so `sequence` and `iteration` OSRM-matrix assignment flows can be selected explicitly.
- Updated `sr_production_map.py` to expose the OSRM Sequence/Iteration Actual output files and the corresponding mode-selection UI.
- Added background rebuild scripts: `sr_production_atlanta_assign_osrm_actual_attendance_sequence_chunks.py`, `sr_production_atlanta_assign_osrm_actual_attendance_iteration_chunks.py`.
- Reprocessed the newly downloaded service file `260310/Service_202603311508.csv`.
- Rebuilt `260310/input/Service_202603311508_geocoded.csv` by reusing existing Census and Google geocode caches and carrying forward missing operating columns from the prior geocoded service export.
- Regenerated profile sync outputs from the new geocoded service file:
  - `260310/input/Zip_Coverage_updated_Service_202603311508_geocoded.csv`
  - `260310/input/Slot_updated_Service_202603311508_geocoded.csv`
  - `260310/output/profile_sync_summary_Service_202603311508_geocoded.csv`
- Regenerated Atlanta production preprocessing outputs from the new geocoded service file, including:
  - `260310/production_input/atlanta_service_filtered.csv`
  - `260310/production_input/atlanta_service_enriched.csv`
  - `260310/production_output/atlanta_region_workload_summary.csv`
- Disabled DMS2 across current routing logic after TV jobs were removed from the service data.
  - Assignment inputs now force DMS-only engineer masters.
  - TV flags are forced off in Atlanta production prep and assignment loading.
  - Profile sync now uses only `DMS`.
  - Production map filters DMS2 out of routes, homes, staffing summaries, and engineer counts.
- Added Streamlit-live runtime path for direct BigQuery routing without overwriting simulation files.
  - Added `.streamlit/secrets.toml` for Streamlit secrets-based BigQuery authentication.
  - Added `smart_routing/bigquery_runtime.py` to render the SQL date range dynamically and execute the BigQuery query.
  - Added `smart_routing/live_atlanta_runtime.py` to geocode queried rows with existing caches and prepare Atlanta runtime inputs in memory.
  - Added `build_atlanta_production_assignment_osrm_from_frames()` in `smart_routing/production_assign_atlanta_osrm.py` so OSRM iteration assignment can run directly from DataFrames.
- Added `sr_live_atlanta_routing.py` as a separate live Streamlit app with start/end date selection, automatic data query, OSRM iteration routing, per-engineer filtering, route map display, schedule grid, and CSV downloads.
- Added live-app status reporting in English so the UI shows step-by-step progress and elapsed time for BigQuery query, cache merge, Atlanta preprocessing, OSRM iteration routing, and schedule finalization.
- Switched the live-app date basis to `PROMISE_DATE` first, with fallback to `PROMISE_TIMESTAMP`, so live routing aligns to future promise dates instead of historical repair-end dates.
- Added receipt-level deduplication for live queried service data so the same `GSFS_RECEIPT_NO` is not routed or shown more than once.
- Added 3-day simulation comparison outputs for `2026-01-12`, `2026-01-19`, and `2026-01-20`:
  - `LNS Assign (Actual Attendance, 3 Days)` using the current OSRM iteration / local-improvement path
  - `VRP Assign (Actual Attendance, 3 Days)` using an OR-Tools multi-vehicle VRP solver with actual-attendance-limited engineers
- Added `smart_routing/production_assign_atlanta_vrp.py` and `sr_production_atlanta_compare_lns_vrp_3days.py` to generate these comparison outputs and wired the new modes into `sr_production_map.py`.
- Added a detailed 2026-04-01 design supplement:
  - `docs/na_routing_design_20260401_search_methods.md`
  - documents current live / production routing architecture
  - records DMS-only and PROMISE_DATE-based live routing assumptions
  - compares Sequence, line iteration, OSRM iteration, LNS direction, and VRP solver approaches with detailed pros and cons
- Switched the live operational Streamlit app from OSRM iteration to VRP:
  - added `build_atlanta_production_assignment_vrp_from_frames()` in `smart_routing/production_assign_atlanta_vrp.py`
  - updated `sr_live_atlanta_routing.py` to use VRP assignment for live queried service data
  - updated progress text and download filenames to reflect VRP routing
- Replaced the live BigQuery service query with an Atlanta source-query version in `smart_routing/select_data.sql`.
  - source now reads from `pjt-lge-edl-ob`.OB_00105 service views instead of the EDW mart views
  - query is Atlanta-only through the current Atlanta ZIP list
  - EDW query was preserved as `smart_routing/select_data_edw_backup.sql`
- Updated `smart_routing/bigquery_runtime.py` to render placeholder tokens for:
  - start/end dates in `YYYYMMDD`
  - start/end months in `YYYYMM`
  - Atlanta ZIP lists for the source query
- Updated `smart_routing/live_atlanta_runtime.py` so missing engineer names can be filled from the current engineer assignment file and the new source-query output continues to flow into the live prep path.
- Verified the new source query against the EDW backup query for `2026-01-12` Atlanta:
  - output file: `260310/production_output/select_data_source_compare_20260112.txt`
  - receipt count matched exactly: `43 vs 43`
  - receipt intersection matched exactly: `43 / 43`
  - engineer, promise date, postal code, state, city, address, symptom code, product code, city label, and center type all matched for the 43 shared receipts
- Tightened the source query with an HS-equivalent product-group filter:
  - `sp.SERVICE_PRODUCT_GROUP_CODE IN ('WM', 'REF', 'COK')`
  - kept `SVC_CENTER_TYPE_CODE = '02'` for DMS-only source filtering
  - rechecked `2026-01-12` Atlanta and the source query still matched the EDW backup exactly at the receipt level

2026-04-01 (OSRM iteration simulation refinement)
- Updated `smart_routing/production_assign_atlanta.py` so summary/objective paths can optionally use OSRM route metrics via `route_client` instead of the default haversine estimate.
- Updated `smart_routing/production_assign_atlanta_osrm.py`:
  - removed the forced no-op override of `_targeted_region_worst_move_rebalance`
  - changed grow-step scoring from nearest-anchor matching to route insertion delta against each engineer's ordered route
  - changed OSRM iteration improvement to use OSRM-backed objective evaluation with `travel_first` priority and 4 improvement passes
- Rebuilt `Iteration OSRM Assign (Actual Attendance)` simulation outputs for:
  - `2026-01-12`
  - `2026-01-19`
  - `2026-01-20`
- Refreshed files:
  - `260310/production_output/atlanta_assignment_result_osrm_actual_attendance_iteration.csv`
  - `260310/production_output/atlanta_engineer_day_summary_osrm_actual_attendance_iteration.csv`
  - `260310/production_output/atlanta_schedule_osrm_actual_attendance_iteration.csv`

2026-04-02 (Simulation compare refactor: Actual / VRP / OSRM / OSRM Iteration)
- Reworked `smart_routing/production_assign_atlanta_osrm.py` to support the new algorithm definitions:
  - `routing`: first seed job per engineer from home-distance minimization, then full-route insertion assignment
  - `iteration`: route-insertion assignment followed by relocate/swap iterative improvement
- Added 3-day output runner scripts:
  - `sr_production_atlanta_assign_osrm_actual_3days.py`
  - `sr_production_atlanta_assign_osrm_iteration_actual_3days.py`
  - `sr_production_atlanta_compare_actual_vrp_osrm_3days.py`
- Updated `sr_production_map.py` comparison modes to target:
  - `Actual Routes`
  - `OSRM Assign (Actual Attendance, 3 Days)`
  - `OSRM Iteration Assign (Actual Attendance, 3 Days)`
  - `VRP Assign (Actual Attendance, 3 Days)`
- Started background generation for the new 3-day OSRM and OSRM Iteration outputs.

2026-04-03 (Salesforce-style VRP API client/server)
- Added a reusable VRP API service layer:
  - `smart_routing/vrp_api_service.py`
  - converts Salesforce-style routing JSON into internal engineer/home/service DataFrames
  - runs VRP routing via `build_atlanta_production_assignment_vrp_from_frames()`
  - stores asynchronous job status/result files under `260310/vrp_api_jobs`
- Added a local REST server implementation:
  - `smart_routing/vrp_api_server.py`
  - endpoints:
    - `POST /api/v1/routing/jobs`
    - `GET /api/v1/routing/jobs/{job_id}`
    - `GET /api/v1/routing/jobs/{job_id}/result`
  - implemented with Python standard library `ThreadingHTTPServer` so no FastAPI dependency is required
- Added a local client module:
  - `smart_routing/vrp_api_client.py`
  - helper functions for submit/status/result plus payload construction from current service/engineer/home frames
- Added runnable entry points:
  - `sr_vrp_api_server.py`
  - `sr_vrp_api_client.py`
- Verified end-to-end HTTP flow locally:
  - submit job
  - poll status
  - fetch result
  - returned completed summary and assignments successfully

2026-04-05 (VRP API stabilization and compare flow update)
- Added restart helpers for the Smart Routing VRP API path:
  - `restart_smart_routing_api.sh`
  - `sr_vrp_api_server.py`
- Expanded the VRP API stack to support the current live/compare flow:
  - `smart_routing/vrp_api_client.py`
  - `smart_routing/vrp_api_server.py`
  - `smart_routing/vrp_api_service.py`
- Updated `smart_routing/production_assign_atlanta_osrm.py` and `sr_production_map.py` so Actual / VRP / OSRM comparison runs can share the same 3-day output flow.
- Added dedicated 3-day runners:
  - `sr_production_atlanta_assign_osrm_actual_3days.py`
  - `sr_production_atlanta_assign_osrm_iteration_actual_3days.py`
  - `sr_production_atlanta_compare_actual_vrp_osrm_3days.py`
- Refined the live-query path in:
  - `smart_routing/live_atlanta_runtime.py`
  - `smart_routing/select_data.sql`
  - `sr_live_atlanta_routing.py`

2026-04-15 (VRP common mode)
- Added common VRP configuration and persistence base:
  - `config_common_vrp.json`
  - `smart_routing/common_vrp_db.py`
  - `smart_routing/common_vrp_runtime.py`
- Added common API/server entry points:
  - `smart_routing/common_vrp_api_server.py`
  - `sr_common_vrp_api_server.py`
  - `restart_common_vrp_api.sh`
- Added reusable routing modes:
  - `smart_routing/vrp_mode_na_general.py`
  - `smart_routing/vrp_mode_z_weekend.py`
  - `smart_routing/vrp_api_common.py`
- Added common client entry and seeded reference/job data:
  - `sr_common_vrp_client.py`
  - `data/common_vrp_job_input.parquet`
  - `data/common_vrp_technician_input.parquet`
  - `data/atlanta_input_store.parquet`
  - `260310/vrp_api_jobs/*`
- Common mode now supports DB-backed routing config, technician master loading, heavy-repair rules, and reusable request/result storage for multiple routing modes.

2026-04-17 (Fixed job support and return-to-home rollback)
- Added fixed-job support across the shared routing clients and Atlanta VRP path:
  - `sr_common_vrp_client.py`
  - `sr_vrp_api_client.py`
  - `smart_routing/vrp_api_client.py`
  - `smart_routing/production_assign_atlanta_vrp.py`
  - `smart_routing/production_assign_atlanta.py`
- Added an optional return-to-home routing branch during the same workstream, then removed it after review so the final behavior keeps fixed-job support without forcing return-to-home routing.
- Updated shared runtime/mode modules to match the final behavior:
  - `smart_routing/common_vrp_runtime.py`
  - `smart_routing/common_vrp_api_server.py`
  - `smart_routing/vrp_mode_na_general.py`
- Final 2026-04-17 state:
  - fixed jobs are preserved end-to-end
  - return-to-home routing is not applied in the final path
