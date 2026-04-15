# UPDATED BY CODEX

## 2026-03-18

### 異붽? 諛섏쁺 ?댁슜

- 吏???듦퀎 吏묎퀎 ??`AREA_NAME` 臾몄옄???뺢퇋??濡쒖쭅 異붽?
- ?묒? ?먮낯??以꾨컮轅? ?욌뮘 怨듬갚, 以묐났 怨듬갚 ?뚮Ц??`area_km2=0`?쇰줈 蹂댁씠??吏???듦퀎 蹂댁젙
- `STRATEGIC_CITY_NAME`, `SVC_ENGINEER_CODE`, `SVC_CENTER_TYPE`, ?щ’ ?대쫫 而щ읆??媛숈? 諛⑹떇?쇰줈 ?뺢퇋??- 吏??愿??蹂댁젙 硫붾え瑜?`docs/area_map_notes_20260318.md`??湲곕줉
- ?꾩떆蹂?吏???꾩쿂由?寃곌낵瑜?`data/cache/area_map` ?꾨옒 pickle 罹먯떆濡???ν븯?꾨줉 異붽?
- ?먮낯 ?꾨줈???뚯씪, ZCTA ZIP ?뚯씪, geocoded ?쒕퉬???뚯씪??寃쎈줈? ?섏젙?쒓컖??媛숈쑝硫?罹먯떆瑜??ъ궗?⑺븯?꾨줉 援ъ꽦
- 吏???쒖떆??ZIP/AREA/context geometry瑜??⑥닚?뷀븳 ??罹먯떆????ν븯?꾨줉 異붽?
- Streamlit?먯꽌 `city + area + service point ?쒖떆 ?щ?` 湲곗??쇰줈 吏??HTML 罹먯떆 異붽?
- 吏???됱긽??24媛?怨좎젙 ?붾젅?????吏???섏뿉 留욌뒗 ?숈쟻 ?됱긽 ?앹꽦 諛⑹떇?쇰줈 蹂寃?- 吏???띾룄 議곗젅??`Show Service Points` 泥댄겕諛뺤뒪 異붽?
- ZIP/AREA 吏??tooltip 媛믪쓣 `Slot Sum`?먯꽌 `POSTAL_CODE` 湲곗? ?ㅼ젣 ?쒕퉬??嫄댁닔(`service_count`)濡?蹂寃?- ?쒕퉬??嫄댁닔 而щ읆 異붽???留욎떠 area map 罹먯떆 踰꾩쟾 媛깆떊
- ZIP 寃쎄퀎?좎쓣 ?뉗? 以묐┰?됱쑝濡?蹂寃쏀븯怨?AREA ?덉씠?대쭔 吏???됱쓣 媛吏?꾨줉 ?쒓컖??議곗젙
- AREA ?덉씠??hover 媛뺤“瑜?異붽???吏??寃쎄퀎? ?고렪踰덊샇 寃쎄퀎瑜????쎄쾶 援щ텇?섎룄濡?蹂댁젙
- 吏?꾩슜 area geometry瑜?ZIP蹂??ㅼ쨷 諛곗젙 ?꾩껜媛 ?꾨땲??`???AREA_NAME` 湲곗? 鍮꾩쨷蹂??곸뿭?쇰줈 ?ш뎄??- LA ?깆뿉??留덉슦???ㅻ쾭 ???щ윭 吏??씠 ?숈떆???≫엳??寃뱀묠 臾몄젣? popup 遺덉씪移?臾몄젣瑜??꾪솕?섎룄濡??섏젙
- `smart_routing/profile_sync.py`? `sr_update_profile_from_service.py` 異붽?
- `DMS/DMS2` ?쒕퉬???곗씠?곕? 湲곗??쇰줈 `Zip Coverage`? `Slot` ?낅뜲?댄듃 CSV ?앹꽦 濡쒖쭅 異붽?
- ?앹꽦 ?뚯씪:
  - `260310/input/Zip_Coverage_updated_Service_202603181109.csv`
  - `260310/input/Slot_updated_Service_202603181109.csv`
  - `260310/output/unmatched_service_sm_Service_202603181109.csv`
  - `260310/output/profile_sync_summary_Service_202603181109.csv`
- profile sync??`Zip Coverage` ?낅뜲?댄듃 ??以묐났 ZIP???щ윭 吏??뿉 ?숈떆??異붽??섏? ?딅룄濡????吏??1媛쒕쭔 ?좏깮?섎뒗 洹쒖튃 異붽?
- ?낅뜲?댄듃??`Zip Coverage` 寃곌낵 寃利? `POSTAL_CODE + STRATEGIC_CITY_NAME + SVC_CENTER_TYPE` 湲곗? `AREA_NAME` 1媛쒕쭔 ?좎?
- 遺곷????쇱슦??1?④퀎 沅뚯뿭 ?ㅺ퀎 紐⑤뱢 `smart_routing/region_design.py` 諛??ㅽ뻾 ?ㅽ겕由쏀듃 `sr_region_design.py` 異붽?
- 沅뚯뿭 ?ㅺ퀎 湲곗?:
  - `DMS/DMS2` ?쒕퉬?ㅻ쭔 ???  - ?꾩떆蹂??됯퇏 Slot ?⑸웾 湲곗? ?꾩슂 SM ??怨꾩궛
  - ?고렪踰덊샇 以묒떖?먭낵 ?쒕퉬?ㅺ굔??媛以묒튂 湲곕컲 ?대윭?ㅽ꽣留곸쑝濡??듯빀 沅뚯뿭 ?ㅺ퀎
- ?앹꽦 ?뚯씪:
  - `260310/output/region_design_city_summary_Service_202603181109_geocoded.csv`
  - `260310/output/region_design_region_summary_Service_202603181109_geocoded.csv`
  - `260310/input/region_design_postal_Service_202603181109_geocoded.csv`
  - `260310/input/region_design_service_Service_202603181109_geocoded.csv`

## 2026-03-10

### 諛섏쁺 ?댁슜

- 臾몄꽌 愿由?洹쒖튃???꾨줈?앺듃 ?댁쁺 ?먯튃?쇰줈 ?뺤젙
- `docs` ?대뜑 湲곕컲 ?ㅺ퀎 臾몄꽌 愿由?諛⑹떇 ?뺤쓽
- 猷⑦듃??Codex 蹂寃??대젰 臾몄꽌 ?좎? 洹쒖튃 ?뺤쓽
- 遺곷? ?쇱슦???꾨줈?앺듃???꾩옱 援ъ“ ?먯튃怨??곗씠?????洹쒖튃???ㅺ퀎??珥덉븞?쇰줈 湲곕줉

### ?앹꽦 ?뚯씪

- `docs/?꾨줈?앺듃_?ㅺ퀎??md`
- `UPDATED_BY_CODEX.md`

### 硫붾え

- ?꾩옱 ?곗씠?곕뒗 理쒖쥌蹂몄씠 ?꾨땲誘濡?臾몄꽌??援ъ“ 以묒떖?쇰줈 ?묒꽦
- ?댄썑 援ъ“ 蹂寃? ?ㅺ퀎 蹂寃? 肄붾뱶 ?앹꽦 ????臾몄꽌瑜?怨꾩냽 ?꾩쟻 媛깆떊

## 2026-03-16

### 諛섏쁺 ?댁슜

- 誘멸뎅 二쇱냼 ?꾧꼍??蹂??湲곕낯 諛⑹떇??`US Census Geocoder`濡??뺤젙
- ?섎（ 理쒕? 10,000嫄??좉퇋 二쇱냼留?蹂?섑븯怨??ㅼ쓬 ???댁뼱??異붽? 蹂?섑븯??援ъ“ ?ㅺ퀎
- ?대? 蹂?섑븳 二쇱냼瑜??ъ궗?⑺븯湲??꾪븳 罹먯떆 ?뚯씪 援ъ“ 諛섏쁺
- ?쒕퉬???뚯씪怨?吏?ㅼ퐫??寃곌낵瑜?蹂묓빀?섎뒗 CLI ?ㅽ겕由쏀듃 異붽?

### ?앹꽦 ?뚯씪

- `smart_routing/__init__.py`
- `smart_routing/census_geocoder.py`
- `sr_geocode.py`

### ?섏젙 ?뚯씪

- `docs/?꾨줈?앺듃_?ㅺ퀎??md`
- `UPDATED_BY_CODEX.md`

### 硫붾え

- ?꾩옱 援ы쁽? 誘멸뎅 二쇱냼留???곸쑝濡??숈옉
- ?낅젰 ?쒕퉬???뚯씪? CSV? Excel??紐⑤몢 吏??- 寃곌낵??罹먯떆 ?꾩쟻 ??`input` ?대뜑??蹂묓빀 CSV濡????- Census ?묐떟 ??꾩븘?껋쓣 ?쇳븯湲??꾪빐 ?대? ?붿껌???뚮같移섎줈 遺꾪븷
- 吏?ㅼ퐫???ㅽ뻾 ???ㅼ젣 誘명빐寃?二쇱냼 湲곗??쇰줈 ?붿뿬 嫄댁닔 怨꾩궛 蹂댁젙
- Census ?묐떟 醫뚰몴 ?꾨뱶 ?뚯떛 ?ㅻ쪟 ?섏젙
- Census 誘명빐寃?二쇱냼????댁꽌留?Google Geocoding API fallback 援ъ“ 異붽?
- 吏?ㅼ퐫???ㅼ젙怨?Google API ?ㅻ? 猷⑦듃 `config.json`?먯꽌 ?쎈룄濡?蹂寃?- Google API ?쒕룄 ?대젰 ?뚯씪??異붽?????踰??쒕룄??二쇱냼 ?ы샇異?諛⑹?
- Google API ?붽컙 ?쒕룄 嫄댁닔 吏묎퀎 諛?10,000嫄?珥덇낵 諛⑹? 濡쒖쭅 異붽?
- Google `REQUEST_DENIED` ?묐떟???쇰? 二쇱냼?먯꽌 諛쒖깮?대룄 ?꾩껜 ?ㅽ뻾??以묐떒?섏? ?딅룄濡?泥섎━
- Google ?몄텧 以?SSL/?ㅽ듃?뚰겕 ?ㅻ쪟媛 諛쒖깮?대룄 ?꾩껜 ?ㅽ뻾??以묐떒?섏? ?딅룄濡?泥섎━
- 吏?ㅼ퐫????二쇱냼 臾몄옄???꾩쿂由щ? 異붽???`ADDRESS_LINE1_INFO` ??以묐났??city/state/postal/country瑜??쒓굅
- 怨쇨굅 Google ?ㅽ뙣 ?대젰???대쾲 ??踰덈쭔 臾댁떆?섍퀬 ?ъ떆?꾪븷 ???덈뒗 1?뚯꽦 ?듭뀡 異붽?
- ?쒕퉬????蹂묓빀 ?쒖뿉???뺤젣??二쇱냼 湲곗? ?ㅻ? ?ъ슜?섎룄濡??섏젙??以묐났 二쇱냼 ?됱뿉 ?숈씪 醫뚰몴媛 梨꾩썙吏寃?蹂댁젙
- 理쒖쥌 蹂묓빀 寃곌낵?먯꽌 醫뚰몴 誘몄깮???됱쓽 `source`瑜?`failed`濡??쒓린?섎룄濡??섏젙
- ?ㅽ뻾 濡쒓렇??Census ?④퀎 寃곌낵? 理쒖쥌 蹂묓빀 寃곌낵瑜?遺꾨━??異쒕젰?섎룄濡?媛쒖꽑

## 2026-03-18

### 諛섏쁺 ?댁슜

- 誘멸뎅 Census ZCTA 怨듭떇 ZIP 寃쎄퀎 ?뚯씪 ?ㅼ슫濡쒕뱶
- `Atlanta, GA` ?꾩슜 ZIP/AREA 寃?좎슜 吏???곗씠??濡쒖쭅 異붽?
- Streamlit 湲곕컲 吏??寃??吏????異붽?
- ZIP ?덉씠?댁? AREA dissolve ?덉씠?대? ?④퍡 ?쒖떆?섎룄濡?援ы쁽

### ?앹꽦 ?뚯씪

- `smart_routing/area_map.py`
- `sr_area_map.py`

### ?섏젙 ?뚯씪

- `docs/?꾨줈?앺듃_?ㅺ퀎??md`
- `UPDATED_BY_CODEX.md`

### 硫붾え

- 誘멸뎅 ZIP 寃쎄퀎??`data/geo/tl_2024_us_zcta520.zip`?????- Atlanta ?곗씠?곗뿉?쒕뒗 ?섎굹??ZIP???щ윭 AREA/SM??以묐났 諛곗젙?섎뒗 耳?댁뒪媛 留롮쓬
- 吏????踰붿쐞瑜?Atlanta ?꾩슜?먯꽌 誘멸뎅 ?꾩껜/?꾩떆蹂??좏깮 援ъ“濡??뺤옣
- 吏???앹뾽??`AREA_NAME`, `SVC_ENGINEER_CODE`, `SVC_CENTER_TYPE` ?쒖떆 異붽?
- geocoded ?쒕퉬??醫뚰몴瑜?吏?????덉씠?대줈 異붽?
- `AREA_NAME`蹂??됱긽 援щ텇 諛?誘몃같??ZIP ?뚯깋 諛곌꼍 ?덉씠??異붽?
- 吏???붾㈃??醫뚯슦 遺꾪븷 ?덉씠?꾩썐?쇰줈 媛쒗렪?섍퀬 吏???듦퀎???고렪踰덊샇 ?? ?쒕퉬???? 硫댁쟻) 異붽?
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
- Fixed sr_area_map.py so ASC black-border numbered markers are applied only in 湲곗〈吏??mode. ?좉퇋吏??markers now use the standard white border even when the original service row came from ASC.
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
- Background rebuild scripts were relaunched using py.exe because the alternate python.exe on the machine did not have the equests dependency required by the routing modules.
## 2026-03-31
- Actual Attendance ?쒗븳 濡쒖쭅???섏젙?? ?ㅼ젣 洹쇰Т??以??꾩옱 roster???녿뒗 ?붿??덉뼱?????댁긽 synthetic 湲곗궗濡?異붽??섏? ?딄퀬 current roster ?덉쓽 ?泥??몄썝?쇰줈留?移섑솚?섎룄濡?蹂寃쏀븿.
- Line/OSRM Actual Attendance ?ъ깮?깆쓣 ?쒖옉?덇퀬, OSRM Actual Attendance 理쒖쥌 ?뚯씪? current roster only 湲곗??쇰줈 ?ъ깮???꾨즺??
- 寃利? `atlanta_assignment_result_osrm_actual_attendance.csv`?먯꽌 non-roster `assigned_sm_code`媛 0嫄댁엫???뺤씤??
- Actual Attendance ?꾩슜 ?ㅽ뿕 紐⑤뱶 2媛쒕? 異붽??? `Sequence Assign (Actual Attendance)`, `Iteration Assign (Actual Attendance)`.
- Sequence 諛⑹떇? ?꾩껜 怨좉컼 ?쒖꽌瑜?nearest-neighbor濡?留뚮뱺 ??contiguous weighted chunk濡??섎닠 ?붿??덉뼱?먭쾶 諛곗젙??
- Iteration 諛⑹떇? 湲곗〈 諛곗젙 ?댄썑 objective(`max total_work`, weighted job std, total travel)瑜?媛쒖꽑?섎뒗 one-job move瑜?諛섎났 ?곸슜??
- `sr_production_map.py`?먯꽌 ?쇰컲 Assign 紐⑤뱶瑜??④린怨?Actual-only 紐⑤뱶 ?꾩＜濡??뺣━?덉쑝硫? ???ㅽ뿕 紐⑤뱶 ?곗텧 ?뚯씪???쎈룄濡??뺤옣??
- 諛깃렇?쇱슫???ъ깮???ㅽ겕由쏀듃 異붽?: `sr_production_atlanta_assign_actual_attendance_sequence_chunks.py`, `sr_production_atlanta_assign_actual_attendance_iteration_chunks.py`.
- OSRM Actual ?ㅽ뿕 紐⑤뱶 2媛쒕? 異붽??? `Sequence OSRM Assign (Actual Attendance)`, `Iteration OSRM Assign (Actual Attendance)`.
- `production_assign_atlanta_osrm.py`??`assignment_strategy` ?뚮씪誘명꽣瑜?異붽???`sequence`/`iteration`??OSRM matrix 湲곕컲 諛곗젙 ?먮쫫?쇰줈 ?ㅽ뻾 媛?ν븯寃???
- `sr_production_map.py`??OSRM Sequence/Iteration Actual ?곗텧 ?뚯씪怨?紐⑤뱶 ?좏깮 UI瑜?異붽???
- 諛깃렇?쇱슫???ъ깮???ㅽ겕由쏀듃 異붽?: `sr_production_atlanta_assign_osrm_actual_attendance_sequence_chunks.py`, `sr_production_atlanta_assign_osrm_actual_attendance_iteration_chunks.py`.
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

