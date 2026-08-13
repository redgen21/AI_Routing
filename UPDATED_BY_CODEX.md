# UPDATED_BY_CODEX

Codex 작업일지. 새 작업은 맨 위에 추가한다.

---

## 2026-08-07 Claude 변경 후 결과 표시/아카이브 미저장 조사

- 사용자가 보고한 증상: 화면에 새 routing result가 반영되지 않고 이전 결과가 계속 표시되며, production job archive 디렉터리에도 새 파일이 보이지 않음.
- 로컬 설정 확인 결과 `config/common_vrp.prod.json`은 `save_job_files: true`이지만 `job_archive_root`가 상대 경로 `data/north_america/runtime/production/common_vrp_jobs`로 되어 있음.
- production 템플릿은 `/home/csda/AI_Routing/state/production/common_vrp_jobs`를 사용하도록 되어 있어, 실제 서버가 어느 설정 파일을 실행하는지 확인이 필요함.
- `common_vrp_runtime.py`는 저장 옵션이 꺼져 있으면 아카이브를 조용히 건너뛰며, 저장 실패 시에도 호출부에서 결과 표시/DB 저장과 구분되는 로그를 추가 확인해야 함.
- 아직 서버 업로드·재시작·원격 데이터 변경은 하지 않음.
- `_safe_write_common_job_archive()`를 추가해 아카이브 경로/권한 오류가 routing worker를 중단시키지 않도록 수정함. 오류는 Python logger에 job ID와 config 경로를 남김.
- `_process_common_routing_job()`의 queued/completed/failed 아카이브 호출과 submit 시 초기 아카이브 호출을 안전 래퍼로 전환함.
- `python -m py_compile smart_routing/common_vrp_runtime.py` 성공.
- `PYTHONPATH=.` 기준 관련 테스트 15개 통과.

## 2026-08-07 첨부 production systemd 로그 확인

- 첨부 로그는 2026-08-06 15:53, 16:22, 16:34의 `common-vrp.service` 시작/중지 기록만 포함함.
- production 설정 검증은 모두 `status: ok`, `environment: production`, `/home/csda/AI_Routing/production/config_common_vrp.json`으로 성공함.
- 시작 직후 health curl의 connection refused 1회 후 API가 정상 listen한 것은 startup race로 보이며, 서비스는 정상 시작됨.
- 해당 로그에는 routing request ID, worker 완료/실패, archive 경로, DB upsert 또는 archive write 오류가 없음. 따라서 이 첨부만으로 새 결과가 저장되지 않은 원인은 확정할 수 없음.
- 실제 원인 확인에는 `journalctl`의 routing 실행 시간대 로그와 production 설정의 `storage` 블록, API `/routing/check` 응답 및 DB의 최신 request/result 확인이 필요함.
- 서버에서 확인한 `common_vrp_runtime.py` SHA-256 `0f6630b901e16adbb90866f66581b3f16c1cb7cbab363bdadccccb4be46bc613`가 로컬 현재 파일과 정확히 일치함. 따라서 서버가 구버전 runtime을 실행 중이라는 가설은 배제됨.
- Archive의 최신 완료 결과 확인: 7/30 `63511cb1` 완료 16:40:08 UTC, 7/31 `65027547` 완료 16:43:23 UTC. 새 routing 실행과 파일 저장은 정상임.
- 사용자가 화면에서 `Load Routing Result`를 반복해도 이전 결과가 표시된다고 보고함. 다음 진단은 8065 API `/routing/latest`가 archive 최신 job ID와 동일한지 직접 비교하는 것임.
- 7/30 새 job `4aa0ff76`가 16:47:28~16:52:54 UTC에 completed 되었고 DB/API도 `assigned_jobs=50`, `unassigned_jobs=1`, `has_result=true`를 반환함.
- 이전 `d4910836`과 새 결과의 assignment 매핑 차이가 0으로 확인됨. 이는 화면 stale의 증거가 아니라 새 실행 결과가 동일했다는 의미이며, 적용 여부는 서버 코드 hash와 결과 diagnostics를 추가 비교해야 함.
- 두 결과 diagnostics 비교 결과, 모두 `mode=capacity_tight`, `priority_load_objective_enabled=True`, `target_penalty_multiplier=1.15`, `co_location_split_penalty=3000`, `unassigned_rescue.attempted=True`, `accepted=False`임. 따라서 Claude의 adaptive/rescue 로직은 실제 실행됨.
- 미배정 `RNN260725043731`은 후보 7명이 `SLOT_CAPACITY_EXCEEDED`, Jason/Marcus는 각각 `WORK_LIMIT_EXCEEDED`(640.6분/728.2분)로 거절되어, 현재 600분·슬롯 제약 아래 feasible 후보가 없음. 동일 결과는 로직 미적용이 아니라 제약상 동일한 최종해임.
- `sr_common_vrp_client_server.py`를 마지막 커밋 `4bd1033`의 내용으로 원복함. Claude가 추가한 solver candidate-analysis 표시/상세 컬럼 변경만 제거했고, solver/runtime 파일은 변경하지 않음.
- 원복 후 Python compile 성공. Git 내용 diff는 없으며 Windows 작업트리의 줄바꿈 상태만 `mixed`로 표시됨.

## 2026-08-07 assignments/Summary 슬롯 불일치 수정

- `smart_routing/vrp_mode_na_general.py` 응답 생성 시 공개 `assignments`의 정규화된 `job_slot_count`를 기준으로 기사별 `job_count`·`slot_count`를 재계산해 `engineer_summary`에 반영하도록 수정.
- 기존 중간 schedule/summary 집계가 서로 다른 슬롯값을 가질 수 있던 경로를 제거함.
- Python compile 성공, `tests/test_vrp_slot_fill_objective.py` 8개 통과.
- UI 원인 추가 확인: `_build_common_result_frames()`가 payload의 `job_slot_count_x`를 routing result의 `job_slot_count_y`보다 먼저 선택해 화면에서 6슬롯을 표시할 수 있었음. routing result 슬롯을 우선하도록 수정함.
- `sr_common_vrp_client_server.py` compile 성공, 관련 테스트 8개 통과.
- 추가 보강: merge 전에 routing 결과의 `job_slot_count`/`service_time_min`을 각각 `routing_job_slot_count`/`routing_service_time_min`으로 분리해 `_x/_y` suffix 우선순위에 의존하지 않도록 변경하고, coalesce 후 임시 컬럼을 제거함.
- 데이터 경로 추가 확인: `_build_result_view_state()`의 비교 모드 기본값이 `Actual`이어서 Routing Result가 Smart Routing 결과 대신 Actual 경로/슬롯을 표시할 수 있었음. 기본값을 `Smart Routing`으로 변경하고 Actual은 명시적 비교 옵션으로 유지함.
- 수정 후 `sr_common_vrp_client_server.py` SHA-256: `ca938785be2b9985ec0f797a1fcae849040f081d043c495c72ccd6ca5d1b6dd7`; compile 및 관련 테스트 8개 통과.
- 사용자가 Smart Routing을 명시적으로 선택해도 화면 KPI가 다르다고 확인함. Smart Routing Summary가 로컬 payload/schedule 재계산값을 사용하던 경로를 제거하고 서버 `engineer_summary`를 직접 표시하도록 보강함.
- 최신 UI 파일 SHA-256: `f70c20ebf07e1820a598c8a2c8af78fb37eef783213307e010c552eedb6c3168`; compile 및 관련 테스트 8개 통과.
- `Not Assigned` stale 표시 방지: `_build_unassigned_job_display_df()`에서 현재 result assignments의 receipt를 제외하도록 보강함. assignments에 이미 있는 receipt가 unassigned 목록에 남아 있어도 화면에 표시하지 않음.
- 최신 UI 파일 SHA-256: `316424819c29dd2d41df899abf37ef396f4273bb5caa6de269f75ced74052c32`; compile 및 관련 테스트 8개 통과.
- Smart Routing 상단 KPI(평균 거리/시간, jobs·slots 평균/표준편차, DMS/DMS2 통계)가 로컬 route 재계산값을 사용하던 문제를 확인해 서버 `engineer_summary` 기준으로 통일함. Actual 및 region filter는 기존 별도 경로를 유지함.
- Region Staffing은 Smart Routing 선택 시 서버 assignments에서 생성된 `filtered_assignment`를 사용하므로, 최신 assignment 기준 region별 인원/서비스 건수를 표시함.
- 최신 UI 파일 SHA-256: `ca702f129f9049cf424e503423470d235a311c293e68f3d0b821d65142d416ca`; compile 및 관련 테스트 8개 통과.
- Actual에도 Smart Routing과 동일한 heavy repair 최소 2슬롯 정규화를 적용하는 `_normalize_heavy_repair_slots()`를 추가하고 `_build_common_actual_frames()`에서 사용함.
- 최신 UI 파일 SHA-256: `298b4d2ef0e7fc094a2cb950f688c50d0d687ac47cec6de5c8f38367a6f93c8e`; compile 및 관련 테스트 8개 통과.
- Fill Rate 분모 보정: heavy-repair 정규화로 assigned slots가 raw technician capacity보다 커질 때 표시용 effective capacity를 `max(raw_capacity, assigned_slots)`로 사용하도록 수정. DMS/DMS2별 Fill Rate에도 동일 적용.
- 최신 UI 파일 SHA-256: `5cd3ea6361c70817ac7d6017bdff0ea515de272c761bfaef8abad3728073f9d4`; compile 및 관련 테스트 8개 통과.
- 사용자 요청에 따라 Fill Rate 분모를 다시 기사별 선언 `slot_count` 합계로 확정하고 effective-capacity 보정을 제거함. Heavy repair 정규화 슬롯은 분자에만 반영됨.
- 최신 UI 파일 SHA-256: `f279edea3a886c2a4bf9ffdd28827e99b476a424ec6cde886992d96dadabb22a`; compile 및 관련 테스트 8개 통과.
- Actual heavy-repair 보정이 flag 누락 payload에서 적용되지 않던 문제를 추가 수정함. `is_heavy_repair=true` 또는 `service_time_min >= 100`인 Actual job은 최소 2슬롯으로 정규화.
- 최신 UI 파일 SHA-256: `a2917a06577e83740156b43d22e36195d6c064e028fd42727d70883436ab4211`; compile 및 관련 테스트 8개 통과.
- `routing_statistics_20260729_20260806.xlsx` 분석: 8/4 제외. 8/5는 65 jobs/91 input slots, 20 fixed jobs/33 fixed slots, 11 technicians이며 Smart Routing 64 assigned/1 unassigned, 92 normalized assigned slots, total route 934.22 mile, average 84.93 mile, RUDY/Richard/Frank 등 장거리 및 Frank 629.96분이 확인됨.
- 8/5 미배정은 `RNN260803084931` 1건이며 Jason은 잔여 1슬롯이나 `NO_FEASIBLE_ROUTE`, Richard/RUDY는 slot full, 두 후보는 unavailable. 따라서 단순 slot-balance 문제로 단정하지 않고, 고정 작업 지리 분산/후보 경로 제약이 원인으로 분류함.
- 8/5만 개선하는 전역 objective 변경은 7/29·7/30·7/31·8/3·8/6 결과를 악화시킬 위험이 있어 아직 Solver 정책을 변경하지 않음.

## 2026-08-07 — Claude 변경사항 검토

### 확인한 파일

- `UPDATED_BY_CLAUDE.md`
- `smart_routing/production_assign_atlanta_vrp.py`
- `smart_routing/common_vrp_runtime.py`
- `smart_routing/vrp_mode_na_general.py`
- `tests/test_vrp_slot_fill_objective.py`

### Claude 변경 요약

- Job 수를 우선하고, 같은 미배정 Job 수에서는 슬롯을 최대화하는 lexicographic drop penalty 추가
- 80km/90km 기준 route distance soft shaping 추가
- area_type routing에서도 일일 200km hard cap을 적용하도록 보강
- relocate-and-make-active 및 extended-swap-active 탐색 operator 활성화
- 미배정 Job에 대한 bounded unassigned rescue 재탐색 추가
- 후보별 `NO_FEASIBLE_ROUTE` 상세 진단 추가
- adaptive objective와 fixed 기사 600분·이동 cap 정책 보강
- 31~60건 입력의 기본 `time_limit_seconds`를 30초에서 60초로 변경

### 검증

- 세 Solver 파일 Python compile 성공
- 관련 테스트 46개 통과
- 3 warnings: OR-Tools SWIG deprecation warning

### 주의사항

- `VRP_CO_LOCATION_EXTRA_SLOTS = 2`는 기본 capacity보다 동일 위치 Job을 추가로 허용할 수 있음
- 80/90km shaping은 soft penalty이며 80km를 hard cap으로 만들지 않음
- 단일 구간 거리 제한은 여전히 km가 아니라 `max_single_leg_min` 시간 기준
- 실제 서버 반영 여부는 배포 후 SHA-256과 diagnostics로 확인해야 함
- 이번 턴에는 서버 업로드·재시작을 하지 않음
- 8/5 fixed 분석: 전역 Solver 변경 없이 fixed 위치 분산과 후보 경로 제약을 진단함.

## 2026-08-07 8/5 fixed 공간분산 및 후보경로 분석

- 8/5 fixed 20건/33슬롯, 전체 65건/91슬롯을 확인함.
- AI103264의 fixed 작업 중 ZIP 30047 두 건이 113.5km 떨어져 있음.
- `4121 GA-78` 좌표 `(33.748902, -85.3644752)`는 같은 ZIP의 다른 fixed 좌표와 크게 이격되어 좌표 이상 후보로 기록함.
- 날짜 전용 예외가 아닌 일반화 방향으로 fixed-anchor route lower bound, fixed 공간분산/시간부담 지표, 후보별 fixed 경로 증분 OSRM 비용을 자동 계산하는 정책을 제안함.
- 2026-08-07 ZIP fallback geocoding: provider 좌표의 ZIP/도시 불일치 및 approximate 결과를 거부하고 ZCTA internal point를 POSTAL_CENTROID로 사용하도록 추가.
- fallback에는 coordinate_warning, coordinate_warning_reason, geocode_status=APPROXIMATE 메타데이터를 기록.
- 4121 GA-78 재현에서 잘못된 HERE 좌표가 30047 ZCTA 중심점으로 대체됨.
- tests/test_live_atlanta_geocode_fallback.py 추가; 관련 테스트 17개 통과 및 compile 확인.
- 7/29~8/6 Jobs 460건 검증: 정상 날짜(7/29, 7/30, 7/31, 8/3, 8/5, 8/6)는 fallback 0건.
- 8/5 `RNN260720013325` 1건만 ZIP polygon 밖 좌표로 fallback.
- 8/4는 별도 데이터 오류로 알려진 `RNN260727050862` 1건도 ZIP 밖으로 탐지됨.
- 2026-08-07: Fixed receipt-based geocode reuse in `smart_routing/live_atlanta_runtime.py`. Existing coordinates are now reused only when the incoming address key matches the address stored with the receipt; edited addresses proceed through fresh geocoding/ZIP fallback. Python compile and `tests/test_live_atlanta_geocode_fallback.py` (2 tests) passed.
- 2026-08-07: Verified `acc11f6b/request.json` still contained the stale `RNN260720013325` coordinate `(33.748902, -85.3644752)`. Disabled receipt-number coordinate reuse entirely in `live_atlanta_runtime.py`; edited jobs now rely on address geocoding/cache and postal fallback, preventing stale same-receipt coordinates from returning.
- 2026-08-07: Found production fallback configuration could point to a missing relative ZCTA path, allowing the bad address-cache coordinate to survive. Added an automatic fallback to the active catalog-resolved `DEFAULT_ZCTA_ZIP_PATH` when the configured path does not exist.
- 2026-08-07: Confirmed API job master stores `RNN260720013325` at `(33.8701249, -84.1123222)`. Updated `sr_common_vrp_client_server.py` so Build Payload refreshes jobs/technicians/capabilities from the API immediately before submission, preventing a stale Streamlit session snapshot from restoring old coordinates.
- 2026-08-07: Fixed Edit Job session invalidation. After a successful job edit, the API row was updated correctly but the pre-edit `common_vrp_payload` remained in Streamlit session state. Edit save now clears payload, request, result, and statistics state before rerun.
- 2026-08-07: Decoupled server geocoding from `area_map.py`'s local `config/config.json` lookup. `live_atlanta_runtime.py` no longer imports or calls `get_latest_geocoded_service_file()`; server geocoding now uses address-key caches/provider results and the active catalog-resolved ZIP fallback only.
- 2026-08-07: Corrected local catalog separation: `config/data_catalog.json` now uses relative `data/north_america`, while production continues to use the explicit `NA_DATA_CATALOG_PATH=/home/csda/AI_Routing/shared/config/data_catalog.production.json`. Local profile/service/ZCTA paths now resolve to existing files; `sr_area_map.py` compile passed.
- 2026-08-07: Fixed local map path resolution in `smart_routing/area_map.py` and `area_map_usa.py`: nonexistent production absolute paths in shared `config/config.json` now fall back to the active catalog artifact, while existing production paths remain authoritative on the server. Local profile path resolution and compile checks pass.
- 2026-08-07: Fixed local route-explorer `POSTAL_CODE` crash by bumping `area_map` cache version and adding a safe fallback for legacy cached service frames missing the canonical postal column. Local map caches will rebuild automatically.
- 2026-08-07: Fixed `service_date` KeyError in the local area map. Service date parsing now supports date and timestamp columns and guarantees a `service_date` column; the UI also handles legacy/empty frames safely.
- 2026-08-07: Added the same missing-`service_date` guard to `sr_area_map.build_map`; both initial explorer rendering and filtered map rendering now handle legacy frames consistently.
- 2026-08-07: Updated `area_map.get_latest_geocoded_service_file()` to prefer the active catalog `service_geocoded` artifact before region-candidate scanning. Local map now resolves `Service_202607071543_normalized_geocoded.csv`; production resolves its catalog-selected service file.
- 2026-08-10: Enhanced `sr_area_map_asia.py` date-filtered job markers. Existing OSRM sequence numbers remain authoritative; when no route sequence exists, markers now receive deterministic per-technician fallback numbers instead of `?`.
- 2026-08-10: Corrected Asia map marker labels: `DSC` is no longer rendered as the marker text; only `ASC`/`DMS2` retain bucket labels, while DSC/DMS jobs show their numeric sequence.
- 2026-08-10: Fixed deployment-console secure-config catalog validation. `config.json` may now contain either the local catalog-relative North America paths or the catalog-derived server-shared paths; the console still validates them against the active catalog and rewrites the uploaded payload to the server paths. Added development secure-config regression coverage; development (12) and production (7) secure-config tests passed.
- 2026-08-10: Fixed the remaining development secure-config mismatch. The console now reads `/home/csda/AI_Routing/shared/config/data_catalog.development.json` for Development, instead of validating against the stale local catalog only; production continues to use `data_catalog.production.json`. The remote catalog is validated before config preparation and re-read under the upload lock. Development/production secure-config tests: 19 passed.
- 2026-08-10: Corrected the final catalog comparison rule: a server-shared path must match the selected remote environment catalog; a local relative path is accepted only when its derived shared path matches that catalog. This prevents stale local catalog versions from being silently uploaded while allowing matching local test fixtures. Secure-config tests: 19 passed.
- 2026-08-10: Added `tools/data/migrate_legacy_region_plans.py` to inventory the eight configured legacy cities and emit reviewable `regions.csv`, `region_postal.csv`, `technician_assignments.csv`, `rejects.csv`, and `manifest.json` bundles under `data/region_plans/legacy_migration`. The converter preserves source checksums and marks missing technician-region mappings or invalid legacy area types as `needs_review`; it never invents assignments or writes the database. A dry-run over the current local catalog found 8 configured cities, with Atlanta's assignment source available (15 assignments) but legacy area types still requiring review, and the other city assignments requiring a source mapping.
- 2026-08-10: Region Plan v2 city registry now unions `common_city_context`, `common_region_master`, active `common_technician_master`, routing config, and existing region plans, with region/technician counts and migration status. This prevents legacy region-only cities from disappearing as `No permitted city registry entries`. Strategic city keys such as `Atlanta, GA` are accepted as bounded legacy business keys while subsidiary IDs remain strict machine identifiers.
- 2026-08-11: Development-only Region Plan selection contract added. VRP Client now has a city Config tab for selecting the Region Plan, keeps Technician Master CRUD, and exposes Plan-scoped Region assignment in Technician Master. Added region_plan_id, region_plan_revision, and region_plan_checksum to the runtime/admin config schema and added compatibility lookup from physical Atlanta, GA to legacy Plan storage contexts such as Atlanta_6area. Production DB/config was not modified.
- 2026-08-11: Area Map now validates common Region/Technician source files and writes a reviewable `data/region_plans/<subsidiary>/<target_city>/<plan_id>/` candidate with the shared Area + Technician workbook, normalized CSVs, manifest, and checksums. Deployment Console Region Plans v2 can select these local candidates and submit them through the existing development-only import/adopt/review/activation lifecycle. Region IDs and explicit region sequences are preserved across the v2 canonical workflow and API.
