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
