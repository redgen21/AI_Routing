# VRP Solver 전체 설계문서

## 1. 목적과 범위

이 문서는 북미 라우팅 시스템의 na_general VRP Solver 설계와 현재 구현 정책을 설명한다. 범위는 입력 정규화, 후보·fixed·capability 판정, OSRM Matrix, OR-Tools 배정, hard constraint, 자동 목적함수, 동일 위치 Job, add-on repair, 결과·진단·검증이다.

핵심 원칙:

> Primary Solver가 전역 Job 배정과 재배치를 담당하고, add-on repair는 primary 결과 이후의 제한적인 보완만 담당한다.

## 2. 시스템 구성

    request payload / DB snapshot
              |
              v
    vrp_mode_na_general.py
      입력 검증·DataFrame 변환·진단
              |
              v
    production_assign_atlanta_vrp.py
      후보 판정·OSRM·OR-Tools·후처리·add-on
              |
              +--> OSRM Table / Route
              |
              v
    assignment + schedule + technician summary + unassigned + diagnostics

vrp_mode_na_general.py는 payload를 Solver frame으로 변환하고 build_atlanta_production_assignment_vrp_from_frames()를 호출한다. 배정 제약과 목적함수의 주 소유 모듈은 production_assign_atlanta_vrp.py이다.

## 3. 입력 계약

### 3.1 Job

| 필드 | 의미 | 단위/규칙 |
|---|---|---|
| GSFS_RECEIPT_NO / receipt_no | Job 식별자 | 날짜 내 unique |
| service_date_key | 서비스 날짜 | ISO 날짜 권장 |
| latitude, longitude | 작업 좌표 | WGS84, OSRM은 (lon, lat) |
| job_slot_count | capacity 소요량 | 양의 정수 |
| service_time_min | 서비스 시간 | 분 |
| fixed | 기존 기사 고정 여부 | boolean |
| reschedule | 재배정 우선 Job | boolean |
| product/group | capability 판정 | 문자열 |
| current_employee_code | fixed 기존 기사 | 기사 코드 |

선택 필드는 area_type, region_name, co_location_group_id, eligible_employee_codes, priority, time_window, is_heavy_repair 등이다.

### 3.2 Technician

필수 정보는 기사 코드·이름·Home 좌표·slot capacity·근무 가능 여부·center type이다. 선택 정보는 max_minutes, max_home_to_job_min, priority_group, region policy이다.

Home 좌표가 유효하지 않은 기사는 Solver 차량으로 만들지 않는다. unavailable 기사는 신규 후보에서 제외한다.

### 3.3 Capability

Capability는 (employee_code, product_group_code, product_code) 조합으로 관리한다.

후보 계산 순서:

1. 명시적 eligible_employee_codes가 있으면 hard 후보 집합으로 사용
2. capability snapshot이 있으면 product/group/repair 조건으로 후보 생성
3. heavy_repair_allowed가 필요한 Job은 flag 확인
4. 후보가 없으면 NO_ELIGIBLE_TECHNICIAN

모든 제품을 수리 가능하다고 가정하는 테스트에서는 모든 기사·제품 조합을 합성할 수 있지만, availability·slot·Home·routing options는 별도 입력이다.

## 4. 전처리와 진단

Solver 전에 Job 중복 제거, 좌표 검사, 슬롯·서비스시간 변환, 기사 중복 제거, Home 좌표 생성, fixed 기사 확인, fixed capacity 확인, Job별 eligible 수, 전체 요청 슬롯·기사 capacity, 동일 위치 group을 계산한다.

주요 진단 필드:

    job_count
    total_job_slots
    total_technician_slots
    jobs_without_eligible_technician_count
    fixed_job_count
    unavailable_fixed_job_count
    fixed_capacity_violations
    adaptive_objective
    matrix_telemetry
    solver_invocation_count
    stage_timings_ms

요청 수요와 결과 Fill Rate는 구분한다.

    requested_capacity_utilization = requested_slots / all_technician_capacity
    result_fill_rate = assigned_slots / capacity_of_assigned_technicians

## 5. OSRM 설계

### 5.1 Matrix

Solver는 Home + Job 좌표를 OSRM Table로 요청한다.

- 거리: km
- 시간: min
- 좌표: (longitude, latitude)
- 기본 profile: driving
- 대상: Home-to-Job, Job-to-Job, Job-to-Home

Matrix cache key에는 도시, profile, 좌표 순서, 정규화 좌표가 포함되어야 한다.

### 5.2 Route

기본 비용은 duration을 주 기준으로 사용한다. 결과와 add-on 검증에서는 Home -> ordered jobs -> Home 경로를 OSRM으로 재계산한다.

OSRM 오류 시 설정에 따라 Haversine fallback을 사용할 수 있으며 matrix_source, request_count, failure_count, fallback_attempted, fallback_used, profile, distance_unit, duration_unit을 기록한다.

## 6. 후보·fixed 정책

### 6.1 일반 Job

capability, availability, 지역 정책, Home/단일구간 조건을 만족하는 기사만 hard 후보가 된다. 후보 간 선택은 전역 Solver 목적함수로 결정한다.

### 6.2 Fixed Job

fixed Job은 기존 기사에게 우선 고정한다.

- active technician 목록에 있고 유효하면 해당 기사 hard candidate
- fixed 기사가 unavailable이거나 목록에 없으면 정책에 따라 대체 후보 허용
- fixed Job이 있다는 이유로 해당 기사의 전체 route 거리·시간 제한을 해제하지 않음
- DMS2 완화는 center type 정책으로만 적용

fixed Job 보호와 fixed 기사에게 일반 Job을 무제한 추가하는 것은 별개 정책이다.

### 6.3 Reschedule

reschedule Job은 일반 Job보다 높은 drop penalty와 priority를 가질 수 있지만 hard feasibility는 항상 지킨다.

## 7. Hard Constraint

### 7.1 슬롯

기본 기사별 슬롯은 max_jobs_by_vehicle로 제한한다. 동일 위치 Job은 개별 Job으로 유지한다.

현재 구현의 예외:

    VRP_CO_LOCATION_EXTRA_SLOTS = 2

동일 위치 Job은 기본 capacity보다 최대 2슬롯 더 허용될 수 있다. 따라서 전체 capacity보다 많은 슬롯이 배정될 수 있다. 엄격한 slot hard cap이 필요한 운영은 이 값을 0으로 하거나 별도 승인 정책을 적용해야 한다.

### 7.2 근무시간

일반 기사 정책:

    soft 기준: 540분
    overtime 허용: 최대 60분
    absolute hard cap: 600분

total_working_min은 서비스시간+이동시간이다. total_day_duration_with_return_min은 귀가 포함 표시 지표이며 600분 hard cap 판정에는 사용하지 않는다.

DMS2는 VRP_UNRESTRICTED_DMS2_WORK_MIN = 1440 예외를 사용할 수 있다. fixed Job이 있는 일반 기사는 이 예외 대상이 아니다.

### 7.3 이동

선택적 제한:

- max_travel_min_per_sm_day: 일일 이동시간
- max_travel_km_per_sm_day: 일일 이동거리
- max_single_leg_min: 단일 구간 이동시간
- max_home_to_job_min: Home에서 첫 Job까지 이동시간

현재 단일 구간의 직접 km hard cap은 없고 시간 기준 max_single_leg_min이 있다. relax_distance_caps_for_feasibility 재시도에서는 일부 cap이 완화될 수 있으므로 진단에 기록한다.

max_travel_km_per_sm_day hard cap은 area-type 라우팅 날에도 적용된다
(RouteDistanceSoft dimension의 CumulVar SetMax). 과거에는 area-type 날에
TravelDistance dimension이 생성되지 않아 설정된 cap(예: 200km)을 넘는 route가
허용되었다. 정책: cap 안에서 도달할 수 없는 Job은 극단적인 route를 만드는 대신
미배정으로 남겨 add-on repair가 처리한다. relax 재시도에서는 이 cap도 완화된다.

## 8. Primary OR-Tools Solver

### 8.1 모델

- 차량: technician
- Start/End: technician Home
- 방문 노드: Job
- 차량별 후보: VehicleVar hard restriction
- Time dimension: 이동+서비스
- SlotCount dimension: Job slot 합
- 선택적 TravelTime dimension
- 선택적 TravelDistance dimension

### 8.2 목적함수

설계상 우선순위:

1. fixed/reschedule과 hard feasibility
2. 미배정 Job 수 최소화
3. 배정 슬롯 최대화
4. 600분 total work 준수
5. 이동시간 최소화
6. 이동거리·귀가 penalty 최소화 (route당 80km soft / 90km strong penalty 포함)
7. 동일 위치 분할 최소화
8. 자동 선택된 기사별 슬롯 균형

Job drop penalty는 lexicographic 구조다. 기본 penalty(VRP_OPTIONAL_JOB_DROP_PENALTY)는
Job당 1회 부과되어 미배정 Job 수 최소화가 1순위가 되고, 슬롯당 추가분
(VRP_SLOT_DROP_PENALTY_STEP × (slot-1))이 더해져 같은 미배정 수에서는 큰 슬롯
Job을 우선 유지한다.

슬롯 추가분의 비용 서열은 다음과 같이 설계한다.

- route-shape soft cost(global span, balance/target, long-leg·귀가,
  preferred region, 80/90km 거리 shaping)보다 크다. span은 일반 route가
  600분 hard cap이므로 현실적 차이가 step 아래에 머문다.
- center-type 정책 penalty(VRP_OVERLAP_DMS2_PENALTY_COST,
  VRP_DMS_AREA_DMS2_FALLBACK_PENALTY_COST)보다는 의도적으로 작다.
  슬롯 최대화가 DMS 영역 Job을 DMS2에게 밀어내는 것을 허용하지 않는다.
- 하루 최대 합(step × 초과 슬롯 수)은 기본 penalty 1건보다 작아
  미배정 Job 수 우선순위를 침범하지 않는다.

기본 Search는 PARALLEL_CHEAPEST_INSERTION + GUIDED_LOCAL_SEARCH이다. time_limit_seconds는 Solver invocation 하나의 제한이며, feasibility 재시도가 있으면 전체 요청시간은 더 길어질 수 있다.

드랍 노드 재삽입 연산자(use_relocate_and_make_active, use_extended_swap_active)를
명시적으로 활성화한다. 기본값(비활성)에서는 "활성 Job을 옮겨 자리를 만들고
드랍된 Job을 되살리는" 복합 이동이 탐색 이웃에 없어서, 주변 기사가 슬롯 만석인
드랍 Job(slot fragmentation)이 제한시간 내 구출되지 못하는 사례가 확인되었다
(2026-07-30 NEWBORN 2슬롯 Job).

Primary 탐색 종료 시 미배정 Job이 남아 있으면 unassigned rescue를 수행한다:
찾은 해에서 warm-start로 GLS를 이어가고(SolveFromAssignment), slice가 개선
(미배정 감소 또는 목적값 감소)되는 동안 최대
VRP_UNASSIGNED_RESCUE_MAX_ATTEMPTS(3)회 반복한다. slice당 시간은
min(VRP_UNASSIGNED_RESCUE_TIME_SECONDS=20, time_limit_seconds)이고, 개선이
없는 slice에서 즉시 중단한다. drop penalty가 모든 soft cost를 지배하므로,
배정 가능한 해가 존재하면 계속 탐색이 그 방향으로 수렴한다. 미배정이 없으면
rescue는 실행되지 않는다. 결과는 adaptive_objective.unassigned_rescue에
attempts/accepted_count/unperformed_before/after로 기록한다.

기본 time_limit_seconds는 payload 옵션이 없을 때 job 수 기반으로 자동 결정되며
(common_vrp_runtime._default_time_limit_seconds), 31~60건 구간은 60초다.

기록할 성능 지표:

    matrix_ms
    solver_search_ms
    primary_solver_pipeline_ms
    add_on_repair_ms
    result_postprocess_ms
    total_request_ms
    solver_invocation_count

## 9. 자동 목적함수 정책

날짜 입력으로부터 capacity_utilization = requested_slots / total_capacity를 계산한다.

### co_location_first

가장 큰 동일 위치 그룹이 8건 이상이거나 동일 위치 비율이 25% 이상이면 선택한다. 기사별 target slot balance를 끄고 같은 위치 집중을 우선한다.

### capacity_surplus

사용률이 75% 미만이면 balance penalty를 약화하고 거리·동일 위치 집중을 우선한다.

### balanced_load

75% 이상 90% 미만이면 동일 위치와 기사별 slot target을 절충한다.

### capacity_tight

90% 이상이면 target slot objective를 활성화한다. 요청 슬롯이 capacity를 초과하면 unassigned=0은 구조적으로 불가능할 수 있다.

진단에는 mode, priority_load_objective_enabled, capacity_utilization, co_location_ratio, largest_co_location_group_size, target_penalty_multiplier, co_location_multiplier를 저장한다.

평균 80km는 mode 선택의 hard 조건이 아니다. 대신 route당 이동거리 soft penalty가
항상 적용된다: 80km(VRP_ROUTE_DISTANCE_SOFT_KM)까지는 penalty가 없고, 80~90km는
미터당 VRP_ROUTE_DISTANCE_SOFT_PENALTY_PER_M, 90km(VRP_ROUTE_DISTANCE_STRONG_KM)
초과분은 추가로 미터당 VRP_ROUTE_DISTANCE_STRONG_PENALTY_PER_M이 부과된다.
이 penalty는 TravelDistance hard cap과 달리 area-type 라우팅 날에도 적용되며,
drop penalty보다 항상 작으므로 배정이 거리 penalty에 우선한다.

거리 shaping은 unrestricted DMS2 차량에도 적용한다. DMS2는 hard cap(근무 1440분,
home-to-job·단일구간·일일 이동 cap)에서 면제되기 때문에, shaping까지 면제하면
일반 기사가 cap 때문에 받지 못하는 원거리 Job이 cap 없는 기사에게 집중되어
극단적인 단일 route가 만들어질 수 있다. soft penalty이므로 대안이 없는 DMS2
배정(TV 등)은 여전히 성립한다.

home-to-job cap이 무시되는 경로는 다음 네 가지뿐이며, 장거리 배정을 조사할 때
이 순서로 확인한다: (1) fixed/reschedule distance-protected Job,
(2) OVERLAP area_type, (3) 기사별 max_home_to_job_min 음수 override,
(4) relax_distance_caps_for_feasibility 재시도(1차 solve 전체 실패 시 모든
거리 cap 해제; distance_caps_relaxed와 routing_condition_messages에 기록됨).

## 10. 동일 위치 Job

동일 위치 Job을 hard bundle로 합치지 않고 개별 Job으로 유지한다.

- AddSoftSameVehicleConstraint로 같은 기사 배정을 선호
- 그룹 크기에 따라 split penalty 자동 조정
- fixed와 일반 Job을 구분해 fixed 보호
- capacity·600분 초과 시 split 허용
- CO_LOCATION_EXTRA_SLOTS는 실제 capacity 초과를 만들 수 있으므로 운영 정책과 일치시켜야 함

## 11. Add-on Repair

Primary에서 빠진 일반 Job을 기존 route에 추가하는 보완 단계다.

1. 미배정 일반 Job 추출
2. candidate technician 생성
3. Home↔candidate 및 기존 Job↔candidate OSRM 근접도 계산
4. 근접 후보부터 시도
5. 기존 route에 Job 추가
6. Actual 방식으로 Home→전체 Job→Home 재정렬
7. 600분·Home 접근성·슬롯 검증
8. 통과한 경우 commit

Add-on은 기존 Job을 기사 간 전역적으로 재배치하는 단계가 아니다. 따라서 1슬롯 Job이 여러 기사에게 흩어져 2슬롯 Job을 받지 못하는 slot fragmentation은 primary Solver가 해결해야 한다.

## 12. 결과 계약

각 Job은 정확히 한 번 배정되거나 명시적인 unassigned reason을 가져야 한다.

Assignment 주요 필드:

    receipt_no
    assigned_sm_code
    assigned_sm_name
    assigned_center_type
    service_date_key
    visit_seq
    job_slot_count
    service_time_min
    fixed
    reschedule

Technician Summary:

    job_count
    slot_count
    route_distance_mile
    return_home_distance_mile
    service_time_min
    total_working_min
    total_day_duration_with_return_min

Unassigned reason:

    NO_ELIGIBLE_TECHNICIAN
    FIXED_TECHNICIAN_NOT_AVAILABLE
    NO_FEASIBLE_MANDATORY_ROUTE
    NO_FEASIBLE_ROUTE
    INVALID_LOCATION

## 13. 실패 진단

add-on repair 이후에도 미배정인 Job은 후보별 candidate analysis를 함께 반환한다.
Solver는 최종 route 기준으로 각 기사에 대해 add-on과 동일한 검사를 재수행하고
처음 위반된 제약을 측정값과 함께 기록한다.

candidate_analysis 필드 (unassigned 항목과 diagnostics.unassigned_candidate_analysis):

    technician_code / technician_name
    home_to_job_min
    current_slots / max_slots / remaining_slots
    candidate_travel_km / candidate_travel_min
    candidate_service_min / candidate_total_work_min / work_limit_min
    rejection_reason

rejection_reason 값:

- CAPABILITY_OR_POLICY_MISMATCH
- SLOT_CAPACITY_EXCEEDED
- HOME_TO_FIRST_EXCEEDED (측정값: home_to_first_min / home_to_first_limit_min)
- WORK_LIMIT_EXCEEDED (600분 add-on 한도)
- NOT_EVALUATED_PROXIMITY_CUTOFF (route 시뮬레이션은 근접 후보
  VRP_UNASSIGNED_DIAGNOSIS_MAX_ROUTE_CANDIDATES명까지만 수행)
- INSERTION_FEASIBLE_NOT_COMMITTED (진단 시점에 삽입 가능 — add-on 누락 신호)
- TECHNICIAN_NOT_IN_MODEL

일일 이동시간/거리·단일 구간 cap은 primary solver의 hard dimension으로만
적용되고 add-on 검사에는 포함되지 않으므로, 위 reason 목록에도 아직 없다.
필요 시 추가 세분화 대상이다. UI의 Unassigned Analysis는 이 solver 진단을
우선 사용해야 하며, 자체 추정(SLOT_FULL/UNAVAILABLE/NO_FEASIBLE_ROUTE 3분기)은
solver 진단이 없을 때의 fallback으로만 유지한다.

## 14. 테스트 전략

단위·정적 테스트:

- Python compile
- NaN/bool coercion
- capability candidate set
- fixed unavailable
- Job drop penalty
- objective mode threshold
- 좌표 순서
- OSRM fallback

시나리오 테스트:

1. 모든 Job 배정 가능
2. 요청 슬롯이 capacity보다 적음
3. 요청 슬롯이 capacity보다 많음
4. 2슬롯과 1슬롯 fragmentation
5. 동일 위치 8건 이상
6. fixed+일반 혼합
7. fixed 기사 unavailable
8. DMS/DMS2/overlap
9. 전체 route는 길지만 total work 600분 이내
10. OSRM 오류·fallback

결과 판정 순서:

    unassigned count 최소화
    -> assigned slot 최대화
    -> max total_working_min <= 600
    -> hard distance/time 준수
    -> total duration 최소화
    -> 평균거리·outlier 개선

## 15. 운영·배포 체크리스트

- development/production이 동일 Solver artifact를 읽는지 확인
- 배포 후 SHA-256 확인
- systemd 재시작 후 MainPID와 startup log 확인
- adaptive_objective 확인
- OSRM matrix source/fallback 확인
- total_job_slots, total_technician_slots, assignment count 비교
- unassigned reason과 candidate diagnosis 확인
- request job count와 결과 job count 비교
- 변경 전 commit과 rollback 지점 보존

