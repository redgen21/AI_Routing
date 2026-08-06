# VRP 미해결 문제

작성 기준: 현재 na_general Solver와 7/29~8/6 라우팅 결과 분석

진행 상태 (2026-08-06 코드 반영, QA 리뷰 반영):
- 1번: 부분 해결 — lexicographic drop penalty 도입, fragmentation 회귀 테스트
  (tests/test_vrp_slot_fill_objective.py) 추가. 7/30 실데이터 재실행에서 67/67
  슬롯 전 건 배정 확인.
- 2번: 해결 — Job 수 최소화 → 배정 슬롯 최대화 lexicographic objective 구현
  (VRP_SLOT_DROP_PENALTY_STEP = 8,000,000; route-shape soft cost보다 크고
  center-type 정책 penalty보다 작게 설계).
- 4번: 해결 — 미배정 Job별 candidate analysis 구현 (rejection_reason + 측정값,
  diagnostics.unassigned_candidate_analysis). route 시뮬레이션은 Job당 8명,
  요청당 80회 budget으로 제한. 일일 이동시간/거리·단일 구간 사유는 add-on
  검사 범위 밖이라 미포함.
- 5번: 부분 해결 — 7/30 재실행에서 Jason Patterson route 202(이전 최대 139)
  outlier 확인. 90km 초과 strong penalty를 8,000/km → 25,000/km로 강화하여
  원거리 Job의 한 명 몰아주기 대신 분산을 유도. 단일 구간 km hard cap은
  미도입(미배정 재발 위험, 정책 결정 필요).
  route 크로스 조사 확정(7/30 좌표 검증): Atlanta 실제 cap은 single-leg 70분,
  home-to-job 80분. 사용자 제안 배정(Jason: SENOIA+BARNESVILLE+MACON+
  WARNER ROBINS)은 cap 안에서 실현 가능함이 확인됨(BARNESVILLE→MACON→
  WARNER ROBINS 체인은 실제 실행에서 배정된 적이 있어 실측 통과 증명).
  따라서 크로스(Jason←SHARPSBURG)는 hard cap이 아니라 탐색 미수렴이 원인.
  조치: (1) 드랍 노드 재삽입 연산자 활성화(use_relocate_and_make_active,
  use_extended_swap_active), (2) unassigned rescue(warm-start 계속 탐색),
  (3) 80/90km 거리 shaping 전 기사 적용. NEWBORN은 Jason의 어느 Job과도
  70분 초과라 그의 체인에 삽입 불가 — 동북부 기사들의 슬롯 repacking으로만
  해결 가능(재삽입 연산자가 이 케이스를 직접 지원).
  잔여 확인: relax 재시도 발생 여부(distance_caps_relaxed)를 다음 실행에서 확인.
- 6번: 해결 — route당 80km soft / 90km strong 이동거리 penalty 상시 적용
  (RouteDistanceSoft/Strong dimension, area-type 라우팅 날 포함).
- 11번: 부분 해결 — fixed 기사도 600분 hard cap·이동 cap·귀가 penalty를 일반
  기사와 동일 적용(예외 4곳 제거), 회귀 테스트 추가. fixed job 자체가 600분을
  넘기면 fixed floor + 30분 buffer 허용.
- 3, 7~10, 12번: 미해결 유지.

## 1. 슬롯 fragmentation

전체 기사 capacity에는 여유가 있지만, 1슬롯 Job이 여러 기사에게 분산되어 2슬롯 Job을 받을 수 없는 문제가 있다.

예:
- 7/30: 요청 67슬롯, capacity 70슬롯, 배정 65슬롯
- 7/31: 요청 58슬롯, capacity 60슬롯, 배정 56슬롯

원인은 기존 1슬롯 Job을 다른 기사로 이동해 빈 슬롯을 재구성하는 전역 slot repacking을 Primary Solver가 충분히 보장하지 않기 때문이다.

해결 방향:
1. 미배정 Job 수 최소화
2. 배정 슬롯 최대화
3. 기존 Job을 기사 간 재배치
4. 이동시간·거리 최소화

이 문제는 add-on보다 Primary Solver에서 해결해야 한다.

## 2. Job 개수와 슬롯 수 목적함수 충돌

2슬롯 Job 보존을 강화하면 1슬롯 Job 여러 개가 미배정되어 Job 개수가 줄 수 있다. 반대로 Job 단위 penalty를 사용하면 2슬롯 Job보다 여러 1슬롯 Job이 선택될 수 있다.

현재 optional Job drop penalty는 Job 단위이다. 따라서 2슬롯 Job이 1슬롯 Job보다 무조건 먼저 배정되지는 않는다.

확정해야 할 우선순위:
- Job 개수 최대화
- 배정 슬롯 최대화
- 또는 두 목적의 lexicographic 조합

권장 순서:
1. fixed/reschedule 보호
2. 미배정 Job 수 최소화
3. 배정 슬롯 최대화
4. 600분 및 hard constraint
5. 이동시간·거리
6. 동일 위치 집중
7. 기사별 슬롯 균형

## 3. 동일 위치 extra slot으로 capacity 초과 가능

현재 VRP_CO_LOCATION_EXTRA_SLOTS = 2 이다. 동일 위치 Job은 기본 capacity보다 최대 2슬롯 더 허용될 수 있다.

이로 인해:
- 기사 기본 capacity가 8인데 9슬롯이 배정될 수 있음
- 전체 capacity보다 많은 슬롯이 배정될 수 있음
- unassigned=0과 엄격한 slot hard cap이 동시에 성립하지 않을 수 있음

운영 정책으로 다음 중 하나를 확정해야 한다.
1. extra slot을 0으로 설정
2. 승인된 co-location Job에만 허용
3. overflow를 결과에 별도 표시
4. slot hard cap과 co-location 집중 중 우선순위 결정

## 4. NO_FEASIBLE_ROUTE 원인 세분화 부족

현재 NO_FEASIBLE_ROUTE만으로는 다음 원인을 구분하기 어렵다.

- 600분 total work 초과
- 일일 이동시간 초과
- 일일 이동거리 초과
- Home-to-first 초과
- 단일 구간 초과
- 후보 기사 unavailable
- 후보 기사 slot 부족
- 기존 route 삽입 불가
- capability 불일치

후보별로 다음을 기록해야 한다.

- available
- current_slots
- remaining_slots
- current_total_work_min
- candidate_total_work_min
- current_travel_min/km
- candidate_travel_min/km
- violated_constraints
- best_insertion_position
- rejection_reason

## 5. 단일 구간 거리 제한 부재

현재 단일 구간 제한은 max_single_leg_min이라는 이동시간 기준이다. 직접적인 max_single_leg_km hard cap은 없다.

따라서 Jason Patterson처럼 전체 route는 매우 길지만 total_working_min이 600분 이내인 경로가 선택될 수 있다.

해결 방향:
1. max_single_leg_km hard cap
2. 단일 구간 거리 soft penalty
3. Home-to-first와 Job-to-Job 거리 cap 분리
4. 전체 route outlier penalty

## 6. 평균 이동거리 80km 목표 미연결

평균 이동거리 80km는 결과 KPI로 확인하지만 현재 자동 objective mode에 직접 연결되지 않는다. 기본 Solver는 duration을 주 목적함수로 사용한다.

권장:
- 80km 이하: penalty 없음
- 80~90km: 중간 penalty
- 90km 초과: 강한 penalty
- 단, penalty 때문에 미배정이 늘어나면 배정 우선

## 7. Solver 실행 비결정성

현재 solver_seed가 ortools_default이고 제한시간 내 Guided Local Search를 사용한다. CPU 상태, 첫 해 생성 순서, 실행 시점, OSRM Matrix 미세 차이로 같은 입력도 달라질 수 있다.

해결 방향:
- 명시적인 random seed
- payload/matrix/solver config version 기록
- 동일 OSRM Matrix snapshot
- 반복 실행 안정성 테스트

## 8. 서버·로컬 결과 비교 계약 부족

서버와 로컬 결과가 다를 때 비교할 값이 충분하지 않다.

필수 비교 필드:
- request payload hash
- job_count
- total_job_slots
- total_technician_slots
- technician availability
- capability snapshot version
- time_limit_seconds
- max_work_min_per_sm_day
- max_travel_min_per_sm_day
- max_travel_km_per_sm_day
- max_home_to_job_min
- max_single_leg_min
- respect_fixed_jobs
- relax_distance_caps_for_feasibility
- OSRM URL/profile
- matrix source/fallback
- solver seed
- solver invocation count

추가 권장 진단:
- input_version
- options_hash
- capability_snapshot_hash
- matrix_version
- solver_config_hash

## 9. Fill Rate와 사전 capacity utilization 혼동

자동 policy는 요청 슬롯을 사용한다.

requested_capacity_utilization = requested_slots / all_technician_capacity

Statistics는 결과 배정 슬롯을 사용한다.

result_fill_rate = assigned_slots / assigned_technician_capacity

따라서 미배정 Job이 있으면 두 값은 다르다. 화면과 diagnostics에서 다음 이름을 분리해야 한다.

- requested_capacity_utilization
- assigned_fill_rate
- unassigned_slot_count
- remaining_capacity_after_assignment

## 10. Add-on repair의 전역 재배치 한계

Add-on은 미배정 Job을 기존 기사 route에 추가하고 Actual OSRM 순서를 재계산한다. 그러나 다음은 하지 않는다.

- 기존 Job을 다른 기사로 이동
- 기사 간 Job swap
- slot repacking
- 전체 assignment 재최적화

따라서 slot fragmentation은 Primary Solver가 해결해야 한다. Add-on은 제한적인 보완 단계로 유지해야 한다.

## 11. Fixed와 일반 Job 혼합 route

정책 요구:
- fixed Job은 기존 기사 보호
- fixed 기사가 unavailable이면 대체 후보 허용
- fixed가 있다는 이유로 전체 route 제한을 해제하지 않음
- fixed 자체의 최소 route는 허용
- 일반 Job 추가는 600분·거리·시간 제한 유지

검증 사례:
- fixed-only route
- fixed+일반 혼합
- fixed 기사 unavailable
- fixed capacity 초과
- fixed outside active plan

## 12. Capacity 초과 날짜 정책

8/6처럼 요청 슬롯이 capacity보다 많은 날은 모든 Job을 배정할 수 없다. 그러나 co-location extra slot을 허용하면 capacity보다 많은 슬롯을 배정할 수 있다.

확정해야 할 사항:
- capacity 초과 허용 여부
- co-location만 예외로 할지
- 초과분을 unassigned로 남길지
- overflow를 승인 상태로 표시할지

## 13. 우선 해결 순서

1. Primary Solver에 Job 수 최소화와 slot 최대화의 lexicographic objective 도입
2. slot fragmentation 전역 재배치 테스트
3. co-location extra slot 정책 확정
4. 후보별 NO_FEASIBLE_ROUTE 원인 세분화
5. 단일 구간 km cap 또는 distance outlier penalty
6. Solver seed와 input/matrix version 고정
7. 서버·로컬 diagnostics contract 강화
8. 7/30·7/31·8/6 회귀 테스트 자동화

## 14. 완료 기준

- 모든 Job은 배정되거나 명확한 reason 보유
- fixed 정책 위반 없음
- 일반 기사 total_working_min <= 600
- 승인되지 않은 slot overflow 없음
- unavailable 기사 신규 배정 없음
- capability 위반 없음
- OSRM 좌표 순서·단위 정상
- 평균거리·최대거리 outlier 확인 가능
- fragmentation 사례 회귀 테스트 통과
- capacity 초과 상황 명시적 진단
- 서버와 로컬의 입력·옵션·Matrix·Solver version 비교 가능

