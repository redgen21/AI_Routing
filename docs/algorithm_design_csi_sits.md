# 신규 배정 알고리즘 상세 설계서

**작성:** Claude (2026-04-02)  
**개정:** Claude (2026-04-05) — v2: Global Objective 방식으로 insertion scoring 전면 교체  
**대상:** Codex 구현 지시용  
**목표:** OSRM 기반 VRP 수준 성능을 달성하는 두 가지 알고리즘 신규 구현

---

## v2 핵심 변경사항 (2026-04-05)

### 문제 진단

초기 구현(v1) 실행 분석 결과 (`2026-01-12`, 13명, 47건):

```
Work Std Dev: 124.43  (목표: <30%,  VRP 기준: 21.99)
AI103317:  0건 배정  ← 완전 배제
AI102933:  6건, 459분
AI102608:  5건, 455분
```

**근본 원인: insertion cost가 "한 기사의 route 증가량"만 비교함**

```
현재 방식:
  기사 A에 삽입 → A의 delta_work = 50분
  기사 B에 삽입 → B의 delta_work = 60분
  → A 선택 (delta 작음)

보지 않는 것:
  A에 계속 넣을수록 A가 과부하가 되는지
  전체 기사 집합의 편차가 악화되는지
```

travel delta가 작은 기사(지리적으로 가까운 기사)에게 계속 job이 쌓임.  
AI103317처럼 먼 기사는 travel pool(`floor + 25분`) 바깥으로 밀려나 후보 자체에서 탈락.  
**편차 증가는 이 설계의 자연스러운 결과.**

---

### v2 핵심: Global Objective 비교

**판단 기준을 "한 기사의 증가량"에서 "전체 기사 집합의 score 변화량"으로 교체.**

```
job 1건을 기사 A에게 넣었을 때의 전체 score
job 1건을 기사 B에게 넣었을 때의 전체 score
job 1건을 기사 C에게 넣었을 때의 전체 score
→ score가 가장 낮은 기사 채택

전체 score = 총 travel_km + α × max_work + β × work_std
```

#### 구현: 부분 재계산으로 O(1) 추가 비용

바뀌는 건 후보 기사 한 명뿐이므로 전체 재계산 불필요.  
`global_summary`를 상태로 유지하고 매 삽입마다 O(1) 업데이트.

```python
# 1. 초기화 (_solve_day_assignment 시작 시)
global_summary = {
    "total_km":   0.0,
    "max_work":   0.0,
    "work_list":  [0.0] * len(engineer_codes),   # 엔지니어 순서 고정
    "eng_index":  {code: i for i, code in enumerate(engineer_codes)},
}

# 2. 후보 기사별 global score 변화량 계산
import numpy as np

def _global_score_delta(
    summary: dict,
    target_code: str,
    delta_km: float,
    delta_work_min: float,
    alpha: float = 1.5,   # max_work 가중치
    beta:  float = 2.0,   # work_std 가중치
) -> float:
    idx = summary["eng_index"][target_code]
    new_work = summary["work_list"][idx] + delta_work_min

    new_max   = max(summary["max_work"], new_work)
    new_works = list(summary["work_list"])
    new_works[idx] = new_work

    delta_total_km  = delta_km
    delta_max_work  = new_max - summary["max_work"]
    delta_std       = float(np.std(new_works)) - float(np.std(summary["work_list"]))

    return delta_total_km + alpha * delta_max_work + beta * delta_std

# 3. 삽입 확정 후 summary O(1) 갱신
def _update_global_summary(summary, confirmed_code, delta_km, delta_work_min):
    idx = summary["eng_index"][confirmed_code]
    summary["work_list"][idx] += delta_work_min
    summary["total_km"]        += delta_km
    summary["max_work"]         = max(summary["work_list"])
```

#### Travel Pool 제거

v1의 `TRAVEL_COMPETITION_SLACK_MIN = 25.0` 필터를 제거.  
global score가 travel/balance trade-off를 자동으로 처리하므로 별도 pool 불필요.

```python
# v1 (제거):
travel_pool = [item for item if delta_travel <= floor + 25]
best = min(travel_pool, key=lambda x: x["score_min"])

# v2 (교체):
best = min(
    all_candidate_engineers,
    key=lambda item: _global_score_delta(
        global_summary, item["engineer_code"],
        item["delta_travel_km"], item["delta_work_min"]
    )
)
```

#### alpha / beta 튜닝 가이드

| 목표 | alpha | beta | 특징 |
|------|-------|------|------|
| travel 우선 | 0.5 | 0.5 | 거리 최적, 편차 허용 |
| **균형 (권장 초기값)** | **1.5** | **2.0** | travel/balance 균형 |
| balance 우선 | 2.0 | 3.0 | 편차 최소, 거리 다소 증가 |

벤치마크 후 `beta` 값을 1.0 단위로 조정하며 Work Std gap을 확인.

#### 계산 비용 비교

| 방식 | job 1건 삽입 추가 비용 | 총 영향 |
|------|----------------------|---------|
| v1 (개인 delta) | O(1) | 기준 |
| v2 전체 재계산 | O(N) | N배 증가 |
| **v2 부분 재계산** | **O(1)** | **거의 동일** |

OSRM 호출 수 변화 없음.

---

### Phase 2 insertion loop v1 → v2 변경 요약

| 항목 | v1 | v2 |
|------|----|----|
| score 기준 | `delta_work_X + penalty(X)` | `_global_score_delta(all, X, ...)` |
| travel pool 필터 | `floor + 25분` | 제거 |
| balance target | 고정 (`avg_service + flex`) | 불필요 (std가 직접 반영됨) |
| AI103317 배제 문제 | 발생 | 해결 (std 감소 효과로 자동 선택) |

---

---

## 공통 전제

### 제거되는 기존 로직
- `DMS2` / `is_tv_job` 구분 완전 제거
- 모든 job은 동일하게 취급, 모든 active DMS 엔지니어가 배정 후보
- `ENABLE_DMS2`, `DMS2_CENTER_TYPE` 관련 분기 없음

### 재사용하는 기존 유틸 (import해서 사용)
```python
from smart_routing.production_assign_atlanta import (
    _build_route_client,          # OSRM 클라이언트
    _build_engineer_master,       # 엔지니어 마스터 빌드
    _build_actual_attendance_master,  # 출근 엔지니어 필터
    _get_engineer_start_coord,    # 엔지니어 집 좌표
    _build_summary_from_assignment,   # summary DataFrame 생성
    _region_centers,              # 지역 중심 좌표
    MAX_WORK_MIN,                 # 480
    SOFT_REGION_DMS_PENALTY_KM,   # 18.0
    _load_inputs,                 # 원본 데이터 로드
)
from smart_routing.osrm_routing import OSRMTripClient
```

### 배정 가능 조건 (feasibility)
```
1. 해당 날짜 출근한 엔지니어
2. 배정 후 total_work_min < MAX_WORK_MIN (480분)
   total_work_min = sum(service_time_min) + travel_time_min (OSRM 실측)
3. region soft penalty: 엔지니어 담당 region이 아닌 job은
   비용 계산 시 SOFT_REGION_DMS_PENALTY_KM (18km) 가산 (하드 제약 아님)
```

### service_time_min 정의
```
is_heavy_repair == True  → 100분 (데이터에 이미 반영됨)
일반 job                 → 45분
데이터의 service_time_min 컬럼 값을 그대로 사용
```

---

## Algorithm 1: CSI (Cluster-Sequential-Insert)

### 파일명
`smart_routing/production_assign_atlanta_csi.py`

### 개념
> 전체 job을 엔지니어 수만큼 클러스터링 → 클러스터와 엔지니어를 최적 매칭 →
> job을 하나씩 꺼내 매 job마다 전체 엔지니어 중 total_work 증가가 가장 작은
> 엔지니어의 최적 위치에 삽입. 한번 배정하면 변경 없음.

---

### Phase 1: Cluster & Hungarian Match

#### 1-1. K-Means 클러스터링
```
입력: jobs_df (latitude, longitude 컬럼 포함), K = active 엔지니어 수

- sklearn.cluster.KMeans(n_clusters=K, random_state=42)
- 학습 데이터: jobs_df[['latitude', 'longitude']]
- 결과: 각 job에 cluster_id (0 ~ K-1) 부여
- 각 클러스터의 centroid 좌표 저장: centroids[k] = (lon, lat)
```

#### 1-2. Hungarian Matching (클러스터-엔지니어 1:1 매칭)
```
입력: centroids[K], engineers[K] (home_coord)

비용 행렬 C (K × K):
  C[e][k] = OSRM pair_distance(engineer_e.home_coord, centroid_k) → km

scipy.optimize.linear_sum_assignment(C) → (engineer_indices, cluster_indices)
결과: match[engineer_code] = cluster_id

목적: 집이 가까운 엔지니어가 해당 클러스터 담당으로 초기 매칭
     이후 Phase 2에서는 클러스터 구분 없이 전체 경쟁
```

#### 1-3. job 처리 순서 결정
```
각 클러스터 내에서 job을 엔지니어 home으로부터 가까운 순으로 정렬
전체 job queue = 클러스터 0 jobs → 클러스터 1 jobs → ... → 클러스터 K-1 jobs
(클러스터 순서는 Hungarian match된 엔지니어 번호 순)

목적: 집 근처부터 처리하면 초기 delta가 작아져 균형 잡힌 배정 유도
```

---

### Phase 2: Sequential Global Insert

#### 핵심 데이터 구조
```python
# 각 엔지니어의 현재 배정 상태
engineer_state[engineer_code] = {
    "route": [(lon, lat), ...],     # home 포함 순서대로, home은 index 0 고정
    "job_indices": [job_idx, ...],  # route에 대응하는 job DataFrame 인덱스
    "service_time_min": float,      # 누적 service time
    "travel_time_min": float,       # 현재 OSRM travel time (route 전체)
    "total_work_min": float,        # service_time_min + travel_time_min
}

# 초기 상태: route = [home_coord], job_indices = [], 나머지 0
```

#### job 삽입 루프
```
for each job_j in job_queue (Phase 1에서 정렬된 순서):

    best = {cost: ∞, engineer: None, position: None}

    for each engineer_e in active_engineers:

        # overflow 사전 체크: 이 엔지니어에 job을 추가해도 480분 초과 불가능한 경우 skip
        if engineer_state[e].total_work_min + job_j.service_time_min >= MAX_WORK_MIN:
            continue

        route = engineer_state[e].route   # [home, stop_1, stop_2, ...]
        R = len(route)                    # home 포함 route 길이

        # OSRM matrix 1회 호출 (route 좌표 + job_j 좌표)
        coords = route + [(job_j.lon, job_j.lat)]
        dist_matrix[R+1][R+1], dur_matrix[R+1][R+1] = route_client.get_distance_duration_matrix(coords)
        # dist_matrix[i][j] = i번째 좌표 → j번째 좌표 거리 (km)
        # job_j의 matrix index = R

        for position p in range(1, R + 1):
            # p=1: home → job_j → stop_1 → ...
            # p=k: ... → stop_{k-1} → job_j → stop_k → ...
            # p=R: ... → stop_{R-1} → job_j (마지막)

            prev_idx = p - 1   # route에서 job_j 앞에 오는 좌표의 matrix index
            next_idx = p       # route에서 job_j 뒤에 오는 좌표의 matrix index (p=R이면 없음)

            if p < R:
                # 중간 삽입
                delta_travel_km = (
                    dist_matrix[prev_idx][R]      # prev → job_j
                  + dist_matrix[R][next_idx]      # job_j → next
                  - dist_matrix[prev_idx][next_idx]  # 기존 prev → next 제거
                )
                delta_travel_min = (
                    dur_matrix[prev_idx][R]
                  + dur_matrix[R][next_idx]
                  - dur_matrix[prev_idx][next_idx]
                )
            else:
                # 마지막 삽입 (p=R)
                delta_travel_km = dist_matrix[prev_idx][R]
                delta_travel_min = dur_matrix[prev_idx][R]

            # region soft penalty 반영
            job_region = job_j.region_seq
            eng_region = engineer_e.assigned_region_seq
            if job_region != eng_region:
                delta_travel_km += SOFT_REGION_DMS_PENALTY_KM

            # 총 작업시간 증가량 = 이동시간 증가 + service 시간
            delta_work_min = delta_travel_min + job_j.service_time_min

            # overflow 체크
            new_total = engineer_state[e].total_work_min + delta_work_min
            if new_total >= MAX_WORK_MIN:
                continue

            # [v2] 비교 기준: 전체 기사 집합의 global score 변화량
            # (v1의 delta_work_min 단독 비교에서 교체)
            global_delta = _global_score_delta(
                global_summary, e,
                delta_travel_km, delta_work_min,
                alpha=1.5, beta=2.0,
            )
            if global_delta < best.cost:
                best = {
                    cost: global_delta,
                    engineer: e,
                    position: p,
                    delta_travel_km: delta_travel_km,
                    delta_travel_min: delta_travel_min,
                    delta_work_min: delta_work_min,
                }

    # 최적 엔지니어의 최적 위치에 삽입
    e = best.engineer
    p = best.position
    engineer_state[e].route.insert(p, (job_j.lon, job_j.lat))
    engineer_state[e].job_indices.insert(p - 1, job_j.idx)
    engineer_state[e].service_time_min += job_j.service_time_min
    engineer_state[e].travel_time_min  += best.delta_travel_min
    engineer_state[e].total_work_min   += best.delta_work_min

    # [v2] global_summary O(1) 갱신
    _update_global_summary(global_summary, e, best.delta_travel_km, best.delta_work_min)

    # 주의: travel_time_min 누적은 근사값. 최종 summary는 OSRM trip으로 재계산
```

#### 예외 처리
```
- 모든 엔지니어가 overflow로 배정 불가한 job:
  → 가장 total_work_min이 낮은 엔지니어에게 강제 배정 (overflow 허용)
  → overflow_480 = True로 기록
- home_coord가 없는 엔지니어: region_center 사용 (_get_engineer_start_coord 동일 로직)
```

---

### 출력 및 마무리
```
assignment_df: 기존 형식과 동일
  컬럼: GSFS_RECEIPT_NO, assigned_sm_code, assigned_sm_name,
        assigned_center_type, home_start_longitude, home_start_latitude,
        service_date_key, latitude, longitude, service_time_min, ...

summary_df: _build_summary_from_assignment() 호출
  route_client 전달하여 OSRM 실측 travel 재계산 (누적 근사값 보정)
```

---

### 진입점 함수 시그니처
```python
def build_atlanta_production_assignment_csi(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns: (assignment_df, summary_df)
    """
```

---

## Algorithm 2: SITS (Sequential-Insert with Targeted-Swap)

### 파일명
`smart_routing/production_assign_atlanta_sits.py`

### 개념
> CSI와 동일하게 순서대로 삽입하되, 3번째 job부터 매 삽입 후
> 전체 배정에서 travel_contribution이 가장 큰 job (동선을 가장 많이 잡아먹는 job)을
> 찾아 다른 엔지니어로 옮겼을 때 전체 travel이 줄어드는지 확인하고
> 줄어들면 swap 실행. swap은 1회만 시도 후 다음 job으로 진행.

---

### Phase 1: 동일 (CSI Phase 1 그대로)
```
- K-Means 클러스터링
- Hungarian Matching
- job 처리 순서 결정
CSI의 Phase 1 함수를 import해서 재사용
```

---

### Phase 2: Sequential Insert + Targeted Swap

#### Step A: 삽입 (CSI Phase 2와 완전 동일)
```
find_best_insertion(job_j) → (best_engineer, best_position, delta)
route[best_engineer].insert(best_position, job_j)
engineer_state 업데이트
job_counter += 1
```

#### Step B: Targeted Swap (job_counter >= 3일 때만)
```
# --- worst_job 탐색 ---
# 현재 배정된 모든 job에 대해 travel_contribution 계산
# travel_contribution = 이 job이 없을 때 route가 줄어드는 거리

for each engineer_e:
    route_e = engineer_state[e].route   # [home, j1, j2, ..., jR]
    
    # 이 route 전체의 matrix 1회 호출
    dist_matrix = route_client.get_distance_duration_matrix(route_e)
    
    for each job at position p (p=1 to R):
        prev_idx = p - 1
        next_idx = p + 1 if p < R else None
        
        if next_idx is not None:
            # 이 job을 제거하면: prev → next (기존: prev → job → next)
            contribution_km = (
                dist_matrix[prev_idx][p]        # prev → job
              + dist_matrix[p][next_idx]        # job → next
              - dist_matrix[prev_idx][next_idx] # 대체 거리
            )
        else:
            # 마지막 job: prev → job만 제거
            contribution_km = dist_matrix[prev_idx][p]
        
        # contribution에 service_time도 포함 (작업 부하 기여도)
        contribution_total = contribution_km + (job.service_time_min / 60.0 * 50.0)
        # ↑ service_time을 km 단위로 환산 (50km/h 기준)하여 travel과 동일 단위로 비교
        
        record (engineer_e, position_p, job_j, contribution_total)

worst_job = argmax(contribution_total)
worst_eng = worst_job.engineer
worst_pos = worst_job.position
```

```
# --- worst_job을 다른 엔지니어로 이동 시도 ---
current_total_travel = sum(engineer_state[e].travel_time_min for all e)
best_swap = None
best_swap_gain = 0.0   # 양수면 개선

for each other_engineer_e != worst_eng:
    
    # overflow 사전 체크
    if engineer_state[e].total_work_min + worst_job.service_time_min >= MAX_WORK_MIN:
        continue
    
    # worst_job을 e의 route에서 최적 위치 찾기 (CSI 삽입 로직 재사용)
    (best_pos, delta_travel_min, delta_km) = find_best_insertion_for_engineer(worst_job, e)
    
    # worst_job을 worst_eng에서 제거했을 때 travel 감소량 계산
    route_worst = engineer_state[worst_eng].route
    dist_mat_worst = route_client.get_distance_duration_matrix(route_worst)
    
    p = worst_pos
    if p < len(route_worst) - 1:
        removal_gain_min = (
            dur_matrix_worst[p-1][p]
          + dur_matrix_worst[p][p+1]
          - dur_matrix_worst[p-1][p+1]
        )
    else:
        removal_gain_min = dur_matrix_worst[p-1][p]
    
    # 전체 travel 변화: 제거로 줄어드는 것 - 추가로 늘어나는 것
    net_gain_min = removal_gain_min - delta_travel_min
    
    if net_gain_min > best_swap_gain:
        best_swap_gain = net_gain_min
        best_swap = {
            "target_engineer": e,
            "target_position": best_pos,
            "removal_gain_min": removal_gain_min,
            "delta_travel_min": delta_travel_min,
        }

# swap 실행
if best_swap is not None:
    # worst_eng에서 worst_job 제거
    route[worst_eng].pop(worst_pos)
    engineer_state[worst_eng].service_time_min -= worst_job.service_time_min
    engineer_state[worst_eng].travel_time_min  -= best_swap.removal_gain_min
    engineer_state[worst_eng].total_work_min    = (
        engineer_state[worst_eng].service_time_min
      + engineer_state[worst_eng].travel_time_min
    )
    
    # target_engineer에 삽입
    e = best_swap.target_engineer
    route[e].insert(best_swap.target_position, worst_job)
    engineer_state[e].service_time_min += worst_job.service_time_min
    engineer_state[e].travel_time_min  += best_swap.delta_travel_min
    engineer_state[e].total_work_min    = (
        engineer_state[e].service_time_min
      + engineer_state[e].travel_time_min
    )
# swap이 없으면 (best_swap_gain <= 0) 현재 배정 유지
```

---

### 출력 및 마무리
```
CSI와 동일:
- assignment_df 생성
- _build_summary_from_assignment(route_client=route_client) 호출하여 OSRM 실측값으로 보정
```

### 진입점 함수 시그니처
```python
def build_atlanta_production_assignment_sits(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns: (assignment_df, summary_df)
    """
```

---

## 공통 내부 함수 (별도 파일 또는 CSI에 정의 후 SITS에서 import)

```python
def _kmeans_cluster_jobs(
    jobs_df: pd.DataFrame,
    n_clusters: int,
) -> tuple[pd.Series, list[tuple[float, float]]]:
    """
    Returns:
        cluster_labels: Series (같은 index as jobs_df)
        centroids: list of (lon, lat)
    """

def _hungarian_match_engineers_to_clusters(
    engineer_home_coords: list[tuple[float, float]],
    cluster_centroids: list[tuple[float, float]],
    route_client: OSRMTripClient,
) -> dict[int, int]:
    """
    Returns: {engineer_index: cluster_id}
    scipy.optimize.linear_sum_assignment 사용
    비용 행렬: OSRM pair_distance(home, centroid)
    """

def _build_job_queue(
    jobs_df: pd.DataFrame,
    engineer_cluster_match: dict[int, int],  # engineer_index → cluster_id
    cluster_labels: pd.Series,
    engineer_home_coords: list[tuple[float, float]],
) -> list[int]:
    """
    Returns: jobs_df의 index 순서 (처리 순서)
    클러스터별로 묶어 엔지니어 home 가까운 순으로 정렬
    """

def _compute_insertion_delta(
    route_coords: list[tuple[float, float]],  # home 포함
    job_coord: tuple[float, float],
    position: int,
    dist_matrix: list[list[float]],
    dur_matrix: list[list[float]],
) -> tuple[float, float]:
    """
    Returns: (delta_km, delta_min)
    dist_matrix, dur_matrix: route_coords + [job_coord] 로 만든 matrix
    job_coord의 matrix index = len(route_coords)
    """

def _compute_travel_contribution(
    route_coords: list[tuple[float, float]],  # home 포함
    position: int,                             # job의 route 내 위치 (1-based)
    dur_matrix: list[list[float]],
) -> float:
    """
    Returns: 이 job을 제거했을 때 줄어드는 travel_min (contribution)
    """
```

---

## 테스트 및 벤치마크

### 유닛 테스트 파일
`sr_test_csi_sits_unit.py`

테스트 항목:
1. `test_phase1_cluster_count`: 클러스터 수 = 엔지니어 수
2. `test_hungarian_1to1`: Hungarian 결과가 1:1 매칭
3. `test_insertion_delta_correctness`: delta 계산이 실제 route 비용 차이와 일치
4. `test_no_overflow`: 배정 결과에 overflow_480 == True인 엔지니어 없음 (정상 데이터)
5. `test_all_jobs_assigned`: 전체 job이 모두 배정됨 (누락 없음)
6. `test_sits_swap_reduces_travel`: swap 후 total travel이 swap 전보다 작거나 같음

### 벤치마크
`sr_benchmark_csi_sits_vs_vrp.py`

- 날짜 범위: `2026-01-01` ~ `2026-01-12` (허용 범위 내)
- 비교 대상: VRP (OR-Tools baseline)
- 측정 지표: Travel Distance (km), Work Std Dev, Max Work (min), Overflow 480
- 결과 출력: `docs/csi_sits_benchmark_YYYYMMDD.md`

---

## 구현 시 주의사항

1. **OSRM matrix 호출 최소화**
   - 같은 route에 대한 반복 호출 금지
   - Phase 2 루프에서 엔지니어별 matrix는 해당 엔지니어 처리 시 1회만 호출

2. **route에 항상 home 포함**
   - route[0] = home_coord (고정, 삽입 위치는 1번 이상)
   - home_coord 없는 엔지니어: `_get_engineer_start_coord()` 동일 로직 적용

3. **travel_time_min 누적은 근사값**
   - Phase 2 루프 내 delta 누적은 matrix 기반 근사
   - 최종 summary는 반드시 `_build_summary_from_assignment(route_client=route_client)` 호출로 OSRM 실측값으로 덮어씀

4. **날짜별 독립 실행**
   - `service_df.groupby("service_date_key")`로 날짜별로 분리
   - 날짜 간 engineer state 공유 없음

5. **scipy 의존성**
   - `scipy.optimize.linear_sum_assignment` 사용
   - `scipy.cluster.vq.kmeans2` 사용 (sklearn 대신 — Python 3.13 호환)
   - numpy `np.std()` 사용 (global_summary std 계산)

6. **[v2] global_summary 유지**
   - `_solve_day_assignment` 진입 시 초기화
   - 삽입 확정 시마다 `_update_global_summary` 호출
   - `work_list`의 인덱스 순서는 `engineer_codes` 리스트와 동일하게 고정
   - alpha=1.5, beta=2.0 상수로 정의하되 튜닝 가능하도록 함수 파라미터로 노출

7. **[v2] travel pool 제거**
   - `TRAVEL_COMPETITION_SLACK_MIN` 상수 및 관련 필터링 로직 삭제
   - 모든 feasible 엔지니어가 매 job마다 후보로 참여
