# UPDATED BY CLAUDE

## Scope

- Performance evaluation must stay inside `2026-01-01` through `2026-01-12`.

---

## Work Log

### 2026-04-02 — Codex 업데이트 감지 및 피드백 작성 (전체 날짜 벤치마크)

### 2026-04-01 — Codex 업데이트 감지 및 피드백 작성

### 2026-04-01 (자율 피드백 루프 세팅)

**역할 분담 확정:**
- Codex: 알고리즘 수정 + 테스트 → `docs/UPDATED_BY_CODEX.md`에 기록
- Claude: 알고리즘 검증 + 피드백 → `docs/UPDATED_BY_CLAUDE.md`에 기록

**자율 감지 메커니즘:**
- 5분마다 `UPDATED_BY_CODEX.md` 라인 수 변화 감지 (상태파일: `docs/.claude_review_state`)
- 새 내용 발견 시 자동으로 피드백 작성

**현재 파악한 기준선 (2026-01-12):**
| 지표 | VRP-Level | VRP | 갭 |
|------|-----------|-----|-----|
| Travel Distance | 1109.29 km | 911.95 km | +21.64% |
| Work Std Dev | 24.07 | 13.91 | +72.99% |
| Max Work | 368.80 min | 327.89 min | +12.48% |

**목표:** 위 갭을 모두 10% 미만으로 줄이는 것

---

## Log Format (참고)

```
### YYYY-MM-DD HH:MM — <작업 제목>
- 변경 내용
- 변경 파일
- 결과 / 비고
```

---

## Feedback Log

### 2026-04-01 — 초기 코드 검증: vrp_level 파이프라인 구조 분석

**검토 대상:** `UPDATED_BY_CODEX.md` 2026-04-01 전체 내용 + 실제 코드 직접 분석  
**검토 파일:** `production_assign_atlanta_osrm.py`, `production_assign_atlanta.py`

---

#### [FAIL] 문제 1: `_iterative_improve_assignment_df`의 이중 break — 가장 심각한 버그

**위치:** `production_assign_atlanta.py:937-974`

```python
for _ in range(max(int(iterations), 1)):  # iterations=4 or 5
    changed = False
    for idx, job_row in improved_df.copy().iterrows():   # 전체 job 순회
        ...
        for _, candidate in candidates_df.iterrows():   # 후보 엔지니어 순회
            if trial_objective < baseline_objective:
                changed = True
                break   # ← 후보 루프 탈출 (OK)
        if changed:
            break       # ← job 루프도 즉시 탈출 (PROBLEM)
    if not changed:
        break
```

**영향:**  
- 1회 iteration당 최대 **1개 이동(move)만** 실행됨
- `iterations=4`이면 전체 배정에서 최대 **4번의 이동**만 시도
- 2026-01-12 같은 heavy day(엔지니어 ~20명, job ~100건)에서는 최적화가 거의 안 됨

**권장 수정 방향:**  
`if changed: break`를 제거하고, 한 iteration에서 전체 job을 다 스캔한 뒤 best improvement를 선택하는 "best-improvement" 방식으로 변경.  
또는 `changed = True`일 때 break 없이 계속 스캔해 같은 pass에서 여러 개를 이동.

---

#### [FAIL] 문제 2: Travel pass가 2번 중복 호출됨 — 의미 없는 반복

**위치:** `production_assign_atlanta_osrm.py:1106-1123`

```python
# Travel improvement pass
assignment_df = base._iterative_improve_assignment_df(..., iterations=4, priority_mode="travel_first")

# Travel refinement pass  ← 완전히 동일한 호출
assignment_df = base._iterative_improve_assignment_df(..., iterations=4, priority_mode="travel_first")
```

**영향:**  
문제 1의 이중 break 때문에 첫 번째 pass가 이미 local optimum에 도달한 상태.  
두 번째 pass는 동일한 함수를 동일 모드로 호출하므로 추가 개선이 없거나 매우 적음.  
4+4 = 8번 이동이 아니라 실질적으로 4번 이동 후 종료.

**권장 수정 방향:**  
Travel refinement pass를 **inter-engineer swap pass**로 교체.  
이미 `_two_opt_improve_routes` 함수가 존재하지만 `vrp_level` 파이프라인에서 **한 번도 호출되지 않음**.

---

#### [FAIL] 문제 3: `_two_opt_improve_routes`가 파이프라인에 미연결

**위치:** `production_assign_atlanta_osrm.py:764-876` (함수 존재)  
**위치:** `production_assign_atlanta_osrm.py:1093-1141` (vrp_level 파이프라인 — 미호출)

`_two_opt_improve_routes`는:
- 엔지니어 내 경로 순서 2-opt 최적화 ✓  
- 엔지니어 간 job swap ✓  

작성되어 있지만 `vrp_level`에서 **호출하지 않음**.  
이 함수를 travel refinement 단계에 투입하면 travel gap 21.64%를 크게 줄일 수 있음.

**단, 별도 버그 존재** (문제 4 참조)

---

#### [WARN] 문제 4: `_two_opt_improve_routes`의 cross-swap이 첫 번째 job만 대상

**위치:** `production_assign_atlanta_osrm.py:852-856`

```python
# Job 교환 시도 (각 그룹의 첫 번째 job)
job1_idx = eng1_jobs.index[0]   # ← 항상 첫 번째만
job2_idx = eng2_jobs.index[0]   # ← 항상 첫 번째만
```

**영향:**  
엔지니어 A와 B 사이에서 최적 job을 교환하는 것이 아니라, 우연히 리스트 맨 앞에 있는 job만 교환 시도.  
cross-swap의 효과가 매우 제한적.

**권장 수정 방향:**  
모든 `(job_from_eng1, job_from_eng2)` 조합을 탐색하고 feasibility 검사 후 best swap 선택.

---

#### [WARN] 문제 5: `_simulated_annealing_improve` 미연결 + cost 함수 오류

**위치:** `production_assign_atlanta_osrm.py:879-952`

SA 함수 자체는 구현되어 있으나 vrp_level에서 호출 안 됨.  
내부에서 사용하는 `_calculate_total_assignment_cost`의 travel time 추정이 단순 선형(`distance_km / 50.0 * 60.0`)으로,  
OSRM 실제 travel time과 괴리가 있어 최적화 기준이 benchmark 지표와 다름.

---

#### [WARN] 문제 6: `_savings_algorithm_assign` 사용불가 상태 (dead code)

**위치:** `production_assign_atlanta_osrm.py:623-638`

```python
def _savings_algorithm_assign(...):
    return _build_pair_gain_seed_assignment(...)   # ← 여기서 무조건 반환
    
    # 아래는 모두 dead code (실행 안 됨)
    if service_day_df.empty or engineer_master_df.empty:
        ...
```

`_build_pair_gain_seed_assignment` 함수 자체는 vrp_level 파이프라인에 미연결.  
Savings-based seed가 현재 `_matrix_seed_assign_jobs`보다 더 좋은 초기해를 만들 가능성이 있음.

---

### 검증 결과 요약

| # | 위치 | 심각도 | 내용 |
|---|------|--------|------|
| 1 | `production_assign_atlanta.py:970` | **CRITICAL** | 이중 break → 1 pass당 1 move만 실행 |
| 2 | `osrm.py:1115-1123` | **HIGH** | travel pass 중복 호출, 효과 없음 |
| 3 | `osrm.py:764` | **HIGH** | `_two_opt_improve_routes` 미연결 |
| 4 | `osrm.py:852` | MEDIUM | cross-swap이 첫 번째 job만 대상 |
| 5 | `osrm.py:879` | LOW | SA 미연결, cost 함수 단순 추정 |
| 6 | `osrm.py:639` | LOW | dead code |

---

### Codex에게 요청하는 다음 작업 우선순위

**P1 (즉시 수정):**  
`_iterative_improve_assignment_df`의 `if changed: break` 제거.  
한 pass에서 모든 job을 스캔하고 개선되는 이동을 **모두 적용**하거나, **best-improvement** 방식으로 전환.

**P2 (travel gap 감소 핵심):**  
`vrp_level` 파이프라인의 "Travel refinement pass"를 `_two_opt_improve_routes` 호출로 교체.  
단, cross-swap의 첫-번째-job 버그를 먼저 수정 후 연결.

**P3 (balance gap 감소):**  
balance pass 후 `_local_rebalance_assignment_df` 또는 `_targeted_region_worst_move_rebalance`를 추가 호출.  
이 함수들은 작성되어 있지만 vrp_level에서 사용 안 됨.

**검증 기준:** 2026-01-12  
- Travel: 1109 → 1000 km 이하 (현재 갭 21.64% → 10% 미만 목표)  
- Work Std Dev: 24.07 → 16 이하 (현재 갭 72.99% → 15% 미만 목표)  
- Max Work: 368.80 → 360 min 이하

수정 후 반드시 `2026-01-01~2026-01-12` 전체 날짜 벤치마크 결과를 `UPDATED_BY_CODEX.md`에 기록 바람.

---

### 2026-04-01 — 2차 검토: 성능 목표 달성 확인 및 VRP 기준선 이상 경고

**검토 대상:** `UPDATED_BY_CODEX.md` 라인 45~67 — 최신 벤치마크 결과 및 현재 상태 요약

**검증 결과:**

- [PASS] 테스트: `7/7` 통과 확인
- [PASS] Travel gap: `+0.43%` — 목표(10% 미만) 달성
- [PASS] Work Std Dev gap: `-5.51%` — 목표(10% 미만) 달성 (VRP-Level이 VRP보다 우수)
- [PASS] Max Work gap: `+8.95%` — 목표(10% 미만) 달성
- [PASS] Overflow 480: `0` — 초과 없음
- [WARN] VRP 기준선 변경: 이전 VRP Travel `911.95 km` → 현재 `619.75 km` (32% 감소)
- [WARN] 전체 날짜 벤치마크 미제출: `2026-01-12` 단일 날짜만 확인됨

---

**피드백:**

1. **VRP 기준선 변경 원인 확인 필요 (중요)**

   이전 기록(`codex_vrp_level_current_status_20260401.md`)의 VRP Travel: `911.95 km`  
   현재 benchmark의 VRP Travel: `619.75 km`  
   동일 날짜(`2026-01-12`)임에도 VRP 기준선이 32% 낮아짐.  
   VRP(OR-Tools)는 같은 입력에 대해 결정적(deterministic)이어야 하므로, 이 변화는:
   - 입력 데이터셋 변경 (엔지니어 수, job 수 등)
   - benchmark 스크립트가 다른 파라미터 사용
   - VRP 제약 조건 변경  
   중 하나일 가능성이 높음. **두 비교가 동일한 입력을 사용하는지 반드시 확인** 바람.  
   엔지니어 수는 동일하게 `13명`으로 확인되나, job 수와 제약 조건 동일 여부 검증 필요.

2. **Work Std Dev에서 VRP-Level이 VRP를 능가 (`-5.51%`) — 검증 필요**

   VRP는 work balance를 최적화 목적 중 하나로 포함하고 있음에도 heuristic이 더 낮은 std dev를 달성한 것은 이례적.  
   원인 가능성:
   - VRP의 목적 함수가 travel 위주이고 balance는 약한 제약
   - VRP-Level의 balance pass가 과도하게 balance에 집중하여 travel을 희생  
   현재 travel gap `+0.43%`는 매우 작으므로 실질적 문제는 아니나, 구조적 이유를 이해해야 추후 튜닝이 가능.

3. **전체 날짜 범위 벤치마크 미제출**

   현재 `2026-01-12` 단 하루만 확인됨. 허용 범위 내 전체 날짜:  
   `2026-01-02`, `2026-01-03`, `2026-01-05`, `2026-01-06`, `2026-01-07`, `2026-01-08`, `2026-01-09`, `2026-01-12`  
   일부 날짜(예: light day vs heavy day)에서 성능이 다를 수 있음. **모든 날짜 결과 제출 필요.**

4. **Max Work gap `8.95%`는 허용 범위 내이나 추가 여지 있음**

   VRP Max Work `306.17 min` vs VRP-Level `333.57 min` — 차이 `27.4분`.  
   `_targeted_region_worst_move_rebalance` 함수가 아직 vrp_level 파이프라인에 미연결.  
   이를 balance pass 이후 추가하면 max work gap을 추가로 줄일 수 있을 것.

---

**다음 우선순위:**

1. **(즉시)** 동일 날짜·동일 입력 기준 VRP 기준선 변경 원인 설명  
   — 이전 `911.95 km`와 현재 `619.75 km`의 차이가 정당한지 확인
2. **(단기)** `2026-01-01~2026-01-12` 전체 날짜 벤치마크 실행 및 결과 `UPDATED_BY_CODEX.md`에 기록
3. **(선택)** `_targeted_region_worst_move_rebalance`를 vrp_level에 연결하여 Max Work gap 추가 개선

---

### 2026-04-02 — 3차 검토: 전체 날짜 벤치마크 분석 및 패턴 기반 피드백

**검토 대상:** `UPDATED_BY_CODEX.md` 라인 69~167  
- VRP baseline 변경 원인 확인 (stale artifact 판명)  
- 전체 날짜 `2026-01-02~2026-01-12` 벤치마크 결과  
- Adaptive travel cap 도입 후 range 전체 개선 결과

---

**검증 결과:**

- [PASS] VRP baseline 변경 원인: stale artifact 설명 납득. 현재 `619.75 km` 기준이 맞음
- [PASS] 전체 날짜 벤치마크 제출: 8개 날짜 모두 확인
- [PASS] `2026-01-12` 성능 유지: Travel `+0.43%`, Std `-5.51%`, Max `+8.95%`
- [PASS] Adaptive travel cap 도입으로 range 평균 Work Std `223%→77%`, Max Work `25%→12%` 대폭 개선
- [PASS] Overflow 480: 전 날짜 `0`
- [FAIL] `2026-01-09` Work Std gap `149.82%`, Max Work gap `27.95%` — 10% 목표 미달
- [FAIL] `2026-01-08` Work Std gap `132.10%`, Max Work gap `15.40%` — 10% 목표 미달
- [FAIL] `2026-01-07` Work Std gap `105.86%`, Max Work gap `13.50%` — 10% 목표 미달
- [FAIL] `2026-01-05` Work Std gap `62.40%`, Max Work gap `20.87%` — 10% 목표 미달
- [FAIL] `2026-01-02` Work Std gap `34.68%`, Max Work gap `7.95%` — Std 10% 목표 미달
- [WARN] `2026-01-06` Travel gap `-6.45%`: VRP-Level이 VRP보다 travel이 적음 — 검증 필요

---

**패턴 분석:**

전체 날짜를 분석하면 두 가지 유형으로 나뉩니다:

**Type A — Travel OK, Balance NG (5개 날짜: 01-02, 01-05, 01-06, 01-07, 01-08, 01-09)**

| 날짜 | Travel gap | Work Std gap | Max Work gap | VRP Std | VRP-Level Std |
|------|-----------|-------------|-------------|---------|--------------|
| 01-02 | +8.68% | +34.68% | +7.95% | 12.48 | 16.81 |
| 01-05 | +14.15% | +62.40% | +20.87% | 21.44 | 34.83 |
| 01-06 | -6.45% | +142.41% | +8.09% | 12.62 | 30.60 |
| 01-07 | +16.85% | +105.86% | +13.50% | 18.43 | 37.93 |
| 01-08 | +3.99% | +132.10% | +15.40% | 21.04 | 48.83 |
| 01-09 | -8.51% | +149.82% | +27.95% | 17.52 | 43.76 |

**핵심 패턴:** VRP Std가 낮은 날(`01-06: 12.62`, `01-09: 17.52`)에서 VRP-Level Std가 2~3배 이상 높음.  
이는 VRP-Level이 travel을 줄이는 과정에서 work balance를 깨뜨리고, 이후 balance pass가 충분히 복구하지 못하는 구조적 문제를 나타냄.

**Type B — Travel OK, Balance OK (2개 날짜: 01-03, 01-12)**

| 날짜 | Travel gap | Work Std gap | Max Work gap |
|------|-----------|-------------|-------------|
| 01-03 | 0.00% | 0.00% | 0.00% |
| 01-12 | +0.43% | -5.51% | +8.95% |

`01-12`가 잘 되는 이유: VRP Std `21.99`로 원래부터 높아서 balance pass의 여지가 큼.

---

**피드백:**

**1. [CRITICAL] Balance pass의 이동 강도가 VRP Std가 낮은 날에 부족**

`01-09`에서 VRP는 Std `17.52`인데 VRP-Level은 `43.76`. 차이 `26.24`.  
Adaptive travel cap이 workload 강도 기준(`work std >= 65, 80, 100`)으로 설정됐는데,  
**현재 기준이 VRP-Level의 std를 보는 것인지 VRP의 std를 기준으로 하는 것인지 명확하지 않음**.  
VRP-Level의 자체 std로 판단하면 이미 높은 값이라 cap이 충분히 열리지 않을 수 있음.  

→ **개선 방향**: travel cap 기준을 "현재 VRP-Level std" 대신 "엔지니어 수 대비 총 work 편차 비율"로 바꾸거나, 날짜 초기 std가 임계값을 넘을 때 balance pass를 추가로 실행.

**2. [HIGH] `01-06`, `01-09`에서 VRP-Level travel이 VRP보다 적음 (-6.45%, -8.51%) — balance 과다 이동 의심**

VRP보다 travel이 더 적다는 것은 이론적으로 가능하지만(VRP는 제약이 많아 sub-optimal),  
동시에 Work Std가 훨씬 크다면 **balance pass가 travel을 sacrifice하지 않으면서도 balance를 못 잡았다는 모순**.  
가능한 원인:
- `travel_first` pass에서 이미 travel이 최소화되었고 balance pass에서 이동이 거의 없음
- balance pass의 이동 조건(`should_move` 기준)이 너무 엄격하여 실제로 몇 건만 이동

→ **개선 방향**: `01-09` 날짜 기준으로 balance pass에서 실제 이동 횟수를 로그로 출력해서 얼마나 이동이 일어나는지 진단 필요.

**3. [HIGH] 단일 pass 구조의 한계 — balance pass가 한 번 수렴하면 더 이상 개선 없음**

현재 파이프라인: `travel(4) → travel swap(4) → balance(5)` → 끝  
worst-case 날짜들은 balance pass 5회가 끝난 후에도 std가 40+으로 높음.  
이는 5번 반복으로는 local optimum을 탈출하지 못한다는 것을 의미.

→ **개선 방향**: balance pass 후 `work std > threshold`이면 balance pass를 다시 실행하는 **adaptive loop** 추가. 예를 들어 "std가 VRP std의 1.5배 초과이면 balance pass 추가 실행 (최대 3회 추가)".

**4. [MEDIUM] `_targeted_region_worst_move_rebalance` 여전히 미연결**

이전 피드백에서도 지적했지만, 이 함수는 **worst-engineer 대상 targeted 이동**을 수행하므로  
현재 best-improvement 방식과 다른 탐색 공간을 커버할 수 있음.  
특히 `01-09`처럼 한 엔지니어에게 work가 집중되는 날에 효과적일 것.

→ balance pass 이후에 `_targeted_region_worst_move_rebalance` 1회 추가 호출 권장.

---

**다음 우선순위:**

**P1 (즉시 — worst-case 진단):**  
`2026-01-09` 날짜에서 balance pass의 실제 이동 횟수를 출력하도록 임시 로그 추가.  
이동이 0~3건이면 `should_move` 조건이 너무 엄격한 것.  
이동이 많아도 std가 높으면 objective 설계 문제.

**P2 (단기 — adaptive balance loop):**  
balance pass 후 `work std > VRP std * 1.5` 조건을 만족하면 balance pass를 최대 3회 추가 실행.  
`2026-01-12` 성능에는 영향 없이 worst-case 날짜 개선 가능.

**P3 (단기 — targeted rebalance 연결):**  
`_targeted_region_worst_move_rebalance`를 balance pass 이후에 추가.

**검증 기준 (전체 날짜):**

| 지표 | 현재 average | 목표 average |
|------|------------|------------|
| Travel gap | 3.64% | <8% |
| Work Std gap | 77.72% | <30% |
| Max Work gap | 12.84% | <12% |

worst date 목표:
- `2026-01-09` Work Std gap: `149%` → `50%` 미만
- `2026-01-07` Work Std gap: `105%` → `40%` 미만

---

### 2026-04-02 — 긴급 점검: sr_test_vrp_level_mode.py 결과 이상 및 범위 외 날짜 사용

**검토 대상:** `sr_test_vrp_level_mode.py` 실행 결과 (`2026-01-12`, `2026-01-19`, `2026-01-20`)  
사용자 질문으로 직접 실행 결과를 분석함.

**검증 결과:**

- [FAIL] `2026-01-12` 테스트 결과가 벤치마크와 전혀 다름
  - 벤치마크: travel `622.40 km`, std `20.78`, max `333.57 min`, overflow `0`
  - 테스트: travel `1183.43 km`, std `763.69`, max `2809.53 min`, overflow `2`
  - 원인: 3일치(131건)를 동시 실행 시 AI103317 엔지니어에게 34건이 집중됨
- [FAIL] `2026-01-19`, `2026-01-20`은 허용 검증 범위(`2026-01-01~2026-01-12`) **밖** — 사용 불가
- [FAIL] `TARGET_DATES = ["2026-01-12", "2026-01-19", "2026-01-20"]` — 범위 외 날짜 포함

**피드백:**

1. **`sr_test_vrp_level_mode.py`의 `TARGET_DATES`에서 `2026-01-19`, `2026-01-20` 제거 필요**  
   이 날짜들은 VRP 기준선이 없어 성능 비교 불가. 검증 범위를 벗어난 날짜로 테스트하면 결과가 misleading함.

2. **3일치를 동시에 돌리는 방식 재검토 필요**  
   현재 테스트는 3개 날짜 데이터를 한 번에 `build_atlanta_production_assignment_osrm_from_frames`에 넘김.  
   각 날짜의 attendance와 engineer state가 섞일 경우 특정 날짜에 job이 과도하게 쏠림.  
   → 날짜별 독립 실행 후 결과를 합산하는 방식으로 수정 권장.

3. **`2026-01-12`의 올바른 결과는 벤치마크 기준 (`622.40 km`)임을 재확인**  
   단일 날짜 실행(`sr_benchmark_vrp_level_vs_vrp.py`)이 신뢰할 수 있는 결과.

**다음 우선순위:**

- `sr_test_vrp_level_mode.py`의 `TARGET_DATES`를 허용 범위 내 날짜로 수정
- 단일 날짜 실행 방식이 올바름을 재확인 후 3일 동시 실행 문제 원인 분석

---

### 2026-04-05 — CSI/SITS 초기 구현 검토

**검토 대상:**
- `smart_routing/production_assign_atlanta_csi.py`
- `smart_routing/production_assign_atlanta_sits.py`
- `docs/csi_sits_benchmark_20260112.md`
- `docs/csi_sits_benchmark_20260101_20260112.md`

**검증 결과 요약:**

| 지표 | CSI | SITS | 목표 | 판정 |
|------|-----|------|------|------|
| Travel gap (01-12) | +16.78% | +4.91% | <8% | CSI FAIL / SITS PASS |
| Work Std gap (01-12) | +349.27% | +368.14% | <30% | 양쪽 FAIL |
| Max Work gap (01-12) | +28.42% | +37.12% | <12% | 양쪽 FAIL |
| 단위 테스트 | 6/6 | — | 6/6 | PASS |
| Overflow 480 | 0 | 0 | 0 | PASS |

Work Std 갭이 330~850%로 현재 vrp_level(77%)보다 오히려 훨씬 나쁨.  
Travel은 SITS 01-12에서 PASS 수준이나 균형 지표가 심각하게 무너져 있어 알고리즘 효과가 없는 상태.

---

#### [CRITICAL] 문제 1: balance 페널티 target이 지나치게 높아 사실상 비활성화

**위치:** `production_assign_atlanta_csi.py:293-299` — `_target_total_work_min`

```python
def _target_total_work_min(jobs_df, engineer_count):
    avg_service_min = total_service_min / engineer_count
    flex_min = max(TARGET_FLEX_MIN, min(TARGET_FLEX_MAX, avg_service_min * 0.25))
    return min(MAX_WORK_MIN - 1.0, avg_service_min + flex_min)
    # TARGET_FLEX_MIN = 45, TARGET_FLEX_MAX = 75
```

**실제 계산 예시 (2026-01-12, ~100 jobs, 13명):**
- `avg_service_min = 100 × 50 / 13 ≈ 385 min`
- `flex_min = max(45, min(75, 96)) = 75`
- **`target = min(479, 385 + 75) = 460 min`**

`MAX_WORK_MIN = 480`이므로 target이 479에 가까운 값이 나옴.  
penalty는 `total_work > target`일 때만 발동 → 배정 내내 거의 모든 엔지니어가 target 미달.  
**결과: 배정 전체가 travel 최소화로 동작하여 job이 지리적으로 가까운 엔지니어에게 집중.**

**수정 방향:**  
target을 `avg_service_min` 단독으로 (flex 없이) 사용하거나, 더 나은 방법으로:
```python
# 현재 모든 엔지니어의 평균 total_work를 동적 기준으로 사용
current_avg = sum(s["total_work_min"] for s in states.values()) / len(states)
target = current_avg + 15.0  # 현재 평균 대비 15분 여유만 허용
```

이렇게 하면 배정 초반부터 불균형이 생기는 즉시 penalty가 작동.

---

#### [CRITICAL] 문제 2: `_select_best_insertion`에서 travel_pool이 balance를 무력화

**위치:** `production_assign_atlanta_csi.py:403-414`

```python
travel_floor = ranked_by_travel[0]["delta_travel_min"]
travel_pool = [item for item if delta_travel <= travel_floor + TRAVEL_COMPETITION_SLACK_MIN]
# TRAVEL_COMPETITION_SLACK_MIN = 25.0
```

travel floor + 25분 이내 엔지니어 전체를 pool에 넣은 뒤, score_min으로 최종 선택.  
문제: `TRAVEL_COMPETITION_SLACK_MIN = 25분`은 서비스 시간의 절반(45min 기준 55%)에 달하는 큰 슬랙.  
13명 중 대부분이 pool에 포함되어 balance 페널티 선택 기회가 생기지만, target이 높아 penalty=0인 상태에서는 delta_work_min이 가장 작은 엔지니어(= 이미 travel이 짧게 추가되는 엔지니어)가 계속 선택됨.

문제 1을 수정하면 이 부분도 개선될 수 있으나, `TRAVEL_COMPETITION_SLACK_MIN = 25`는 너무 크므로 `10~15`로 줄이는 것이 안전함.

---

#### [HIGH] 문제 3: SITS — 삽입 중 swap 호출이 비효율적이고 불안정

**위치:** `production_assign_atlanta_csi.py:813-814`

```python
if enable_targeted_swap and assigned_count >= 3:
    _targeted_swap_once(states, ...)   # 매 job 삽입마다 호출
```

100개 job이면 `_targeted_swap_once`가 97회 호출됨.  
`_targeted_swap_once`는 **전체 엔지니어 × route를 스캔**하므로 OSRM 호출 수가 폭발적으로 증가.  
또한 삽입 중간에 swap하면 이후 삽입의 route 상태가 변동되어 queue 계획과 충돌 가능.

**수정 방향:**  
삽입 중 swap 호출 제거. 아래 후처리 루프만 유지:
```python
# 삽입 완료 후
for _ in range(min(20, len(job_df))):
    if not _targeted_swap_once(...):
        break
```

---

#### [HIGH] 문제 4: `_targeted_swap_once`의 worst_job 선정 기준이 balance 개선과 무관

**위치:** `production_assign_atlanta_csi.py:486`

```python
"contribution_score": float(removal_min) + service_time_min,
```

`removal_min + service_time` = 이 job을 없애면 travel이 얼마나 줄고 service도 얼마인가.  
즉 "가장 서비스하기 비싼 job"을 swap 대상으로 삼음.  
**이것은 balance 개선 목표와 무관함.**

balance를 실질적으로 개선하려면:
1. 가장 total_work가 높은 (가장 과부하된) 엔지니어를 찾고
2. 그 엔지니어의 job 중 다른 엔지니어에게 이동 시 total_work 불균형이 가장 줄어드는 job을 선택

```python
# 수정 예시
overloaded_code = max(states, key=lambda c: states[c]["total_work_min"])
source_state = states[overloaded_code]
# source_state의 job 중 이동 가능한 것 탐색
```

---

#### [MEDIUM] 문제 5: SITS travel gap이 CSI보다 더 나쁜 날짜 존재

2026-01-07: CSI +18.04%, SITS +29.35%  
삽입 중 swap이 route 구조를 불필요하게 바꿔 travel이 증가하는 것으로 추정.  
문제 3 수정(삽입 중 swap 제거)으로 개선될 가능성 높음.

---

#### [PASS] 잘된 부분

- `_compute_insertion_delta`, `_compute_removal_delta` 구현 정확함 — 설계 spec 그대로 반영
- `_hungarian_match_engineers_to_clusters` — OSRM matrix 1회 호출로 cost matrix 구성 효율적
- `_optimize_route_order` — reinsertion + 2-opt 조합으로 route 품질 개선, max_iterations=12 적절
- `_finalize_state_metrics` — 최종 OSRM 재계산으로 metric 정확성 보장
- sklearn → scipy.cluster.vq.kmeans2 교체 — Python 3.13 호환성 확보
- 단위 테스트 6/6 통과, overflow 0

---

**다음 작업 우선순위:**

**P1 — balance objective 재설계 (핵심, Work Std 개선 직결)**

`_target_total_work_min` 및 `_objective_delta` 수정:

```python
# _find_best_insertion_for_engineer 내부에서
# 기존: target은 고정값
# 수정: 현재 시점의 동적 평균 기준 사용

def _select_best_insertion(job_row, engineer_df, engineer_lookup, states, route_client):
    total_work = [s["total_work_min"] for s in states.values()]
    current_avg = sum(total_work) / max(len(total_work), 1)
    dynamic_target = current_avg + 10.0   # 현재 평균 + 10분 여유
    # 이 dynamic_target을 _find_best_insertion_for_engineer에 전달
```

또는 더 간단하게: penalty를 target 기반 quadratic이 아닌 **"현재 최대 total_work와의 차이"**를 score에 반영:
```python
score = delta_work_min + alpha × (current_total_work / max_current_total_work)
# alpha = 0.5~1.0
```

**P2 — SITS swap 개선 (삽입 중 호출 제거 + worst_job 기준 교체)**

- `_solve_day_assignment` 내 `if enable_targeted_swap and assigned_count >= 3:` 블록 제거
- `_targeted_swap_once` worst_job 선정을 "가장 과부하된 엔지니어의 가장 이동 가능한 job"으로 변경

**P3 — CSI travel gap 개선**

- `TRAVEL_COMPETITION_SLACK_MIN = 25.0` → `10.0`으로 줄임
- travel이 훨씬 tight한 pool에서 selection → CSI travel 16.78% → 10% 이하로 기대

**검증 기준 (수정 후):**

| 날짜 | Travel 목표 | Work Std 목표 | Max Work 목표 |
|------|------------|--------------|--------------|
| 2026-01-12 | <8% | <30% | <12% |
| 2026-01-02 | <10% | <100% | <20% |
| 2026-01-09 | <10% | <80% | <20% |

수정 후 `python sr_test_csi_sits_unit.py` 6/6 확인 후 전체 날짜 벤치마크 제출 바람.

---

### 2026-04-05 — v2 수정 후 편차 증가 원인 분석

**벤치마크 비교:**

| 버전 | CSI Std gap | SITS Std gap |
|------|------------|-------------|
| v1 (원본) | +349% | +368% |
| v2 (Phase 1 seed 적용 후) | +538% | +494% |

편차가 오히려 악화됨. 원인 2가지 확인.

---

**[CRITICAL] 원인 1: Phase 1 Seed가 전체 job을 모두 배정**

`_solve_day_assignment` Phase 1 코드:
```python
for engineer_idx, cluster_id in engineer_cluster_match.items():
    cluster_job_indices = [job for job in job_queue
                           if cluster_labels[job] == cluster_id]  # 클러스터 전체
    for job_index in cluster_job_indices:
        _insert_job_into_state(...)
        seeded_job_indices.add(job_index)   # ← 전부 seeded

remaining_queue = [...]  # → 항상 비어있음
# Phase 2가 실행되지 않음
```

13 clusters × all jobs = 47/47 jobs가 Phase 1에서 소진됨.  
Phase 2 (balance 조정) 실행 없음 → 배정이 K-Means 지리 클러스터만으로 결정.  
K-Means는 좌표 기반이라 각 클러스터의 service_time 합계가 불균형 → Std 폭증.

**[CRITICAL] 원인 2: Global objective score 미구현**

요청 사항은 `_global_score_delta`로 travel pool 교체였으나,  
현재 코드에 travel pool 필터가 여전히 존재:
```python
# 여전히 남아있음
travel_pool = [item for item if delta_travel <= floor + TRAVEL_COMPETITION_SLACK_MIN]
```
global score 함수 자체가 구현되지 않음.

---

**수정 지시 (최우선):**

**1. Phase 1 Seed 블록 전체 제거**

`_solve_day_assignment`에서 seeded_job_indices 관련 코드 전부 삭제.  
K-Means + Hungarian은 job_queue 순서 결정에만 사용 (현재대로).

**2. `_global_score_delta` + `_update_global_summary` 구현 및 연결**

`_solve_day_assignment` 초기화:
```python
engineer_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
global_summary = {
    "total_km":  0.0,
    "max_work":  0.0,
    "work_list": [0.0] * len(engineer_codes),
    "eng_index": {code: i for i, code in enumerate(engineer_codes)},
}
```

`_select_best_insertion` 내 travel pool 제거, global score로 교체:
```python
import numpy as np

def _global_score_delta(summary, target_code, delta_km, delta_work, alpha=1.5, beta=2.0):
    idx = summary["eng_index"][target_code]
    new_work = summary["work_list"][idx] + delta_work
    new_works = list(summary["work_list"])
    new_works[idx] = new_work
    return (
        delta_km
        + alpha * (max(summary["max_work"], new_work) - summary["max_work"])
        + beta  * (float(np.std(new_works)) - float(np.std(summary["work_list"])))
    )

# _select_best_insertion: travel pool 제거, 모든 엔지니어 후보
best = min(
    all_moves,
    key=lambda item: _global_score_delta(
        global_summary, item[0], item[1]["delta_travel_km"], item[1]["delta_work_min"]
    )
)
```

삽입 확정 후:
```python
def _update_global_summary(summary, code, delta_km, delta_work):
    idx = summary["eng_index"][code]
    summary["work_list"][idx] += delta_work
    summary["total_km"]       += delta_km
    summary["max_work"]        = max(summary["work_list"])
```

**3. VRP 기준선 불일치 확인**

`sr_benchmark_csi_sits_vs_vrp.py`의 VRP Travel이 911.95 km로  
기존 올바른 기준선 619.75 km와 다름.  
`_metrics` 함수의 `work_std` ddof 확인 (ddof=0 → ddof=1로 변경)  
및 VRP 함수 호출 방식이 `sr_benchmark_vrp_level_vs_vrp.py`와 동일한지 확인 필요.

**검증:**
- `python sr_test_csi_sits_unit.py` → 6/6
- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12`
- 기대값: CSI/SITS Work Std gap < 100% (v1 349%에서 개선)

---

### 2026-04-05 — 배정 구조 직접 실행 분석 (AI103317 0건 원인 규명)

**실행 분석 결과 (2026-01-12, 13명, 47건):**

```
Work Std Dev: 124.43  (VRP 기준: 21.99)
AI103317: 0건 배정  ← 가장 큰 이상 신호
AI102933: 6건, 459.84분
AI102608: 5건, 455.08분
```

**원인 1 — Travel Pool이 AI103317을 매 job마다 차단:**

AI103317 집 좌표: lat=33.3634 (job 중심 lat=33.965 대비 가장 멀리 위치)

첫 번째 job 기준 travel delta:
```
AI102087:  39.8분 ← floor
pool 상한 = 39.8 + 25 = 64.8분
─────────────────────────
AI103317:  65.0분 ← 0.2분 차이로 pool 탈락
```

47개 job 거의 전체에서 AI103317이 pool 밖에 위치 → 47건 모두 후보 제외 → 0건.

**원인 2 — Balance penalty가 target(282분) 도달 전까지 비활성:**

```
target_total_work_min = 282.69분
초기 상태: 모든 엔지니어 0분 → penalty = 0
→ 초반 배정은 travel 최소화만 작동
→ 가까운 엔지니어(AI102448, AI102087)가 먼저 누적
→ 282분 초과 후 penalty 발동되지만 AI103317은 이미 pool 밖
```

**K-Means/Hungarian은 정상:** AI103317 → cluster 0(3건)으로 올바르게 매칭됨.  
문제는 sequential insertion 단계에서 cluster 배정이 무력화된다는 점.

**[설계 재검토 — 2026-04-05] insertion cost 방식의 근본적 한계 확인**

현재 `_objective_delta`는 엔지니어 X 하나의 변화만 봄:
```python
score = delta_work_X + (penalty(new_X) - penalty(old_X))
```
→ X에 넣을 때 X만 나빠지는지 계산. 전체 집합 균형은 안 봄.
→ delta_work가 작은 엔지니어(지리적으로 가까운 엔지니어)에게 계속 job이 쌓임.
→ 편차 증가는 이 설계의 자연스러운 결과.

**P1 — 핵심 수정: global objective 비교로 전환**

`_find_best_insertion_for_engineer` / `_select_best_insertion`의 score 계산을 전체 집합 관점으로 교체:

구현 방식: **부분 재계산 (O(1) 추가 비용)**

바뀌는 건 후보 기사 한 명뿐이므로 전체 재계산 불필요.
`global_summary`를 상태로 유지하고 매 삽입마다 O(1) 업데이트.

```python
# 1. _solve_day_assignment 초기화 시 global_summary 구성
global_summary = {
    "total_km":    0.0,
    "max_work":    0.0,
    "work_list":   [0.0] * len(engineer_codes),  # 엔지니어 순서 고정
    "eng_index":   {code: i for i, code in enumerate(engineer_codes)},
}

# 2. score 계산 — 후보 기사 한 명 변경 시 전체 score 변화량
def _global_score_delta(
    summary: dict,
    target_code: str,
    delta_km: float,
    delta_work: float,
    alpha: float = 1.5,   # max_work 가중치 (overflow 방지)
    beta:  float = 2.0,   # std 가중치 (편차 제어)
) -> float:
    idx = summary["eng_index"][target_code]
    new_work = summary["work_list"][idx] + delta_work

    new_max  = max(summary["max_work"], new_work)
    # std: 후보 한 명만 교체
    new_works = list(summary["work_list"])
    new_works[idx] = new_work
    new_std = float(np.std(new_works))
    old_std = float(np.std(summary["work_list"]))

    return (
        delta_km
        + alpha * (new_max - summary["max_work"])
        + beta  * (new_std - old_std)
    )

# 3. _select_best_insertion — travel pool 제거, global score만으로 선택
best = min(
    all_engineers,
    key=lambda item: _global_score_delta(
        global_summary, item[0],
        item[1]["delta_travel_km"], item[1]["delta_work_min"]
    )
)

# 4. 삽입 확정 후 summary O(1) 갱신
def _update_global_summary(summary, confirmed_code, delta_km, delta_work):
    idx = summary["eng_index"][confirmed_code]
    summary["work_list"][idx] += delta_work
    summary["total_km"]       += delta_km
    summary["max_work"]        = max(summary["work_list"])
```

alpha/beta 초기값: `alpha=1.5, beta=2.0` — 벤치마크 후 튜닝.

**계산 복잡도:**
- 현재 (개인 delta): O(N × M × R)
- global score 부분 재계산: O(N × M × R) + O(N) — 거의 동일, OSRM 호출 수 변화 없음

**기대 효과:**
- AI103317처럼 std 감소 효과가 크면 travel이 다소 멀어도 자동 선택
- travel pool 필터 제거 → AI103317 0건 문제 해결
- Work Std 124 → 40 이하 기대

**수정 지시 (P1 — 핵심): 2단계 배정 구조로 재설계 (선택적 병행 적용)**

현재 구조의 근본 문제: cluster 배정이 "큐 순서"만 결정하고 실제 배정을 보장하지 않음.  
→ cluster 0의 남부 3건이 queue에 올라오면 다른 엔지니어가 travel pool에서 뺏어감.

수정 방향 — Phase 1 (Seed) + Phase 2 (Fill) 분리:

```python
def _solve_day_assignment(...):
    # Phase 1: Cluster Seed — 매칭된 cluster job을 해당 엔지니어에게 강제 배정
    #   travel pool 없이, Hungarian 결과 그대로 seed 삽입
    #   (cluster job은 지리적으로 가까우므로 travel 증가 없음)
    seeded_job_indices = set()
    for eng_idx, cluster_id in engineer_cluster_match.items():
        engineer_code = engineer_codes[eng_idx]
        cluster_job_idxs = [i for i in job_df.index if cluster_labels[i] == cluster_id]
        for job_index in cluster_job_idxs:
            job_row = job_df.loc[job_index]
            move = _find_best_insertion_for_engineer(
                job_row, engineer_lookup[engineer_code], states[engineer_code],
                route_client, enforce_max_work=True, target_total_work_min=target_total_work_min
            )
            if move:
                _insert_job_into_state(states[engineer_code], job_index, job_row, ...)
                seeded_job_indices.add(job_index)

    # Phase 2: Fill — 남은 job을 전체 경쟁으로 배정 (travel pool 유지)
    remaining_queue = [i for i in job_queue if i not in seeded_job_indices]
    for job_index in remaining_queue:
        best_choice = _select_best_insertion(...)  # 기존 로직 그대로
        if best_choice:
            _insert_job_into_state(...)
```

**기대 효과:**
- AI103317은 cluster 0의 남부 3건을 seed로 확보 → travel 증가 없음
- Phase 2에서 balance를 보며 추가 job 가능
- Work Std 124 → 50 이하 기대, travel 현상 유지

**수정 지시 (P2 — 즉시):**

`_find_best_insertion_for_engineer`에 dynamic target 적용:

```python
# _select_best_insertion 내부에서 states의 현재 평균으로 target 재계산
current_avg = sum(s["total_work_min"] for s in states.values()) / max(len(states), 1)
dynamic_target = current_avg + 20.0
# 이 값을 _find_best_insertion_for_engineer에 전달
```

**기대 효과:**
- 초반부터 balance penalty가 작동해 편중 방지
- Phase 2 fill 단계에서 balance가 travel과 실질적으로 경쟁 가능

---

### 2026-04-02 — 알고리즘 재설계: CSI / SITS 상세 설계 완료 → Codex 구현 지시

**배경:**  
현재 vrp_level 파이프라인은 구조적으로 worst-case 날짜(01-07, 01-08, 01-09)에서 Work Std gap이 100~150%에 달하며, balance pass 개선만으로는 한계가 있음.  
근본 원인: 초기 seed 배정이 travel 최적화만 고려하고 balance를 무시하기 때문.  
→ 초기 배정 단계부터 balance를 함께 고려하는 **새로운 알고리즘 2개**를 설계 완료.

**신규 알고리즘 설계 문서:**  
`docs/algorithm_design_csi_sits.md` — Codex는 이 문서를 반드시 전체 숙독 후 구현할 것.

---

**Algorithm 1: CSI (Cluster-Sequential-Insert)**

핵심 아이디어:
1. K-Means로 job을 N개 클러스터로 분류 (N = 출근 엔지니어 수)
2. Hungarian Algorithm으로 클러스터-엔지니어 1:1 최적 매칭 (home coord 기준 거리 비용)
3. 각 엔지니어 담당 클러스터에서 job 순서 큐 구성 (홈에서 가까운 순)
4. **Sequential Insert**: 모든 job을 하나씩 꺼내어 "total_work 증가 최소 엔지니어의 최적 삽입 위치"에 배정
   - feasibility: max_work ≤ 480 + region soft penalty
   - OSRM matrix 1회 호출로 delta 계산
5. 미배정 잔여 job은 feasibility 완화 후 재시도

출력: `(assignment_df, summary_df)` — vrp_level과 동일 스키마

**Algorithm 2: SITS (Sequential-Insert with Targeted-Swap)**

CSI Phase 1~2 동일 수행 후, Phase 3 추가:

Phase 3 — Targeted Swap:
- job 3번째부터 순서대로, 현재 배정된 엔지니어에서 `travel_contribution` 가장 높은 job을 `worst_job`으로 선정
- `travel_contribution = d[prev→job] + d[job→next] - d[prev→next]`
- worst_job을 다른 엔지니어의 최적 삽입 위치로 이동 시도
- 이동 조건: `(현재 엔지니어 total_work - work(worst_job)) + (대상 엔지니어 total_work + work(worst_job)) < 현재 두 엔지니어 total_work 합`
- feasibility 통과 시에만 swap 실행

출력: 동일

---

**구현 요구사항:**

파일 구조:

| 파일 | 내용 |
|------|------|
| `smart_routing/production_assign_atlanta_csi.py` | Algorithm 1 (CSI) |
| `smart_routing/production_assign_atlanta_sits.py` | Algorithm 2 (SITS) |
| `sr_test_csi_sits_unit.py` | 단위 테스트 (6개) |
| `sr_benchmark_csi_sits_vs_vrp.py` | VRP baseline 비교 벤치마크 |

Entry function signatures (변경 불가):

```python
# production_assign_atlanta_csi.py
def build_atlanta_production_assignment_csi(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:   # (assignment_df, summary_df)

# production_assign_atlanta_sits.py
def build_atlanta_production_assignment_sits(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:   # (assignment_df, summary_df)
```

의존성:
- `from smart_routing.osrm_routing import OSRMTripClient`
- `from smart_routing.production_assign_atlanta import (MAX_WORK_MIN, SOFT_REGION_DMS_PENALTY_KM, _build_route_client, _build_engineer_master, _build_actual_attendance_master, _get_engineer_start_coord, _build_summary_from_assignment, _region_centers, _load_inputs)`
- `from sklearn.cluster import KMeans`
- `from scipy.optimize import linear_sum_assignment`

핵심 제약 (반드시 준수):
- DMS2/TV 구분 완전 제거 — 모든 job은 DMS 엔지니어에게만 배정
- `MAX_WORK_MIN = 480` 하드 상한 (region soft penalty 제외)
- OSRM matrix 호출은 `get_distance_duration_matrix(route + [new_job])` 방식으로 최소화
- engineer route의 `route[0]`은 항상 home 좌표 (고정)
- 날짜별 독립 실행 — 복수 날짜를 동시에 처리하지 말 것

단위 테스트 (6개):
1. 모든 job이 배정되는지 확인 (unassigned = 0)
2. MAX_WORK_MIN 초과 엔지니어 없는지 확인
3. Duplicate job 배정 없는지 확인
4. assignment_df schema 검증 (필수 컬럼 존재)
5. summary_df 집계 정합성 (total_work_min 합산 일치)
6. SITS에서 swap이 실제로 일어나는지 확인 (swap_count > 0)

벤치마크 (`sr_benchmark_csi_sits_vs_vrp.py`):
- `--date YYYY-MM-DD` 또는 `--date-from / --date-to` 지원
- `--write PATH` 옵션으로 markdown 저장
- 출력 형식: 기존 `sr_benchmark_vrp_level_vs_vrp.py`와 동일 스키마

검증 순서 (Codex가 따를 것):

1. `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py`
2. `python sr_test_csi_sits_unit.py` → 6/6 통과 확인
3. `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
4. `python sr_benchmark_csi_sits_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/csi_sits_benchmark_20260101_20260112.md`
5. 결과를 `docs/UPDATED_BY_CODEX.md`에 기록

성능 목표:

| 지표 | 현재 vrp_level 평균 | 목표 |
|------|-------------------|------|
| Travel gap | 3.64% | < 8% |
| Work Std gap | 77.72% | < 30% |
| Max Work gap | 12.84% | < 12% |

worst date (`2026-01-09`): Work Std gap `149%` → `50%` 미만, Max Work gap `27.95%` → `15%` 미만

추가 지시사항:
- 상세 설계 문서(`docs/algorithm_design_csi_sits.md`)의 내부 함수 시그니처, pseudocode, insertion delta 계산 공식을 정확히 따를 것
- SITS의 targeted swap 조건: 조건을 만족하지 않으면 swap 하지 않음 (feasibility 위반 swap 금지)
- KMeans seed: `random_state=42`로 고정 (재현 가능성)
- Hungarian matching: `scipy.optimize.linear_sum_assignment(cost_matrix)` 사용
- 구현 완료 후 반드시 `docs/UPDATED_BY_CODEX.md`에 결과 기록

---

## 2026-04-02 22:20:59 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:03:02 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:03:06 — Coordinator Round 2 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:03:09 — Coordinator Round 3 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:05:04 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:07:16 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-02 23:10:07 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-05 12:53:50 — Coordinator Round 1 검토

[ERROR] Claude API call failed: BadRequestError: Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.'}, 'request_id': 'req_011CZjwvM9BuFDevUTqCSUdF'}

## 2026-04-05 13:04:39 — Coordinator Round 1 검토

[DRY-RUN] 피드백 생략

## 2026-04-05 13:17:15 — Coordinator Round 1 검토

[ERROR] Claude API call failed: BadRequestError: Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.'}, 'request_id': 'req_011CZjyhvZUj3EYMWrpZGu1R'}

## 2026-04-05 13:20:46 — Coordinator Round 2 검토

[ERROR] Claude API call failed: BadRequestError: Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.'}, 'request_id': 'req_011CZjyyXWbDWAXf6KFwwJCC'}

## 2026-04-05 13:24:17 — Coordinator Round 3 검토

---

## 2026-04-05 — CSI/SITS 편차 악화 원인 진단 및 수정 지시 (v3)

**검토 결과:** 벤치마크 편차 +258% (VRP 대비) — 원인 4가지 확인, 모두 수정 필요

---

### [FAIL] 문제 1: Phase 1 Seed가 여전히 실행 중 — 가장 심각

**위치:** `production_assign_atlanta_csi.py:896-928`  
**상수:** `PHASE1_MAX_SEED_JOBS = 2` (라인 27)

```python
for engineer_idx, cluster_id in sorted(engineer_cluster_match.items(), ...):
    seeded_count = 0
    for seed_job_index in cluster_job_indices:
        if seeded_count >= PHASE1_MAX_SEED_JOBS:
            break
        # _find_best_insertion_for_engineer 호출 → per-engineer _objective_delta 사용
        _insert_job_into_state(...)
        seeded_count += 1
```

**영향:**  
- 13명 × 2 seed = 최대 26개 job(47개 중 55%)이 global score 없이 배정됨  
- Phase 1 seed는 per-engineer `_objective_delta`를 사용 — balance를 무시하고 지리적 근접성 위주  
- Phase 2에는 21개만 남아 global score가 전체 배정의 45%에만 적용됨

**수정 방법:**  
Phase 1 Seed 블록 **전체 삭제**. `seeded_job_indices` set과 `PHASE1_MAX_SEED_JOBS` 상수도 제거.  
`remaining_queue`가 아닌 `job_queue` 전체를 Phase 2 global score 루프에 넘기도록 변경:

```python
# 삭제할 블록 (라인 896~928)
seeded_job_indices: set[int] = set()
for engineer_idx, cluster_id in ...:
    ...  ← 이 블록 전체 제거

# 변경 전
remaining_queue = [int(job_index) for job_index in job_queue if int(job_index) not in seeded_job_indices]
for job_index in remaining_queue:

# 변경 후
for job_index in job_queue:
```

---

### [FAIL] 문제 2: `_global_score_delta`에서 `overload_delta` 중복 계산

**위치:** `production_assign_atlanta_csi.py:472-481`

```python
overload_delta = _load_balance_penalty(new_work, target_total_work_min) - _load_balance_penalty(
    old_work, target_total_work_min
)
return (
    float(delta_travel_min)
    + GLOBAL_MAX_WORK_WEIGHT * (new_max - old_max)
    + GLOBAL_STD_WORK_WEIGHT * std_delta
    + overload_delta          # ← 이것이 문제
    + float(region_penalty_min)
)
```

**영향:**  
- `GLOBAL_STD_WORK_WEIGHT * std_delta`와 `overload_delta`가 동일한 balance를 두 가지 방식으로 페널티함  
- `overload_delta`는 `target` 초과분에 이차함수 페널티 → target 미만 엔지니어에게는 0 반환  
- 결과: target 이하 엔지니어끼리는 std 신호만 작동하지만 cap(-12)으로 신호 약화  
- 두 신호가 충돌하여 balance 최적화 방향이 불일치

**수정 방법:**  
`overload_delta` 계산 및 반환값에서 **제거**:

```python
def _global_score_delta(...) -> float:
    # ... (same computation)
    # overload_delta 계산 라인 삭제
    return (
        float(delta_travel_min)
        + GLOBAL_MAX_WORK_WEIGHT * (new_max - old_max)
        + GLOBAL_STD_WORK_WEIGHT * std_delta
        # overload_delta 제거
        + float(region_penalty_min)
    )
```

같은 방식으로 `_global_swap_score_delta`(라인 669-681)에서도 `overload_delta` 제거.

---

### [FAIL] 문제 3: `std_delta` cap이 너무 작아 balance 신호 약화

**위치:** `production_assign_atlanta_csi.py:469`

```python
std_delta = max(-GLOBAL_STD_REWARD_CAP_MIN, min(GLOBAL_STD_PENALTY_CAP_MIN, new_std - old_std))
# GLOBAL_STD_REWARD_CAP_MIN = 12.0  ← 너무 작음
# GLOBAL_STD_PENALTY_CAP_MIN = 30.0
```

**영향:**  
- 현재 std = ~50 min, 좋은 삽입 시 std가 25 min으로 감소 → 실제 개선 25 min
- cap으로 인해 reward = -12 min만 인정 (실제의 48%만 반영)
- travel 증가(예: 15 min)가 balance 보상(-12 min)보다 커서 bad balance move가 선택됨

**수정 방법:**  
cap을 완전히 제거하거나 크게 확대:

```python
# 변경 전
GLOBAL_STD_REWARD_CAP_MIN = 12.0
GLOBAL_STD_PENALTY_CAP_MIN = 30.0

# 변경 후 — cap 제거 (또는 큰 값으로)
# std_delta cap 라인을 다음으로 변경:
std_delta = new_std - old_std
```

그리고 weight도 올림:
```python
# 변경 전
GLOBAL_STD_WORK_WEIGHT = 2.0

# 변경 후
GLOBAL_STD_WORK_WEIGHT = 3.0
```

`_global_swap_score_delta`(라인 661)의 동일한 cap 라인도 같이 수정.

---

### [FAIL] 문제 4: SITS swap이 전체 엔지니어 중 최대 contribution job을 선택 — 잘못된 로직

**위치:** `production_assign_atlanta_csi.py:527-553`

```python
# 현재: ALL 엔지니어 중 contribution_score 최대 job 선택
for engineer_code, state in states.items():
    for job_position, job_index in enumerate(state["job_indices"], start=1):
        if worst_job is None or candidate["contribution_score"] > worst_job["contribution_score"]:
            worst_job = candidate
```

**영향:**  
- 가장 바쁜 엔지니어(max work)의 job이 아니라, 어느 엔지니어든 가장 큰 job을 선택  
- 예: 엔지니어 A(300 min)에 100 min job, 엔지니어 B(200 min)에 110 min job  
  → B의 110 min job을 선택하지만 B는 이미 여유 있음 → swap이 balance에 역효과

**수정 방법:**  
먼저 work_list 기준 최대 엔지니어를 찾고, 그 엔지니어의 가장 큰 contribution job만 선택:

```python
# 가장 바쁜 엔지니어 찾기
max_work_code = max(states.keys(), key=lambda code: states[code]["total_work_min"])
# 그 엔지니어의 job 중 contribution_score 최대 선택
worst_job = None
state = states[max_work_code]
route_coords = list(state["route_coords"])
dist_matrix, dur_matrix = route_client.get_distance_duration_matrix(route_coords)
for job_position, job_index in enumerate(state["job_indices"], start=1):
    removal_km, removal_min = _compute_removal_delta(route_coords, job_position, dist_matrix, dur_matrix)
    service_time_min = float(jobs_df.loc[int(job_index)].get("service_time_min", 45.0))
    candidate = {
        "engineer_code": max_work_code,
        "job_index": int(job_index),
        "job_position": int(job_position),
        "removal_km": float(removal_km),
        "removal_min": float(removal_min),
        "contribution_score": float(removal_min) + service_time_min,
        "service_time_min": float(service_time_min),
    }
    if worst_job is None or candidate["contribution_score"] > worst_job["contribution_score"]:
        worst_job = candidate
```

---

### 수정 우선순위

| 순위 | 문제 | 예상 효과 |
|------|------|-----------|
| 1 | Phase 1 Seed 완전 제거 | std 개선 가장 큼 — 전체 job이 global score 통과 |
| 2 | overload_delta 제거 | 이중 페널티 해소, 신호 명확화 |
| 3 | std_delta cap 제거 + weight 3.0 | balance 신호 강화 |
| 4 | SITS swap 로직 수정 | swap 효과 실현 (CSI≠SITS) |

---

### 수정 후 예상 결과

- Phase 1 Seed 제거 → 모든 job이 global score 통과 → std 대폭 개선 예상
- cap 제거 + weight 상향 → travel 소폭 증가 가능하지만 std/max 개선 우선
- 목표: `Work Std Dev gap < 50%` (현재 258% → 목표 10%)
- 수정 후 반드시 `sr_benchmark_csi_sits_vs_vrp.py`로 `2026-01-12` 단일 날짜 먼저 검증

---

### 추가: VRP baseline 이상 확인 필요

벤치마크 기준 VRP Travel = 911.95 km는 과다 추정으로 의심됨 (실제 최적값 ~619 km).  
`sr_benchmark_csi_sits_vs_vrp.py`에서 `_metrics` 함수의 `std()` ddof 확인 필요:
- `ddof=0` (population std) vs `ddof=1` (sample std) 혼용 여부  
- VRP 기준선이 올바르지 않으면 gap % 자체가 의미 없음

---

## 2026-04-05 — v3 수정 결과 재진단 및 v4 수정 지시

**이번 결과:** Phase 1 Seed 완전 제거 후 travel +73.57%로 폭발, std는 오히려 소폭 악화

---

### 근본 원인 재분석

**이력 비교:**

| 이터레이션 | Phase 1 Seed | Travel gap | Std gap |
|---|---|---|---|
| 13:04 | 전체 cluster 배정 | **-1.84%** | +538% |
| 14:08 | 2개 seed | +19.75% | +258% |
| 14:36 (최신) | 완전 제거 | **+73.57%** | +283% |

→ **K-Means + Hungarian의 지리적 배정이 travel 효율의 핵심**이었다.  
Phase 1 Seed가 없으면 global score만으로는 지리적 효율을 재현할 수 없다.

**왜 travel이 폭발하는가:**  
`GLOBAL_STD_WORK_WEIGHT = 3.0`이면 15 min 표준편차 개선 = -45 min 점수 보상.  
반면 원거리 엔지니어 투입의 travel 증가는 +30 min 수준.  
→ 점수상 원거리 엔지니어 배정이 유리 → 지리 배정 완전 파괴

---

### [FAIL] v3 수정의 핵심 실수: Phase 1 Seed 완전 제거

Phase 1 Seed는 두 가지 역할을 한다:
1. **지리적 앵커링**: 각 엔지니어가 자기 cluster 근처에서 시작하도록 보장
2. **route shape 초기화**: 첫 job 없이는 global score가 모든 job을 "가장 여유 있는" 엔지니어에게 몰아넣음

Seed 없이 47개 job 전체를 global score만으로 배정하면:
- Job 1: 13명 모두 work=0 → 1번 엔지니어가 선택됨
- Job 2: 1번 엔지니어 work > 0 → std_delta 기준으로 2번이 유리
- Job 3~13: 각각 다음 엔지니어에게 1개씩 배정
- Job 14~: 이제부터 global score가 std를 기준으로 배정 → 지리 무시

→ 처음 13개가 균등 분포되어 std는 개선되지만, job들이 엔지니어 근처에 없음 → 이후 모든 배정이 원거리

---

### v4 수정 지시

#### 수정 1: Phase 1 Seed를 1개로 복원

```python
# 변경 전 (v3에서 완전 제거됨)
# for job_index in job_queue: (Phase 1 없음)

# 변경 후
PHASE1_MAX_SEED_JOBS = 1  # 엔지니어당 1개만 seed
```

라인 27: `PHASE1_MAX_SEED_JOBS = 1`  
Phase 1 seed 루프를 복원하되, **반드시 1개만** seed.  
13명 × 1 = 13개 job이 지리적으로 앵커링, 나머지 34개는 global score.

---

#### 수정 2: GLOBAL_STD_WORK_WEIGHT를 3.0 → 1.5로 낮춤

```python
# 변경 전 (v3에서 올림)
GLOBAL_STD_WORK_WEIGHT = 3.0

# 변경 후
GLOBAL_STD_WORK_WEIGHT = 1.5
```

**이유:**  
weight=3.0이면 std 15 min 개선 = travel 45 min과 동등 → 원거리 배정이 너무 유리.  
weight=1.5이면 std 20 min 개선 = travel 30 min과 동등 → 적절한 trade-off.  
목표: cross-cluster 배정은 balance 이득이 travel 손실보다 클 때만 발생.

---

#### 수정 3 (유지): overload_delta 제거 유지 ✓

이건 올바르게 제거됨. 그대로 유지.

---

#### 수정 4 (유지): std_delta cap 제거 유지 ✓

cap 제거는 올바름. weight만 조정하면 됨.

---

#### 수정 5 (유지): SITS worst-job 최대 work 엔지니어 기준 유지 ✓

SITS가 이제 CSI보다 약간 좋아짐 (+234% vs +283%). 유지.

---

### 예상 결과

| 수정 | 예상 효과 |
|------|-----------|
| Seed 1개 복원 | travel 안정화 (+20% 수준으로 복귀) |
| weight 1.5 | std 신호 유지하되 travel 폭발 방지 |
| 종합 | travel +15~25%, std +150~200% 예상 |

**단계별 목표:**
1. 이번 수정으로 travel +25% 미만 복귀 확인
2. std gap을 150% 미만으로 낮추면 다음 단계에서 weight를 소폭 올려 추가 balance 확보
3. travel과 std를 동시에 10% 미만으로 맞추는 건 여러 번 튜닝이 필요함

---

## 2026-04-05 — CSI/SITS OR-Tools 기반 재작성 설계 (v5 — 최종 방향 전환)

**배경:** 지금까지 greedy sequential insertion을 계속 튜닝했지만 VRP 대비 travel +20~70%, std +200~280% 수준에서 벗어나지 못함.  
VRP가 우수한 이유를 분석한 결과 핵심은 OR-Tools의 `GUIDED_LOCAL_SEARCH` — 20초 동안 수만 번의 이동 조합을 탐색하는 것임.  
greedy 알고리즘으로는 이 탐색량을 따라잡는 것이 근본적으로 불가능 → **OR-Tools 기반으로 완전 재작성** 결정.

---

### 설계 원칙

- **VRP 코드(`production_assign_atlanta_vrp.py`)를 베이스**로 사용
- VRP와의 유일한 차이: **`SetGlobalSpanCostCoefficient`** 값 (balance 강도 조절)
- VRP = 100 (travel 중시), CSI = 200 (travel/balance 균형), SITS = 400 (balance 강화)
- K-Means는 제거 — OR-Tools가 초기 해를 자체적으로 생성
- 기존 CSI/SITS 코드는 완전 교체 (greedy 로직 불필요)

---

### 수정 내용: `smart_routing/production_assign_atlanta_csi.py` 완전 재작성

기존 코드를 모두 삭제하고 아래 구조로 재작성.

#### 상수 (파일 상단)

```python
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

PRODUCTION_OUTPUT_DIR = Path("260310/production_output")

# OR-Tools balance coefficient
# VRP uses 100. Higher = more balance weight, more travel cost accepted.
CSI_SPAN_COEFFICIENT = 200
SITS_SPAN_COEFFICIENT = 400

# Solver time limits (seconds)
CSI_SOLVE_TIME_SECONDS = 25
SITS_SOLVE_TIME_SECONDS = 35
```

---

#### 핵심 함수: `_solve_day_assignment`

아래 함수를 VRP의 `_solve_vrp_day`를 기반으로 작성. **변경점만 표시**.

```python
def _solve_day_assignment(
    service_day_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    route_client,
    region_centers: dict[int, tuple[float, float]],
    *,
    enable_targeted_swap: bool = False,          # True = SITS 모드
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # [1~4단계: VRP._solve_vrp_day와 완전 동일]
    # - job_df dedup
    # - engineer_df 준비, start_coord 계산
    # - OSRM matrix 호출 (start_coords + job_coords)
    # - OR-Tools manager, routing model 생성
    # - transit_cost_callback, time_callback 등록 (VRP와 동일)
    # - vehicle constraints: candidate_engineers 적용 (VRP와 동일)

    # [5단계: balance coefficient — VRP와 다른 부분]
    span_coeff = SITS_SPAN_COEFFICIENT if enable_targeted_swap else CSI_SPAN_COEFFICIENT
    time_dimension.SetGlobalSpanCostCoefficient(span_coeff)

    # [6단계: solver 설정 — time limit만 다름]
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    time_limit = SITS_SOLVE_TIME_SECONDS if enable_targeted_swap else CSI_SOLVE_TIME_SECONDS
    search_params.time_limit.FromSeconds(time_limit)

    solution = routing.SolveWithParameters(search_params)
    if solution is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # [7단계: 결과 추출 — VRP._solve_vrp_day와 완전 동일]
    # - vehicle 순서대로 ordered_rows 구성
    # - assignment_df, summary_df, schedule_df 생성
    # - summary_df는 base._build_summary_from_assignment 사용
```

---

#### `_build_assignment_from_frames` (공통 진입점 유지)

기존 함수 시그니처는 유지하되 내부에서 `_solve_day_assignment` 호출:

```python
def _build_assignment_from_frames(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    *,
    attendance_limited: bool = True,
    enable_targeted_swap: bool = False,       # SITS 모드 스위치
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # ... (날짜별 루프, engineer master 준비 등 기존과 동일)
    # _solve_day_assignment 호출 시 enable_targeted_swap 전달
```

---

#### `smart_routing/production_assign_atlanta_sits.py` — 변경 없음

SITS는 기존대로 `enable_targeted_swap=True`로 CSI를 호출하므로 수정 불필요:

```python
# 기존 코드 유지
def _build_assignment_from_frames(...):
    return csi._build_assignment_from_frames(..., enable_targeted_swap=True)
```

---

### 제거할 것들 (더 이상 불필요)

기존 CSI 코드에서 아래 함수들은 OR-Tools 재작성 후 사용 안 함 — 전부 삭제:
- `_kmeans_cluster_jobs`
- `_hungarian_match_engineers_to_clusters`
- `_build_job_queue`
- `_build_global_summary`, `_update_global_summary`
- `_global_score_delta`, `_global_swap_score_delta`
- `_find_best_insertion_for_engineer`, `_select_best_insertion`
- `_targeted_swap_once`
- `_insert_job_into_state`, `_remove_job_from_state`
- `_build_states`
- `GLOBAL_STD_WORK_WEIGHT`, `GLOBAL_MAX_WORK_WEIGHT`, `PHASE1_MAX_SEED_JOBS` 등 greedy 상수
- `scipy.cluster.vq`, `scipy.optimize.linear_sum_assignment` import

유지할 것:
- `_output_paths`
- `_dedupe_day_jobs`
- `_prepare_service_df`
- `_build_day_engineer_master`
- `AtlantaProductionSequentialAssignmentResult` dataclass
- `build_atlanta_production_assignment_csi` (공개 진입점)

---

### 예상 결과

| 알고리즘 | span_coeff | time_limit | 예상 성능 |
|----------|-----------|-----------|-----------|
| VRP | 100 | 20s | Travel +0.4%, Std -5.5% |
| CSI-ORT | 200 | 25s | Travel +2~5%, Std +10~30% 목표 |
| SITS-ORT | 400 | 35s | Travel +5~10%, Std +5~20% 목표 |

VRP보다 balance가 더 좋아질 수 있음 (span_coeff 높음). Travel은 약간 손해 예상.

---

### 검증 방법

1. 컴파일 확인:
```bash
python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py
```

2. 단위 테스트:
```bash
python sr_test_csi_sits_unit.py
```

단위 테스트 중 greedy 알고리즘 관련 테스트는 OR-Tools 기반에 맞게 수정 필요.  
최소 검증: CSI와 SITS 모두 job_count 개의 job이 배정되고, 아무도 overflow_480이 없어야 함.

3. 벤치마크:
```bash
python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md
```

**합격 기준:**
- CSI Travel gap: **+10% 이하**
- CSI Std gap: **+50% 이하**
- SITS Std gap < CSI Std gap (balance 더 강함)
- Overflow 480: 0

---

## 2026-04-05 — 진짜 CSI/SITS 알고리즘 설계 (OR-Tools 없이)

**배경:** 현재 CSI/SITS는 OR-Tools VRP에 파라미터(span_coeff, time_limit)만 다르게 설정한 것으로, 독립 알고리즘이 아님. OR-Tools 없이 동작하는 진짜 알고리즘을 구현.

---

### 핵심 원칙: VRP 목적함수를 직접 구현

VRP가 내부적으로 최소화하는 값은:

```
total_cost = Σ(travel_min per engineer) + span_coeff × (max_work - min_work)
```

`SetGlobalSpanCostCoefficient(100)`이 하는 일 = `100 × (가장 바쁜 엔지니어 - 가장 한가한 엔지니어)`를 비용에 추가.

따라서 greedy 삽입 스코어도 동일한 목적함수의 delta로 정의하면 VRP와 같은 방향으로 최적화됨:

```
insertion_score(job j → engineer e, position p) =
    Δtravel_min(e, j, p)           # e의 route travel 증가분
  + α × Δspan(work_list, e, j)    # 전체 엔지니어 (max_work - min_work) 변화량
```

여기서:
- `Δspan = new_span - old_span`
- `new_span = max(new_work_list) - min(new_work_list)`
- `old_span = max(work_list) - min(work_list)`
- `α = 1.0` (VRP span_coeff=100과 travel_min 스케일이 같으므로 1:1 대응)

**이전 구현의 실수:** `std`를 사용했지만, VRP는 `span(max-min)`을 사용함. std는 이차 통계라 작은 불균형에 둔감하고 방향이 불명확함.

---

### CSI 알고리즘 (OR-Tools 없음)

#### Phase 1: 지리적 초기화 (K-Means + Hungarian)

```
1. K-Means로 job들을 N개 cluster로 분류 (N = 엔지니어 수)
2. Hungarian matching: 각 엔지니어를 가장 가까운 cluster centroid에 배정
3. 각 엔지니어의 "담당 cluster" 결정 → job 처리 순서만 결정 (실제 배정은 Phase 2)
```

Phase 1은 **job 처리 순서(queue)만 결정**. 실제 배정은 모두 Phase 2.

#### Phase 2: 전역 삽입 (Global Span Insertion)

```python
# work_list = [0.0] * N  (엔지니어별 현재 총 작업시간)
# job_queue = K-Means cluster 순서로 정렬된 전체 job 목록

for job in job_queue:
    best_engineer, best_position, best_score = None, None, +inf

    for engineer e in all_engineers:
        if work_list[e] + job.service_time > MAX_WORK_MIN:
            continue  # overflow 방지
        for position p in e.route (1 ~ len+1):
            delta_travel = compute_insertion_delta(e.route, job, p)
            delta_work = delta_travel + job.service_time
            new_work_list = work_list.copy()
            new_work_list[e] += delta_work
            new_span = max(new_work_list) - min(new_work_list)
            old_span = max(work_list) - min(work_list)
            score = delta_travel + SPAN_WEIGHT * (new_span - old_span)
            if score < best_score:
                best_score = score
                best_engineer, best_position = e, p

    insert job into best_engineer at best_position
    work_list[best_engineer] += delta_work
```

#### 상수

```python
CSI_SPAN_WEIGHT = 1.0      # travel_min 단위와 동일 스케일
REGION_PENALTY_MIN = ...   # 기존 region 페널티 유지
```

---

### SITS 알고리즘 (CSI + Multi-Pass Relocation)

CSI 완료 후 **반복 재배정(Relocation)** 단계 추가.

VRP의 `GUIDED_LOCAL_SEARCH`가 하는 일을 직접 구현:

```python
SITS_RELOCATION_PASSES = 20     # 최대 pass 수
SITS_SPAN_WEIGHT = 2.0           # balance를 CSI보다 더 강조

def relocation_pass(states, work_list, jobs_df, engineer_df, route_client):
    """
    가장 바쁜 엔지니어부터 시작해 각 job을 다른 엔지니어로 이동 시도.
    score < 0이면 이동 실행. 1 pass에서 개선이 없으면 종료.
    """
    improved = False
    # 가장 바쁜 엔지니어 순으로 정렬
    sorted_engineers = sorted(all_engineers, key=lambda e: work_list[e], reverse=True)

    for source_engineer in sorted_engineers:
        for job in source_engineer.route (copy):
            removal_delta_travel, removal_delta_work = compute_removal(source_engineer, job)

            for target_engineer != source_engineer:
                for position in target_engineer.route:
                    ins_travel, ins_work = compute_insertion(target_engineer, job, position)

                    # 목적함수 delta 계산 (source 감소 + target 증가)
                    new_work_list = work_list.copy()
                    new_work_list[source] -= removal_delta_work
                    new_work_list[target] += ins_work
                    delta_span = (max(new_wl)-min(new_wl)) - (max(wl)-min(wl))
                    delta_travel = ins_travel - removal_delta_travel
                    score = delta_travel + SITS_SPAN_WEIGHT * delta_span

                    if score < -0.01:   # 개선됨
                        실행: source에서 job 제거, target에 삽입
                        work_list 업데이트
                        improved = True
                        break  # 이 job에 대해 best target 찾으면 바로 이동

    return improved

# 최대 SITS_RELOCATION_PASSES번 반복
for _ in range(SITS_RELOCATION_PASSES):
    if not relocation_pass(...):
        break
```

---

### 전체 흐름 비교

| 단계 | VRP (OR-Tools) | CSI (새 알고리즘) | SITS (새 알고리즘) |
|------|---------------|-----------------|-----------------|
| 초기 해 | PATH_CHEAPEST_ARC | K-Means+Hungarian 순서로 Global Span Insertion | 동일 (CSI) |
| 탐색 | GUIDED_LOCAL_SEARCH (수만 번) | 없음 (1회 greedy) | Multi-Pass Relocation (최대 20 pass) |
| 목적함수 | travel + 100×span | travel + 1.0×span | travel + 2.0×span |
| 탐색 수 | ~수만 | 47×13×~4 = ~2,500 | +47×13×~4×20 = ~52,000 |

SITS의 52,000번 평가 = VRP와 비슷한 탐색량.

---

### 구현 지시

#### `smart_routing/production_assign_atlanta_csi.py` 재작성

**import에서 제거:**
```python
# 삭제
import smart_routing.production_assign_atlanta_vrp as vrp
from ortools.constraint_solver import ...
```

**추가할 상수:**
```python
CSI_SPAN_WEIGHT = 1.0
SITS_SPAN_WEIGHT = 2.0
SITS_RELOCATION_PASSES = 20
```

**구현할 핵심 함수:**

1. `_compute_span(work_list: list[float]) -> float`
   - `return max(work_list) - min(work_list) if work_list else 0.0`

2. `_insertion_score(delta_travel_min, work_list, engineer_idx, delta_work_min, span_weight) -> float`
   - work_list[engineer_idx] += delta_work_min 가정하고 new_span 계산
   - `return delta_travel_min + span_weight * (new_span - old_span)`

3. `_select_best_insertion(job_row, engineer_df, states, work_list, route_client, span_weight) -> (engineer_code, move)`
   - 모든 엔지니어 × 모든 position 탐색
   - `_insertion_score`로 best 선택

4. `_relocation_pass(states, work_list, jobs_df, engineer_df, route_client, span_weight) -> bool`
   - 가장 바쁜 엔지니어부터 job 재배정 시도
   - score < -0.01이면 이동 실행, `return True`
   - 개선 없으면 `return False`

5. `_solve_day_assignment(..., enable_targeted_swap=False)`
   ```
   Phase 1: K-Means + Hungarian → job_queue 생성
   Phase 2: job_queue 전체를 _select_best_insertion (span_weight=CSI_SPAN_WEIGHT)
   Phase 3 (SITS only): _relocation_pass 최대 SITS_RELOCATION_PASSES번
   Phase 4: _optimize_route_order (2-opt, 기존 코드 재사용 가능)
   Phase 5: _finalize_state_metrics
   ```

---

### 검증 방법

```bash
python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py
python sr_test_csi_sits_unit.py
python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md
```

**합격 기준:**
- CSI Travel gap: **+15% 이하** (greedy이므로 OR-Tools보다 약간 열위 허용)
- CSI Work Std gap: **+30% 이하**
- SITS Travel gap: **+10% 이하**
- SITS Work Std gap: **+15% 이하** (relocation으로 OR-Tools 수준 기대)
- Overflow 480: 0
- `ortools` import 없음 확인

---

## 2026-04-05 — v5 구현 결과 분석 및 v6 수정 지시

**최신 벤치마크 (2026-01-12):**

| 지표 | CSI | SITS | VRP | CSI Gap | SITS Gap |
|------|-----|------|-----|---------|----------|
| Travel (km) | 913.14 | 964.06 | 911.95 | +0.13% | +5.71% |
| Work Std | 45.82 | 36.47 | 13.91 | +229% | +162% |
| Max Work | 398.52 | 342.06 | 327.89 | +21.54% | **+4.32%** |
| Overflow 480 | 0 | 0 | 0 | 0 | 0 |

**OR-Tools 제거 확인:** `import ortools` 없음 ✓  
**SITS relocation 작동 확인:** SITS Std +162% < CSI Std +229% ✓  
**SITS Max Work 거의 완벽:** +4.32% ✓

---

### 핵심 문제 발견: 두 가지 다른 스코어링 함수가 혼용됨

코드를 분석하면 Phase 2에 **두 개의 독립된 삽입 선택 함수**가 존재:

**함수 1: `_select_best_insertion` (라인 284)**
- 사용 스코어: `delta_travel + span_weight × Δspan`
- `Δspan = new_span - old_span`, `span = max_work - min_work`
- **올바른 구현** ✓

**함수 2: `_select_best_global_insertion` (라인 344)**
- 사용 스코어: `_global_work_score_delta(max_weight=1.9, std_weight=2.7)`
- 내부: `delta_travel + 1.9×Δmax + 2.7×Δstd`
- **std를 사용** — 설계 의도와 다름 ✗

이 두 함수 중 실제 Phase 2에서 어느 것이 호출되는지 확인 필요. 벤치마크 Notes에 `max_weight=1.9, std_weight=2.7`이 명시되어 있으므로 `_select_best_global_insertion`이 주 경로임.

---

### [FAIL] `_select_best_global_insertion`이 std 기반 스코어링 사용

**위치:** `production_assign_atlanta_csi.py:381-388`

```python
score = _global_work_score_delta(
    work_list, code_to_idx[code],
    float(move["delta_work_min"]),
    float(move["delta_travel_min"]) + float(move["region_penalty_min"]),
    max_weight=max_weight,   # 1.9
    std_weight=std_weight,   # 2.7
)
```

**문제:** `_global_work_score_delta`는 `1.9×Δmax + 2.7×Δstd`를 계산함.  
- `std`는 이차 통계 — 작은 불균형 변화에 둔감
- `max`와 `std` 두 가지를 동시에 최소화하면 방향이 충돌
- SITS relocation은 `span(max-min)`을 사용하므로 CSI 삽입 결과와 목적함수가 다름

---

### 수정 지시

#### 수정 1: `_select_best_global_insertion`의 스코어를 span 기반으로 교체

```python
# 변경 전 (라인 381~388)
score = _global_work_score_delta(
    work_list, code_to_idx[code],
    float(move["delta_work_min"]),
    float(move["delta_travel_min"]) + float(move["region_penalty_min"]),
    max_weight=max_weight,
    std_weight=std_weight,
)

# 변경 후
old_span = _compute_span(work_list)
new_work_list = list(work_list)
new_work_list[code_to_idx[code]] += float(move["delta_work_min"])
score = float(move["delta_travel_min"]) + float(move["region_penalty_min"]) + float(max_weight) * (_compute_span(new_work_list) - old_span)
```

`max_weight` 파라미터 이름을 `span_weight`로 리네임하고 값은 **1.0**으로 설정.  
`std_weight` 파라미터와 `_global_work_score_delta` 함수는 더 이상 필요 없으므로 **삭제**.

#### 수정 2: 상수 정리

```python
# 삭제
CSI_MAX_WORK_WEIGHT = 1.9
CSI_STD_WORK_WEIGHT = 2.7

# 추가
CSI_SPAN_WEIGHT = 1.0      # insertion: travel + 1.0 × Δspan
SITS_RELOCATION_SPAN_WEIGHT = 2.0  # 유지 (relocation은 balance 더 강조)
```

---

### 수정 후 예상 결과

`_select_best_global_insertion`과 `_relocation_pass`가 동일한 `span(max-min)` 목적함수를 사용하게 됨.

- CSI: K-Means가 지리를 잡고, span 스코어가 balance를 균등하게 분배 → Std gap **+50% 이하** 목표
- SITS: CSI 결과 위에 20 pass relocation → Std gap **+20% 이하** 목표
- Travel은 지금처럼 거의 동일 수준 유지 기대

---

### 벤치마크 명령

```bash
python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py
python sr_test_csi_sits_unit.py
python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md
```

**합격 기준 (이번 이터레이션):**
- CSI Travel gap: **+5% 이하** 유지
- CSI Std gap: **+100% 이하** (현재 +229%에서 개선)
- SITS Std gap: **+50% 이하** (현재 +162%에서 개선)
- Overflow 480: 0

[ERROR] Claude API call failed: BadRequestError: Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.'}, 'request_id': 'req_011CZjzF3UZicz2tYpRfNPgK'}
