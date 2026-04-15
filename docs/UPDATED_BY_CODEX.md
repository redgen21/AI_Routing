# UPDATED BY CODEX

## 2026-04-01 22:53:23

- 성능 검증 범위를 문서로 정리했습니다.
- 당시 기준으로 `vrp_level` 활성 파이프라인이 `Cluster-aware seed -> Travel improvement -> Travel refinement -> Balance improvement`라는 점을 기록했습니다.

## 2026-04-01 22:56:00

- `OSRM matrix seed/grow`가 engineer feasibility를 무시하던 문제를 수정했습니다.
- `_assign_day()` leftover fallback이 region 제한 때문에 `0건 배정`을 만들던 문제를 수정했습니다.
- `_assignment_objective(...)`를 실제 `total_work_min` 편차 기준으로 맞췄습니다.
- 회귀 테스트를 보강했고 `7/7` 통과 상태를 만들었습니다.

## 2026-04-01 23:01:31

- 작업 로그 경로를 `docs/UPDATED_BY_CODEX.md`로 고정했습니다.
- Claude 피드백 문서 경로를 `docs/UPDATED_BY_CLAUDE.md`로 고정했습니다.
- 문서 기반 feedback loop 프로토콜을 정리했습니다.

## 2026-04-01 23:41:30

- Claude 피드백을 읽고 `P1/P2` 우선순위를 반영했습니다.

변경 파일:

- `smart_routing/production_assign_atlanta.py`
- `smart_routing/production_assign_atlanta_osrm.py`
- `sr_watch_vrp_feedback.py`
- `docs/vrp_feedback_loop_protocol.md`

핵심 수정:

- `_iterative_improve_assignment_df(...)`를 first-improvement에서 best-improvement 방식으로 변경했습니다.
- `balance_first`에서는 travel이 직전 상태 대비 `+2%`를 넘지 않도록 상한을 넣었습니다.
- assignment metadata 갱신용 `_apply_assignment_engineer_metadata(...)` 헬퍼를 추가했습니다.
- 중복된 두 번째 travel pass를 `travel swap refinement` 단계로 교체했습니다.
- `balance` 뒤에 travel을 다시 망가뜨리던 추가 rebalance 호출은 제거했습니다.
- 자동 검증 기준을 `2026-01-12` 단일 benchmark로 맞췄습니다.

검증:

- `python -m py_compile smart_routing/production_assign_atlanta.py smart_routing/production_assign_atlanta_osrm.py sr_watch_vrp_feedback.py`
- `python sr_test_vrp_level_unit.py`
- 결과: `7/7` 통과

최신 benchmark:

- 명령: `python sr_benchmark_vrp_level_vs_vrp.py --date 2026-01-12 --write docs/vrp_level_benchmark_20260112.md`
- Travel Distance: `622.40 km` vs `619.75 km` (`+0.43%`)
- Work Std Dev: `20.78` vs `21.99` (`-5.51%`)
- Max Work: `333.57 min` vs `306.17 min` (`+8.95%`)
- Overflow 480: `0`

현재 상태:

- `2026-01-12` 기준으로
  - travel gap 10% 미만
  - std gap 10% 미만
  - max work gap 10% 미만
  을 모두 만족했습니다.

다음 단계:

1. Claude가 새 benchmark와 로그를 읽고 남긴 추가 피드백 확인
2. 현재 성능을 유지하면서 runtime을 줄일 수 있는지 검토
3. 필요하면 swap refinement와 balance cap을 더 정교하게 튜닝

## 2026-04-02 06:16:09

- Claude의 추가 피드백에 따라 `VRP baseline` 차이와 `2026-01-01~2026-01-12` 전체 가용 날짜 benchmark를 다시 확인했습니다.

원인 확인:

- `2026-01-12` 현재 코드 기준 VRP baseline은 raw input과 deduped input 모두 `619.75 km`로 동일했습니다.
- 따라서 예전 문서의 `911.95 km`는 현재 코드 기준이 아니라 이전 상태에서 생성된 stale artifact로 판단했습니다.
- 현재 benchmark 비교는 `docs/vrp_level_benchmark_20260112.md` 값을 기준으로 보는 것이 맞습니다.

전체 날짜 benchmark:

- 명령: `python sr_benchmark_vrp_level_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/vrp_level_benchmark_20260101_20260112.md`
- 가용 날짜:
  - `2026-01-02`
  - `2026-01-03`
  - `2026-01-05`
  - `2026-01-06`
  - `2026-01-07`
  - `2026-01-08`
  - `2026-01-09`
  - `2026-01-12`

요약:

- `2026-01-12`는 목표 수치를 만족했습니다.
- 하지만 다른 날짜, 특히 `2026-01-05` ~ `2026-01-09` 구간에서는 work std / max work가 아직 크게 나쁩니다.
- worst date는 `2026-01-07`이었고:
  - Work Std gap `521.10%`
  - Max Work gap `52.36%`

해석:

- 현재 알고리즘은 `2026-01-12` heavy day에는 맞춰졌지만, 전체 날짜 generalization은 아직 부족합니다.
- `balance_first + travel cap 2%`가 `2026-01-12`에는 잘 맞지만, 다른 날짜에는 지나치게 보수적이거나 반대로 구조적으로 맞지 않을 가능성이 큽니다.

다음 포커스:

1. `2026-01-07`을 대표 worst-case로 잡고 balance cap을 adaptive하게 바꾸는 실험
2. `2026-01-12` 성능을 유지한 채 다른 날짜 std / max work 개선
3. 필요하면 날짜별 workload 강도에 따라 balance 단계 강도를 다르게 주는 방식 검토

## 2026-04-02 06:16:09

- `balance_first`의 travel cap을 고정 `1.02`에서 workload 기반 adaptive cap으로 변경했습니다.
- 현재 규칙:
  - work std `>= 100` -> `1.15`
  - work std `>= 80` -> `1.10`
  - work std `>= 65` -> `1.06`
  - 그 외 -> `1.02`

의도:

- `2026-01-12` 같은 날짜는 현재 좋은 수치를 유지
- `2026-01-07` 같은 worst-case 날짜는 balance 단계에 더 많은 이동 여지를 허용

검증:

- `python -m py_compile smart_routing/production_assign_atlanta.py smart_routing/production_assign_atlanta_osrm.py`
- `python sr_test_vrp_level_unit.py` -> `7/7` 통과
- `python sr_benchmark_vrp_level_vs_vrp.py --date 2026-01-12 --write docs/vrp_level_benchmark_20260112.md`
  - Travel gap `+0.43%`
  - Work Std gap `-5.51%`
  - Max Work gap `+8.95%`
  - `2026-01-12` 목표 유지 확인

전체 날짜 benchmark:

- `python sr_benchmark_vrp_level_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/vrp_level_benchmark_20260101_20260112.md`

변화 요약:

- 이전 range 평균 gap
  - Travel `-4.48%`
  - Work Std `223.04%`
  - Max Work `25.05%`
- 현재 range 평균 gap
  - Travel `3.64%`
  - Work Std `77.72%`
  - Max Work `12.84%`

해석:

- `2026-01-12` 성능은 그대로 유지했습니다.
- range 전체 기준으로 work std / max work는 크게 개선됐습니다.
- 아직 남은 worst date:
  - `2026-01-05`
  - `2026-01-07`
  - `2026-01-09`
- 특히 `2026-01-09`는
  - Work Std gap `149.82%`
  - Max Work gap `27.95%`
  로 여전히 큽니다.

다음 포커스:

1. `2026-01-09`와 `2026-01-07`에서 왜 balance가 충분히 퍼지지 않는지 원인 분석
2. travel이 좋은 날에 balance만 추가로 개선하는 후처리 설계
3. `2026-01-12` 성능을 유지하면서 range worst-case를 더 끌어내리기

## 2026-04-02 22:20:49 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 22:20:59 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 22:59:15

- Added the new CSI/SITS implementation track from `docs/algorithm_design_csi_sits.md`.
- New files:
  - `smart_routing/production_assign_atlanta_csi.py`
  - `smart_routing/production_assign_atlanta_sits.py`
  - `sr_test_csi_sits_unit.py`
  - `sr_benchmark_csi_sits_vs_vrp.py`
- The new path removes TV/DMS2 branching and treats all active DMS engineers as the candidate pool.
- Implemented:
  - K-Means clustering + Hungarian cluster-home matching
  - global sequential insertion with OSRM matrix delta scoring
  - targeted worst-job swap for SITS
  - route local-search reorder after assignment

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `6/6` passed

Benchmark artifacts:

- Single date: `docs/csi_sits_benchmark_20260112.md`
- Range: `docs/csi_sits_benchmark_20260101_20260112.md`

Current benchmark summary:

- `2026-01-12`
  - CSI: travel `+8.40%`, work std `+426.62%`, max work `+35.71%`
  - SITS: travel `+12.18%`, work std `+330.43%`, max work `+35.16%`
- `2026-01-01 ~ 2026-01-12`
  - CSI average travel gap `-0.61%`, average work std gap `252.46%`, average max work gap `24.17%`
  - SITS average travel gap `2.96%`, average work std gap `243.80%`, average max work gap `25.98%`

Assessment:

- The new CSI/SITS code is implemented and benchmarkable.
- Travel is sometimes competitive with VRP, but workload balance is still far from the target.
- The main unresolved issue is heavy per-engineer work skew on multi-job days, especially `2026-01-02`, `2026-01-08`, and `2026-01-12`.
- Next review should focus on a stronger balance objective that does not destroy travel, or on a post-insertion rebalancing phase that moves load without large route inflation.

## 2026-04-02 23:10:13

- Fixed coordinator runtime compatibility so the automation loop can run under the user-provided Python 3.13 launcher while compile/test/benchmark use a worker interpreter with local routing dependencies.
- Updated `coordinator.py`:
  - added `COORDINATOR_WORKER_PYTHON` support
  - auto-detects a worker python that has `pandas`, `numpy`, `scipy`, and `ortools`
  - prints the selected worker interpreter in the coordinator banner
- Updated `smart_routing/production_assign_atlanta_csi.py` to replace the `sklearn` dependency with `scipy.cluster.vq.kmeans2`, because Python 3.13 in this environment does not have `sklearn`.

Coordinator validation:

- `C:\\Program Files (x86)\\Python313-64\\python.exe coordinator.py --dry-run --rounds 1`
- Result:
  - worker python selected: `C:\\Program Files\\Python311\\python.exe`
  - compile: `PASS`
  - unit test: `PASS`
  - benchmark: executed successfully

Current dry-run benchmark snapshot from coordinator (`2026-01-12`):

- CSI: travel `+16.78%`, work std `+349.27%`, max work `+28.42%`
- SITS: travel `+4.91%`, work std `+368.14%`, max work `+37.12%`

Current blocker for real full-loop execution:

- `OPENAI_API_KEY` is not set in process/user/machine environment.
- `ANTHROPIC_API_KEY` is not set in process/user/machine environment.
- Because of that, the actual Codex/Claude API roundtrip has not been started yet. Only dry-run validation was executed.

## 2026-04-02 23:02:54 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 23:03:02 — Coordinator Round 2

Round 2 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 23:03:06 — Coordinator Round 3

Round 3 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 23:04:49 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 23:07:00 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-02 23:08:46 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-05 12:33:00 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

[ERROR] Codex API call failed: RateLimitError: Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}

## 2026-04-05 19:18:45

- Rebuilt `smart_routing/production_assign_atlanta_csi.py` as a real non-OR-Tools solver instead of the old OR-Tools wrapper pattern.
- Core changes:
  - changed Phase 2 from fixed queue insertion to dynamic global insertion over all remaining jobs
  - changed insertion scoring to global workload deltas with `CSI_MAX_WORK_WEIGHT = 1.9` and `CSI_STD_WORK_WEIGHT = 2.7`
  - kept SITS as a relocation-based local search with `SITS_RELOCATION_SPAN_WEIGHT = 2.0`
  - switched insertion, route ordering, and relocation to a single per-day OSRM matrix
  - merged exact route duration/distance from schedule output back into summary output
- Stability fix:
  - `kmeans2(..., minit="points", seed=1)` now produces repeatable daily results
  - repeated `2026-01-12` SITS runs now stay fixed at the same metrics
- Supporting updates:
  - updated `sr_test_csi_sits_unit.py` for the renamed relocation constant
  - updated `sr_benchmark_csi_sits_vs_vrp.py` notes for the new non-OR-Tools algorithm

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `6/6` passed

Latest benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
  - CSI: travel `+11.46%`, work std `+203.22%`, max work `+21.54%`
  - SITS: travel `+16.29%`, work std `+14.12%`, max work `+4.32%`
- `python sr_benchmark_csi_sits_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/csi_sits_benchmark_20260101_20260112.md`
  - CSI average gaps: travel `+8.50%`, work std `+254.66%`, max work `+24.70%`
  - SITS average gaps: travel `+12.35%`, work std `+101.36%`, max work `+11.53%`

Assessment:

- The new non-OR-Tools solver is now fast enough for daily validation.
- `2026-01-12` runtime is about `13s` for CSI and `13s` for SITS.
- SITS now gets close to VRP on `2026-01-12` for balance and max work, but travel is still above target.
- The next quality step should be a stronger SITS local search:
  - add exchange/swap moves, not only one-job relocation
  - or add a travel refinement phase that preserves the improved work distribution

## 2026-04-05 21:23:25

- Added a new `hybrid` mode as `CSI + capped relocation`.
- New file:
  - `smart_routing/production_assign_atlanta_hybrid.py`
- Core logic updates in `smart_routing/production_assign_atlanta_csi.py`:
  - added relocation travel budget control inside `_relocation_pass(...)`
  - relocation now optionally enforces:
    - `baseline_total_travel_km`
    - `max_travel_budget_ratio`
  - added Hybrid constants:
    - `HYBRID_RELOCATION_SPAN_WEIGHT = 1.3`
    - `HYBRID_TRAVEL_BUDGET_RATIO = 0.03`
    - `HYBRID_RELOCATION_PASSES = 15`
  - `_solve_day_assignment(...)` and `_build_assignment_from_frames(...)` now accept relocation tuning parameters
- Benchmark script updated:
  - `sr_benchmark_csi_sits_vs_vrp.py` now reports `CSI / Hybrid / SITS / VRP`
- Unit test updates:
  - added Hybrid real-day validation
  - added `hybrid_travel_budget_blocks_large_increase`

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_hybrid.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `7/7` passed

Latest benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
  - CSI: travel `+11.46%`, work std `+203.22%`, max work `+21.54%`
  - Hybrid: travel `+10.76%`, work std `+54.12%`, max work `+4.32%`
  - SITS: travel `+16.29%`, work std `+14.12%`, max work `+4.32%`
- `python sr_benchmark_csi_sits_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/csi_sits_benchmark_20260101_20260112.md`
  - CSI average gaps: travel `+8.50%`, work std `+254.66%`, max work `+24.70%`
  - Hybrid average gaps: travel `+2.53%`, work std `+170.76%`, max work `+11.84%`
  - SITS average gaps: travel `+12.35%`, work std `+101.36%`, max work `+11.53%`

Assessment:

- Hybrid behaves as intended:
  - much better travel than SITS
  - much better balance than CSI
- On `2026-01-12`, Hybrid is the clean middle point between CSI and SITS.
- Across `2026-01-01 ~ 2026-01-12`, Hybrid strongly improves travel while keeping max-work close to SITS, but work-std on dates like `2026-01-05` and `2026-01-08` is still far from VRP.
- I also checked slightly more aggressive Hybrid presets. They improved some std cases, but usually overshot travel and did not dominate the current default. The current `1.3 / +3% / 15 passes` setting is the best compromise among the tested presets.

## 2026-04-05 21:33:57

- Ran an experimental comparison for Claude's suggestion:
  - unify `CSI` insertion scoring to span-based insertion
  - keep `SITS` relocation logic unchanged
- I tested this as a temporary monkeypatch only. I did not change the checked-in default because the result was worse overall.

Comparison summary:

- `2026-01-12` under the same baseline run:
  - current CSI: `1016.50 km`, std `42.19`, max `398.52`
  - span-based CSI: `1124.85 km`, std `53.73`, max `432.02`
  - current SITS: `1060.49 km`, std `15.88`, max `342.06`
  - span-based SITS: `1052.46 km`, std `34.06`, max `377.12`

- `2026-01-01 ~ 2026-01-12` average gap comparison:
  - current CSI: travel `+7.08%`, std `+221.38%`, max `+24.30%`
  - span-based CSI: travel `+5.20%`, std `+254.03%`, max `+28.95%`
  - current SITS: travel `+10.89%`, std `+82.76%`, max `+11.16%`
  - span-based SITS: travel `+4.50%`, std `+159.68%`, max `+14.92%`

Assessment:

- Span-based insertion improved travel on average.
- But it made workload balance materially worse for both CSI and SITS.
- For CSI, the change is not acceptable because std and max-work both moved in the wrong direction.
- For SITS, the travel improvement was real, but std and max-work deteriorated too much, so it is not a net win.
- Conclusion: do not replace the current insertion score with pure span-based scoring as the default.

## 2026-04-05 22:19:19

- Connected `Hybrid` results into `sr_production_map.py`.
- Added Hybrid map file paths:
  - `atlanta_assignment_result_hybrid_actual_selected.csv`
  - `atlanta_engineer_day_summary_hybrid_actual_selected.csv`
  - `atlanta_schedule_hybrid_actual_selected.csv`
- Added Streamlit mode:
  - `Hybrid Assign (Actual Attendance, Selected Dates)`
- Generated Hybrid outputs for requested dates:
  - requested range: `2026-01-01 ~ 2026-01-12`, `2026-01-19`, `2026-01-20`
  - actual available dates in generated output:
    - `2026-01-02`
    - `2026-01-03`
    - `2026-01-05`
    - `2026-01-06`
    - `2026-01-07`
    - `2026-01-08`
    - `2026-01-09`
    - `2026-01-12`
    - `2026-01-19`
    - `2026-01-20`

Validation:

- `python -m py_compile sr_production_map.py smart_routing/production_assign_atlanta_hybrid.py smart_routing/production_assign_atlanta_csi.py`
- Generated files:
  - assignment rows: `310`
  - summary rows: `101`
  - schedule rows: `310`

Note:

- I also tried to start the Streamlit app, but the available local Python environments (`3.10`, `3.11`, `3.13`) do not have the `streamlit` module installed.
- The map integration itself is complete; once `streamlit` is installed in one of those environments, `sr_production_map.py` will expose the Hybrid mode.

## 2026-04-05 15:12:17

- Replaced the old greedy CSI/SITS implementation with the new OR-Tools-based design from Claude.
- Updated `smart_routing/production_assign_atlanta_vrp.py`:
  - extracted `_solve_vrp_day_with_config(...)`
  - added configurable `global_span_cost_coefficient` and `solve_time_seconds`
  - kept the existing VRP path on the same defaults (`100`, `20s`)
- Rebuilt `smart_routing/production_assign_atlanta_csi.py` as a thin OR-Tools wrapper:
  - `CSI_SPAN_COEFFICIENT = 200`
  - `SITS_SPAN_COEFFICIENT = 400`
  - `CSI_SOLVE_TIME_SECONDS = 25`
  - `SITS_SOLVE_TIME_SECONDS = 35`
  - removed the old greedy internals and switched daily solve to the shared VRP core
- `smart_routing/production_assign_atlanta_sits.py` continues to call the CSI frame builder with `enable_targeted_swap=True`, which now maps to the stronger OR-Tools balance setting instead of the old custom swap logic.
- Rewrote `sr_test_csi_sits_unit.py` to validate public CSI/SITS behavior instead of deleted greedy helper functions.
- Updated benchmark note text in `sr_benchmark_csi_sits_vs_vrp.py` and the generated benchmark docs so they describe the current OR-Tools structure.

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_vrp.py smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `6/6` passed

Single-date benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
- `2026-01-12`
  - CSI: travel `905.21 km` vs VRP `911.95 km` (`-0.74%`)
  - CSI: work std `13.77` vs VRP `13.91` (`-1.05%`)
  - CSI: max work `327.89 min` vs VRP `327.89 min` (`0.00%`)
  - SITS: travel `1014.91 km` vs VRP `911.95 km` (`+11.29%`)
  - SITS: work std `11.62` vs VRP `13.91` (`-16.49%`)
  - SITS: max work `326.35 min` vs VRP `327.89 min` (`-0.47%`)

Range benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date-from 2026-01-01 --date-to 2026-01-12 --write docs/csi_sits_benchmark_20260101_20260112.md`
- Available dates: `2026-01-02`, `2026-01-03`, `2026-01-05`, `2026-01-06`, `2026-01-07`, `2026-01-08`, `2026-01-09`, `2026-01-12`
- CSI average gaps:
  - travel `1.90%`
  - work std `11.90%`
  - max work `0.85%`
- SITS average gaps:
  - travel `2.22%`
  - work std `4.46%`
  - max work `-0.04%`

Assessment:

- The OR-Tools rewrite is materially better than the old greedy track and is now close to the stated target window on the active `2026-01-01 ~ 2026-01-12` benchmark range.
- CSI is currently the safer travel-first mode.
- SITS is currently the stronger balance mode, but on `2026-01-12` its travel gap is still slightly above the informal `+10%` line (`+11.29%`), so it still needs one more tuning round if that threshold is strict.

## 2026-04-05 14:36:22

- Applied Claude's latest v3 feedback directly to the CSI/SITS path.
- Updated `smart_routing/production_assign_atlanta_csi.py`:
  - removed Phase 1 seed allocation entirely; all jobs now go through the global insertion loop
  - changed `GLOBAL_STD_WORK_WEIGHT` from `2.0` to `3.0`
  - removed `overload_delta` from both `_global_score_delta(...)` and `_global_swap_score_delta(...)`
  - removed the `std_delta` reward/penalty caps so the full balance signal is used
  - changed SITS worst-job selection to start from the highest-work engineer, then choose the highest-contribution job inside that engineer only
- The prior regression test still passes, and the SITS swap test still passes after the source-selection change.

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `7/7` passed

Latest benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
- `2026-01-12`
  - CSI: travel `1582.89 km` vs VRP `911.95 km` (`+73.57%`)
  - CSI: work std `53.38` vs VRP `13.91` (`+283.69%`)
  - CSI: max work `419.79 min` vs VRP `327.89 min` (`+28.03%`)
  - SITS: travel `1578.70 km` vs VRP `911.95 km` (`+73.11%`)
  - SITS: work std `46.55` vs VRP `13.91` (`+234.60%`)
  - SITS: max work `412.30 min` vs VRP `327.89 min` (`+25.74%`)

Assessment:

- The requested structural changes are implemented correctly.
- However, performance got worse: removing seed ownership entirely caused travel to explode, and balance is still far from the VRP target.
- SITS is now slightly better than CSI, which means the swap change is doing something, but the current global insertion objective is still not usable for the target benchmark.

## 2026-04-05 14:08:28

- Corrected the CSI/SITS implementation to reflect Claude's core feedback instead of only the earlier seed/fill change.
- Updated `smart_routing/production_assign_atlanta_csi.py`:
  - removed the old `travel_pool` gate from `_select_best_insertion`
  - added `global_summary` tracking and `_global_score_delta(...)` for cross-engineer insertion choice
  - kept the dynamic target logic, but now apply it inside the global candidate comparison
  - reduced Phase 1 from "seed nearly all cluster jobs" to a bounded cluster seed (`PHASE1_MAX_SEED_JOBS = 2`)
  - updated SITS swap evaluation to use the same global objective family
- Added a regression test in `sr_test_csi_sits_unit.py` to ensure a lightly farther but underloaded engineer can win when it improves balance.

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py`
- `python sr_test_csi_sits_unit.py`
- Result: `7/7` passed

Latest benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
- `2026-01-12`
  - CSI: travel `1092.08 km` vs VRP `911.95 km` (`+19.75%`)
  - CSI: work std `49.85` vs VRP `13.91` (`+258.32%`)
  - CSI: max work `406.36 min` vs VRP `327.89 min` (`+23.93%`)
  - SITS currently matches CSI on this date

Assessment:

- The earlier coding mistake is fixed: the algorithm is no longer using the old travel-pool shortlist, and Phase 2 is no longer effectively bypassed by seeding almost every job.
- The new logic improved balance versus the previous broken version, but it still does not meet the VRP target.
- Current tradeoff is still poor:
  - travel is much worse than VRP
  - work std is better than the previous `+538%` state, but still far too high
  - SITS is not yet adding measurable value over CSI, so the next iteration should focus on making the post-insert move actually reduce either travel or work skew.

## 2026-04-05 13:01:06 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

dry-run: 변경 없음

## 2026-04-05 13:04:48

- Reflected the latest Claude algorithm guidance into `smart_routing/production_assign_atlanta_csi.py`.
- Main changes:
  - tightened `TRAVEL_COMPETITION_SLACK_MIN` from `25.0` to `10.0`
  - changed insertion target to a dynamic target inside `_select_best_insertion`
    - `dynamic_target = max(static_target, current_avg_total_work + 20.0)`
  - split daily assignment into two phases:
    - Phase 1: cluster seed assignment to the Hungarian-matched engineer
    - Phase 2: remaining job fill through global insertion
  - removed the targeted swap call during every insertion step
  - kept only the post-fill swap refinement loop for SITS

Validation:

- `python -m py_compile smart_routing/production_assign_atlanta_csi.py smart_routing/production_assign_atlanta_sits.py sr_test_csi_sits_unit.py sr_benchmark_csi_sits_vs_vrp.py coordinator.py`
- `python sr_test_csi_sits_unit.py`
- Result: `6/6` passed

Latest benchmark:

- `python sr_benchmark_csi_sits_vs_vrp.py --date 2026-01-12 --write docs/csi_sits_benchmark_20260112.md`
- `2026-01-12`
  - CSI: travel `-1.84%`, work std `+538.31%`, max work `+32.18%`
  - SITS: travel `-1.15%`, work std `+494.46%`, max work `+32.18%`

Coordinator check:

- `C:\\Program Files (x86)\\Python313-64\\python.exe coordinator.py --dry-run --rounds 1`
- compile: `PASS`
- unit test: `PASS`
- benchmark: executed successfully with the same metrics as above

Assessment:

- Claude's seed/fill suggestion successfully restored travel quality.
- The main failure is still workload balance; the new seed phase helps route ownership, but the current scoring still over-concentrates work.
- Next change should target the global insertion score itself, not the cluster/queue layer.

## 2026-04-05 13:13:44 — Coordinator Round 1

Round 1 구현

변경 파일: 없음

[ERROR] Codex API call failed: RateLimitError: Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}

## 2026-04-05 13:17:18 — Coordinator Round 2

Round 2 구현

변경 파일: 없음

[ERROR] Codex API call failed: RateLimitError: Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}

## 2026-04-05 13:20:49 — Coordinator Round 3

Round 3 구현

변경 파일: 없음

[ERROR] Codex API call failed: RateLimitError: Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}
