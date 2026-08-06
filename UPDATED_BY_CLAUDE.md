# UPDATED_BY_CLAUDE

Claude 작업일지. 새 작업은 맨 위에 추가한다.

---

## 2026-08-06 ~ 08-07 — VRP Solver 슬롯 채움·이동거리 최적화

### 배경

기사 슬롯에 여유가 있는데도 미배정 Job이 남는 문제(7/29~8/6 통계 기준)와
이동거리 outlier/크로스 동선 문제를 해결. 추적 문서: `docs/vrp 미해결 문제.md`,
설계 반영: `docs/VRP_Solver_전체_설계문서.md`.

### 변경 파일

| 파일 | 변경 내용 |
|---|---|
| `smart_routing/production_assign_atlanta_vrp.py` | 아래 1~7 전부 |
| `smart_routing/vrp_mode_na_general.py` | 미배정 후보별 진단을 API 응답에 노출 (`diagnostics.unassigned_candidate_analysis`, unassigned 항목별 `candidate_analysis`) |
| `sr_common_vrp_client_server.py` | Unassigned Analysis 시트가 UI 추정 대신 솔버 실측 사유를 우선 표시 (`home_to_job_min`, `candidate_total_work_min`, `work_limit_min` 컬럼 추가) |
| `smart_routing/common_vrp_runtime.py` | 자동 time_limit: 31~60건 구간 30→60초 |
| `tests/test_vrp_slot_fill_objective.py` | 신규 회귀 테스트 8건 (fragmentation repacking, lexicographic drop, fixed 기사 600분 cap, adaptive policy 분기) |
| `.claude/agents/*.md` | terra-vrp-solver 등 에이전트 6종 신규 (별건) |

### 솔버 변경 상세 (production_assign_atlanta_vrp.py)

1. **Lexicographic drop penalty**: Job당 base 1e9 + 슬롯당 `VRP_SLOT_DROP_PENALTY_STEP`(8e6).
   우선순위: 미배정 Job 수 최소화 → 배정 슬롯 최대화 → soft route cost.
   step은 route-shape cost(span·balance·거리)보다 크고 center-type 정책
   penalty(OVERLAP 1e7, DMS2 fallback 5e7)보다 작게 설계.
2. **80/90km 이동거리 shaping**: RouteDistanceSoft/Strong dimension 상시 생성.
   80km까지 무penalty, 80~90km 2,000/km, 90km 초과 +25,000/km. DMS2 포함
   전 기사 적용 (hard cap의 DMS2 면제는 유지).
3. **일일 200km hard cap을 area-type 라우팅 날에도 적용** (기존엔 skip되어
   202km route 발생). relax 재시도에서는 완화.
4. **드랍 노드 재삽입 연산자 활성화**: `use_relocate_and_make_active`,
   `use_extended_swap_active` — 만석 기사의 Job을 옮겨 자리를 만들고 드랍
   Job을 되살리는 복합 이동을 탐색에 추가.
5. **Unassigned rescue**: primary 종료 후 미배정이 남으면 warm-start 계속
   탐색을 최대 3회(slice당 min(20초, time_limit)), 개선 없으면 즉시 중단.
   진단: `adaptive_objective.unassigned_rescue`.
6. **미배정 후보별 진단**: add-on repair 이후 남은 Job마다 근접 후보 8명
   (요청당 80회 budget) route 시뮬레이션 → 처음 위반된 제약과 측정값 기록
   (`SLOT_CAPACITY_EXCEEDED` / `HOME_TO_FIRST_EXCEEDED` / `WORK_LIMIT_EXCEEDED` 등).
7. 이전 세션 변경 유지: adaptive objective 정책 함수 분리, fixed 기사의
   600분 cap·이동 cap·귀가 penalty 예외 4곳 제거 (회귀 테스트 추가).

### 진단 과정 요약

- 7/30·7/31·8/6에서 slot fragmentation(미배정 2슬롯 vs 잔여 1슬롯 기사들) 실측 확인.
- UI의 "NO_FEASIBLE_ROUTE"는 솔버 출력이 아닌 UI 추정임을 확인 → 솔버 실측 진단 신설.
- 크로스 동선(예: Jason←SHARPSBURG)은 hard cap 위반이 아니라 **탐색 미수렴**으로
  확정 (Atlanta cap은 단일구간 70분/home 80분; 사용자 제안 배정이 cap 내 실현
  가능함을 좌표로 검증). Atlanta에는 OVERLAP 없음, Jason은 DMS 기사.
- "결과가 소수점까지 동일" 이슈: 솔버는 원격 서버(`/home/csda/AI_Routing/production/`,
  systemd `common-vrp`)에서 실행되며 로컬 수정은 업로드+재시작 전까지 무효.
  통계 xlsx는 export 시점 스냅샷이라 재export 필요.

### 검증 결과 (7/30 실데이터, 신버전 첫 실행 d4910836)

- 배정 슬롯 66→67 (+1), 미배정 2슬롯(NEWBORN)→1슬롯: **2슬롯 Job 구제 성공** (lexicographic 의도대로)
- 총 이동거리 1,334km→1,040km (**−22%**), 최장 route(Jason) 277km→182km (**−35%**)
- `distance_caps_relaxed=False` (1차 solve 실패→relax 가설 기각)
- 남은 미배정 1건(RNN260725043731): 자격 기사 6명 전원 슬롯 만석 — 실측 사유 확보
- 기존 솔버 테스트 91건 통과. qa-routing-reviewer 게이트 **APPROVE**
  (High 2건 지적 → step 상향·회귀 테스트로 해소)

### 미해결 / 다음 단계

- rescue 반복 + 60초 time_limit 배포 후 043731 구제 여부 확인
  (`production_assign_atlanta_vrp.py`, `common_vrp_runtime.py` 서버 업로드 필요)
- 화면 stale 이슈: latest API로 DB 저장 여부 확인 → Streamlit(8501) 재시작
- co-location extra slot capacity 초과(RUDY 9/8 등) 정책 확정 (미해결 문제 3번)
- Jason의 지리적 장거리(남부 클러스터)는 권역/기사 배치 차원 검토 필요
- 로컬 변경 커밋 권장 (서버·로컬 버전 불일치 재발 방지)

---
