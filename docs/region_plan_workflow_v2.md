# Region Plan Workflow v2

## 목적

도시별 CSV, SQL, migration package를 만들지 않는다. 모든 도시는 Deployment
Console에서 동일한 Excel 한 개를 업로드하고 다음 순서로 처리한다.

```text
Prepare common schema -> Upload -> Review -> Activation preview -> Activate
```

이 흐름은 Development 전용이다. 브라우저와 Console은 DB에 직접 연결하지 않고,
같은 서버의 loopback Region Plan API만 호출한다. Production 변경은 이 계약의 범위가
아니다.

## 입력 workbook

필수 sheet는 `Area`, `Technician`이다. 기존 호환 이름 `1. Area`,
`2. Technician`도 허용한다.

`Area` 최소 열:

- `ZIPCode`: 5자리 우편번호
- `Territory`: 지역 표시명
- `Area Type`: `DMS` 또는 `DMS2`

고급 overlap을 사용할 때만 다음 열을 명시한다.

- `membership_rank`
- `is_primary`
- `overflow_allowed`
- `overflow_penalty_minutes`
- `overflow_reason` (선택)

하나의 ZIP에는 primary가 정확히 하나여야 한다. 중복 ZIP의 alternate membership은
overflow 허용 여부와 양의 정수 penalty를 명시해야 한다. 누락값은 임의로 만들지 않는다.

`Technician` 최소 열:

- `Tech ID`
- `Assignment`
- `Tech Name`은 입력 편의를 위한 열이며 candidate artifact/DB에는 저장하지 않는다.

Assignment가 빈 행은 의도된 제외 행으로 집계한다. Tech ID 누락, 알 수 없는 지역,
중복 Tech ID, 잘못된 center type은 전체 candidate를 거절한다. 비활성 기사 행은 보존할
수 있지만 activation의 기사 수와 runtime projection에는 포함하지 않는다.

## 서버 계약

API prefix는 `/api/region-plans/v2`이다. 현재 workbook 크기에서는 동기 요청으로
처리하므로 별도 job queue나 `/jobs` endpoint를 제공하지 않는다.

| Endpoint | 동작 |
| --- | --- |
| `GET /cities` | 활성 source city와 지원 policy 목록 |
| `POST /imports` | workbook 검증, canonical candidate 및 검증 checksum 저장 |
| `POST /plans/list` | subsidiary + target city 범위의 plan 목록 |
| `POST /adopt` | 기존 candidate의 모든 child row checksum 검증 및 adoption receipt 저장 |
| `POST /plans/{id}/review` | immutable candidate 검토 및 revision 증가 |
| `POST /plans/{id}/activation-preview` | plan/roster/capability/content digest token 발급 |
| `POST /plans/{id}/activate` | 한 transaction으로 runtime master와 active pointer 교체 |
| `GET /cities/{city}/active` | subsidiary + city의 active plan 조회 |
| `POST /cities/{city}/rollback` | superseded plan을 새 activation revision으로 재활성화 |

Upload는 항상 candidate만 만든다. 자동 activate 옵션은 없다. Review, preview,
activate는 Console에서 각각 명시적으로 실행한다.

## 신뢰 경계

Mutation endpoint는 다음 조건을 모두 만족해야 한다.

- API runtime environment가 `development`
- DB가 `vrp_db_dev`
- HTTP client가 `127.0.0.1` 또는 `::1`
- server-owned principal이 `deployment-console`

외부 IP와 caller-provided principal header는 거절하거나 무시한다. 이 모델은 서버 OS
접근 권한을 가진 Development 운영자만 Console을 사용할 수 있다는 전제다.

## 무결성과 재시도

- workbook/canonical/child-row SHA-256을 기록한다.
- adoption, review, preview, activation 때 DB child-row digest를 다시 계산한다.
- 같은 idempotency key와 같은 fingerprint는 같은 결과를 재생한다.
- 같은 key에 다른 workbook/plan/preview를 사용하면 conflict다.
- artifact 저장 실패 후 같은 upload를 재시도하면 staging artifact와 receipt를 복구한다.
- 모든 조회와 변경은 `subsidiary + target city + plan` 범위로 제한한다.

## DB와 schema

운영자가 선택하는 migration ID는 없다. Console의 **Prepare common Region Plan
schema**가 `region_plan_schema_backend reconcile`을 한 번 호출한다. reconciler는 반복
실행 가능하며 V001~V004의 역사적 호환 구조와 공통 Schema v2 제약/검증 열을 한
transaction에서 맞춘다. V005 또는 도시별 schema package를 만들지 않는다.

Activation transaction은 다음을 함께 처리한다.

1. plan/content/revision/roster/capability 재검증
2. `common_region_master` projection
3. target city technician/capability projection
4. 이전 active supersede
5. 새 activation revision과 active pointer
6. city context CAS update

한 단계라도 실패하면 전체 transaction을 rollback한다. 고정 작업, DMS/DMS2 fallback,
지역 soft penalty는 선택한 versioned policy를 solver runtime이 해석하며 import API가
별도로 추측하지 않는다.

## LA 첫 acceptance 기준

`la_new_region_6_canonical_workbook.xlsx`의 기대값:

- 6 regions
- 413 unique ZIPs
- 54 accepted/active technicians
- blank Assignment 제외 6행
- policy `active_roster_area_type_fallback_region_soft/v1`

기존 DB candidate도 동일한 일반 adoption 절차를 사용한다. LA 전용 SQL이나 checksum을
코드에 하드코딩하지 않는다. adoption receipt에 실제 DB child-row digest를 남긴 뒤에만
Review가 가능하다.
