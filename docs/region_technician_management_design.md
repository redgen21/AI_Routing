# Region·Technician 관리 기능, 테이블 및 마이그레이션 설계

작성일: 2026-08-11  
범위: `area_map`, Deployment Console/Admin Tools, Region Plan v2, `vrp_client`

## 1. 결론

세 구성요소의 책임을 다음처럼 고정한다.

```text
Area Map       후보 생성·공간 검증
Admin Tools    승인·DB 적용·버전·활성화·rollback
VRP Client     Active 데이터 조회·Job 라우팅 실행
```

Region과 Technician의 기준 데이터는 Admin Tools에서만 DB에 적용한다. Area Map은
후보 파일을 만들고, VRP Client는 Active Plan과 Technician Master를 읽기만 한다.

## 2. 기능 책임

### 2.1 Area Map

Area Map은 로컬 Region 설계·검증 도구다.

담당 기능:

- 법인·도시·Plan ID 선택
- Region CSV 및 Technician–Region assignment CSV 업로드
- 우편번호, Region, 기사 배정 지도 표시
- 빈 Region, 중복 우편번호, DMS/DMS2 불일치 검증
- 후보 Plan 생성
- canonical bundle과 manifest 생성
- `data/region_plans/`에 저장
- Admin Tools에 전달할 ZIP 다운로드

하지 않는 기능:

- Production DB 직접 수정
- Active Plan 활성화
- Technician Master 직접 수정
- Solver 실행

Area Map의 Technician CSV는 기사 프로필이 아니라 Region 배정 입력이다. 주소,
좌표, 센터, 활성 여부, capability가 없는 파일만으로 Technician Master를 만들지
않는다.

### 2.2 Admin Tools / Console

Admin Tools artifact 자체는 서버에서 DB 관리 명령을 실행하는 immutable release다.
Console의 Data Administration은 다음 workflow를 제공한다.

#### Technician Master

- 전체 Technician profile 업로드
- 주소·좌표·센터·활성 여부·우선순위 관리
- 제품·수리 capability 관리
- preview → apply → receipt
- 신규 기사 등록 및 기존 기사 업데이트

`common_technician_master`와 `common_technician_capability_master`는 이 전용
workflow에서만 변경한다. 일반 CSV upsert에서 같은 테이블을 중복 노출하지 않는다.

#### Region Plans

- Area Map bundle 업로드
- 법인·도시·Plan ID·checksum 검증
- Region/Postal/Technician 참조 무결성 검증
- candidate 저장
- review
- activation preview
- active Plan 지정
- 이전 Plan supersede 및 rollback
- runtime projection 갱신

#### 기타 DB Master

Region과 Technician workflow에 속하지 않는 allowlist 데이터만 일반 CSV
preview/apply로 관리한다.

- `common_heavy_repair_rule_master`
- 승인된 routing configuration master
- 기타 명시된 참조 Master

임의 SQL, 임의 테이블, 임의 파일 경로는 콘솔에서 받지 않는다.

### 2.3 VRP Client

VRP Client는 운영 라우팅 화면이다.

담당 기능:

- 법인·도시·날짜·routing mode 선택
- Active Region Plan 및 Technician Master 조회
- Job upload/edit/save
- 주소 변경 geocoding 및 postal fallback
- Routing request 생성과 polling
- 결과, KPI, 지도, 다운로드

Region Plan, Technician Master, capability, routing config를 직접 수정하지 않는다.
라우팅 request/result에는 다음 버전을 저장한다.

```text
region_plan_id
region_plan_revision
region_plan_checksum
technician_roster_checksum
routing_config_version
```

## 3. 목표 DB 구조

### 3.1 도시 기준

#### `common_city_context`

법인과 도시를 구분하는 상위 context다.

```text
subsidiary_name
strategic_city_name
source_strategic_city_name
context_version
policy_version
verification_only
context_status              -- candidate/reviewed/active/retired
activation_revision
created_at
updated_at
```

기본키는 `(subsidiary_name, strategic_city_name)`이다.
`Atlanta, GA`와 `Atlanta_6area`처럼 원본 roster 도시와 정책 도시가 다르면
`source_strategic_city_name`으로 연결한다.

### 3.2 Region Plan

#### `common_region_plan`

Plan header와 원본 lineage를 저장한다.

```text
subsidiary_name
strategic_city_name
plan_id
schema_version
policy_version
plan_status                  -- candidate/reviewed/active/superseded/rejected
source_file_name
source_sha256
manifest_sha256
bundle_sha256
fixed_region_sha256
boundary_policy_sha256
technician_policy_sha256
membership_input_rows
membership_accepted_rows
membership_rejected_rows
unique_postal_count
technician_count
ambiguous_postal_count
import_idempotency_key
imported_by
reviewed_by
review_reference
reviewed_at
created_at
updated_at
```

기본키는 `(subsidiary_name, strategic_city_name, plan_id)`이다.

#### `common_region_plan_region`

```text
subsidiary_name
strategic_city_name
plan_id
region_seq
region_id
region_name
source_territory
required_center_type          -- DMS/DMS2
created_at
```

#### `common_region_plan_postal`

최종 우편번호–Region 배정이다.

```text
subsidiary_name
strategic_city_name
plan_id
postal_code
region_seq
area_type                     -- DMS/DMS2
source_membership_count
resolution_status              -- not_required/pending/resolved
source_region_seqs
resolution_metadata
created_at
```

현재는 원본 중복 Region을 `source_region_seqs` JSON으로 보존한다. 원본 행 단위
lineage가 필요하면 다음 보조 테이블을 추가한다.

#### `common_region_plan_postal_source`

```text
subsidiary_name
strategic_city_name
plan_id
postal_code
source_region_seq
source_row_number
```

#### `common_region_plan_technician`

Plan별 기사–Region 배정이다. 기사 이름과 주소는 저장하지 않고
`employee_code`만 저장한다.

```text
subsidiary_name
strategic_city_name
plan_id
employee_code
assigned_region_seq
policy_mode
active_flag
created_at
updated_at
```

#### `common_region_plan_boundary_overflow`

```text
subsidiary_name
strategic_city_name
plan_id
postal_code
primary_region_seq
alternate_region_seq
allow_overflow
penalty_cost
rationale
policy_version
created_at
updated_at
```

#### `common_region_plan_activation`

도시별 active Plan은 하나만 허용한다.

```text
subsidiary_name
strategic_city_name
activation_revision
plan_id
plan_revision
active_flag
preview_digest
idempotency_key
activated_by
activation_reference
activated_at
superseded_at
created_at
```

### 3.3 Technician

#### `common_technician_master`

```text
subsidiary_name
strategic_city_name
employee_code
employee_name
center_type
home_address
home_city
home_state
home_country
home_postal_code
home_latitude
home_longitude
active_flag
priority_group
max_home_to_job_min
created_at
updated_at
```

#### `common_technician_capability_master`

```text
subsidiary_name
strategic_city_name
employee_code
product_group_code
product_code
repair_allowed
heavy_repair_allowed
priority_score
effective_start_date
effective_end_date
```

### 3.4 Runtime projection 및 기타 테이블

`common_region_master`는 Active Region Plan에서 생성되는 runtime projection이다.
사용자가 직접 CSV로 수정하지 않는다.

```text
common_region_master
  subsidiary_name
  strategic_city_name
  postal_code
  region_seq
  region_name
  area_type
```

기타 관리 대상은 다음과 같다.

```text
common_heavy_repair_rule_master
common_routing_config_master
common_avoid_area
```

다음 운영 테이블은 Region/Technician 관리 대상이 아니다.

```text
common_job_input
common_request_technician_input
common_routing_request
common_routing_result
common_geocode_cache
common_geocode_attempt_log
common_geocode_daily_log
```

## 4. 표준 파일 및 저장 위치

### 4.1 Area Map 후보 파일

사람이 업로드하는 입력은 다음 형식을 사용한다.

Region:

```text
POSTAL_CODE
STRATEGIC_CITY_NAME
region_id
region_seq
AREA_NAME
new_region_name
area_type
```

Technician assignment:

```text
Tech ID
Tech Name
Assignment
```

UI에서 선택한 법인·도시·Plan ID를 시스템이 주입하고, 정규화한 결과를 다음에
저장한다.

```text
data/region_plans/<subsidiary>/<strategic_city>/<plan_id>/
├── source/regions.csv
├── source/technician_assignments.csv
├── normalized/regions.csv
├── normalized/region_postal.csv
├── normalized/technician_assignments.csv
├── manifest.json
└── plan_bundle.zip
```

### 4.2 Technician Profile 파일

Region assignment와 분리된 전체 profile 파일을 사용한다.

```text
employee_code
employee_name
center_type
home_address
home_city
home_state
home_country
home_postal_code
home_latitude
home_longitude
active_flag
priority_group
max_home_to_job_min
```

Capability 적용은 Technician Profile transaction과 함께 처리한다.

## 5. 생성·업로드·업데이트 전체 흐름

### 5.1 새 도시 또는 새 Region Plan

```text
Area Map에서 법인·도시·Plan ID 선택
  → Region/assignment 업로드
  → 지도·품질 검증
  → candidate bundle 생성
  → Admin Tools 업로드
  → checksum/참조 무결성 검증
  → common_region_plan* candidate 저장
  → review
  → activation preview
  → active Plan 활성화
  → common_region_master projection 갱신
  → VRP Client에서 조회
```

### 5.2 새 Technician

```text
Admin Tools > Technician Master
  → profile 업로드
  → 컬럼/중복/좌표/센터 검증
  → 현재 DB와 diff
  → preview
  → apply
  → technician/capability master 갱신
  → Region Plan assignment에 employee_code 추가
  → 새 Plan review/activation
```

Technician Master에 없는 기사를 Region Plan에 먼저 등록할 수 없다.

### 5.3 Technician 정보 업데이트

주소·좌표·센터·활성 여부 변경은 profile workflow에서 수정한다. 이 작업은
Region 배정을 자동으로 바꾸지 않는다. Region도 변경하려면 새 Plan을 만든다.

### 5.4 Region 업데이트

Active Plan을 직접 수정하지 않는다.

```text
atlanta_6area_v001 (active)
  → atlanta_6area_v002 (candidate)
  → review/activation
  → v002 active, v001 superseded
```

### 5.5 기타 DB Master 업데이트

```text
CSV 업로드
  → 스키마/행 검증
  → preview
  → apply
  → receipt
```

Region Plan과 Technician Master 테이블은 일반 CSV 경로에서 제외한다.

### 5.6 VRP Client 조회

```text
city context
  → active activation
  → active region plan
  → region/postal/technician assignment
  → technician master/capability
  → routing request
```

## 6. 기존 DB에서의 마이그레이션

### 6.1 현재 legacy 데이터

현재 legacy runtime의 핵심 테이블은 다음과 같다.

```text
common_region_master
common_technician_master
common_technician_capability_master
common_routing_config_master
common_heavy_repair_rule_master
```

기존 `common_region_master`는 도시·우편번호 기준의 최종 projection이고,
Region Plan의 원본 checksum·review·activation 이력이 없다. 따라서 이것을
그대로 active Plan으로 표시하면 안 된다.

### 6.2 마이그레이션 원칙

- 기존 DB를 먼저 백업한다.
- migration tool은 읽기 전용 inventory와 bundle만 생성한다.
- DB에 바로 쓰지 않는다.
- 기사–Region assignment가 확인되지 않으면 자동으로 만들지 않는다.
- 누락·중복·area type 불일치는 `needs_review`로 남긴다.
- 검토된 bundle만 Region Plan candidate로 import한다.
- 기존 legacy table은 검증이 끝날 때까지 삭제하지 않는다.

현재 제공된 `tools/data/migrate_legacy_region_plans.py`는 이 원칙에 따라
다음 bundle을 생성한다.

```text
regions.csv
region_postal.csv
technician_assignments.csv
rejects.csv
manifest.json
```

### 6.3 자동 변환 대상

#### Region

`common_region_master`에서 다음을 추출한다.

```text
subsidiary_name
strategic_city_name
postal_code
region_seq
region_name
area_type
```

Region별 고유 `region_id`가 없으면 다음 규칙으로 임시 ID를 생성하고
`needs_review` 또는 migration manifest에 기록한다.

```text
<city_slug>_r<region_seq:02d>
```

#### Technician Master

기존 `common_technician_master`는 그대로 보존하면서 profile 기준 데이터로
검증한다.

```text
employee_code
employee_name
center_type
home_address
home_postal_code
home_latitude
home_longitude
active_flag
```

#### Capability

`common_technician_capability_master`는 capability profile로 연결한다. 이
데이터는 Region Plan의 child row가 아니다.

#### Routing Config와 기타 Master

Region Plan migration과 분리하여 기존 값을 checksum과 함께 inventory한다.

### 6.4 반드시 사람 검토가 필요한 항목

- legacy Region 이름과 `region_id`가 대응되지 않는 경우
- 같은 우편번호가 여러 Region에 존재하는 경우
- `area_type`가 비어 있거나 `DMS/DMS2`가 아닌 경우
- Technician의 Region assignment 원본이 없는 경우
- Technician Master에는 있지만 assignment 파일에는 없는 기사
- assignment 파일에는 있지만 Technician Master에는 없는 기사
- inactive 기사 배정
- 하나의 Plan에 필요한 Region이 비어 있는 경우
- 도시명이 `Atlanta, GA`, `Atlanta_6area`, `Atlanta_6area_new`처럼 분리된 경우

### 6.5 기존 도시 migration 절차

```text
1. catalog/config에서 대상 도시 목록 수집
2. common_region_master inventory 생성
3. technician/capability inventory 생성
4. Region/Postal bundle 생성
5. Technician assignment source 탐색
6. rejects.csv와 needs_review 목록 생성
7. 운영자가 Region/기사 매핑 검토
8. common_city_context candidate 생성
9. Region Plan candidate import
10. child row checksum 검증
11. review
12. activation preview
13. active Plan 활성화
14. common_region_master projection 비교
15. 기존 runtime 결과와 회귀 검증
16. 일정 기간 legacy 원본 보존 후 read-only 전환
```

### 6.6 Projection 비교

활성화 전후 다음 결과가 같거나, 차이가 승인되어야 한다.

```text
legacy common_region_master
        vs
new active plan → generated common_region_master
```

비교 항목:

- 도시·법인
- 우편번호 수
- Region 수
- 우편번호별 region_seq
- area_type
- 기사 수
- 기사별 assignment
- 누락·추가·변경 행

차이가 있으면 activation을 중단하고 `migration_diff.csv`로 남긴다.

## 7. 현재 구현과 목표 설계의 불일치

### 7.1 Technician 수 13명 대 코드 14명 — High

사용자가 제시한 Atlanta assignment는 13명이다. 그러나 현재
`tools/data/atlanta_6area_plan.py`의 `EXPECTED_TECHNICIAN_ROWS`와
`admin_tools/db/technician_profile_backend.py`는 14명을 요구한다.

따라서 현재 파일을 그대로 사용하면 bundle 또는 profile preview가 실패할 수
있다. 하드코딩 14를 제거하고 Plan의 검증된 technician count와 active roster를
비교해야 한다. 누락 기사를 자동 생성해서는 안 된다.

### 7.2 Atlanta 전용 parser와 공통 도시 설계가 충돌 — High

현재 parser에는 `Atlanta_6area`, source city `Atlanta, GA`, 고정 Region 파일,
고정 boundary ZIP, 고정 기사 수가 남아 있다. 공통 도시 기능을 제공하려면 이
조건은 Atlanta adapter로 격리하고 공통 parser는 동적으로 만들어야 한다.

### 7.3 Technician Master 중복 write path — High

현재 다음 경로가 동시에 존재한다.

- 일반 `common_technician_master` CSV upsert
- Technician profile backend
- Region Plan activation 과정의 technician/capability projection 동기화

목표는 다음 두 write path만 허용하는 것이다.

```text
Technician 정보 변경 → Technician Profile workflow
Region 배정 변경     → 새 Region Plan workflow
```

### 7.4 `common_region_master` 직접 업로드 위험 — Medium

Region Plan activation이 `common_region_master`를 재생성하므로 일반 CSV 업로드
목록에서는 read-only로 고정해야 한다.

### 7.5 입력 CSV와 canonical bundle 포맷 차이 — Medium

사용자 CSV는 간단한 입력 포맷이고 현재 bundle은 manifest, checksum, boundary
policy, technician policy를 포함한다. 다음 변환 계층이 필요하다.

```text
사용자 CSV → Area Map normalization → canonical bundle → Admin Tools validation → DB
```

### 7.6 Migration UI와 실제 backend 연결 미완성 — Medium

콘솔에 일반 Migration 영역은 있으나 현재 `console_backend.py`의 legacy
migration 호출은 비활성화되어 있다. Region Plan schema reconciliation은 별도
Region Plan backend 경로다. 일반 Migration을 제공하려면 원격 CLI/receipt 경로를
추가하고, 그렇지 않으면 UI에서 실행 가능한 것처럼 표시하지 않는다.

### 7.7 Production 적용 제한 — High

현재 master CSV, Technician profile, Region Plan workflow는 Development 중심으로
제한되어 있고 Production write는 차단되어 있다. Production 적용을 허용하려면
backup, 승인, 권한, rollback, receipt를 포함한 별도 promotion 절차가 필요하다.

## 8. 구현 순서

1. migration inventory와 legacy bundle을 모든 도시에서 생성한다.
2. `needs_review` 항목을 도시별로 검토한다.
3. 14명 하드코딩을 동적 roster 검증으로 교체한다.
4. Atlanta 전용 parser와 공통 parser를 분리한다.
5. 일반 CSV의 Technician Master write를 숨긴다.
6. Technician Profile을 유일한 Technician Master write path로 지정한다.
7. `common_region_master`를 read-only projection으로 전환한다.
8. 사용자 CSV → canonical bundle 변환기를 고정한다.
9. City Context와 Plan checksum/version을 연결한다.
10. Region Plan activation을 projection 갱신과 하나의 transaction으로 보장한다.
11. VRP request/result에 Plan·roster·config version을 저장한다.
12. legacy projection과 active Plan projection을 비교한다.
13. Development 회귀검증 후 Production promotion/rollback을 별도로 구현한다.

## 9. 완료 조건

- 법인·도시별 City Context가 존재한다.
- Plan마다 source/bundle checksum이 있다.
- Region에 빈 Zone이나 중복 Region이 없다.
- 모든 postal이 승인된 Region에 연결된다.
- 모든 Plan Technician이 활성 Technician Master에 존재한다.
- Technician 이름·주소는 Plan에 중복 저장되지 않는다.
- 도시별 Active Plan은 하나만 존재한다.
- `common_region_master`는 Active Plan에서만 생성된다.
- 일반 CSV와 전용 workflow가 같은 테이블을 중복 수정하지 않는다.
- 기존 DB에서 migration diff와 rejects를 확인할 수 있다.
- VRP 결과에 사용한 Plan/roster/config 버전을 재현할 수 있다.
- 신규 Plan은 rollback 가능한 activation 이력을 가진다.
- Production 적용은 preview, 승인, backup, receipt, rollback을 갖는다.

## 10. 테이블 참조 파일 전수 목록

2026-08-11 기준으로 Region/Technician 관련 테이블명과 API 호출을 검색한 결과다.
직접 SQL을 실행하는 파일과, API 또는 catalog 파일을 통해 간접적으로 데이터를
사용하는 파일을 구분한다.

### 10.1 Area Map 및 로컬 지도

Area Map 소스에는 해당 DB 테이블명을 직접 조회하는 SQL은 없다. Area Map은
catalog가 지정한 CSV/XLSX와 Region Plan API를 사용한다.

- `smart_routing/area_map.py`
  - local service/profile/region 파일과 ZIP geometry 로드
  - legacy region seed 및 catalog 경로 의존
- `smart_routing/area_map_usa.py`
  - USA 도시 map data 및 region 파일 로드
- `sr_area_map.py`
  - `area_map.py` facade
  - Active Atlanta Region Plan API 조회
  - Region/Technician policy CSV 다운로드 및 지도 표시
- `sr_area_map_asia.py`
  - Asia 지도·날짜·기사별 표시
  - 향후 같은 canonical Region Plan 파일을 사용하도록 연결 필요

Area Map migration 시 위 파일의 파일명 탐색, `STRATEGIC_CITY_NAME` 매핑,
Atlanta 고정값, legacy seed fallback을 확인해야 한다.

### 10.2 VRP Client 및 공통 runtime

- `sr_common_vrp_client.py`
  - map data 로드
  - `/api/v1/common/engineers/*`를 통한 Technician Master 직접 수정 UI
  - request Technician draft 저장
- `sr_common_vrp_client_server.py`
  - 서버 API에서 Technician Master를 읽고 수정
  - `/api/v1/common/technicians/replace`를 통한 날짜별 request roster 저장
  - routing payload와 Active Plan 정보 표시
- `sr_common_vrp_client_asia.py`
  - Technician Master 직접 수정 UI가 존재하므로 동일한 정책 정리 필요
- `smart_routing/common_vrp_api_server.py`
  - `/api/v1/common/engineers/upsert`
  - `/api/v1/common/engineers/delete`
  - `/api/v1/common/technicians/replace`
  - `/api/v1/common/routing-config/upsert`
  - `/api/v1/common/avoid-areas/*`
- `smart_routing/common_vrp_db.py`
  - `common_region_master` schema/legacy projection
  - `common_technician_master` CRUD 및 seed
  - `common_technician_capability_master` CRUD/seed
  - `common_region_plan*` active snapshot 조회
  - request-level Technician roster 조회/교체
- `smart_routing/common_vrp_runtime.py`
  - Active Region Plan snapshot을 routing payload에 적용
  - `region_plan_id`, policy, unassigned marker, execution metadata 기록

현재 VRP Client의 Technician Master 편집은 목표 설계와 충돌한다. 반면
날짜별 `common_request_technician_input` 편집은 routing 실행용 operational
roster이므로 별도 기능으로 유지할 수 있다. 두 기능을 UI에서 명확히 분리해야 한다.

### 10.3 Region Plan API

- `services/api/region_plan_v2.py`
  - candidate import
  - city registry
  - Region Plan 목록/상세
  - review/activation/rollback 요청
  - `common_city_context`
  - `common_region_plan*`
  - legacy `common_region_master`와 Technician Master를 registry fallback으로 조회
- `services/api/region_plan_repository_v2.py`
  - Region Plan child rows 조회
  - review/activation transaction
  - active Plan으로 `common_region_master` projection 생성
  - target city Technician/capability projection 동기화

### 10.4 Console 및 Admin Tools

- `deployment_console_ui/app.py`
  - Admin Tools artifact 화면
  - DB overview/master CSV 화면
  - Technician managed-data 화면
  - Region Plans v2 화면
- `services/deploy/console_backend.py`
  - remote Admin Tools release pin
  - master overview/spec/preview/apply/receipt
  - Region Plan schema/workflow bridge
  - Technician profile bridge
  - 현재 일반 migration compatibility entrypoint
- `admin_tools/db/master_data_backend.py`
  - 13개 DB table registry
  - `common_technician_master`와 `common_heavy_repair_rule_master` 개발 write
  - region master, capability, request/result/log/cache read-only
- `admin_tools/db/technician_profile_backend.py`
  - Technician profile/capability preview/apply
  - active Region Plan assignment과 roster 검증
- `admin_tools/db/region_plan_backend.py`
  - Region Plan bundle validate/import/review/activation/rollback
- `admin_tools/db/region_plan_schema_backend.py`
  - Region Plan schema reconciliation
- `admin_tools/db/region_plan_schema_v2.sql`
  - common schema v2 additive constraints/columns/index policy
- `admin_tools/db/common_vrp.py`
  - legacy schema/preflight/seed/upsert helper
  - 기존 `common_region_master` 직접 upsert 경로를 정리해야 함
- `admin_tools/db/seeds/build_la_bucket_vrp_inputs.py`
  - LA candidate 입력 및 Technician seed
- `admin_tools/db/seeds/import_asia_technician_centroids.py`
  - Asia Technician/capability/region seed

### 10.5 변환기·Migration·검증 파일

- `tools/data/migrate_legacy_region_plans.py`
  - legacy city → reviewable Region Plan migration bundle
- `tools/data/build_region_plan_import.py`
  - Region Plan import SQL 생성
- `tools/data/technician_profile_data.py`
  - Technician profile canonicalization
- `admin_tools/db/migrations/V001__atlanta_6area_region_plan.sql`
  - Region Plan v2 base schema
- `admin_tools/db/migrations/V002__region_plan_unbounded_region_seq.sql`
  - Region sequence 1..6 제한 제거
- `admin_tools/db/migrations/V003__region_plan_technician_source_id.sql`
  - Technician source ID 허용 범위 확대
- `admin_tools/db/migrations/V004__region_plan_area_type_region_soft.sql`
  - DMS/DMS2 및 policy mode 확장
- `260310/insert_atlanta_6area_active_plan_vrp_db_dev.sql`
  - Atlanta 개발 DB active plan fixture/import
- `260310/upsert_atlanta_6area_common_region_master_vrp_db_dev.sql`
  - legacy Region projection 직접 upsert SQL
- `260310/upsert_atlanta_6area_technicians_vrp_db_dev.sql`
  - legacy Technician/capability 직접 upsert SQL
- `260310/atlanta_6area_legacy_import_manifest.json`
  - legacy import lineage/제약 기록

### 10.6 Schema snapshot 및 테스트

- `log/vrp_db_schema.sql`
- `log/vrp_db_dev_schema.sql`
- `tests/test_region_plan_backend.py`
- `tests/test_region_plan_v2_api.py`
- `tests/test_region_plan_schema_v2.py`
- `tests/test_technician_profile_backend.py`
- `tests/test_master_data_backend.py`
- `tests/test_deployment_console_master_admin.py`
- `tests/test_deployment_console_ui.py`
- `tests/test_build_region_plan_import.py`
- `tests/test_db_admin_transaction.py`

Snapshot SQL과 기존 260310 SQL은 운영 migration source가 아니라 legacy
reference/fixture로 취급한다. 새 DB 변경은 reviewed migration 또는 Region Plan
schema reconciler에만 추가한다.

## 11. DB 구조 수정이 필요한 목록

### 11.1 반드시 적용할 Schema v2 정합성 변경

현재 V001 단독 실행은 Atlanta 전용 제약을 남긴다. V001~V004를 개별 실행하는
대신 `admin_tools/db/region_plan_schema_v2.sql`의 reconciler를 기준으로 적용한다.

필요한 변경:

- `common_region_plan.verified_content_sha256` 추가
- `common_region_plan.verified_at` 추가
- `common_region_plan.verified_by` 추가
- `common_region_plan_region.region_seq > 0`으로 변경
- `common_region_plan_region.required_center_type` 추가
- `common_region_plan_technician.employee_code` 일반 source ID 허용
- Region Plan technician `policy_mode` 확장
- `common_region_plan_postal.source_membership_count > 0` 허용
- 중복 membership resolution constraint 정리
- `common_region_plan_boundary_overflow` 기본키에 `alternate_region_seq` 포함
- Region Plan 테이블에 필요한 `vrp_agent` 권한 부여

### 11.2 신규로 추가할 것을 검토할 테이블

#### `common_region_plan_postal_source`

현재 `source_region_seqs` JSON만으로 보관하는 원본 membership을 정규화한다.
중복 ZIP, 원본 행, 검토 근거를 정확히 재현하려면 추가하는 것이 좋다.

#### `common_region_plan_event`

Plan import/review/activation/rollback을 DB에서도 추적한다.

```text
event_id
subsidiary_name
strategic_city_name
plan_id
event_type
from_status
to_status
actor
reference
content_sha256
created_at
```

현재 일부 receipt는 state 파일에만 있으므로, 운영 감사가 필요하면 DB event
기록을 추가한다.

#### `common_technician_profile_revision`

Technician profile 변경과 rollback을 추적한다.

```text
revision_id
subsidiary_name
strategic_city_name
roster_version
source_sha256
capability_sha256
status
created_by
approved_by
created_at
activated_at
```

최소 구현에서는 기존 master에 source version/checksum과 변경 receipt를 추가하고,
원본 profile artifact를 재적용하여 rollback할 수 있다. 완전한 row-level rollback이
필요하면 revision child table까지 별도로 둔다.

#### `common_region_projection_state`

`common_region_master`가 어느 Active Plan에서 생성됐는지 보존한다.

```text
subsidiary_name
strategic_city_name
plan_id
plan_revision
bundle_sha256
generated_at
generated_by
```

### 11.3 기존 테이블의 의미를 변경할 목록

- `common_region_master`: 직접 Master → Active Plan의 read-only projection
- `common_technician_master`: 일반 CSV/VRP Client 직접수정 → Technician Profile 전용 write
- `common_technician_capability_master`: Region Plan이 직접 소유하지 않고 Technician Profile이 소유
- `common_region_plan_technician`: 기사 profile 저장 금지, `employee_code`와 Region assignment만 저장
- `common_request_technician_input`: VRP 날짜별 operational roster로 유지
- `common_routing_config_master`: Region Plan과 분리된 city policy master로 유지

### 11.4 DB 외 코드 수정 목록

#### Area Map

- `smart_routing/area_map.py`
- `smart_routing/area_map_usa.py`
- `sr_area_map.py`

legacy region 파일 fallback보다 `data/region_plans`의 canonical bundle과 Active
Plan API를 우선하도록 한다. Atlanta 고정 Plan ID와 기사 수 검증을 제거하고
도시 context에서 동적으로 읽는다.

#### VRP Client/API

- `sr_common_vrp_client.py`
- `sr_common_vrp_client_server.py`
- `sr_common_vrp_client_asia.py`
- `smart_routing/common_vrp_api_server.py`
- `smart_routing/common_vrp_db.py`
- `smart_routing/common_vrp_runtime.py`

Technician Master dialog와 `/api/v1/common/engineers/upsert|delete`는 Admin Tools
전용 workflow로 이동하거나 관리자 권한으로 제한한다. VRP Client에 남길 것은
`common_request_technician_input`의 날짜별 운영 roster 편집이다.

Active Plan snapshot, Plan checksum, roster checksum을 payload와 result에 연결한다.

#### Console/Admin Tools

- `deployment_console_ui/app.py`
- `services/deploy/console_backend.py`
- `admin_tools/db/master_data_backend.py`
- `admin_tools/db/technician_profile_backend.py`
- `admin_tools/db/region_plan_backend.py`
- `admin_tools/db/common_vrp.py`

일반 CSV의 Technician Master 항목을 전용 Technician Profile 화면으로 이동하고,
`common_region_master`를 read-only로 표시한다. 일반 migration UI는 실제 원격
CLI가 준비되기 전까지 숨기거나 명시적으로 unavailable로 표시한다.

#### 변환·Migration

- `tools/data/migrate_legacy_region_plans.py`
- `tools/data/build_region_plan_import.py`
- `tools/data/technician_profile_data.py`

모든 도시에서 `regions.csv`, `region_postal.csv`,
`technician_assignments.csv`, `rejects.csv`, `manifest.json`을 생성하고,
`needs_review` 항목을 자동 승인하지 않도록 한다.

## 12. 마이그레이션 실행 순서

```text
1. DB backup 및 schema snapshot 생성
2. V001 기반 존재 여부 확인
3. region_plan_schema_v2 reconciler 실행
4. legacy 도시/Region/Technician inventory 생성
5. migration bundle과 rejects 생성
6. 법인·도시·source city 검토
7. Technician Master/capability 기준 roster 검증
8. Region Plan candidate import
9. child row 및 checksum 검증
10. legacy projection과 candidate projection diff 생성
11. 운영자 review
12. activation preview
13. Development DB activation
14. VRP Client/API/Area Map 회귀검증
15. Production promotion 승인
16. Production activation 및 projection 비교
17. 문제 시 이전 activation revision으로 rollback
18. 검증 완료 후 legacy direct write 경로 read-only 전환
```

## 13. Migration 완료 조건

- Area Map, Console, VRP Client가 같은 Plan ID/checksum을 사용한다.
- 세 실행면에서 Region/Technician 참조 경로가 문서와 일치한다.
- VRP Client의 Technician Master 직접 수정 경로가 제거 또는 관리자 전용으로 제한된다.
- legacy `common_region_master`와 Active Plan projection의 diff가 0이거나 승인된다.
- 모든 legacy 도시에 `common_city_context`가 생성된다.
- assignment 누락·중복·inactive 기사가 rejects/needs_review로 남는다.
- 13명/14명과 같은 기사 수 불일치가 하드코딩이 아니라 데이터 검증 결과로 처리된다.
- DB migration, source bundle, runtime projection, API response, client display에
  Plan version과 checksum이 연결된다.
## 2026-08-11 설계 보정: 도시와 Region Plan의 분리

이 부록은 문서 앞부분의 이전 설명보다 우선한다. `Atlanta, GA`는 물리적·운영
도시이며 `Atlanta_6area`는 별도 도시가 아니라 Region Plan의 표시명/정책 식별자다.
따라서 VRP Client의 도시 선택에는 `Atlanta, GA`만 노출하고, `Atlanta_3area`,
`Atlanta_6area` 등은 해당 도시 Config에서 선택하는 Plan으로 취급한다.

### VRP Client 변경

- Technician Master의 기존 추가·수정·삭제·CSV 업로드 기능은 유지한다.
- Technician Master 편집 표와 추가/수정 폼에 `Region` 선택을 추가한다.
- 선택된 도시 Config의 `region_plan_id`에 속한 Region만 선택할 수 있다. 저장 시
  프로필은 `common_technician_master`에, Plan별 배정은
  `common_region_plan_technician`에 저장한다.
- 날짜별 운영 roster(`common_request_technician_input`)의 편집과 영구 Technician
  Master/Plan 배정은 서로 다른 기능으로 유지한다.
- 라우팅 화면에는 Plan ID 선택기를 두지 않는다. 사용자는 먼저 `Atlanta, GA`를
  고르고 Config 탭에서 Plan을 저장한 뒤 Jobs/Technicians에서 라우팅한다.

### Config와 DB 계약

`common_routing_config_master`에 다음 개발 DB 컬럼을 추가한다.

```text
region_plan_id
region_plan_revision
region_plan_checksum
```

Config 저장 시 Plan ID가 해당 물리 도시에 등록된 Plan인지, revision/checksum이
최신 immutable Plan과 일치하는지 API가 검증한다. 기존 레거시 Plan이
`Atlanta_6area` context에 저장되어 있어도 `common_city_context.source_strategic_city_name`
이 `Atlanta, GA`이면 호환 조회할 수 있다. 이 필드는 도시를 새로 만드는 식별자가
아니라 레거시 Plan 저장 위치를 찾기 위한 연결 정보다. 신규 데이터는 physical city
context 하나와 그 하위 Plan ID들을 기준으로 만든다.

### 개발 우선 적용

이번 변경의 DB 대상은 `vrp_db_dev`뿐이다. Production DB/config에는 migration,
Plan 선택, Technician Region 배정을 적용하지 않는다. 개발 검증 항목은 다음과 같다.

1. Config에서 `Atlanta, GA`의 Plan을 저장하고 revision/checksum이 기록되는지 확인
2. Technician Master에서 Region을 바꾸고 해당 Plan child row가 바뀌는지 확인
3. 다른 Plan의 assignment와 Technician profile이 변하지 않는지 확인
4. Routing Request 화면에 Plan 선택이 없고 저장된 Config Plan이 payload/result에
   기록되는지 확인
5. Config가 없는 기존 도시는 기존 active-plan fallback으로 동작하는지 확인
6. 개발 검증 완료 후에만 Production migration/배포 계획을 별도로 승인
