# File Management · Data Management · Domain Import 상세 설계

## 1. 확정 원칙

- Development 파일은 artifact/release를 만들지 않고 **선택 파일만 직접 업로드**한다.
- Admin Tools는 `/home/csda/AI_Routing/admin_tools/` 단일 공유 경로만 사용한다. `releases/`, release pin, Admin Tools artifact hash 비교는 일상 운영에서 제거한다.
- Production runtime만 clean Git revision 기반 immutable package, manifest 검증, promotion/rollback을 사용한다.
- File Management는 파일 inventory와 전송만, Data Management는 DB/SQL/migration만, Domain Import는 업무 파일 검증·적재만 담당한다.
- Region Plan은 Area Map workbook을 DB에 업로드·조회·삭제만 한다. Routing policy는 저장하지 않고 VRP Client가 Plan과 별도로 선택한다.

## 2. 경로와 재시작 기준

| 대상 | 경로 | 적용 | 재시작 |
|---|---|---|---|
| Admin Tools | `/home/csda/AI_Routing/admin_tools/` | 파일 직접 업로드 | 실행 중인 프로세스가 import할 때만 해당 프로세스 재시작 |
| Development API/runtime | `/home/csda/AI_Routing/development/` | 파일 직접 업로드 | 서버 Python 변경 시 `sudo systemctl restart common-vrp-dev.service` |
| Local Console | 로컬 `deployment_console_ui/`, `sr_deployment_console.py` | 로컬 저장 | `streamlit run .\sr_deployment_console.py` |
| Production | `/home/csda/AI_Routing/production/` | 검증 package만 | Production 절차 |

Development 서비스의 실제 config는 systemd가 지정한 파일이다.

```bash
systemctl cat common-vrp-dev.service | grep -E -- '--config|common_vrp'
# /home/csda/AI_Routing/development/config_common_vrp.dev.json
```

## 3. 책임과 메뉴

```text
File Management
  local/remote 파일 inventory, 변경 탐지, 선택 업로드
Data Management
  DB Explorer, SQL Console, SQL Files, Migration, 실행 이력
Domain Import
  파일 계약 검증, staging, preview, apply, row accounting
Area Map
  Region/Technician 원본으로 workbook 생성
Region Plans
  workbook DB 업로드, 목록, 삭제
VRP Client
  Region Plan + Routing policy 선택, 라우팅 및 결과 표시
```

UI는 DB에 직접 접속하거나 solver 내부를 호출하지 않는다. 서버 API 또는 allowlist된 Admin Tools 명령만 DB write와 서버 작업을 수행한다.

## 4. File Management

### 4.1 화면

최상위 메뉴를 다음과 같이 둔다.

```text
File Management
  ├─ Admin Tools
  ├─ Development Runtime
  ├─ Managed Source Files
  └─ Production Package
```

Development scope 화면은 탐색기 구조다.

1. 좌측: 로컬 폴더 tree와 파일 목록
2. 중앙: `선택 업로드 >`, `변경 파일 업로드 >`, `신규 파일 업로드 >`, `전체 허용 파일 업로드 >`
3. 우측: 원격 폴더 tree와 파일 목록

각 파일 행에는 상대 경로, 상태, local/remote 크기와 수정시각, local/remote hash, Git revision/dirty, 관리 여부, 마지막 업로드 시각과 deployment ID를 표시한다. 기본 필터는 `관리 대상`이고 `신규`, `변경됨`, `원격 변경됨`, `오류`, `전체` 필터를 제공한다.

신규 발견 파일은 자동으로 업로드하지 않는다. 사용자가 scope, 원격 상대경로, 파일 유형을 확인하여 managed로 등록한 뒤에만 전송 대상이 된다.

### 4.2 scope allowlist

| scope_id | local root | remote root | 직접 업로드 |
|---|---|---|---|
| `admin_tools` | `admin_tools/` | `/home/csda/AI_Routing/admin_tools/` | 허용 |
| `development_runtime` | `development/`와 명시 공유 코드 | Development runtime root | 허용 |
| `managed_source` | 승인된 Excel/CSV 원본 | managed-source root | 허용 |
| `secure_config` | allowlisted config template | 지정 config path | 추가 확인 후 허용 |
| `production_runtime` | Production package source | production root | 직접 업로드 금지 |

`.venv/`, `__pycache__/`, cache, log, report, `.git/`, test output, local SQLite, secret, 개인 원본 데이터는 기본 제외한다.

### 4.3 로컬 inventory DB

```text
.deployment-console/local_inventory.sqlite3
```

```sql
create table managed_scope (
  scope_id text primary key,
  local_root text not null,
  remote_root text not null,
  environment text not null check (environment in ('development','production','common')),
  allow_direct_upload integer not null,
  created_at text not null
);

create table local_file_inventory (
  scope_id text not null references managed_scope(scope_id),
  relative_path text not null,
  remote_relative_path text not null,
  managed integer not null default 0,
  upload_enabled integer not null default 0,
  file_kind text,
  exclusion_reason text,
  first_seen_at text not null,
  primary key (scope_id, relative_path)
);

create table local_file_snapshot (
  scope_id text not null,
  relative_path text not null,
  size_bytes integer not null,
  modified_at_utc text not null,
  sha256 text,
  git_revision text,
  git_dirty integer not null,
  scanned_at_utc text not null,
  primary key (scope_id, relative_path)
);

create table local_upload_baseline (
  scope_id text not null,
  relative_path text not null,
  remote_sha256 text,
  remote_size_bytes integer,
  remote_modified_at_utc text,
  deployed_at_utc text not null,
  deployment_id text not null,
  primary key (scope_id, relative_path)
);
```

### 4.4 빠른 변경 탐지

1. 최초 scan은 `상대경로 + size + mtime + Git 상태`만 수집한다.
2. 직전 snapshot과 size/mtime이 같으면 SHA-256을 재계산하지 않는다.
3. 신규, size/mtime/Git 변경, 업로드 직전 선택 파일, remote metadata 부족 파일만 SHA-256을 계산한다.
4. 원격은 SFTP `stat`을 먼저 읽고 불일치 파일만 hash를 계산한다.
5. baseline과 local/remote metadata가 모두 일치하면 `동일`이다.

상태 우선순위는 `제외됨 → 신규 발견 → 신규 업로드 대상 → 원격 변경됨 → 변경됨 → 동일`이다. 원격 변경 시에는 덮어쓰기 전에 원격 metadata를 경고한다.

### 4.5 API와 업로드 원자성

```text
POST /admin/file-sync/scan-local
GET  /admin/file-sync/remote-inventory?target_id=...
POST /admin/file-sync/register-managed-file
POST /admin/file-sync/preview
POST /admin/file-sync/apply
GET  /admin/file-sync/runs/{deployment_id}
```

`preview`는 target, 선택 방식, 상대경로, local snapshot ID를 받고 allowlist, target root, remote stat, overwrite 여부를 검증한다. 응답은 `preview_id`, preview checksum, 파일별 before metadata/action이다.

`apply`는 `preview_id`, preview checksum, `APPLY FILE SYNC <target_id>` 확인문구를 요구한다. 파일별로 임시 파일 업로드 → remote hash/size 검증 → atomic rename → inventory/baseline 기록 순서로 처리한다. 일부 파일 실패는 이미 성공한 파일을 되돌리지 않으며 파일별 상태와 오류 코드를 반환한다.

Development/Admin Tools에는 artifact manifest, release pin, pinned hash 비교를 적용하지 않는다. Production package에만 적용한다.

### 4.6 `vrp_admin_db` 서버 inventory/audit

```sql
create table deployment_target (
  target_id text primary key,
  scope_id text not null,
  environment text not null,
  remote_root text not null,
  enabled boolean not null default true
);

create table remote_file_inventory (
  target_id text not null references deployment_target(target_id),
  relative_path text not null,
  size_bytes bigint not null,
  modified_at timestamptz,
  sha256 char(64),
  git_revision text,
  source_dirty boolean,
  observed_at timestamptz not null,
  deployed_at timestamptz,
  deployment_id uuid,
  primary key (target_id, relative_path)
);

create table file_deployment_run (
  deployment_id uuid primary key,
  target_id text not null references deployment_target(target_id),
  initiated_by text not null,
  mode text not null check (mode in ('selected','changed','new','all')),
  source_git_revision text,
  source_dirty boolean,
  started_at timestamptz not null,
  completed_at timestamptz,
  status text not null check (status in ('running','completed','partial_failed','failed'))
);

create table file_deployment_item (
  deployment_id uuid not null references file_deployment_run(deployment_id),
  relative_path text not null,
  action text not null check (action in ('create','replace','skip')),
  before_sha256 char(64), after_sha256 char(64), size_bytes bigint,
  status text not null, error_code text,
  primary key (deployment_id, relative_path)
);
```

secret, password, SQL 결과 본문은 inventory/audit에 저장하지 않는다.

## 5. Data Management

### 5.1 기능

```text
Data Management
  ├─ Database Explorer
  ├─ SQL Console
  ├─ SQL Files
  ├─ Migration
  ├─ Domain Import
  └─ Execution History
```

대상 DB는 `vrp_db_dev`, `vrp_db`, `vrp_admin_db`를 명시 선택한다. Explorer는 schema/table, column/type/null/default, PK/FK/unique/check, index, grant, DDL, page preview를 제공한다. 기본 화면에서 전체 row 조회나 `count(*)`를 실행하지 않는다.

| 대상 | SELECT | DML/DDL |
|---|---|---|
| Development | 허용 | preview + typed confirmation 후 허용 |
| Production | 허용 | 기본 차단, 승인 migration/query만 |
| `vrp_admin_db` | 관리자 역할 | 관리자 역할 및 migration 규칙 |

SQL File은 `admin_tools/db/runners/`의 allowlisted `.sql`만 표시하며 폴더에 저장해도 자동 실행하지 않는다. 화면은 query checksum, target, 파라미터, 예상 영향, timeout을 보여주고 Preview 후 Apply한다.

모든 write는 principal, target DB, query checksum, preview checksum, execution ID, 시작/종료, 상태, row count, 오류 코드를 기록한다.

## 6. Migration

### 6.1 파일과 history

```text
admin_tools/db/migrations/
  V001__description.sql
  V002__description.sql
  manifest.json
admin_tools/db/runners/
  large_backfill_or_operational_runner.sql
```

적용된 migration은 수정하지 않고 새 forward migration을 추가한다.

```json
{
  "migration_id": "V005__add_example.sql",
  "description": "Add example table",
  "scope": "common",
  "depends_on": ["V004__previous.sql"],
  "checksum_sha256": "...",
  "transactional": true,
  "rollback": "forward_fix_only",
  "min_schema_contract": "common-region-plan/v2"
}
```

```sql
create table admin_schema_migration_history (
  migration_id text primary key,
  checksum_sha256 char(64) not null,
  status text not null check (status in ('success','failed','superseded')),
  applied_at timestamptz, applied_by text, execution_id uuid,
  rollback_metadata jsonb
);
```

같은 migration ID에 다른 checksum이 있으면 즉시 실패한다.

### 6.2 Migration Draft

`Data Management > Migration > New Migration Draft`는 Development/`vrp_db_dev`만 대상으로 한다. 표준 경로는 browser에서 즉시 DDL을 실행하는 방식이 아니다.

입력: table/column/type/default/nullability/constraint/index/grant/backfill, 대상 object, migration ID, 설명, dependency, transactional 여부, lock 위험, rollback 또는 forward-fix, precondition schema checksum.

생성: `V00N__...sql`, manifest 항목, pre/postcondition SQL, object diff, 위험 경고.

- `drop table`, `drop column`, type 축소, `set not null`, 대형 index/constraint validation은 destructive 경고와 별도 확인이 필요하다.
- 대형 backfill은 DDL transaction과 분리한 versioned runner로 만든다.
- 신규 NOT NULL column은 `nullable add → backfill → validate → set not null` 단계로 생성한다.
- Preview checksum과 `APPLY MIGRATION <migration_id> TO DEVELOPMENT`이 일치해야 Apply한다.

### 6.3 schema snapshot / Dev-Prod drift

```sql
create table database_schema_snapshot (
  snapshot_id uuid primary key, database_name text not null,
  environment text not null, captured_at timestamptz not null,
  migration_head text, schema_checksum char(64) not null
);
create table database_schema_object_snapshot (
  snapshot_id uuid not null references database_schema_snapshot(snapshot_id),
  object_type text not null, object_schema text not null, object_name text not null,
  definition_checksum char(64) not null, definition_json jsonb not null,
  primary key (snapshot_id, object_type, object_schema, object_name)
);
```

snapshot에는 table/column/type/default, PK/FK/unique/check, index, sequence/identity, grant 및 allowlisted view/function checksum을 보관한다. 업무 row 값은 비교하지 않는다.

| drift | 처리 |
|---|---|
| Expected lag: Dev에만 있는 미승격 migration | Production 승격 후보로 표시 |
| Unexpected: history에 없는 column/index 차이 | Production 적용 차단 후 원인 확인 |
| Incompatible: runtime contract의 column/constraint/grant 누락 | 해당 API/write 차단 및 수정 |

Development 흐름은 `manifest/snapshot 확인 → preview → confirmation apply → postcondition/API smoke/import 검증 → history/snapshot 기록`이다.

Production은 Development에서 성공한 **같은 checksum** migration이 clean Production package에 포함된 경우만 적용한다. preflight drift, backup/restore 또는 forward-fix, 예상 lock/시간을 확인하고 migration별 postcondition과 health check를 수행한다.

Region Plan schema reconcile은 Development의 특수 관리 명령이다. `preview → requires_confirmation 그대로 사용 → reconcile → contract check` 순서만 허용한다. Production에는 같은 효과를 versioned migration으로 구현한다.

## 7. Domain Import

각 Import는 다음 계약을 가진다.

```text
contract_id/version, 허용 확장자·sheet, 필수 컬럼/type/null 규칙,
업무 검증, staging/대상 table, natural key, upsert/delete 규칙,
preview/apply query 또는 runner checksum, idempotency/rollback/owner
```

흐름은 `파일 선택 → 계약 선택 → 구조/업무 검증 → staging → preview(create/update/unchanged/reject) → typed apply → commit → row accounting/history`다.

| 계약 | 원본 | 대상 | natural key |
|---|---|---|---|
| Technician List | roster | `common_technician_master` | 법인 + 운영도시 + technician ID |
| Capability/Product | Profile workbook `3. Product` | capability master | 법인 + 운영도시 + technician + product group + product |
| Capacity/Slot | capacity 파일 | capacity master | 법인 + 운영도시 + technician ID |
| Technician Address | 주소 파일 | technician master | 법인 + 운영도시 + technician ID |
| Region/ZIP Coverage | CSV/XLSX | coverage table | 법인 + 도시 + ZIP |
| Region Plan | Area Map workbook | plan/region/postal/assignment | 법인 + source city + plan ID |

기본은 upsert-only다. 원본에 없는 행을 삭제하는 sync/delete mode는 삭제 건수와 키를 preview한 뒤에만 실행한다. Capability는 Plan별 데이터가 아니라 도시/기사/Product master이므로 roster/Plan 정리 시 삭제하면 안 된다.

## 8. Region Plan / Area Map

식별은 `Subsidiary → Source city → Plan` 세 단계다. `target_city_id`와 legacy storage city는 호환 컬럼일 뿐 UI 도시 선택 기준이 아니다.

Area Map은 Region Data와 Technician Data만 저장한다. 정책 UI는 제공하지 않는다. Region Plans 화면은 workbook Upload, Source city별 목록, 단일 Plan 삭제(`CONFIRM`)만 제공한다.

업로드 검증을 통과한 Plan은 `active`로 기록하되 기존 active Plan을 자동 비활성화/교체하지 않는다. 어떤 Plan을 사용할지는 VRP Client가 선택한다.

업로드는 다음을 하나의 transaction에서 기록하고 실패 시 모두 rollback한다.

- `common_area_plan`
- `common_region_plan` 및 region/postal/boundary overflow/technician assignment
- normalized `common_region_set` 및 region/postal
- normalized `common_routing_plan` 및 technician

응답에는 plan ID, checksum, region/postal/overflow/technician의 accepted/rejected count, lifecycle을 반환한다. review, activation preview, activate, pin은 호출하지 않는다.

중복 ZIP의 `source_region_seqs=[5,4]`는 primary 5와 overflow 4로 보존하며 임의로 sequence를 바꾸지 않는다.

VRP Client는 Plan과 정책을 별도 선택한다. Plan 미선택의 기본 정책은 `home_distance_only`(Region 미사용 · Home 기반)다. request/result에는 최종 `plan_id`, `plan_checksum`, `routing_policy`, solver/matrix version을 기록한다.

## 9. 라우팅 진단

`slot_count=9`, `max_minutes=600`은 상한이다. 여유가 있어도 capability, fixed/reschedule, availability, DMS/DMS2, time window, home-to-job, single-leg, 총 이동시간/거리, 실제 route duration을 모두 만족해야 배정된다.

Atlanta 기본 제약을 실행 결과에 명시한다.

```text
max_home_to_job_min = 80
max_single_leg_min = 70
max_travel_min_per_sm_day = 150
max_travel_km_per_sm_day = 200
```

Unassigned를 숨기지 않는다. receipt별 reason, 후보 기사별 탈락 제약, slot/time/travel 소비량을 Result JSON, Unassigned Analysis, CSV에서 제공한다.

## 10. API 계약

```text
File: scan_local_inventory, list_remote_inventory, register_managed_file,
      preview_file_sync, apply_file_sync, get_file_sync_run
Data: list_database_objects, describe_table, preview_table_rows, preview_sql,
      execute_sql, list_migrations, preview_migration, apply_migration
Import: list_import_contracts, stage_import, preview_import, apply_import
Plan: import_region_plan_v2_workbook, list_region_plan_v2_candidates,
      delete_region_plan_v2(confirmation='CONFIRM')
```

모든 write는 target environment, principal, correlation ID, execution/deployment ID, preview/query/contract checksum, 상태, 오류 코드, 재시도 가능 여부를 반환·기록한다.

## 11. 권한

| 기능 | Development | Production |
|---|---|---|
| 관리 코드 직접 업로드 | 허용 | 불가 |
| runtime 배포 | 직접 파일 허용 | clean immutable package만 |
| DB Explorer/SELECT | 허용 | 허용 |
| SQL DML/DDL | preview/확인 후 허용 | 기본 차단 |
| SQL file | allowlist/확인 | 승인 migration/query만 |
| Domain Import | 계약 검증 후 허용 | 승인된 데이터만 |
| schema migration | preview/확인 | 검증 package/migration만 |

password, SSH/SFTP secret, config secret, SQL secret literal, 민감 원본값은 UI/audit에 저장하거나 표시하지 않는다. 서버 DB role과 command allowlist가 최종 권한을 강제한다.

## 12. 구현 점검과 위험 관리 (2026-08-21)

이 절은 목표 설계와 현재 구현을 구분한다. 아래 항목은 설계 완료가 아니라, 실제 코드·테스트로 확인된 전환 잔여 위험이다. 해결 전에는 Production 확장 또는 추가 DB 구조 변경을 진행하지 않는다.

### 12.1 높은 위험

| ID | 확인 사항 | 영향 | 해결 기준 |
|---|---|---|---|
| H-01 | Admin Tools artifact build/upload, release pin, target-root 검증 경로가 Console/backend에 남아 있다. | `Admin-tools target_root mismatch`, pinned release hash 오류가 다시 발생해 직접 파일 동기화를 막을 수 있다. | Admin Tools는 shared `/home/csda/AI_Routing/admin_tools/` health/import check만 사용하고, routine update에서 artifact/release/pin/hash 검증을 제거한다. |
| H-02 | Migration 화면은 존재하지만 backend `list_migrations()`는 빈 목록을 반환하며 preview/apply는 `REMOTE_ADMIN_CLI_REQUIRED`다. | Development schema 변경을 Console에서 안전하게 preview/apply/history로 관리할 수 없다. | 원격 Admin Tools migration CLI, manifest/history, schema snapshot/diff를 구현하고 local DB 실행 compatibility path를 제거한다. |
| H-03 | Region Plan upload가 legacy `common_region_plan*`, `common_area_plan`, normalized `common_region_set*`/`common_routing_plan*`을 함께 기록한다. | 목록·삭제·VRP snapshot의 기준 테이블이 달라져 partial delete, stale catalog, Plan 미노출 위험이 있다. | 각 기능의 authoritative table을 확정한다. 전환 기간에는 한 구조만 write source로 두고 나머지는 명시적 read-only migration adapter로 축소한다. |
| H-04 | runtime은 VRP Client 정책을 우선하지만 solver 진입부에는 persisted Plan `policy_version`과 Client 정책의 충돌을 오류로 보는 legacy 검사가 남아 있다. | API caller, 재시도 payload, batch 경로에서 정책 충돌로 라우팅이 거부될 수 있다. | Plan의 `policy_version`을 compatibility metadata로만 취급하고, 최종 routing policy는 Client/request 값만 사용하도록 모든 진입 경로를 통일한다. |
| H-05 | `vrp_admin_db`, File Management inventory/direct sync, server audit은 설계만 존재한다. | 실제 파일 배포는 여전히 artifact/manifest 경로에 의존하고 변경 이력을 DB로 일원화할 수 없다. | `vrp_admin_db` migration, local inventory SQLite, remote inventory API, preview/apply sync UI를 함께 적용한다. |

### 12.2 중간 위험

| ID | 확인 사항 | 영향 | 해결 기준 |
|---|---|---|---|
| M-01 | Local Console은 기본 loopback Region Plan API를 먼저 시도하고 실패하면 SSH/SFTP bridge를 사용한다. 현재 read cache와 failure cooldown은 적용됐지만 direct/bridge 지표는 없다. | 최초 요청 지연과 bridge 병목을 수치로 확인할 수 없다. | `transport=direct_api|admin_bridge`, cache hit/miss, request duration, fallback reason을 구조화 로그/메트릭으로 기록한다. |
| M-02 | Dev/Prod schema drift snapshot, migration history, Production promotion gate가 Console에서 작동하지 않는다. | Dev reconcile 결과와 Production schema가 조용히 달라질 수 있다. | DB별 migration checksum/history와 schema snapshot을 `vrp_admin_db`에 저장하고 Production preflight에서 unexpected/incompatible drift를 차단한다. |
| M-03 | 기존 설계 문서 일부는 Admin Tools immutable release/pin을 전제로 한다. | 후속 구현이 상충된 문서를 따라 artifact/release 흐름을 되살릴 수 있다. | 이 문서를 기준 설계로 지정하고 기존 문서는 deprecated 또는 Production-only artifact 설명으로 정정한다. |
| M-04 | backend read cache는 프로세스 전역이며 외부 변경은 TTL(현재 Region Plan 15초) 동안 보이지 않는다. | 다중 운영자 환경에서 짧은 stale list 또는 불필요한 bridge 재조회가 발생할 수 있다. | cache key에 target/authorization scope를 포함하고, 모든 write·manual refresh·서버 inventory change event에서 정확히 무효화한다. |
| M-05 | worktree에 다수의 수정/추가/삭제 파일이 공존한다. | Production package에 의도하지 않은 변경이 포함되거나 rollback 기준을 잃을 수 있다. | Production build는 clean checkout에서만 허용하고, 변경 묶음을 기능 단위 commit/검증으로 분리한다. |

### 12.3 검증 기준과 현재 결과

| 검증 | 결과 | 해석 |
|---|---|---|
| 핵심 Python module compile | 통과 | 문법 오류는 발견되지 않음 |
| Region Plan Console backend 단위 테스트 | 5건 통과 | read cache와 write invalidation 계약 확인 |
| artifact/master-admin/Region Plan/API/workflow 회귀 묶음 | 80건 중 11건 실패/오류 | 기존 artifact/release/pin 계약과 shared-path 전환이 충돌함 |
| Region Plan backend/active-selection/cache 회귀 묶음 | 63건 중 2건 실패 | schema grant 및 active-plan SQL 기대가 현재 구조와 불일치함 |

실패한 테스트를 삭제해서 green으로 만들면 안 된다. 각 테스트를 **Production artifact 전용**, **Development direct-sync 전용**, **Region Plan topology 전용** 계약으로 재분류한 뒤 새 설계의 정상/실패/재시도 시나리오로 교체한다.

### 12.4 전환 중 운영 금지 사항

- Development/Admin Tools의 routine update를 위해 artifact, release pin, 전체 manifest hash를 요구하지 않는다.
- 아직 구현되지 않은 Console migration 기능을 실제 DB 변경 수단으로 안내하지 않는다. 승인된 서버 runner/명령만 사용한다.
- `common_area_plan`과 legacy/normalized Plan 테이블을 사람이 각각 직접 수정하지 않는다.
- Plan에 저장된 compatibility policy를 VRP Client가 선택한 policy보다 우선하지 않는다.
- schema drift가 확인되지 않은 상태에서 Development 구조를 Production에 수동 복사하지 않는다.

## 13. 전환 대상과 구현 순서

현재 `Admin-tools target_root mismatch`, pinned release hash verification, full artifact manifest 오류의 원인은 다음 legacy 경로다.

1. `Package Management > Admin Tools` artifact build/upload UI
2. Settings/DB admin의 Admin Tools release pin 및 hash 검증
3. target root와 release staging을 비교하는 backend 검증
4. Region Plan fixed bundle/review/activation/pin adapter
5. 위 동작을 전제로 한 artifact/master-admin/console UI 테스트

구현은 아래 순서로 한다.

1. legacy Admin Tools artifact/release/pin/hash 경로를 제거하고 shared path import/health check로 교체한다.
2. File Management(local SQLite, allowlist, scan, remote inventory, preview/apply direct sync, audit)를 구현한다.
3. `vrp_admin_db`의 inventory, deployment, SQL/import/migration audit, schema snapshot을 migration으로 만든다.
4. remote DB Admin CLI(migration list/preview/apply, schema snapshot/diff, SQL file preview/apply)를 구현해 `REMOTE_ADMIN_CLI_REQUIRED` 임시 경로를 제거한다.
5. Data Management Explorer/SQL/Migration/History 화면을 전환한다.
6. Technician, Capability, Capacity, Address, Coverage, Region Plan을 Domain Import 계약으로 전환한다.
7. Region Plan UI/API의 review/activate/pin legacy 경로를 제거한다.
8. Production package/promotion/rollback만 immutable release로 유지하고 Development direct-sync와 테스트를 분리한다.

## 14. 완료 기준

- 신규 로컬 파일은 자동 업로드되지 않고 `신규 발견`으로 표시된다.
- 사용자는 폴더별로 신규/변경 파일만 필터링하고 선택 업로드할 수 있다.
- Development/Admin Tools 파일 하나는 artifact 없이 업로드하고 필요한 프로세스만 재시작할 수 있다.
- Admin Tools 화면/backend는 release pin, artifact hash, target_root mismatch를 요구하지 않는다.
- Data Management는 DB/table/schema 조회, Migration Draft/Preview/Apply, SQL 실행 이력을 제공한다.
- 업무 Excel/CSV는 명시적인 Import Contract와 고정 apply query/runner로만 DB를 변경한다.
- Dev/Prod schema 차이는 snapshot/migration history로 확인되고 Production은 Dev에서 검증된 동일 checksum migration만 적용한다.
- Region Plan은 Area Map Upload → DB 목록 → VRP Client Plan/정책 선택으로 정책 중복 없이 동작한다.
- direct sync, migration, domain import, Region Plan의 정상/실패/재시도 테스트가 통과한다.
