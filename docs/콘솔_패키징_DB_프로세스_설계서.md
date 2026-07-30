# 배포·운영 콘솔 및 실행 플랫폼 설계서

문서 버전: 1.0  
기준일: 2026-07-30  
적용 범위: 북미 VRP Routing repository

## 1. 설계 범위와 현재 구현 여부

이 문서는 로컬 Streamlit 배포 콘솔, 서버 runtime package, server-data/admin-tools package,
PostgreSQL, OSRM, systemd 프로세스의 경계와 운영 절차를 정의한다.

현재 저장소에는 콘솔 설계가 이미 일부 존재한다.

- 진입점: `sr_deployment_console.py`
- 화면/UI: `deployment_console_ui/`
- 배포·DB·원격 작업 backend: `services/deploy/console_backend.py`
- 패키징: `services/deploy/build_deploy_package.ps1`, `build_server_data_package.ps1`, `build_admin_tools_package.ps1`
- 운영 프로세스: `systemd/*.service`
- 관련 기존 문서: `docs/deployment_console.md`, `docs/배포_파일_구분.md`

기존 문서는 사용법 중심이고 패키지·파일·DB·프로세스의 전체 계약이 분산되어 있었다. 이 문서는 그 내용을 운영 설계의 단일 참조점으로 정리한다.

## 2. 목표 아키텍처

```text
[운영자 PC]
  Streamlit console
    ├─ local artifact builder ──> runtime/server-data/admin-tools artifact
    ├─ read-only inventory/diff <── SFTP/SSH ──> [운영 서버]
    └─ explicit authorized mutation ───────────> upload / DB admin / systemd

[운영 서버]
  /home/csda/AI_Routing/
    production/       clean runtime code + venv + production config
    development/      development runtime code + venv + development config
    shared/           catalog and shared managed data
    state/            jobs, receipts, deployment history
  /home/osrm/         OSRM graph/profile/runtime
  PostgreSQL          vrp_db, vrp_db_dev
  systemd             API / client / OSRM units
```

콘솔은 라우팅 solver를 직접 실행하지 않는다. 콘솔은 artifact 생성·검증, 원격 파일 차이 확인, 명시적으로 승인된 업로드, DB admin 작업, allowlist 된 systemd 제어만 수행한다. 일반 라우팅 실행은 client/API가 담당한다.

## 3. 콘솔 구성

### 3.1 계층

| 계층 | 책임 | 주요 위치 |
|---|---|---|
| Entry point | Streamlit 실행 | `sr_deployment_console.py` |
| UI | Build, Deployment, DB, Monitoring 화면과 확인문구 | `deployment_console_ui/` |
| Backend | artifact/manifest, SFTP, DB admin, service control | `services/deploy/console_backend.py` |
| Policy | 연결정보·원격 mutation 허용 여부 | `config/server_deploy.local.json` |
| DB adapters | migration/preview/apply/receipt | `admin_tools/db/` |

Import 시 원격 연결이나 DB 접속을 하지 않으며, mutation은 다음을 모두 만족해야 한다: UI 명시 액션, 정확한 typed confirmation, allowlist, 정책 플래그, manifest/checksum 검증.

### 3.2 화면과 기능 계약

1. **Build artifact**: 환경·버전·Git revision·dirty 상태를 표시하고 로컬 staging/ZIP을 생성한다. 이 단계는 업로드·재시작을 하지 않는다.
2. **Deployment**: 선택 artifact의 전체 manifest와 원격 diff를 비교한다. create/update만 업로드 후보로 표시한다.
3. **DB**: migration/spec 목록, preview, apply, receipt 조회를 제공한다. 직접 SQL 입력은 제공하지 않는다.
4. **Managed data**: dataset/version/metadata/checksum을 검증하고 preview 후 import한다.
5. **Monitoring**: development/production/공용 OSRM의 systemd 상태와 HTTP health를 read-only로 표시한다.
6. **Service control**: allowlist unit에 대해서만 start/restart를 수행하며 stop/disable/원격 파일 삭제는 제공하지 않는다.

## 4. 패키징 설계

### 4.1 artifact 종류

| 종류 | 목적 | 대상 |
|---|---|---|
| `runtime` | API/client/solver 실행 코드 | `production/` 또는 `development/` |
| `server-data` | DB seed, managed data, catalog, import 자료 | 서버 `shared/`, `state/`, DB admin 입력 |
| `admin-tools` | migration·운영 점검·관리 CLI | 관리자 작업용 별도 디렉터리 |

세 artifact는 lifecycle을 공유하지 않는다. candidate/reviewed/seed/runtime 데이터도 분리한다.

### 4.2 runtime ZIP 포함 원칙

포함: 실행에 필요한 Python package, `smart_routing/`, `services/api/`, `admin_tools` 중 runtime 의존 부분, `config/*.template.json`, `verify_deployment.py`, 시작 스크립트, 필요한 systemd template.

제외: 비밀값이 든 local config, 원본/가공 CSV·XLSX·parquet, OSRM graph, 로그·cache·`__pycache__`, 테스트와 개발 산출물, 임의의 raw data.

production artifact는 clean checkout에서만 생성한다. development dirty-source artifact는 개발 검증 전용이며 production으로 복사·이름 변경해 승격하지 않는다.

### 4.3 manifest와 release

각 artifact는 다음을 포함한다.

```text
manifest.json       # 상대경로, 크기, SHA-256, artifact/environment/release
<artifact>.zip
staging/<release>/
```

manifest는 UTF-8 no-BOM이며 파일별 SHA-256을 기록한다. 기존 release 디렉터리/ZIP은 덮어쓰지 않는다. 업로드 후에는 원격 전체 manifest를 다시 계산하여 local artifact와 일치해야 한다. release ID, Git revision, environment, artifact checksum, 업로드 시간과 수행자를 history에 남긴다.

### 4.4 서버 디렉터리

```text
/home/csda/AI_Routing/
├─ production/       # production runtime, .venv, config_common_vrp.json
├─ development/      # development runtime, .venv, config_common_vrp.dev.json
├─ shared/
│  ├─ config/        # production data catalog 등 비밀 아닌 공유 설정
│  └─ north_america/ # 승인된 managed data
└─ state/
   ├─ production/vrp_api_jobs/
   ├─ development/vrp_api_jobs/
   └─ {env}/common_vrp_jobs/
```

OSRM graph는 runtime ZIP과 분리하여 `/home/osrm`에서 관리한다.

## 5. 설정과 비밀정보

template은 Git에 두고 실제 값은 local/server secret로 주입한다.

- `config/server_ftp.local.json`: 콘솔 SFTP/SSH 자격정보
- `config/server_deploy.local.json`: remote root 및 `allow_upload`, `allow_service_control`
- `config/common_vrp.dev.json`, `common_vrp.prod.json`: DB/API/solver 환경 설정
- `config/config.json`: geocoder·외부 서비스 등 runtime secret 설정

실제 비밀 설정은 artifact에 포함하지 않으며 파일 권한은 서버에서 최소 `0600`으로 유지한다. production과 development는 DB명·포트·job root·catalog를 반드시 분리한다.

## 6. DB 설계와 운영

### 6.1 DB 분리

| 환경 | DB |
|---|---|
| development | `vrp_db_dev` |
| production | `vrp_db` |

운영 콘솔의 DB 작업은 환경을 명시하고 연결 설정을 검증한다. 개발 DB 작업이 production DB로 향하면 fail-closed 한다.

### 6.2 논리 영역

- Master: `common_routing_config_master`, `common_region_master`, `common_technician_master`, capability/rule master
- Input: `common_job_input`, `common_request_technician_input`
- Request/result: `common_routing_request`, `common_routing_result`
- Geocode/support: geocode cache·attempt/daily log, avoid area
- Region plan v2: migration으로 관리되는 versioned plan tables

원본 입력, 정제 입력, 승인된 region plan, runtime request/result를 같은 테이블에 덮어쓰지 않는다. request/result에는 request ID, routing job ID, 상태, payload/result와 생성·수정 시각을 보존한다.

### 6.3 migration과 seed 절차

1. migration manifest와 현재 DB 버전을 read-only 확인한다.
2. 변경 대상, SQL checksum, 영향 테이블, rollback/복구 방법을 preview receipt로 만든다.
3. 운영자 confirmation 후 동일 환경의 transaction에서 migration을 적용한다.
4. schema/version/checksum과 row count·constraint 검증 결과를 receipt에 기록한다.
5. seed/import는 metadata와 source checksum을 검증한 뒤 idempotent upsert 또는 versioned activation으로 처리한다.

Reset·delete·임의 SQL은 콘솔의 표준 workflow에 포함하지 않는다. 백업/복구는 플랫폼 운영 절차로 별도 승인한다.

## 7. 프로세스와 포트

| 환경 | Common API | Smart API | Client | OSRM |
|---|---:|---:|---:|---|
| production | 8065 | 8055 | 8501 | Korea 5000, LA 5001, Atlanta 5002 |
| development | 8066 | 8056 | 8503 | 공용 OSRM 사용 정책에 따름 |

systemd unit은 `systemd/`에 보관한다.

- `common-vrp[-dev].service`: Common VRP API
- `smart-routing[-dev].service`: Smart Routing API
- `common-vrp-client[-dev].service`: Streamlit client
- `osrm-usa.service`, `osrm-korea.service`: OSRM 프로세스/컨테이너

각 unit은 고정 `User=csda`, `WorkingDirectory`, 환경별 config/job root, 실행 전 파일·Python·deployment 검증, health check, `Restart=on-failure`, 시작/종료 timeout을 정의한다. production unit은 production path만 참조하고 development unit은 development path만 참조한다.

### 7.1 시작·배포 순서

```text
artifact build -> manifest/secret scan -> remote diff
  -> authorized upload -> remote manifest verify
  -> DB migration/seed (필요 시) -> API restart
  -> API health -> client restart -> client health
  -> routing smoke test -> release receipt
```

API가 정상화되기 전 client를 먼저 재시작하지 않는다. OSRM graph/profile 변경 시 OSRM health와 matrix 단위·방향 검증을 API smoke test에 포함한다.

### 7.2 장애와 rollback

서비스 실패는 `systemctl is-active`, HTTP health, 최근 journal로 확인한다. rollback은 이전 release의 manifest·checksum과 DB migration 호환성을 확인한 경우에만 수행한다. DB migration은 코드 rollback과 독립적으로 forward-fix 또는 승인된 복구 절차를 사용한다. 진행 중 upload가 실패하면 성공한 파일 목록과 receipt를 남기고 자동으로 전체 삭제하지 않는다.

## 8. 보안·권한·감사

기본 정책은 `allow_upload=false`, `allow_service_control=false`다. 운영자가 명시적으로 정책을 활성화하고 UI confirmation을 통과해야 원격 mutation이 가능하다. systemd sudoers는 allowlist unit의 start/restart만 허용하며 `sudoers`는 `0440`, `visudo -cf` 검증을 거친다.

모든 mutation에는 operator, environment, release/artifact checksum, 대상 파일·unit·DB, preview ID, confirmation, 시작/종료 시각, 성공/실패, 오류를 기록한다. 원격 inventory·monitoring·diff는 read-only다.

## 9. 운영 점검표

- [ ] clean source와 Git revision 확인
- [ ] environment가 production/development와 일치
- [ ] artifact manifest·SHA-256·secret scan 통과
- [ ] ZIP에 raw data·비밀 설정·cache가 없음
- [ ] 원격 전체 manifest diff와 대상 경로 확인
- [ ] DB migration preview·checksum·backup/복구 계획 확인
- [ ] DB row/constraint/schema 검증 및 receipt 저장
- [ ] systemd unit allowlist와 포트 확인
- [ ] API/OSRM/client health 및 smoke test 통과
- [ ] release history와 rollback 대상 보존

## 10. 관련 구현·문서

- `docs/deployment_console.md`: 콘솔 사용 절차
- `docs/배포_파일_구분.md`: 배포 파일 분류
- `services/deploy/console_backend.py`: fail-closed backend
- `services/deploy/build_*.ps1`: artifact builder
- `admin_tools/db/migrations/`: migration manifest/SQL
- `log/vrp_db_schema.sql`, `log/vrp_db_dev_schema.sql`: DB schema snapshot
- `systemd/`: process definitions
