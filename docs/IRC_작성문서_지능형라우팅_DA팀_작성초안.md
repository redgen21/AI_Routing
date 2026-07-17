# IRC 작성 초안 - 북미 지능형 라우팅 서버 신청

본 문서는 `docs/별첨1) IRC_작성문서_지능형라우팅_DA팀.docx` 양식에 붙여 넣기 위한 작성 초안입니다.  
수치는 초기 운영 기준의 보수 추정치이며, 실제 운영 대상 법인/도시 확대 시 조정이 필요합니다.

## 1. 개요

### 1.1 프로젝트 요약

북미 지능형 라우팅 프로젝트는 북미 서비스 운영 지역의 일별 서비스 요청 건을 대상으로, 서비스 엔지니어의 근무 가능 여부, 시작 위치, 보유 skill/product capability, 고정 배정 작업, 작업별 소요시간, 도로 이동시간 및 교통통제 구역을 반영하여 최적 방문 순서와 담당 엔지니어를 산출하는 라우팅 시스템입니다.

기존에는 담당자 경험과 수작업 기준으로 엔지니어 배정 및 방문 순서를 조정해야 했으나, 본 프로젝트를 통해 OSRM 기반 도로 네트워크 이동시간 계산, PostgreSQL 기반 요청/결과 저장, Python OR-Tools 기반 VRP 최적화 API를 구축하여 일별 배정 업무를 자동화하고 운영자가 결과를 지도와 테이블에서 확인할 수 있도록 합니다.

### 1.2 주요 부분 요약

본 서비스는 Streamlit 기반 운영 화면, Python 기반 Routing API, OR-Tools VRP Solver, OSRM Routing Engine, PostgreSQL Database로 구성됩니다.

운영자는 서비스 요청 데이터와 Technician List를 업로드하고, 날짜/도시/법인 기준으로 routing payload를 생성합니다. Routing API는 payload를 비동기 job으로 접수하고, OSRM distance/duration matrix와 OR-Tools VRP Solver를 사용하여 엔지니어별 작업 배정, 방문 순서, 이동거리, 이동시간, 미배정 사유를 계산합니다.

PostgreSQL에는 technician master, capability master, job input, technician availability, avoid area polygon, routing request, routing result가 저장됩니다. OSRM은 Docker container로 실행하며, 북미 지역 도로망 데이터 기반으로 route distance/duration을 제공합니다. 교통통제/회피 구역은 Leaflet 지도에서 polygon으로 관리하고, 라우팅 시 해당 구역 통과 경로에 penalty를 부여하여 우회 경로를 유도합니다.

### 1.3 목표

프로젝트의 정량 목표는 다음과 같습니다.

| 구분 | 목표 |
|---|---|
| 일별 라우팅 대상 규모 | 초기 1개 도시 기준 50~500 jobs/day, 10~50 technicians/day 처리 |
| 확장 목표 | 북미 주요 도시/법인으로 확장 시 1,000~5,000 jobs/day까지 단계적 확장 가능 구조 확보 |
| 라우팅 수행 시간 | 1개 도시 100 jobs 기준 2분 이내, 500 jobs 기준 10분 이내 완료 |
| 일반 화면/API 응답 | 조회/저장/상태확인 API roundtrip 2.5초 이내 |
| 비동기 routing status 조회 | job 접수 후 queued/running/completed 상태를 2.5초 이내 반환 |
| 배정 품질 | technician skill/product capability, fixed job, max slot, max working minutes hard constraint 준수 |
| 운영 효율 | 일별 수작업 배정/검토 시간을 30% 이상 절감 |
| 라우팅 품질 | 이동거리/이동시간 최소화 및 technician별 load 편차 완화 |
| 장애 복구 | 서비스 장애 시 RTO 24시간 이내, DB 데이터 RPO 1시간 이내 |

본 시스템은 일반 사용자 대량 접속형 서비스가 아니라 운영자/배치성 분석 서비스입니다. 예상 동시 접속자는 초기 5명 이하, 확장 후에도 20명 이하로 예상합니다. 성능 병목은 사용자 트래픽보다 OSRM matrix 계산, OR-Tools 최적화 연산, PostgreSQL I/O에 발생하므로 CPU, memory, disk I/O 중심의 서버 사양이 필요합니다.

### 1.4 주요 일정

| 구분 | 주관 | 설명 | 시작 | 종료 | 담당자 |
|---|---|---|---|---|---|
| 기획 | DA팀/운영부서 | 북미 라우팅 요구사항, 배정 조건, 회피구역 요구사항 정의 | 2026-03 | 2026-04 | TBD |
| 인프라 검토 | 플랫폼인프라팀/DA팀 | OSRM/API/DB 서버 구성 및 보안/네트워크 검토 | 2026-05 | 2026-05 | TBD |
| 시스템 구축 | 플랫폼인프라팀 | Cloud VM, data disk, PostgreSQL, Docker, 방화벽 구성 | 2026-05 | 2026-06 | TBD |
| 개발 | DA팀 | Routing API, Streamlit 운영 화면, DB schema, VRP solver 개발 | 2026-03 | 2026-06 | TBD |
| 성능테스트 | DA팀/플랫폼인프라팀 | 100/500/1,000 jobs 기준 라우팅 수행시간 및 API 응답 검증 | 2026-06 | 2026-06 | TBD |
| QA | QA/운영부서 | 결과 검증, 미배정 사유, 우회 polygon, technician constraint 검증 | 2026-06 | 2026-07 | TBD |
| Release | 전체 | 운영 전환 및 모니터링 | 2026-07 | 2026-07 | TBD |

### 1.5 위험요인

| 위험요인 | 내용 | 대응방안 |
|---|---|---|
| OSRM 데이터 용량 증가 | 북미 전체 또는 다수 region OSM 데이터를 처리할 경우 disk/memory 사용량 증가 | 초기 대상 region 단위로 OSRM profile 분리, 필요 시 OSRM 전용 서버 분리 |
| 라우팅 연산시간 증가 | jobs/technicians 증가 시 OR-Tools 탐색 시간이 증가 | 비동기 job 구조 적용, time_limit_seconds 조정, 도시/날짜 단위 분할 처리 |
| DB/OSRM 단일 장애점 | 단일 VM 구성 시 장애 시 서비스 중단 | 초기 PoC/운영은 단일 구성, 운영 중요도 증가 시 API/DB/OSRM 분리 및 백업/복구 절차 적용 |
| 주소/좌표 품질 | 주소 geocoding 실패 또는 잘못된 좌표로 배정 품질 저하 | geocode cache, manual correction, unassigned reason 제공 |
| 교통통제 polygon 정확도 | polygon 경계/도로망 매칭 오차로 회피가 완전하지 않을 수 있음 | penalty 방식 적용, polygon buffer/검증 화면 제공, 필요 시 OSRM custom profile/edge restriction 검토 |
| 개인정보/운영정보 | 고객 주소, 엔지니어 이름/코드 등 민감 정보 포함 가능 | DB 접근 제한, 내부망 접근, 계정 권한 분리, 로그 마스킹 기준 적용 |

## 2. Architectural Overview

### 2.1 설계 요구 사항

#### 개발/검증 환경

- Python 3.10 이상
- PostgreSQL 14 이상
- Docker 및 OSRM backend container
- Streamlit Client 실행 환경
- Routing API 실행 환경
- OSRM route/table API 접근 가능
- Git 기반 source 관리 및 수동/자동 배포 가능 환경

#### 운영 환경

초기 운영은 비용과 운영 단순성을 고려하여 단일 Cloud VM에 API, Streamlit, PostgreSQL, OSRM container를 구성합니다. 단, OSRM과 VRP Solver가 CPU/memory를 많이 사용하는 특성이 있어 VM은 일반 웹서버보다 높은 CPU/memory가 필요합니다.

운영 확장 시에는 다음과 같이 분리할 수 있도록 설계합니다.

- App/API Server: Streamlit Client, Common VRP API, Routing API
- Routing Engine Server: OSRM Docker, OR-Tools VRP Solver
- Database Server: PostgreSQL/PostGIS

#### OS 및 미들웨어

- OS: Ubuntu 22.04 LTS 권장
- Python: 3.10 이상
- DB: PostgreSQL 14 이상, PostGIS 확장 사용 가능
- Container: Docker
- Routing Engine: OSRM backend
- Python Library: pandas, numpy, streamlit, requests, ortools, psycopg2, shapely/geopandas 계열, folium/streamlit-folium

#### 네트워크/보안

- Streamlit: 내부 운영자 접근 포트
- Common VRP API: 내부 API 포트
- Smart Routing API: 내부 API 포트
- OSRM: 외부 공개 금지, API 서버 localhost 또는 내부망에서만 접근
- PostgreSQL: 외부 공개 금지, 서버 내부 또는 허용된 private subnet에서만 접근
- SSH: 운영자/관리자 IP 제한

### 2.2 Logical Architecture

```text
Operator Browser
  |
  | HTTPS/Internal Access
  v
Streamlit Routing Client
  |
  | Build Payload / Submit Routing / Check Result
  v
Common VRP API Server
  |
  | Read/Write
  v
PostgreSQL
  |  - job input
  |  - technician list
  |  - technician capability
  |  - avoid area polygon
  |  - routing request/result
  |
Common VRP Runtime
  |
  | distance/duration matrix
  v
OSRM Docker Container
  |
  | road network route/table
  v
OSM Road Network Data

Common VRP Runtime
  |
  | optimization constraints
  v
OR-Tools VRP Solver
  |
  | assignment/schedule/unassigned/result json
  v
PostgreSQL + Streamlit Result View
```

### 2.3 Architectural Components

| Component | Comment |
|---|---|
| Streamlit Routing Client | 운영자가 jobs, technician list, avoid polygon을 관리하고 payload 생성, routing 요청, 결과 확인을 수행하는 UI |
| Common VRP API Server | jobs/technicians/config/result를 관리하고 routing build/run/check API를 제공 |
| Routing Runtime | DB 입력 데이터를 공통 payload로 변환하고, OSRM/VRP solver를 호출하여 결과를 생성 |
| OR-Tools VRP Solver | skill/product capability, fixed job, max slot, max working minutes, priority group, 이동시간 최소화 조건을 반영하여 최적 배정 계산 |
| OSRM Engine | OSM 도로망 기반 route/table API를 제공하여 실제 도로 이동거리/이동시간 산출 |
| PostgreSQL DB | master/input/request/result/avoid area polygon 저장 |
| Avoid Area Manager | Leaflet 지도에서 polygon을 등록하고 routing 시 회피 penalty 적용 |
| systemd Service | API/Streamlit/OSRM 자동 시작 및 프로세스 운영 관리 |

### 2.4 Database Scheme

주요 테이블은 다음과 같습니다.

| Table | Description |
|---|---|
| common_routing_config_master | 법인/도시별 routing 설정, timezone, OSRM URL, 옵션 저장 |
| common_region_master | 도시/지역/ZIP 기준 master 정보 |
| common_technician_master | technician 기본 정보, home address, active flag, priority group 저장 |
| common_technician_capability_master | technician별 product group/product 수리 가능 여부 저장 |
| common_heavy_repair_rule_master | symptom/product 기준 heavy repair 및 service time rule 저장 |
| common_job_input | 일별 service job 입력 데이터 저장 |
| common_request_technician_input | 일별 technician availability, shift, slot capacity 저장 |
| common_avoid_area | 교통통제/회피 polygon 정보 저장 |
| common_routing_request | routing request payload, 상태, job id 저장 |
| common_routing_result | routing result JSON, assignment, summary 저장 |

DB는 운영 데이터 조회/저장과 결과 이력 보관 목적이며, 대량 OLTP 트래픽보다는 운영자가 요청한 batch/routing 결과 저장 성격입니다.

## 3. Service Architecture

### 3.1 성능 및 용량 요구 사항

| Component | Traffic or Transactions | Peak 예상 | Peak 시간 |
|---|---:|---:|---|
| Streamlit UI | 5~20 internal users/day | 5 concurrent users | 업무 시간 |
| Common VRP API | 100~1,000 API calls/day | 5 qps 이하 | 라우팅 실행 전후 |
| Routing Job | 5~50 routing jobs/day | 2~5 concurrent jobs | 오전 배정 시간 |
| OSRM table/route | routing job당 수십~수백 회 내부 호출 | job 실행 중 집중 발생 | routing 실행 시 |
| PostgreSQL | 1,000~20,000 row read/write/day | 20 connections 이하 | upload/routing/result 조회 시 |

서버 1대 기준 예상 최대 요청:

| 구분 | 예상치 |
|---|---|
| WEB/WAS | API 응답 JSON 1KB~10MB, 일반 조회 2.5초 이내 |
| Routing Solver | 100 jobs 기준 2분 이내, 500 jobs 기준 10분 이내 |
| DB Connection Pool | 초기 5~20 connections |
| Log 증가량 | 100~500MB/day |
| Result JSON 증가량 | 10~100MB/day |
| OSM/OSRM 데이터 | region별 수 GB~수십 GB, 북미 확장 시 100GB 이상 가능 |

### 3.2 장애 복구 목표

| 항목 | 목표 |
|---|---|
| RTO | 24시간 이내 |
| RPO | 1시간 이내 |
| 무중단 요구 | 초기 단계에서는 필수 아님. 운영 중요도 증가 시 이중화 구성 검토 |

Routing 결과는 재실행 가능하지만, 사용자가 업로드한 job/technician 입력과 routing result history는 DB 백업으로 복구 가능해야 합니다.

### 3.3 데이터 백업 요구 사항

| Component | Data | 개인정보 포함 여부 | 보관기간 | 백업주기 | 1일 예상 증가량 |
|---|---|---|---|---|---:|
| PostgreSQL | job input, technician list, capability, routing request/result, avoid polygon | 고객 주소/엔지니어 정보 포함 가능 | 1년 | daily + WAL 또는 hourly snapshot 권장 | 10~100MB |
| Log | API log, routing execution log, error log | payload 일부 포함 시 개인정보 가능 | 6개월 | daily | 100~500MB |
| OSRM/OSM Data | 도로망 데이터, OSRM graph files | 개인정보 없음 | 재생성 가능 | 변경 시 백업 또는 재빌드 | region별 수 GB |
| Source/Config | application source, config file, systemd file | DB password 등 secret 포함 가능 | 버전관리/별도 secret 관리 | 변경 시 | 소량 |

### 3.4 물리적 구성

초기 Production Environment 권장 구성:

```text
Cloud VM: ai-routing-prod-01
  - Ubuntu 22.04
  - Streamlit Client
  - Common VRP API
  - Smart Routing API
  - PostgreSQL 14
  - Docker
  - OSRM containers
  - /data mounted managed disk

Internal Users
  -> Streamlit port
  -> API port
  -> PostgreSQL local/private only
  -> OSRM local/private only
```

운영 확장 시 권장 구성:

```text
ai-routing-app-01
  - Streamlit
  - Common VRP API

ai-routing-engine-01
  - OSRM containers
  - OR-Tools routing workers

ai-routing-db-01 or Managed PostgreSQL
  - PostgreSQL/PostGIS
  - automated backup
```

### 3.5 하드웨어 구성

초기 운영 권장 사양:

| Node name | CPU | Memory | Disk | OS | Comment |
|---|---:|---:|---:|---|---|
| ai-routing-prod-01 | 8 vCPU | 32GB | OS 128GB + Data 256GB SSD | Ubuntu 22.04 LTS | API, Streamlit, PostgreSQL, OSRM, VRP Solver 통합 운영 |

최소 사양:

| Node name | CPU | Memory | Disk | OS | Comment |
|---|---:|---:|---:|---|---|
| ai-routing-prod-01 | 4 vCPU | 16GB | OS 64GB + Data 128GB SSD | Ubuntu 22.04 LTS | 1개 도시/소규모 job 기준. OSRM 지역 확장 시 부족 가능 |

확장 권장 사양:

| Node name | CPU | Memory | Disk | OS | Comment |
|---|---:|---:|---:|---|---|
| ai-routing-engine-01 | 16 vCPU | 64GB | Data 512GB SSD 이상 | Ubuntu 22.04 LTS | 다도시/대량 OSRM graph 및 동시 routing job 처리 |
| ai-routing-db-01 | 4~8 vCPU | 16~32GB | Data 256GB~1TB SSD | Ubuntu 22.04 LTS 또는 Managed PostgreSQL | DB 분리 시 |

CPU와 memory가 중요한 이유:

- OSRM graph build/extract 및 table API가 memory를 많이 사용합니다.
- OR-Tools VRP Solver는 jobs/technicians 증가 시 CPU 사용량이 증가합니다.
- PostgreSQL은 routing request/result JSON, job input, capability 조회를 안정적으로 처리해야 합니다.
- `/data`는 PostgreSQL data directory와 OSRM data file 저장소로 사용하므로 OS disk가 아닌 별도 managed disk가 필요합니다.

## 4. Check List

### 4.1 DBA

| Check Point | Question | Answer |
|---|---|---|
| I/O | Database read/write 비율 | Read 70%, Write 30% 예상. Upload/routing 결과 저장 시 write 집중, 결과 조회/화면 표시 시 read 발생 |
| Capacity | 초기 data 용량 | 초기 5~20GB 예상. OSRM 파일 제외 DB 기준 |
| Capacity | 일 증가 data 용량 | 10~100MB/day 예상. routing result 보관 정책에 따라 증가 |
| Capacity | DB 동시 접속자 수 | Average 5 이하, Peak 20 이하 |
| Type of Data | 저장 데이터 성격 | 서비스 job 입력, technician master/availability/capability, avoid polygon, routing request/result JSON |
| Recovery | 허용 가능한 데이터 유실 범위 | 1시간 이내 RPO 권장. routing 결과는 재실행 가능하나 입력 데이터는 백업 필요 |
| Transaction Support | 트랜잭션 필요 여부 | 필요. jobs/technicians replace, routing request/result 저장 시 일관성 필요 |
| Query Characteristics | 쿼리 특성 | 짧은 조회/저장, 날짜/도시 기준 paging/filter, summary 집계, JSON result 조회 |
| DB CharacterSet | Charset | UTF8 또는 UTF8MB4 상당의 UTF-8 사용 |
| DBMS/NoSQL | DBMS 선택 | PostgreSQL 권장. JSON, geometry/polygon, relational master 관리에 적합 |
| Traffic Type | 업무 유형 | OLTP + batch result 저장 혼합. 운영자 중심의 소규모 트랜잭션과 routing 결과 저장 |
| 서비스 연동 | 타 DB 연동 | 초기에는 직접 연동 없음. 향후 ERP/서비스 접수 시스템 또는 BigQuery 연동 가능 |

### 4.2 Release Management

| 질문 | 답 |
|---|---|
| 모든 code/configuration을 Hudson/Jenkins로 배포 예정입니까? | 초기에는 Git 기반 수동 배포 또는 script 배포를 사용합니다. 운영 전환 시 Jenkins/Hudson 연동 가능하도록 source/config 분리 예정입니다. |
| Release environments guideline에 따라 release path 구성 예정입니까? | 예. dev/stg/prod 경로와 config 분리, systemd service 기준 운영을 권장합니다. |
| Production과 독립된 staging 요소를 launch 전에 사용할 수 있습니까? | 예. 동일 VM 사양 축소 또는 별도 staging VM에서 payload build/routing/result 검증을 수행할 수 있습니다. |
| Staging environment에서 모든 component를 stage할 수 있습니까? | 예. Streamlit, API, PostgreSQL, OSRM container를 staging에 동일 구성할 수 있습니다. 단 OSRM data 용량에 따라 staging은 region subset 사용 가능 |

### 4.3 Security

| 항목 | 내용 |
|---|---|
| 접근 대상 | 내부 운영자 및 개발/운영 담당자 |
| 인증/접근제어 | 사내망/VPN/IP allowlist 기반 접근 권장 |
| DB 접근 | application 계정과 DBA/admin 계정 분리 |
| 외부 공개 여부 | OSRM/PostgreSQL은 외부 공개 금지. Streamlit/API도 내부망 또는 제한된 IP만 허용 |
| 개인정보 | 고객 주소, 엔지니어 이름/코드, 작업 위치 좌표가 포함될 수 있음 |
| 로그 정책 | payload 전문 로그 저장 지양, 필요 시 masking 적용 |
| Secret 관리 | DB password/API 설정은 config 파일 권한 제한 또는 secret manager 사용 권장 |

### 4.4 Development

개발 언어는 Python이며, 주요 framework/library는 Streamlit, pandas, OR-Tools, PostgreSQL driver, OSRM API client, Folium/Leaflet 지도 UI입니다.  
Routing API는 비동기 job 방식으로 요청을 접수하고, routing status/result를 별도로 조회합니다.  
주요 검증 항목은 max slot hard limit, skill/product capability, fixed job, shift max minutes, avoid polygon, unassigned reason, route distance/duration 정확도입니다.

## 5. Project 관리자

| 역할 | 이름 |
|---|---|
| Project Manager | TBD |
| 개발 PM | TBD |
| 개발 PL | TBD |
| 인프라 담당 | TBD |
| 운영 담당 | TBD |

## 6. AWS Service

본 시스템은 Lambda/API Gateway 중심의 serverless 구조보다는 OSRM container, PostgreSQL, OR-Tools solver가 필요한 IaaS/Container 기반 workload입니다. 아래 항목은 AWS 양식 기준으로 작성했습니다.

### 6.1 Serverless Lambda

| Check Point | Question | Answer |
|---|---|---|
| 비용 | 개발 예정 Function 총 개수 | 해당 없음. 초기 구성은 VM/API server 방식 |
| 비용 | 한달 Lambda 호출 건수 | 해당 없음 |
| 비용 | Function별 메모리 | 해당 없음 |
| 비용 | 평균 실행 시간 | 해당 없음. Routing job은 수십 초~수분으로 Lambda 제한/비용에 부적합 |
| 비용 | 데이터 전송 사이즈 | 해당 없음 |
| 적합성 | 대기 시간이 짧은 읽기/업데이트에 적합한가 | 단순 조회 API 일부는 가능하나, OSRM/VRP solver workload는 Lambda 부적합 |
| 적합성 | CPU 집약적 작업 또는 많은 네트워크 요청 수행 여부 | 예. OR-Tools solver와 OSRM matrix 호출이 CPU/network intensive |
| 적합성 | 무거운 library dependency 제거 가능 여부 | OR-Tools, pandas, geospatial library가 필요하여 제거 어려움 |
| 적합성 | 연계 서비스 존재 여부 | PostgreSQL, OSRM container, Streamlit UI와 연계 |

### 6.2 CloudFront(CDN)

| Check Point | Question | Answer |
|---|---|---|
| 비용 | 인터넷망을 통한 Region 데이터 전송량 | 내부 운영자 서비스이므로 매우 낮음. 월 1~10GB 예상 |
| 비용 | Origin으로 Region 데이터 전송량 | 낮음 |
| 비용 | 한달 HTTP 요청 건수 | 초기 1만~10만 requests/month 예상 |
| 기능 | Origin 위치 | Cloud VM 또는 내부 Load Balancer |
| 기능 | 도메인 sharding 필요 여부 | 불필요 |
| 기능 | 글로벌 Edge 서비스 필요 여부 | 불필요. 내부 운영자 서비스이며 CDN 대상 static 대용량 content 없음 |

### 6.3 API Gateway

| Check Point | Question | Answer |
|---|---|---|
| 비용 | 한달 API 호출 건수 | 초기 1만~10만 calls/month 예상 |
| 비용 | 응답 데이터 전송 사이즈 | 일반 API 1KB~100KB, routing result JSON 1MB~10MB 가능 |
| 비용 | 캐시 사용 여부 | 초기 불필요. routing result는 DB 저장 후 조회 |
| 비용 | 로깅 설정 여부 | API access/error log 필요. 개인정보 masking 필요 |
| 기능 | 적용 여부 | 내부망 전용이면 필수 아님. 외부/사내 표준 API 노출 필요 시 적용 검토 |

### 6.4 SQS(Simple Queue Service)

| Check Point | Question | Answer |
|---|---|---|
| 비용 | 한달 표준 Queue 요청 개수 | 초기 1천~1만 requests/month 예상 가능 |
| 비용 | 한달 FIFO Queue 요청 개수 | 초기 불필요 |
| 비용 | 데이터 송신 사이즈 | routing job metadata 수준, 건당 1KB 이하 권장 |
| 기능 | 적용 여부 | 현재는 DB 기반 request/status 관리 가능. 동시 job 증가 시 SQS로 routing worker queue 분리 검토 |

### 6.5 Code Deploy

| Check Point | Question | Answer |
|---|---|---|
| 비용 | Source 저장소는 S3인가요 | Git repository 사용 권장 |
| 비용 | Source version 관리 여부 | 예, Git 기반 version 관리 |
| 비용 | Source size/version 보관 | source 수백 MB 이하 예상. OSRM/DB data는 source에 포함하지 않음 |
| 기능 | Source는 S3와 git 중 어느 것입니까 | Git |
| 기능 | 배포 대상 서버가 AWS EC2인가요 | AWS 사용 시 EC2 대상 |
| 기능 | Auto Scale 환경인가요 | 초기에는 Auto Scale 미적용. routing stateful workload 특성상 worker 분리 후 검토 |
| 기능 | Jenkins Plugin 연동 필요 여부 | 사내 표준 배포 체계에 따라 검토 |

### 6.6 Code Build

| Check Point | Question | Answer |
|---|---|---|
| 비용 | Build 완료까지 걸리는 시간 | Python dependency 설치 포함 5~15분 예상 |
| 비용 | Build memory | 3GB 이상 권장. geospatial/OR-Tools dependency 포함 시 7GB 검토 |
| 비용 | 월 Build 횟수 | 초기 개발/운영 전환 기간 20~50회/month 예상 |
| 기능 | 적용 여부 | CI 검증, py_compile/unit test, package build에 사용 가능. OSRM graph build는 별도 batch/server 작업 권장 |

