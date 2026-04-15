# 공통모드 (Common VRP) 수정 내역 — 2026-04-15

## 배경

공통모드에서 Payload 빌드 → Routing 요청의 2단계 흐름을 정비하고,
테크니션 순서에 따른 VRP 최적화 품질 편차 문제를 해결하기 위한 일련의 수정.

---

## 1. DMS2 → DMS 정규화 제거 (`common_vrp_runtime.py`)

- Codex가 추가한 `_runtime_center_type()` 함수 제거
- `center_type` 처리를 inline으로 변경: `str(...).strip().upper() or "DMS"`
- **이유**: DMS2 정규화가 VRP 결과에 영향을 미쳐 성능 저하 발생

---

## 2. VRP 시간 제한 하드코딩 제거 (`vrp_mode_na_general.py`, `production_assign_atlanta_vrp.py`)

- `time_limit.FromSeconds(20)` 하드코딩 제거
- payload `options.time_limit_seconds` 값 적용 (기본값 30초, 최소 10초)
- `_solve_vrp_day()` 및 `build_atlanta_production_assignment_vrp_from_frames()`에 `time_limit_seconds` 파라미터 추가

---

## 3. Routing Mode 선택 제거 (`sr_common_vrp_client.py`)

- na_general / z_weekend 선택 드롭박스 제거
- `na_general` 고정 사용

---

## 4. 테크니션 지리적 정렬 추가 (System 1 & System 2)

### System 2 (`common_vrp_runtime.py`)
- `active_technicians`를 경도(서→동) 기준으로 정렬 후 위도(남→북) 2차 정렬
- OR-Tools `PATH_CHEAPEST_ARC` 초기 해 품질 향상 목적

### System 1 (`vrp_api_client.py`)
- `build_payload_from_service_frame()` 내 엔지니어 목록을 홈 좌표 기준 경도 정렬
- 두 시스템 간 일관성 확보

---

## 5. Payload 빌드 / Routing 요청 분리 구조 개선

### 기존 문제
- Streamlit 프로세스에서 `build_payload_from_inputs` 직접 호출 시 psycopg2 EUC-KR 인코딩 오류 발생
  (`UnicodeDecodeError: 'utf-8' codec can't decode byte 0xb8`)
- 원인: Windows 한국어 로케일(EUC-KR)과 psycopg2 C 레이어 충돌

### 해결
- Build Payload: 클라이언트에서 `/api/v1/common/routing/build-payload` API 호출로 변경 (서버 측 DB 접근)
- Request Routing: 클라이언트에서 `/api/v1/common/routing/submit` API 호출

### 추가된 엔드포인트 (`common_vrp_api_server.py`)
- `POST /api/v1/common/routing/submit` — 클라이언트가 완성한 payload를 받아 VRP 큐에 등록

### 추가된 함수 (`common_vrp_runtime.py`)
- `submit_routing_from_payload(payload, subsidiary_name, strategic_city_name, promise_date)` — payload를 받아 job 등록 및 스레드 실행

---

## 6. HTTP 응답 안정화 (`common_vrp_api_server.py`)

- `ensure_ascii=False` → `ensure_ascii=True`, `encode("utf-8")` → `encode("ascii")`
- `handler.wfile.flush()` 추가
- **이유**: 대용량 응답(~222KB) 전송 시 `IncompleteRead` 오류 방지

---

## 7. is_heavy_repair / service_minutes를 submit 단계로 이동

### 변경 전
- `/build-payload` 시 서버가 DB의 중수리 룰을 조회해 payload jobs에 `is_heavy_repair`, `service_minutes` 포함

### 변경 후
- `/build-payload`: 중수리 룰 조회 없이 job 기본 정보만 포함 (빌드 속도 향상)
- `/submit`: `_enrich_jobs_heavy_repair()` 헬퍼가 DB에서 중수리 룰 조회 후 `is_heavy_repair`, `service_minutes` 자동 추가

### 관련 함수 (`common_vrp_runtime.py`)
- `_enrich_jobs_heavy_repair(jobs, config_path)` 신규 추가
- `_build_payload_from_dataframes()` — heavy repair 룰 로딩 코드 제거

---

## 8. UI 개선 (`sr_common_vrp_client.py`)

- "Request Routing" / "Check Routing Result" 버튼을 나란히 (2컬럼) 배치
- Build Payload 성공 메시지에서 heavy repair 카운트 제거 (submit 시점에 결정되므로)

---

## 수정 파일 목록

| 파일 | 변경 내용 |
|------|-----------|
| `smart_routing/common_vrp_runtime.py` | DMS2 정규화 제거, 지리 정렬 추가, submit_routing_from_payload 추가, heavy repair enrichment를 submit 단계로 이동 |
| `smart_routing/common_vrp_api_server.py` | /submit 엔드포인트 추가, ASCII 인코딩, flush, debug 간소화 |
| `smart_routing/vrp_mode_na_general.py` | time_limit_seconds 파라미터 적용 |
| `smart_routing/production_assign_atlanta_vrp.py` | _solve_vrp_day time_limit_seconds 파라미터 추가 |
| `smart_routing/vrp_api_client.py` | 테크니션 지리 정렬 추가 |
| `sr_common_vrp_client.py` | Routing Mode 선택 제거, Build/Submit 2단계 API 흐름, 버튼 2컬럼 배치 |
