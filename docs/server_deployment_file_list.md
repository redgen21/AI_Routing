# 서버 배포 파일 목록

이 문서는 2026-07-19 SFTP 읽기 전용 조사와 실제 Python import graph를 기준으로 한다.
현재 서버 인벤토리는 `docs/server_inventory_current.md`에 저장되어 있다.

현재 서버 `smart_routing`은 30개 파일이고 새 allowlist는 26개다. 현재 서버에만
남아 있는 로컬 전용 파일은 다음 6개다.

현재 서버에는 아직 다음 목표 디렉터리가 없다.

```text
/home/csda/AI_Routing/development
/home/csda/AI_Routing/production
/home/csda/AI_Routing/shared
/home/csda/AI_Routing/state
/home/csda/AI_Routing/services
/home/csda/AI_Routing/systemd
```

```text
asia_geocode_cleaner.py
bigquery_runtime.py
export_daily_stats.py
prewarm_map_cache.py
profile_sync.py
vrp_api_client.py
```

새 runtime에는 필요하지만 현재 서버에는 없는 파일은 다음 2개다.

```text
data_catalog.py
geocode_storage.py
```

기존 파일을 개별 덮어쓰기하는 방식으로는 이 차이를 안전하게 반영하기 어려우므로,
새 `development/` release 디렉터리에 완전한 allowlist 패키지를 배치해야 한다.

## 배포 원칙

- 코드 패키지는 `services/deploy/build_deploy_package.ps1`의 명시적 allowlist만 포함한다.
- `smart_routing/` 전체를 복사하지 않는다.
- 데이터는 코드 ZIP과 분리하여 `deployment/server_data/<version>/`에서 공급한다.
- 개발과 운영은 각각 `/home/csda/AI_Routing/development`, `production`에서 실행한다.
- 공통 읽기 데이터는 `/home/csda/AI_Routing/shared`, 쓰기 상태는 `state/<environment>`를 사용한다.
- 기존 서버 루트의 legacy 파일은 자동 삭제하거나 덮어쓰지 않는다.

## 코드 패키지에 포함하는 루트 파일

```text
runtime_env.sh
restart_smart_routing_api.sh
sr_common_vrp_api_server.py
sr_common_vrp_client_server.py
sr_vrp_api_server.py
verify_deployment.py
```

개발 패키지에만 다음 파일을 추가한다.

```text
bootstrap_common_vrp_dev.sh
start_common_vrp_client_server_dev.sh
start_common_vrp_dev.sh
```

운영 패키지에만 다음 파일을 추가한다.

```text
restart_common_vrp_api.sh
restart_common_vrp_client_server.sh
start_common_vrp_client_server_prod.sh
start_common_vrp_prod.sh
```

## 서버 필수 `smart_routing` 파일

```text
smart_routing/__init__.py
smart_routing/area_map.py
smart_routing/census_geocoder.py
smart_routing/common_vrp_api_server.py
smart_routing/common_vrp_db.py
smart_routing/common_vrp_runtime.py
smart_routing/data_catalog.py
smart_routing/geocode_storage.py
smart_routing/google_geocoder.py
smart_routing/here_geocoder.py
smart_routing/live_atlanta_runtime.py
smart_routing/nominatim_geocoder.py
smart_routing/osrm_routing.py
smart_routing/production_atlanta.py
smart_routing/region_design.py
smart_routing/region_sweep.py
smart_routing/routing_compare.py
smart_routing/service_preprocess.py
smart_routing/us_geocode_cleaner.py
smart_routing/vrp_api_common.py
smart_routing/vrp_api_server.py
smart_routing/vrp_api_service.py
smart_routing/vrp_mode_na_general.py
smart_routing/vrp_mode_z_weekend.py
smart_routing/production_assign_atlanta.py
smart_routing/production_assign_atlanta_vrp.py
```

마지막 네 파일은 요청 시 동적으로 import되므로 정적 import 검색에서 제외하면 안 된다.

## 서버 필수 adapter, config, systemd

```text
services/__init__.py
services/api/__init__.py
services/api/common_vrp_config.py
services/api/run_common_vrp_api.py
services/api/sr_vrp_api_server.py
config/config.template.json
config/common_vrp.<environment>.template.json
config/data_catalog.json
requirements.txt
deploy_manifest.json
```

개발 systemd:

```text
systemd/common-vrp-dev.service
systemd/common-vrp-client-dev.service
systemd/smart-routing-dev.service
```

운영 systemd:

```text
systemd/common-vrp.service
systemd/common-vrp-client.service
systemd/smart-routing.service
```

## 코드 패키지에서 제외하는 로컬 전용 파일

```text
smart_routing/area_map_usa.py
smart_routing/asia_geocode_cleaner.py
smart_routing/bigquery_runtime.py
smart_routing/export_daily_stats.py
smart_routing/prewarm_map_cache.py
smart_routing/production_assign_atlanta_osrm.py
smart_routing/profile_sync.py
smart_routing/vrp_api_client.py
smart_routing/select_data.sql
```

다음 범위도 runtime 코드 패키지에서 제외한다.

```text
260310/
data/
admin_tools/
tools/
services/deploy/
services/tests/
tests/
scripts/
docs/                 # -IncludeDocs를 지정한 경우만 포함
log/
__pycache__/
*.csv, *.xlsx, *.parquet, OSRM graph
config/server_ftp.local.json
실제 config와 secret
```

## 현재 생성된 개발 검증 패키지

```text
deployment/development/2026.07.19-server-allowlist/
deployment/development/ai-routing-runtime-development-2026.07.19-server-allowlist.zip
```

이 패키지는 dirty source에서 생성한 개발 검증본이다. 운영 배포에는 사용할 수 없다.
운영 패키지는 변경사항을 commit한 clean checkout에서 `-AllowDirtySource` 없이 생성한다.

## 서버 업로드 대상

1. 개발 코드 ZIP 내용 → `/home/csda/AI_Routing/development/`
2. 서버 데이터 번들 내용 → `/home/csda/AI_Routing/`
3. 서버에서 template을 복사해 실제 config 작성
4. 개발 systemd 설치·재시작 후 세 서비스 health 확인
5. 검증 완료 후 clean source에서 운영 ZIP 생성 → `/home/csda/AI_Routing/production/`

현재 작업에서는 서버 조회만 수행했으며 업로드, 삭제, 이동, systemd 변경은 수행하지 않았다.
