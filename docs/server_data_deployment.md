# 서버 데이터 배포

코드와 데이터는 별도 패키지로 배포한다. 코드 ZIP에는 CSV, Excel, Parquet,
지도 ZIP, 실행 결과를 넣지 않는다.

## 로컬 데이터 분류

```text
data/north_america/
├─ raw/                 # 수정하지 않는 원본 snapshot, 서버 업로드 제외
├─ processed/           # 전처리·좌표 생성이 끝난 service/profile
├─ planning/            # 검토 전 clustering 후보, 서버 업로드 제외
├─ reviewed/regions/    # 검토가 끝난 지역 경계
├─ db_input/            # region/technician/lookup DB 입력
├─ reference/           # client master, ZCTA, symptom 기준정보
├─ runtime/             # 로컬 개발 실행 상태
└─ reports/             # 분석 결과, 서버 업로드 제외
```

`260310/`은 남아 있는 Asia·Atlanta 보조 스크립트의 호환 archive다. 신규 파일을
추가하지 않으며, 참조가 모두 catalog로 전환되기 전에는 삭제하지 않는다.

## 서버 업로드 데이터 생성

```powershell
powershell -File services/deploy/build_server_data_package.ps1 -Version 2026.07.19 -AcknowledgeSensitiveData
```

생성 결과:

```text
deployment/server_data/<version>/
├─ shared/config/data_catalog.development.json
├─ shared/config/data_catalog.production.json
├─ shared/north_america/
│  ├─ processed/
│  ├─ reviewed/regions/
│  ├─ db_input/
│  └─ reference/
├─ state/development/{common_vrp_jobs,vrp_api_jobs,cache,logs}/
├─ state/production/{common_vrp_jobs,vrp_api_jobs,cache,logs}/
└─ manifest.json
```

이 폴더의 내용을 `/home/csda/AI_Routing/`에 업로드한다. systemd의 개발 서비스는
`data_catalog.development.json`, 운영 서비스는 `data_catalog.production.json`을
사용한다. 각 catalog의 `state_root`가 서로 다르다. `manifest.json`에는
모든 읽기 데이터의 SHA-256이 기록된다. 개발과 운영은 `shared` 읽기 데이터만
공유하며 `state`는 절대로 공유하거나 symlink하지 않는다.

service, 운영 profile, DB technician 자료에는 주소·기사 식별정보가 포함될 수 있다.
그래서 빌더는 `-AcknowledgeSensitiveData` 없이는 실행되지 않는다. 서버에서는
`shared` 디렉터리를 서비스 계정과 관리자만 읽을 수 있게 제한하고, FTP 공개 경로나
웹 문서 루트에는 두지 않는다. 원본 service와 원본 profile은 번들에 포함하지 않는다.
지도용 technician 목록도 `Home Address`, `Home Zip`을 제거한 processed 파일만 포함한다.

실제 서버 업로드와 DB 입력은 자동으로 수행되지 않는다.
