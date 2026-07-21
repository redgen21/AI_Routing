# North America Data Pipeline

## 1. 목적

북미 라우팅 데이터의 canonical root는 `data/north_america/`로 한다.
`260310/`은 migration 동안의 호환본으로만 유지하고, 신규 데이터나 산출물을
추가하는 active root로 사용하지 않는다.

실행 코드는 파일명을 직접 선택하지 않고 `config/data_catalog.json`의 dataset ID로
active 경로를 해결해야 한다. Catalog에 등록되지 않은 백업, 중간 산출물,
실험 결과는 active 입력이 아니다.

## 2. Canonical 구조

```text
data/north_america/
├─ raw/
│  ├─ service/<version>/       # 변경하지 않는 source snapshot
│  └─ profile/<version>/       # 원본 profile workbook
├─ processed/
│  ├─ service/<version>/       # schema/coordinate/quality 검증을 통과한 service
│  └─ profile/<version>/       # 운영용으로 정규화된 profile
├─ planning/
│  └─ regions/
│     └─ candidates/           # region-count/algorithm별 후보
├─ reviewed/
│  └─ regions/                 # 정량 평가와 리뷰를 통과한 plan
├─ db_input/
│  └─ regions/                 # DB/API seed contract에 맞춰 물리화한 plan
├─ runtime/
│  ├─ development/             # 개발 DB import, job, cache, export
│  └─ production/              # 운영 DB import, job, cache, export
└─ reports/                         # 비교, 품질, route-score, release evidence
```

`raw` 및 `processed`는 환경과 무관한 버전 데이터다. `runtime/development`와
`runtime/production`은 서로의 파일, cache, job ID, DB export를 공유하면 안 된다.
`reports`는 다른 단계의 입력이 아니며 재생성 가능해야 한다.

## 3. 현재 canonical 파일 mapping

| Dataset ID | Legacy compatibility path | Canonical target path | 상태 |
| --- | --- | --- | --- |
| `service_raw` | `260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv` | `data/north_america/raw/service/202607101026/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv` | 원본 snapshot |
| `service_geocoded` | `260310/input/Service_202603181109_geocoded.csv` | `data/north_america/processed/service/202603181109/Service_202603181109_geocoded.csv` | 현재 코드 default |
| `profile_raw` | `260310/Top 10_DMS_DMS2_Profile_20260317.xlsx` | `data/north_america/raw/profile/20260317/Top 10_DMS_DMS2_Profile_20260317.xlsx` | 원본 profile |
| `profile_production` | `260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx` | `data/north_america/processed/profile/20260317/Top 10_DMS_DMS2_Profile_20260317_production.xlsx` | 운영용 profile |

첫 번째 service raw와 두 번째 processed service는 현재 이름과 내용상 하나의
lineage로 볼 수 없다. Raw는 2026-07-10 snapshot 160,949행/90열이고 processed는
2026-03-18 snapshot 26,605행/44열이다. Migration은 두 파일을 그대로 연결하지
않는다. 2026-07-10 raw에서 새 processed service를 재생성하고 row accounting,
schema, rejects, coordinate 품질을 검증한 후 `service_geocoded`를 새 version으로
교체한다.

`260310/input/fixed_region_maps/*.csv`는 migration 시점의 reviewed compatibility baseline으로
`reviewed/regions/`에 copy한다. 이후 새 region 후보는 plan ID와 version을 부여해
`planning/regions/candidates/<plan_id>/`에서 시작한다. 승인된 파일만
`reviewed/regions/<plan_id>/`로 promotion하고, 별도 변환으로 `db_input/regions/`를 만든다.

Atlanta 3-region baseline은 기존 reviewed 184 ZIP을 그대로 유지하면서 active service에만
존재하던 15 ZIP을 기존 운영 seed의 region_seq로 보완했다. 그 결과 reviewed와 DB seed는
동일한 199 ZIP/region assignment를 가지며 active Atlanta service 191 ZIP을 모두 포함한다.
이 migration extension은 `catalog/migration_manifest.json`에 추가 ZIP과 입력 checksum을
기록한다. 다음 정식 clustering에서는 이 임시 보완을 새 plan 평가로 대체해야 한다.

## 4. Data catalog contract

`config/data_catalog.json`은 schema, canonical root, active role-to-path mapping을 갖는다.
Version은 canonical root 아래의 경로에 표현한다.

```json
{
  "schema": "north-america-routing-data-catalog/v1",
  "data_root": "data/north_america",
  "active": {
    "service_raw": "raw/service/202607101026/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv",
    "service_geocoded": "processed/service/202603181109/Service_202603181109_geocoded.csv",
    "profile_raw": "raw/profile/20260317/Top 10_DMS_DMS2_Profile_20260317.xlsx",
    "profile_production": "processed/profile/20260317/Top 10_DMS_DMS2_Profile_20260317_production.xlsx",
    "region_candidates_dir": "planning/regions/candidates",
    "reviewed_regions_dir": "reviewed/regions",
    "region_seed_dir": "db_input/regions",
    "development_runtime_dir": "runtime/development",
    "production_runtime_dir": "runtime/production",
    "reports_dir": "reports",
    "migration_manifest": "catalog/migration_manifest.json"
  }
}
```

Catalog 변경은 데이터 배포다. 변경 PR은 기존/신규 path, checksum, row/sheet
count, lineage, 품질 결과, rollback role/path를 포함해야 한다. Checksum과 copy
결과는 catalog 대신 `catalog/migration_manifest.json`에 기록한다.
Catalog loader는 없는 ID, root 밖 path, 없는 파일, stage 불일치를 fail-closed로
거부해야 한다.

## 5. Region lifecycle와 평가

1. Curated service를 postal/coordinate 기준으로 집계한다.
2. Region-count, seed, algorithm, parameter별 후보를 `planning/regions/candidates`에 생성한다.
3. 모든 후보는 공개 routing evaluation contract로 distance, travel time, balance,
   max radius, capacity, unassigned, runtime을 비교한다.
4. Coverage 100%, postal 중복 0, empty region 0, fixed-boundary 보존과 정량 기준을
   통과한 plan만 `reviewed/regions`으로 promotion한다.
5. Reviewed plan을 DB/API schema로 변환해 `db_input/regions`에 저장한다. Candidate를
   DB에 직접 seed하지 않는다.

## 6. 도시별 기사 배정 정책

Region plan과 일자별 기사 배정은 다른 contract다. Region 파일은 지리적 경계와
region ID를 정의하고, 아래 정책은 routing 평가와 실행에서 적용한다.

| City | Policy | Contract |
| --- | --- | --- |
| Atlanta | `home_distance_only` | 기사의 preferred region을 점수화하지 않고 home-to-job/route 거리로 배정한다. 자격, fixed job, slot, time window, work/travel limit은 여전히 hard constraint다. |
| Los Angeles | `preferred_region_soft` | `preferred_region_name`이 다른 작업에 soft penalty를 주되 hard ban으로 사용하지 않는다. 담당 region 우선을 유지하면서 불가피한 cross-region 배정으로 미배정을 줄인다. |

평가 report는 city policy, penalty 값, OSRM/matrix version, fallback, solver seed를 기록해야
한다. Policy가 다른 두 도시의 점수를 동일 목적함수로 단순 비교하지 않는다.

## 7. Migration 규칙

1. 현재 hard-coded `260310/` 참조를 inventory로 고정한다.
2. Target으로 copy한 뒤 size/checksum/row count/sheet count를 비교한다. Raw는 copy
   과정에서 변환하지 않는다.
3. `config/data_catalog.json`과 loader를 먼저 배포하고, consumer를 dataset ID로
   전환한다.
4. 개발에서 row accounting, schema/null/duplicate/coordinate, region coverage, routing
   regression을 통과한다.
5. `runtime/development`로 dry run한 후 운영 catalog을 별도 승인으로 cutover한다.
6. 호환 기간에만 legacy alias를 허용한다. Alias 사용은 warning과 dataset ID를
   남기며, active consumer가 0이 되면 `260310/`을 read-only archive로 전환한다.

현재 `Service_202603181109_geocoded_recovered.csv`는
`Service_202603181109_geocoded.csv`와 checksum이 같은 중복본이다. Production profile backup과
2026-07-10 전처리 실험 산출물도 active reference가 없으면 checksum manifest를 만든 뒤
canonical tree 밖 archive로 옮긴다. 이들을 `raw`, `processed`, `reviewed`에 임의로
섞지 않는다.

배포 artifact에는 catalog, reviewed maps, region seeds, migration manifest가 포함된다.
대용량/PII raw·processed service와 profile은 Git 및 artifact에서 제외되므로 DB bootstrap과
지도 실행 전에 catalog의 네 active 파일을 승인된 데이터 저장소에서 hydrate하고 checksum을
확인해야 한다.

### 실행 명령

```powershell
# Legacy 기준 파일을 canonical root로 checksum 검증 복사
python -m tools.data.migrate_legacy_layout --dry-run
python -m tools.data.migrate_legacy_layout

# Candidate를 coverage/중복 검사 후 reviewed와 DB seed로 승격
python -m tools.data.promote_region_plan `
  data/north_america/planning/regions/candidates/<candidate>.csv `
  --plan-id <immutable_plan_id> `
  --reviewed-name fixed_region_postal_<city>_<count>_<immutable_plan_id>.csv `
  --seed-name <city>_fixed_region_zip_<count>_<immutable_plan_id>.csv `
  --evaluation-file <routing_evaluation.json> `
  --approved-by <reviewer> `
  --approval-reference <ticket_or_review_id> `
  --city "Los Angeles, CA"
```

Region 생성기는 `planning/regions/candidates`에만 쓴다. `promote_region_plan`만
`reviewed/regions`와 `db_input/regions`를 변경할 수 있다. Promotion은 coverage,
postal 중복, empty region, fixed boundary, routing evaluation gate가 모두 통과한
증빙과 승인 정보를 요구한다. 기존 파일 교체는 현재 checksum을 명시한
compare-and-swap 방식만 허용하며 기본 동작은 overwrite 거부다.

## 8. Definition of done

- Raw/processed/profile/region dataset이 catalog ID와 version을 갖는다.
- Raw-to-processed row accounting이 input, accepted, rejected, duplicate로 맞는다.
- 모든 postal이 하나의 reviewed region에 속하고 빈 region이 없다.
- Region candidate와 reviewed/seed plan을 파일명만으로 혼동할 수 없다.
- Development와 production runtime이 물리적으로 분리된다.
- 기준 routing 메트릭과 도시별 배정 정책 회귀가 통과한다.
- 운영 cutover/rollback dataset ID와 checksum이 release record에 남는다.
