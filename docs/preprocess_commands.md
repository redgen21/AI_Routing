# 전처리 실행 명령

북미 canonical 입력/출력은 `config/data_catalog.json`에서 결정한다. 현재 active
원본 service와 geocoded 출력, 원본/운영 profile을 사용할 때는 경로 인자가 필요 없다.

```powershell
python -m tools.preprocess.sr_preprocess_service --config-file config/config.json --geocode-backend auto
python -m tools.operations.sr_production_atlanta_prep
```

아래 명시적 경로 예시는 과거 snapshot 재현 또는 별도 실험용이다.

## 기본 원칙

`tools.preprocess.sr_preprocess_service`의 `--geocode-backend auto`는 국가를 보고 자동으로 전처리 경로를 선택한다.

- `IDN`, `THA`, `MYS`: `Nominatim -> HERE -> Google`
- `USA`: `Census -> HERE -> Google`

`Nominatim`은 `config/config.json`의 아래 서버 설정을 사용한다.

```json
"nominatim": {
  "url": "http://20.51.244.68:8080"
}
```

## 아시아 데이터 전처리

일반적으로 아시아 데이터는 `auto`로 실행하면 된다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_auto_geocoded.csv" --config-file config/config.json --geocode-backend auto
```

아시아 fallback 흐름을 명시해서 실행하려면 `asia-fallback`을 사용한다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_asia_fallback_geocoded.csv" --config-file config/config.json --geocode-backend asia-fallback
```

## Nominatim만 테스트

비용이 드는 HERE/Google을 호출하지 않고 Nominatim 서버만 확인하려면 `nominatim`을 사용한다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_nominatim_geocoded.csv" --config-file config/config.json --geocode-backend nominatim
```

## 미국 데이터 전처리

미국 데이터는 `auto`를 사용하면 기존 미국 경로인 `Census -> HERE -> Google`로 실행된다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\원본파일.csv" --output-file "260310\input\Service_미국데이터_normalized_geocoded.csv" --config-file config/config.json --geocode-backend auto
```

## 유용한 옵션

이미 실패로 캐시된 주소를 다시 시도하려면 `--retry-failed`를 붙인다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_retry_geocoded.csv" --config-file config/config.json --geocode-backend auto --retry-failed
```

일부 주소만 테스트하려면 `--limit`을 붙인다. 이 값은 신규 조회 대상 고유 주소 수 기준이다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_sample_geocoded.csv" --config-file config/config.json --geocode-backend auto --limit 100
```

좌표 변환 없이 정규화와 접수번호 중복 제거만 하려면 `--skip-geocode`를 사용한다.

```powershell
python -m tools.preprocess.sr_preprocess_service --service-file "260310\_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202607101026.csv" --output-file "260310\input\Service_202607101026_normalized_only.csv" --config-file config/config.json --skip-geocode
```

## 출력에서 확인할 값

실행 후 콘솔에 아래 값들이 출력된다.

- `source_rows`: 원본 행 수
- `output_rows`: 전처리 후 행 수
- `dropped_blank_address_rows`: 주소가 비어 제거된 행 수
- `dropped_blank_receipt_rows`: 접수번호가 비어 제거된 행 수
- `dropped_duplicate_receipt_rows`: 중복 접수번호 제거 수
- `unique_address_rows`: Nominatim 조회 기준 고유 주소 수
- `nominatim_attempted_rows`: Nominatim 신규 조회 수
- `here_attempted_rows`: HERE 신규 조회 수
- `google_attempted_rows`: Google 신규 조회 수
- `geocoded_rows`: 최종 좌표 변환 성공 행 수
- `failed_geocode_rows`: 최종 좌표 미변환 행 수
