from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import time
from urllib import error, parse, request

import pandas as pd

from .nominatim_geocoder import NominatimGeocoder, clean_text


TEXT_COLUMNS = [
    "SUBSIDIARY_NAME",
    "STRATEGIC_CITY_NAME",
    "SVC_ENGINEER_CODE",
    "SVC_ENGINEER_NAME",
    "SVC_CENTER_TYPE",
    "SVC_RECEIPT_TYPE",
    "SVC_TYPE_CODE",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_GROUP_NAME",
    "SERVICE_PRODUCT_CODE",
    "SERVICE_PRODUCT_NAME",
    "SVC_PRODUCT_GROUP_NAME",
    "SVC_PRODUCT_NAME",
    "RECEIPT_DETAIL_SYMPTOM_CODE",
    "GSFS_RECEIPT_NO",
    "STATE_NAME",
    "CITY_NAME",
    "COUNTRY_NAME",
    "POSTAL_CODE",
    "ADDRESS_LINE1_INFO",
]


@dataclass(frozen=True)
class ServicePreprocessSummary:
    source_rows: int
    output_rows: int
    dropped_blank_address_rows: int
    dropped_blank_receipt_rows: int
    dropped_duplicate_receipt_rows: int
    geocoded_rows: int = 0
    failed_geocode_rows: int = 0
    unique_address_rows: int = 0
    nominatim_attempted_rows: int = 0
    nominatim_remaining_rows: int = 0
    here_attempted_rows: int = 0
    google_attempted_rows: int = 0


ADDRESS_COLUMNS = ["ADDRESS_LINE1_INFO", "CITY_NAME", "STATE_NAME", "COUNTRY_NAME", "POSTAL_CODE"]
ASIA_COUNTRIES = {"IDN", "INDONESIA", "THA", "THAILAND", "MYS", "MALAYSIA"}
COUNTRY_DISPLAY_NAMES = {
    "IDN": "Indonesia",
    "INDONESIA": "Indonesia",
    "THA": "Thailand",
    "THAILAND": "Thailand",
    "MYS": "Malaysia",
    "MALAYSIA": "Malaysia",
    "USA": "USA",
    "US": "USA",
    "UNITED STATES": "USA",
}
COUNTRY_CODES = {
    "IDN": "id",
    "INDONESIA": "id",
    "JAKARTA": "id",
    "BEKASI": "id",
    "THA": "th",
    "THAILAND": "th",
    "BANGKOK": "th",
    "MYS": "my",
    "MALAYSIA": "my",
    "KUALA LUMPUR": "my",
    "USA": "us",
    "US": "us",
    "UNITED STATES": "us",
}
PROVIDER_CACHE_COLUMNS = [
    "asia_address_key",
    "geocode_query",
    "query_variant",
    "geocode_status",
    "latitude",
    "longitude",
    "matched_address",
    "location_type",
    "place_id",
    "importance",
    "source",
    "error_message",
    "geocoded_date",
]


def _load_json_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _read_service_csv(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ["utf-8-sig", "cp949", "euc-kr", "latin1"]:
        try:
            return pd.read_csv(path, encoding=encoding, low_memory=False)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


def _clean_text_series(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .replace(
            {
                "nan": "",
                "None": "",
                "none": "",
                "NaN": "",
                "NAN": "",
                "NaT": "",
                "nat": "",
            }
        )
    )


def _normalize_postal_code(series: pd.Series) -> pd.Series:
    working = series.astype("object")
    postal_numeric = pd.to_numeric(working, errors="coerce")
    has_postal_numeric = postal_numeric.notna()
    normalized = _clean_text_series(working.astype(str).str.replace(r"\.0+$", "", regex=True))
    normalized.loc[has_postal_numeric] = postal_numeric.loc[has_postal_numeric].astype("Int64").astype(str)
    nonempty = normalized.ne("")
    normalized.loc[nonempty] = normalized.loc[nonempty].str.zfill(5)
    return normalized


def _normalize_yyyymmdd(series: pd.Series) -> pd.Series:
    working = series.astype("object")
    numeric = pd.to_numeric(working, errors="coerce")
    has_numeric = numeric.notna()
    normalized = _clean_text_series(working.astype(str).str.replace(r"\.0+$", "", regex=True))
    normalized.loc[has_numeric] = numeric.loc[has_numeric].astype("Int64").astype(str)
    return normalized


def _clean_text(value: object) -> str:
    return clean_text(value)


def _country_display(value: object) -> str:
    text = _clean_text(value).upper()
    return COUNTRY_DISPLAY_NAMES.get(text, _clean_text(value))


def _address_key(row: pd.Series) -> str:
    return "|".join(_clean_text(row.get(col, "")) for col in ADDRESS_COLUMNS)


def _query_parts(row: pd.Series) -> dict[str, str]:
    return {
        "address": _clean_text(row.get("ADDRESS_LINE1_INFO", "")),
        "city": _clean_text(row.get("CITY_NAME", "")),
        "state": _clean_text(row.get("STATE_NAME", "")),
        "postal": _clean_text(row.get("POSTAL_CODE", "")),
        "country": _country_display(row.get("COUNTRY_NAME", "")),
    }


def _build_here_address_query(row: pd.Series) -> str:
    parts = _query_parts(row)
    city_state_postal = " ".join(part for part in [parts["city"], parts["state"], parts["postal"]] if part)
    return ", ".join(part for part in [parts["address"], city_state_postal, parts["country"]] if part)


def _build_nominatim_address_query(row: pd.Series) -> str:
    parts = _query_parts(row)
    admin_query = ", ".join(
        part
        for part in [parts["city"], parts["state"], parts["postal"], parts["country"]]
        if part
    )
    if admin_query:
        return admin_query
    return ", ".join(
        part
        for part in [parts["address"], parts["city"], parts["state"], parts["postal"], parts["country"]]
        if part
    )


def _country_code(row: pd.Series, configured: dict[str, str]) -> str:
    for col in ["STRATEGIC_CITY_NAME", "COUNTRY_NAME"]:
        text = _clean_text(row.get(col, "")).upper()
        if text in configured:
            return _clean_text(configured[text]).lower()
        if text in COUNTRY_CODES:
            return COUNTRY_CODES[text]
    return ""


def _missing_coord_mask(df: pd.DataFrame) -> pd.Series:
    lat = pd.to_numeric(df.get("latitude"), errors="coerce")
    lon = pd.to_numeric(df.get("longitude"), errors="coerce")
    return lat.isna() | lon.isna()


def _is_asia_service_df(df: pd.DataFrame) -> bool:
    if "COUNTRY_NAME" not in df.columns:
        return False
    countries = {_clean_text(value).upper() for value in df["COUNTRY_NAME"].dropna().unique()}
    countries.discard("")
    return bool(countries) and countries.issubset(ASIA_COUNTRIES)


def _add_address_query_formats(df: pd.DataFrame, configured_country_codes: dict[str, str]) -> pd.DataFrame:
    output = df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in output.columns:
            output[col] = ""
        output[col] = output[col].map(_clean_text)
    output["asia_address_key"] = output.apply(_address_key, axis=1)
    output["here_address_query"] = output.apply(_build_here_address_query, axis=1)
    output["nominatim_address_query"] = output.apply(_build_nominatim_address_query, axis=1)
    output["geocode_query"] = output["nominatim_address_query"]
    output["nominatim_country_code"] = output.apply(lambda row: _country_code(row, configured_country_codes), axis=1)
    output["nominatim_lookup_key"] = output["nominatim_country_code"] + "|" + output["nominatim_address_query"]
    return output


def _apply_nominatim_cache(df: pd.DataFrame, cache_df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    if "latitude" not in output.columns:
        output["latitude"] = pd.NA
    if "longitude" not in output.columns:
        output["longitude"] = pd.NA

    if cache_df.empty:
        output["source"] = "failed"
        output["geocode_status"] = "NO_CACHE"
        output["geocoded_ok"] = False
        return output

    cache = cache_df.rename(
        columns={
            "geocode_query": "nominatim_geocode_query",
            "query_variant": "nominatim_query_variant",
            "geocode_status": "nominatim_geocode_status",
            "latitude": "nominatim_latitude",
            "longitude": "nominatim_longitude",
            "matched_address": "nominatim_matched_address",
            "location_type": "nominatim_location_type",
            "place_id": "nominatim_place_id",
            "importance": "nominatim_importance",
            "source": "nominatim_source",
            "error_message": "nominatim_error_message",
            "geocoded_date": "nominatim_geocoded_date",
        }
    )
    cache_cols = [
        "asia_address_key",
        "nominatim_geocode_query",
        "nominatim_query_variant",
        "nominatim_geocode_status",
        "nominatim_latitude",
        "nominatim_longitude",
        "nominatim_matched_address",
        "nominatim_location_type",
        "nominatim_place_id",
        "nominatim_importance",
        "nominatim_source",
        "nominatim_error_message",
        "nominatim_geocoded_date",
    ]
    cache = cache[cache_cols].drop_duplicates(subset=["asia_address_key"], keep="last")
    if "nominatim_lookup_key" in output.columns:
        cache = cache.rename(columns={"asia_address_key": "nominatim_lookup_key"})
        output = output.merge(cache, on="nominatim_lookup_key", how="left")
    else:
        output = output.merge(cache, on="asia_address_key", how="left")

    nom_lat = pd.to_numeric(output.get("nominatim_latitude"), errors="coerce")
    nom_lon = pd.to_numeric(output.get("nominatim_longitude"), errors="coerce")
    ok_mask = nom_lat.notna() & nom_lon.notna() & output["nominatim_geocode_status"].astype(str).eq("OK")
    output.loc[ok_mask, "latitude"] = nom_lat[ok_mask]
    output.loc[ok_mask, "longitude"] = nom_lon[ok_mask]
    output.loc[ok_mask, "matched_address"] = output.loc[ok_mask, "nominatim_matched_address"]
    output.loc[ok_mask, "location_type"] = output.loc[ok_mask, "nominatim_location_type"]
    output.loc[ok_mask, "place_id"] = output.loc[ok_mask, "nominatim_place_id"]
    output["latitude"] = pd.to_numeric(output["latitude"], errors="coerce")
    output["longitude"] = pd.to_numeric(output["longitude"], errors="coerce")
    output["geocoded_ok"] = output["latitude"].notna() & output["longitude"].notna()
    output["source"] = "failed"
    output.loc[output["geocoded_ok"], "source"] = "nominatim"
    output["geocode_status"] = output["nominatim_geocode_status"].fillna("NO_CACHE")
    return output


def _merge_with_nominatim_geocodes(
    normalized_df: pd.DataFrame,
    config: dict,
    *,
    retry_failed: bool = False,
    limit: int | None = None,
) -> tuple[pd.DataFrame, int, int]:
    nominatim_cfg = config.get("nominatim", {})
    base_url = _clean_text(nominatim_cfg.get("url", ""))
    if not base_url:
        raise ValueError("Missing nominatim.url in config.json.")

    country_codes = {str(k).strip().upper(): str(v).strip().lower() for k, v in nominatim_cfg.get("country_codes", {}).items()}
    working = _add_address_query_formats(normalized_df, country_codes)
    unique_addresses = (
        working[ADDRESS_COLUMNS + ["STRATEGIC_CITY_NAME", "nominatim_lookup_key", "here_address_query", "nominatim_address_query", "geocode_query", "nominatim_country_code"]]
        .drop_duplicates(subset=["nominatim_lookup_key"], keep="first")
        .reset_index(drop=True)
    )
    unique_addresses["asia_address_key"] = unique_addresses["nominatim_lookup_key"]

    cache_file = Path(str(nominatim_cfg.get("cache_file", "data/nominatim_geocode_cache.csv")))
    geocoder = NominatimGeocoder(
        base_url=base_url,
        cache_path=cache_file,
        timeout=int(nominatim_cfg.get("timeout_sec", 30)),
        sleep_sec=float(nominatim_cfg.get("sleep_sec", 0.1)),
        user_agent=str(nominatim_cfg.get("user_agent", "ai-routing-service-preprocess")),
    )
    result = geocoder.geocode_missing(unique_addresses, retry_failed=retry_failed, limit=limit)
    output = _apply_nominatim_cache(working, geocoder.load_cache())
    return output, int(result.attempted), int(len(unique_addresses))


def _provider_cache_path(config: dict, provider: str) -> Path:
    nominatim_cfg = config.get("nominatim", {})
    geocoding_cfg = config.get("geocoding", {})
    if provider == "here":
        return Path(
            str(
                nominatim_cfg.get("here_cache_file")
                or geocoding_cfg.get("asia_here_cache_file")
                or "data/asia_here_fallback_geocode_cache.csv"
            )
        )
    return Path(
        str(
            nominatim_cfg.get("google_cache_file")
            or geocoding_cfg.get("asia_google_cache_file")
            or "data/asia_google_fallback_geocode_cache.csv"
        )
    )


def _read_provider_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=PROVIDER_CACHE_COLUMNS)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in PROVIDER_CACHE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[PROVIDER_CACHE_COLUMNS].copy()


def _save_provider_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _failed_provider_result(provider: str, query: str, status: str, message: str = "") -> dict[str, object]:
    return {
        "asia_address_key": "",
        "geocode_query": query,
        "query_variant": "",
        "geocode_status": status,
        "latitude": "",
        "longitude": "",
        "matched_address": "",
        "location_type": "",
        "place_id": "",
        "importance": "",
        "source": provider,
        "error_message": _clean_text(message),
        "geocoded_date": date.today().isoformat(),
    }


def _to_float(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _here_geocode(query: str, api_key: str, timeout: int) -> dict[str, object]:
    params = parse.urlencode({"q": query, "apiKey": api_key, "limit": "1"})
    url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return _failed_provider_result("here_geocoding_api", query, f"HTTP_{exc.code}", str(exc))
    except error.URLError as exc:
        return _failed_provider_result("here_geocoding_api", query, "URL_ERROR", str(exc))
    except json.JSONDecodeError as exc:
        return _failed_provider_result("here_geocoding_api", query, "BAD_JSON", str(exc))

    items = payload.get("items") or []
    if not items:
        return _failed_provider_result("here_geocoding_api", query, "NO_RESULTS")
    top = items[0]
    position = top.get("position") or {}
    lat = _to_float(position.get("lat"))
    lon = _to_float(position.get("lng"))
    if lat is None or lon is None:
        return _failed_provider_result("here_geocoding_api", query, "NO_COORDS")
    scoring = top.get("scoring") or {}
    return {
        "asia_address_key": "",
        "geocode_query": query,
        "query_variant": "",
        "geocode_status": "OK",
        "latitude": lat,
        "longitude": lon,
        "matched_address": _clean_text(top.get("title", "")),
        "location_type": _clean_text(top.get("resultType", "")),
        "place_id": _clean_text(top.get("id", "")),
        "importance": scoring.get("queryScore", ""),
        "source": "here_geocoding_api",
        "error_message": "",
        "geocoded_date": date.today().isoformat(),
    }


def _google_geocode(query: str, api_key: str, timeout: int) -> dict[str, object]:
    params = parse.urlencode({"address": query, "key": api_key})
    url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return _failed_provider_result("google_geocoding_api", query, f"HTTP_{exc.code}", str(exc))
    except error.URLError as exc:
        return _failed_provider_result("google_geocoding_api", query, "URL_ERROR", str(exc))
    except json.JSONDecodeError as exc:
        return _failed_provider_result("google_geocoding_api", query, "BAD_JSON", str(exc))

    status = _clean_text(payload.get("status")) or "UNKNOWN"
    if status != "OK":
        return _failed_provider_result("google_geocoding_api", query, status, _clean_text(payload.get("error_message", "")))
    results = payload.get("results") or []
    if not results:
        return _failed_provider_result("google_geocoding_api", query, "NO_RESULTS")
    top = results[0]
    geometry = top.get("geometry") or {}
    location = geometry.get("location") or {}
    lat = _to_float(location.get("lat"))
    lon = _to_float(location.get("lng"))
    if lat is None or lon is None:
        return _failed_provider_result("google_geocoding_api", query, "NO_COORDS")
    return {
        "asia_address_key": "",
        "geocode_query": query,
        "query_variant": "",
        "geocode_status": "OK",
        "latitude": lat,
        "longitude": lon,
        "matched_address": _clean_text(top.get("formatted_address", "")),
        "location_type": _clean_text(geometry.get("location_type", "")),
        "place_id": _clean_text(top.get("place_id", "")),
        "importance": "",
        "source": "google_geocoding_api",
        "error_message": "",
        "geocoded_date": date.today().isoformat(),
    }


def _provider_query_variants(row: pd.Series) -> list[tuple[str, str]]:
    candidates = [
        ("here_address_query", row.get("here_address_query", "")),
        ("full_address_query", row.get("full_address_query", "")),
        ("nominatim_address_query", row.get("nominatim_address_query", "")),
    ]
    output: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, query in candidates:
        cleaned = _clean_text(query)
        key = cleaned.lower()
        if cleaned and key not in seen:
            output.append((name, cleaned))
            seen.add(key)
    return output


def _unique_missing_provider_addresses(df: pd.DataFrame) -> pd.DataFrame:
    missing = df[_missing_coord_mask(df)].copy()
    if missing.empty:
        return missing
    missing["asia_address_key"] = missing["asia_address_key"].map(_clean_text)
    missing = missing[missing["asia_address_key"].ne("")].copy()
    missing["full_address_query"] = missing.apply(
        lambda row: ", ".join(
            part
            for part in [
                _clean_text(row.get("ADDRESS_LINE1_INFO", "")),
                _clean_text(row.get("CITY_NAME", "")),
                _clean_text(row.get("STATE_NAME", "")),
                _clean_text(row.get("POSTAL_CODE", "")),
                _country_display(row.get("COUNTRY_NAME", "")),
            ]
            if part
        ),
        axis=1,
    )
    keep_cols = [
        "asia_address_key",
        "here_address_query",
        "full_address_query",
        "nominatim_address_query",
    ]
    return missing[keep_cols].drop_duplicates(subset=["asia_address_key"], keep="first").reset_index(drop=True)


def _apply_provider_cache(df: pd.DataFrame, cache_df: pd.DataFrame, provider_label: str) -> pd.DataFrame:
    output = df.copy()
    ok_col = f"{provider_label}_geocoded_ok"
    if cache_df.empty:
        output[ok_col] = False
        return output

    cache = cache_df[cache_df["geocode_status"].eq("OK")].copy()
    if cache.empty:
        output[ok_col] = False
        return output

    rename_map = {
        "geocode_query": f"{provider_label}_geocode_query",
        "query_variant": f"{provider_label}_query_variant",
        "geocode_status": f"{provider_label}_geocode_status",
        "latitude": f"{provider_label}_latitude",
        "longitude": f"{provider_label}_longitude",
        "matched_address": f"{provider_label}_matched_address",
        "location_type": f"{provider_label}_location_type",
        "place_id": f"{provider_label}_place_id",
        "importance": f"{provider_label}_importance",
        "source": f"{provider_label}_source",
        "error_message": f"{provider_label}_error_message",
        "geocoded_date": f"{provider_label}_geocoded_date",
    }
    cache = cache.rename(columns=rename_map)
    cache_cols = ["asia_address_key"] + list(rename_map.values())
    output = output.drop(columns=[col for col in cache_cols if col != "asia_address_key" and col in output.columns], errors="ignore")
    output = output.merge(cache[cache_cols], on="asia_address_key", how="left")

    missing_mask = _missing_coord_mask(output)
    provider_lat = pd.to_numeric(output.get(f"{provider_label}_latitude"), errors="coerce")
    provider_lon = pd.to_numeric(output.get(f"{provider_label}_longitude"), errors="coerce")
    fill_mask = missing_mask & provider_lat.notna() & provider_lon.notna()
    source_value = "here_geocoding_api" if provider_label == "here" else "google_geocoding_api"
    for col in ["matched_address", "location_type", "place_id"]:
        if col not in output.columns:
            output[col] = ""
        output[col] = output[col].astype("object")
    output.loc[fill_mask, "latitude"] = provider_lat[fill_mask]
    output.loc[fill_mask, "longitude"] = provider_lon[fill_mask]
    output.loc[fill_mask, "source"] = source_value
    output.loc[fill_mask, "geocode_status"] = "OK"
    output.loc[fill_mask, "matched_address"] = output.loc[fill_mask, f"{provider_label}_matched_address"]
    output.loc[fill_mask, "location_type"] = output.loc[fill_mask, f"{provider_label}_location_type"]
    output.loc[fill_mask, "place_id"] = output.loc[fill_mask, f"{provider_label}_place_id"]
    output["latitude"] = pd.to_numeric(output["latitude"], errors="coerce")
    output["longitude"] = pd.to_numeric(output["longitude"], errors="coerce")
    output["geocoded_ok"] = output["latitude"].notna() & output["longitude"].notna()
    output[ok_col] = fill_mask
    return output


def _run_provider_fallback(
    current_df: pd.DataFrame,
    *,
    provider_label: str,
    cache_file: Path,
    api_key: str,
    timeout: int,
    sleep_sec: float,
    retry_failed: bool,
    limit: int | None,
    geocode_func,
) -> tuple[pd.DataFrame, int]:
    if not api_key:
        current_df[f"{provider_label}_geocoded_ok"] = False
        print(f"{provider_label} skipped: missing API key", flush=True)
        return current_df, 0

    unique_missing = _unique_missing_provider_addresses(current_df)
    cache = _read_provider_cache(cache_file)
    cached_ok = cache[cache["geocode_status"].eq("OK")]
    done_keys = set(cached_ok["asia_address_key"].astype(str))
    if not retry_failed:
        done_keys.update(cache["asia_address_key"].astype(str))
    pending = unique_missing[~unique_missing["asia_address_key"].astype(str).isin(done_keys)].copy()
    if limit is not None and limit >= 0:
        pending = pending.head(limit).copy()

    rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        variants = _provider_query_variants(row)
        last_result = _failed_provider_result(provider_label, variants[0][1] if variants else "", "NO_QUERY")
        for variant_name, query in variants:
            result = geocode_func(query, api_key, timeout)
            result["asia_address_key"] = _clean_text(row.get("asia_address_key", ""))
            result["query_variant"] = variant_name
            last_result = result
            if _clean_text(result.get("geocode_status", "")) == "OK":
                break
        rows.append(last_result)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 200 == 0 or idx == len(pending):
            if rows:
                cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
                cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
                _save_provider_cache(cache_file, cache)
                rows = []
            print(f"{provider_label} progress={idx}/{len(pending)}", flush=True)

    output = _apply_provider_cache(current_df, _read_provider_cache(cache_file), provider_label)
    return output, int(len(pending))


def _merge_with_asia_fallback_geocodes(
    normalized_df: pd.DataFrame,
    config: dict,
    *,
    retry_failed: bool = False,
    limit: int | None = None,
) -> tuple[pd.DataFrame, int, int, int, int]:
    output, nominatim_attempted, unique_address_rows = _merge_with_nominatim_geocodes(
        normalized_df,
        config,
        retry_failed=retry_failed,
        limit=limit,
    )
    geocoding_cfg = config.get("geocoding", {})
    output, here_attempted = _run_provider_fallback(
        output,
        provider_label="here",
        cache_file=_provider_cache_path(config, "here"),
        api_key=_clean_text(geocoding_cfg.get("here_api_key", "")),
        timeout=int(geocoding_cfg.get("here_timeout_sec", 30)),
        sleep_sec=float(geocoding_cfg.get("here_sleep_sec", 0.05)),
        retry_failed=retry_failed,
        limit=limit,
        geocode_func=_here_geocode,
    )
    output, google_attempted = _run_provider_fallback(
        output,
        provider_label="google",
        cache_file=_provider_cache_path(config, "google"),
        api_key=_clean_text(geocoding_cfg.get("google_api_key", "")),
        timeout=int(geocoding_cfg.get("google_timeout_sec", 30)),
        sleep_sec=float(geocoding_cfg.get("google_sleep_sec", 0.05)),
        retry_failed=retry_failed,
        limit=limit,
        geocode_func=_google_geocode,
    )
    return output, nominatim_attempted, unique_address_rows, here_attempted, google_attempted


def normalize_service_df(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, ServicePreprocessSummary]:
    df = raw_df.copy()
    source_rows = int(len(df))
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed:")].copy()
    df = df.rename(
        columns={
            "SERVICE_CENTER_TYPE": "SVC_CENTER_TYPE",
            "DETAIL_SYMPTOM_CODE": "RECEIPT_DETAIL_SYMPTOM_CODE",
            "SERVICE_PRODUCT_NAME": "SVC_PRODUCT_NAME",
            "SERVICE_PRODUCT_GROUP_NAME": "SVC_PRODUCT_GROUP_NAME",
        }
    )

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = _clean_text_series(df[col])

    if "COUNTRY_NAME" in df.columns:
        df["COUNTRY_NAME"] = df["COUNTRY_NAME"].replace(
            {
                "US": "USA",
                "usa": "USA",
                "United States": "USA",
                "UNITED STATES": "USA",
            }
        )

    if "SVC_ENGINEER_NAME" not in df.columns and "SVC_ENGINEER_CODE" in df.columns:
        df["SVC_ENGINEER_NAME"] = _clean_text_series(df["SVC_ENGINEER_CODE"])
    elif "SVC_ENGINEER_NAME" in df.columns and "SVC_ENGINEER_CODE" in df.columns:
        missing_name_mask = df["SVC_ENGINEER_NAME"].astype(str).str.strip().eq("")
        df.loc[missing_name_mask, "SVC_ENGINEER_NAME"] = df.loc[missing_name_mask, "SVC_ENGINEER_CODE"].astype(str).str.strip()

    if "POSTAL_CODE" in df.columns:
        df["POSTAL_CODE"] = _normalize_postal_code(df["POSTAL_CODE"])
    if "PROMISE_DATE" in df.columns:
        df["PROMISE_DATE"] = _normalize_yyyymmdd(df["PROMISE_DATE"])

    dropped_blank_address_rows = 0
    if "ADDRESS_LINE1_INFO" in df.columns:
        before_address = len(df)
        df = df[df["ADDRESS_LINE1_INFO"].astype(str).str.strip().ne("")].copy()
        dropped_blank_address_rows = int(before_address - len(df))

    dropped_blank_receipt_rows = 0
    dropped_duplicate_receipt_rows = 0
    if "GSFS_RECEIPT_NO" in df.columns:
        before_receipt = len(df)
        df = df[df["GSFS_RECEIPT_NO"].astype(str).str.strip().ne("")].copy()
        dropped_blank_receipt_rows = int(before_receipt - len(df))
        sort_cols = [col for col in ["PROMISE_DATE", "PROMISE_TIMESTAMP", "REPAIR_END_DATE_YYYYMMDD", "GSFS_RECEIPT_NO"] if col in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        before_dedupe = len(df)
        df = df.drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
        dropped_duplicate_receipt_rows = int(before_dedupe - len(df))

    summary = ServicePreprocessSummary(
        source_rows=source_rows,
        output_rows=int(len(df)),
        dropped_blank_address_rows=dropped_blank_address_rows,
        dropped_blank_receipt_rows=dropped_blank_receipt_rows,
        dropped_duplicate_receipt_rows=dropped_duplicate_receipt_rows,
    )
    return df.reset_index(drop=True), summary


def preprocess_service_file(
    source_file: Path,
    output_file: Path,
    *,
    config_file: Path = Path("config.json"),
    geocode: bool = True,
    geocode_backend: str = "auto",
    retry_failed: bool = False,
    limit: int | None = None,
) -> ServicePreprocessSummary:
    df = _read_service_csv(source_file)
    normalized_df, summary = normalize_service_df(df)
    output_df = normalized_df
    geocoded_rows = 0
    failed_geocode_rows = 0
    unique_address_rows = 0
    nominatim_attempted_rows = 0
    here_attempted_rows = 0
    google_attempted_rows = 0
    effective_backend = geocode_backend

    if geocode and not normalized_df.empty:
        config = _load_json_config(config_file)
        if geocode_backend == "auto" and _is_asia_service_df(normalized_df):
            effective_backend = "asia-fallback"
        if effective_backend == "nominatim":
            output_df, nominatim_attempted_rows, unique_address_rows = _merge_with_nominatim_geocodes(
                normalized_df.copy(),
                config,
                retry_failed=retry_failed,
                limit=limit,
            )
        elif effective_backend == "asia-fallback":
            (
                output_df,
                nominatim_attempted_rows,
                unique_address_rows,
                here_attempted_rows,
                google_attempted_rows,
            ) = _merge_with_asia_fallback_geocodes(
                normalized_df.copy(),
                config,
                retry_failed=retry_failed,
                limit=limit,
            )
        else:
            # Import lazily to avoid a circular import: live_atlanta_runtime imports normalize_service_df.
            from .live_atlanta_runtime import _merge_service_geocodes

            output_df = _merge_service_geocodes(normalized_df.copy(), config)
        output_df["latitude"] = pd.to_numeric(output_df.get("latitude"), errors="coerce")
        output_df["longitude"] = pd.to_numeric(output_df.get("longitude"), errors="coerce")
        failed_mask = output_df["latitude"].isna() | output_df["longitude"].isna()
        failed_geocode_rows = int(failed_mask.sum())
        geocoded_rows = int((~failed_mask).sum())

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    return ServicePreprocessSummary(
        source_rows=summary.source_rows,
        output_rows=summary.output_rows,
        dropped_blank_address_rows=summary.dropped_blank_address_rows,
        dropped_blank_receipt_rows=summary.dropped_blank_receipt_rows,
        dropped_duplicate_receipt_rows=summary.dropped_duplicate_receipt_rows,
        geocoded_rows=geocoded_rows,
        failed_geocode_rows=failed_geocode_rows,
        unique_address_rows=unique_address_rows,
        nominatim_attempted_rows=nominatim_attempted_rows,
        nominatim_remaining_rows=failed_geocode_rows if effective_backend in {"nominatim", "asia-fallback"} else 0,
        here_attempted_rows=here_attempted_rows,
        google_attempted_rows=google_attempted_rows,
    )
