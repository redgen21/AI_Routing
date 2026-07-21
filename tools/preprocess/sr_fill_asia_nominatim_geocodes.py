from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path
from urllib import error, parse, request

import pandas as pd

from smart_routing.nominatim_geocoder import NominatimGeocoder, clean_text


COUNTRY_CODES = {
    "THA": "th",
    "IDN": "id",
    "MYS": "my",
    "BANGKOK": "th",
    "JAKARTA": "id",
    "KUALA LUMPUR": "my",
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


def _load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _missing_coord_mask(df: pd.DataFrame) -> pd.Series:
    lat = pd.to_numeric(df.get("latitude"), errors="coerce")
    lon = pd.to_numeric(df.get("longitude"), errors="coerce")
    return lat.isna() | lon.isna()


def _build_country_code(row: pd.Series, configured: dict[str, str]) -> str:
    for col in ["STRATEGIC_CITY_NAME", "COUNTRY_NAME", "translated_country"]:
        value = clean_text(row.get(col, "")).upper()
        if value in configured:
            return clean_text(configured[value]).lower()
        if value in COUNTRY_CODES:
            return COUNTRY_CODES[value]
    return ""


def _unique_missing_addresses(df: pd.DataFrame, configured_country_codes: dict[str, str]) -> pd.DataFrame:
    missing = df[_missing_coord_mask(df)].copy()
    if "asia_address_key" not in missing.columns:
        raise ValueError("Missing required column: asia_address_key")
    missing["asia_address_key"] = missing["asia_address_key"].map(clean_text)
    missing = missing[missing["asia_address_key"].ne("")].copy()
    missing["nominatim_country_code"] = missing.apply(
        lambda row: _build_country_code(row, configured_country_codes),
        axis=1,
    )
    return missing.drop_duplicates(subset=["asia_address_key"], keep="first").reset_index(drop=True)


def _apply_nominatim_cache(df: pd.DataFrame, cache_df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    if cache_df.empty:
        output["nominatim_geocoded_ok"] = False
        return output

    cache = cache_df[cache_df["geocode_status"].eq("OK")].copy()
    if cache.empty:
        output["nominatim_geocoded_ok"] = False
        return output

    cache = cache.rename(
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
    output = output.merge(cache[cache_cols], on="asia_address_key", how="left")

    missing_mask = _missing_coord_mask(output)
    nom_lat = pd.to_numeric(output.get("nominatim_latitude"), errors="coerce")
    nom_lon = pd.to_numeric(output.get("nominatim_longitude"), errors="coerce")
    fill_mask = missing_mask & nom_lat.notna() & nom_lon.notna()

    output.loc[fill_mask, "latitude"] = nom_lat[fill_mask]
    output.loc[fill_mask, "longitude"] = nom_lon[fill_mask]
    output.loc[fill_mask, "source"] = "nominatim"
    output.loc[fill_mask, "geocode_status"] = "OK"
    output.loc[fill_mask, "matched_address"] = output.loc[fill_mask, "nominatim_matched_address"]
    output.loc[fill_mask, "location_type"] = output.loc[fill_mask, "nominatim_location_type"]
    output.loc[fill_mask, "place_id"] = output.loc[fill_mask, "nominatim_place_id"]

    output["latitude"] = pd.to_numeric(output["latitude"], errors="coerce")
    output["longitude"] = pd.to_numeric(output["longitude"], errors="coerce")
    output["geocoded_ok"] = output["latitude"].notna() & output["longitude"].notna()
    output["nominatim_geocoded_ok"] = fill_mask
    return output


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


def _query_variants(row: pd.Series) -> list[tuple[str, str]]:
    return NominatimGeocoder._query_variants(row)


def _failed_result(provider: str, query: str, status: str, message: str = "") -> dict[str, object]:
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
        "error_message": clean_text(message),
        "geocoded_date": date.today().isoformat(),
    }


def _to_float(value: object) -> float | None:
    text = clean_text(value)
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
        return _failed_result("here_geocoding_api", query, f"HTTP_{exc.code}", str(exc))
    except error.URLError as exc:
        return _failed_result("here_geocoding_api", query, "URL_ERROR", str(exc))
    except json.JSONDecodeError as exc:
        return _failed_result("here_geocoding_api", query, "BAD_JSON", str(exc))

    items = payload.get("items") or []
    if not items:
        return _failed_result("here_geocoding_api", query, "NO_RESULTS")
    top = items[0]
    position = top.get("position") or {}
    lat = _to_float(position.get("lat"))
    lon = _to_float(position.get("lng"))
    if lat is None or lon is None:
        return _failed_result("here_geocoding_api", query, "NO_COORDS")
    scoring = top.get("scoring") or {}
    return {
        "asia_address_key": "",
        "geocode_query": query,
        "query_variant": "",
        "geocode_status": "OK",
        "latitude": lat,
        "longitude": lon,
        "matched_address": clean_text(top.get("title", "")),
        "location_type": clean_text(top.get("resultType", "")),
        "place_id": clean_text(top.get("id", "")),
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
        return _failed_result("google_geocoding_api", query, f"HTTP_{exc.code}", str(exc))
    except error.URLError as exc:
        return _failed_result("google_geocoding_api", query, "URL_ERROR", str(exc))
    except json.JSONDecodeError as exc:
        return _failed_result("google_geocoding_api", query, "BAD_JSON", str(exc))

    status = clean_text(payload.get("status")) or "UNKNOWN"
    if status != "OK":
        return _failed_result("google_geocoding_api", query, status, clean_text(payload.get("error_message", "")))
    results = payload.get("results") or []
    if not results:
        return _failed_result("google_geocoding_api", query, "NO_RESULTS")
    top = results[0]
    geometry = top.get("geometry") or {}
    location = geometry.get("location") or {}
    lat = _to_float(location.get("lat"))
    lon = _to_float(location.get("lng"))
    if lat is None or lon is None:
        return _failed_result("google_geocoding_api", query, "NO_COORDS")
    return {
        "asia_address_key": "",
        "geocode_query": query,
        "query_variant": "",
        "geocode_status": "OK",
        "latitude": lat,
        "longitude": lon,
        "matched_address": clean_text(top.get("formatted_address", "")),
        "location_type": clean_text(geometry.get("location_type", "")),
        "place_id": clean_text(top.get("place_id", "")),
        "importance": "",
        "source": "google_geocoding_api",
        "error_message": "",
        "geocoded_date": date.today().isoformat(),
    }


def _geocode_with_variants(
    provider: str,
    row: pd.Series,
    geocode_func,
    api_key: str,
    timeout: int,
) -> dict[str, object]:
    variants = _query_variants(row)
    last_result = _failed_result(provider, variants[0][1] if variants else "", "NO_QUERY")
    for variant_name, query in variants:
        result = geocode_func(query, api_key, timeout)
        result["asia_address_key"] = clean_text(row.get("asia_address_key", ""))
        result["query_variant"] = variant_name
        if clean_text(result.get("geocode_status", "")) == "OK":
            return result
        last_result = result
    return last_result


def _run_paid_provider(
    provider_label: str,
    current_df: pd.DataFrame,
    country_codes: dict[str, str],
    cache_file: Path,
    api_key: str,
    timeout: int,
    sleep_sec: float,
    limit: int | None,
    retry_failed: bool,
    geocode_func,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if not api_key:
        print(f"{provider_label} skipped: missing API key", flush=True)
        current_df[f"{provider_label}_geocoded_ok"] = False
        return current_df, {"attempted": 0, "geocoded": 0, "failed": 0}

    unique_missing = _unique_missing_addresses(current_df, country_codes)
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
        rows.append(_geocode_with_variants(provider_label, row, geocode_func, api_key, timeout))
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 200 == 0 or idx == len(pending):
            if rows:
                cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
                cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
                _save_provider_cache(cache_file, cache)
                rows = []
            print(f"{provider_label} progress={idx}/{len(pending)}", flush=True)

    cache = _read_provider_cache(cache_file)
    output = _apply_provider_cache(current_df, cache, provider_label)
    attempted_keys = set(pending["asia_address_key"].astype(str))
    attempted_cache = cache[cache["asia_address_key"].astype(str).isin(attempted_keys)]
    geocoded = int(attempted_cache["geocode_status"].eq("OK").sum())
    return output, {"attempted": int(len(pending)), "geocoded": geocoded, "failed": int(len(pending) - geocoded)}


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

    prefix = provider_label
    rename_map = {
        "geocode_query": f"{prefix}_geocode_query",
        "query_variant": f"{prefix}_query_variant",
        "geocode_status": f"{prefix}_geocode_status",
        "latitude": f"{prefix}_latitude",
        "longitude": f"{prefix}_longitude",
        "matched_address": f"{prefix}_matched_address",
        "location_type": f"{prefix}_location_type",
        "place_id": f"{prefix}_place_id",
        "importance": f"{prefix}_importance",
        "source": f"{prefix}_source",
        "error_message": f"{prefix}_error_message",
        "geocoded_date": f"{prefix}_geocoded_date",
    }
    cache = cache.rename(columns=rename_map)
    cache_cols = ["asia_address_key"] + list(rename_map.values())
    output = output.merge(cache[cache_cols], on="asia_address_key", how="left")

    missing_mask = _missing_coord_mask(output)
    provider_lat = pd.to_numeric(output.get(f"{prefix}_latitude"), errors="coerce")
    provider_lon = pd.to_numeric(output.get(f"{prefix}_longitude"), errors="coerce")
    fill_mask = missing_mask & provider_lat.notna() & provider_lon.notna()

    output.loc[fill_mask, "latitude"] = provider_lat[fill_mask]
    output.loc[fill_mask, "longitude"] = provider_lon[fill_mask]
    source_value = "here_geocoding_api" if provider_label == "here" else "google_geocoding_api"
    output.loc[fill_mask, "source"] = source_value
    output.loc[fill_mask, "geocode_status"] = "OK"
    output.loc[fill_mask, "matched_address"] = output.loc[fill_mask, f"{prefix}_matched_address"]
    output.loc[fill_mask, "location_type"] = output.loc[fill_mask, f"{prefix}_location_type"]
    output.loc[fill_mask, "place_id"] = output.loc[fill_mask, f"{prefix}_place_id"]

    output["latitude"] = pd.to_numeric(output["latitude"], errors="coerce")
    output["longitude"] = pd.to_numeric(output["longitude"], errors="coerce")
    output["geocoded_ok"] = output["latitude"].notna() & output["longitude"].notna()
    output[ok_col] = fill_mask
    return output


def run(
    input_file: Path,
    output_file: Path,
    config_file: Path,
    nominatim_cache_file: Path,
    here_cache_file: Path,
    google_cache_file: Path,
    limit: int | None,
    retry_failed: bool,
    enable_paid_fallback: bool,
) -> None:
    cfg = _load_config(config_file)
    nominatim_cfg = cfg.get("nominatim", {})
    base_url = clean_text(nominatim_cfg.get("url", ""))
    if not base_url:
        raise ValueError("Missing nominatim.url in config/config.json.")

    country_codes = {clean_text(k).upper(): clean_text(v).lower() for k, v in nominatim_cfg.get("country_codes", {}).items()}
    sleep_sec = float(nominatim_cfg.get("sleep_sec", 0.1))
    timeout = int(nominatim_cfg.get("timeout_sec", 30))

    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    before_missing = int(_missing_coord_mask(df).sum())
    unique_missing = _unique_missing_addresses(df, country_codes)
    print(f"input_rows={len(df)}", flush=True)
    print(f"missing_coord_rows_before={before_missing}", flush=True)
    print(f"unique_missing_addresses={len(unique_missing)}", flush=True)

    geocoder = NominatimGeocoder(
        base_url=base_url,
        cache_path=nominatim_cache_file,
        timeout=timeout,
        sleep_sec=sleep_sec,
    )
    result = geocoder.geocode_missing(unique_missing, retry_failed=retry_failed, limit=limit)
    print(
        f"nominatim attempted={result.attempted} geocoded={result.geocoded} failed={result.failed} cache={result.cache_path}",
        flush=True,
    )

    cache = geocoder.load_cache()
    output = _apply_nominatim_cache(df, cache)

    if enable_paid_fallback:
        geocoding_cfg = cfg.get("geocoding", {})
        here_key = clean_text(geocoding_cfg.get("here_api_key", ""))
        google_key = clean_text(geocoding_cfg.get("google_api_key", ""))
        output, here_result = _run_paid_provider(
            provider_label="here",
            current_df=output,
            country_codes=country_codes,
            cache_file=here_cache_file,
            api_key=here_key,
            timeout=int(geocoding_cfg.get("here_timeout_sec", 30)),
            sleep_sec=float(geocoding_cfg.get("here_sleep_sec", 0.05)),
            limit=limit,
            retry_failed=retry_failed,
            geocode_func=_here_geocode,
        )
        print(f"here attempted={here_result['attempted']} geocoded={here_result['geocoded']} failed={here_result['failed']} cache={here_cache_file}", flush=True)

        output, google_result = _run_paid_provider(
            provider_label="google",
            current_df=output,
            country_codes=country_codes,
            cache_file=google_cache_file,
            api_key=google_key,
            timeout=int(geocoding_cfg.get("google_timeout_sec", 30)),
            sleep_sec=float(geocoding_cfg.get("google_sleep_sec", 0.05)),
            limit=limit,
            retry_failed=retry_failed,
            geocode_func=_google_geocode,
        )
        print(f"google attempted={google_result['attempted']} geocoded={google_result['geocoded']} failed={google_result['failed']} cache={google_cache_file}", flush=True)
    else:
        output["here_geocoded_ok"] = False
        output["google_geocoded_ok"] = False
        print("paid fallback disabled: HERE/Google were not called", flush=True)

    after_missing = int(_missing_coord_mask(output).sum())

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_file, index=False, encoding="utf-8-sig")

    summary = (
        output.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(
            total=("GSFS_RECEIPT_NO", "count"),
            geocoded_ok=("geocoded_ok", "sum"),
            nominatim_filled=("nominatim_geocoded_ok", "sum"),
            here_filled=("here_geocoded_ok", "sum"),
            google_filled=("google_geocoded_ok", "sum"),
        )
        .reset_index()
    )
    summary["geocoded_failed"] = summary["total"] - summary["geocoded_ok"]
    total = pd.DataFrame(
        [
            {
                "COUNTRY_NAME": "TOTAL",
                "STRATEGIC_CITY_NAME": "TOTAL",
                "total": int(len(output)),
                "geocoded_ok": int(output["geocoded_ok"].sum()),
                "nominatim_filled": int(output["nominatim_geocoded_ok"].sum()),
                "here_filled": int(output["here_geocoded_ok"].sum()),
                "google_filled": int(output["google_geocoded_ok"].sum()),
                "geocoded_failed": after_missing,
            }
        ]
    )
    summary = pd.concat([summary, total], ignore_index=True)
    summary_file = output_file.with_name(output_file.stem + "_summary.csv")
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")

    print(f"missing_coord_rows_after={after_missing}", flush=True)
    print(f"fallback_filled_rows={before_missing - after_missing}", flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"summary_file={summary_file}", flush=True)
    print(summary.to_string(index=False), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill missing Asia service coordinates with Nominatim -> HERE -> Google.")
    parser.add_argument(
        "--input-file",
        default="260310/input/Service_202606161433_asia_google_cached_geocoded.csv",
    )
    parser.add_argument(
        "--output-file",
        default="260310/input/Service_202606161433_asia_fallback_geocoded.csv",
    )
    parser.add_argument("--config-file", default="config/config.json")
    parser.add_argument("--nominatim-cache-file", default="data/asia_nominatim_geocode_cache_202606161433.csv")
    parser.add_argument("--here-cache-file", default="data/asia_here_fallback_geocode_cache_202606161433.csv")
    parser.add_argument("--google-cache-file", default="data/asia_google_fallback_geocode_cache_202606161433.csv")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of unique addresses to attempt.")
    parser.add_argument("--retry-failed", action="store_true", help="Retry keys already cached as failed.")
    parser.add_argument(
        "--enable-paid-fallback",
        action="store_true",
        help="Allow HERE and Google after Nominatim misses. Disabled by default.",
    )
    args = parser.parse_args()

    run(
        input_file=Path(args.input_file),
        output_file=Path(args.output_file),
        config_file=Path(args.config_file),
        nominatim_cache_file=Path(args.nominatim_cache_file),
        here_cache_file=Path(args.here_cache_file),
        google_cache_file=Path(args.google_cache_file),
        limit=args.limit,
        retry_failed=bool(args.retry_failed),
        enable_paid_fallback=bool(args.enable_paid_fallback),
    )


if __name__ == "__main__":
    main()

