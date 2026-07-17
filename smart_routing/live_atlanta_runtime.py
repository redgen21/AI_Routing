from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .census_geocoder import CensusBatchGeocoder, build_address_key, load_geocode_cache, merge_service_with_geocodes
from .google_geocoder import GoogleGeocoder
from .here_geocoder import HereGeocoder
from .us_geocode_cleaner import build_us_geocode_query_variants
from . import production_atlanta as prod
from .area_map import get_latest_geocoded_service_file
from .service_preprocess import normalize_service_df


DEFAULT_PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
DEFAULT_SYMPTOM_FILE = Path("data/Notification_Symptom_mapping_20241120_3depth.xlsx")
DEFAULT_CONFIG_FILE = Path("config.json")
DEFAULT_REGION_ZIP_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3.csv")
FALLBACK_REGION_ZIP_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3_manual320.csv")
DEFAULT_ENGINEER_REGION_PATH = Path("260310/production_input/atlanta_engineer_region_assignment.csv")
DEFAULT_HOME_GEOCODE_PATH = Path("260310/production_input/atlanta_engineer_home_geocoded.csv")
DEFAULT_HEAVY_REPAIR_LOOKUP_PATH = Path("260310/production_input/atlanta_heavy_repair_lookup.csv")


@dataclass
class RuntimeAtlantaPrepResult:
    queried_service_df: pd.DataFrame
    geocoded_service_df: pd.DataFrame
    region_zip_df: pd.DataFrame
    engineer_region_df: pd.DataFrame
    home_geocode_df: pd.DataFrame
    service_filtered_df: pd.DataFrame
    service_enriched_df: pd.DataFrame


def _load_config(config_file: Path = DEFAULT_CONFIG_FILE) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _normalize_service_columns(raw_df: pd.DataFrame) -> pd.DataFrame:
    df, _summary = normalize_service_df(raw_df)
    return df


def _combined_geocode_cache(*cache_paths: Path) -> pd.DataFrame:
    frames = []
    for order, cache_path in enumerate(cache_paths):
        frame = load_geocode_cache(cache_path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["_cache_order"] = order
        frame["_is_failed"] = frame["source"].astype(str).eq("failed") | frame["latitude"].isna() | frame["longitude"].isna()
        frames.append(frame)
    if not frames:
        return load_geocode_cache(Path("__missing_geocode_cache__.csv"))
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["address_key", "_is_failed", "_cache_order"]).drop_duplicates(subset=["address_key"], keep="first")
    return combined.drop(columns=["_cache_order", "_is_failed"], errors="ignore").reset_index(drop=True)


def _merge_service_geocodes(raw_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    merged_input_df = raw_df.copy().reset_index(drop=True)
    latest_geocoded_service = get_latest_geocoded_service_file()
    if latest_geocoded_service and latest_geocoded_service.exists() and "GSFS_RECEIPT_NO" in merged_input_df.columns:
        try:
            receipt_geo_df = pd.read_csv(latest_geocoded_service, encoding="utf-8-sig", low_memory=False)
            keep_cols = [col for col in ["GSFS_RECEIPT_NO", "latitude", "longitude", "matched_address", "match_indicator", "match_type", "census_state_fips", "census_county_fips", "census_tract", "census_block", "geocoded_date", "source"] if col in receipt_geo_df.columns]
            if {"GSFS_RECEIPT_NO", "latitude", "longitude"}.issubset(keep_cols):
                receipt_geo_df = (
                    receipt_geo_df[keep_cols]
                    .dropna(subset=["GSFS_RECEIPT_NO"])
                    .drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first")
                )
                merged_input_df = merged_input_df.merge(
                    receipt_geo_df.rename(
                        columns={
                            "latitude": "receipt_latitude",
                            "longitude": "receipt_longitude",
                            "matched_address": "receipt_matched_address",
                            "match_indicator": "receipt_match_indicator",
                            "match_type": "receipt_match_type",
                            "census_state_fips": "receipt_census_state_fips",
                            "census_county_fips": "receipt_census_county_fips",
                            "census_tract": "receipt_census_tract",
                            "census_block": "receipt_census_block",
                            "geocoded_date": "receipt_geocoded_date",
                            "source": "receipt_source",
                        }
                    ),
                    on="GSFS_RECEIPT_NO",
                    how="left",
                )
        except Exception:
            pass

    geocoding_cfg = config.get("geocoding", {})
    census_cache_path = Path(str(geocoding_cfg.get("census_cache_file", "data/geocode_cache_us_census.csv")))
    here_cache_path = Path(str(geocoding_cfg.get("here_cache_file", "data/geocode_cache_here.csv")))
    here_attempt_log_path = Path(str(geocoding_cfg.get("here_attempt_log_file", "data/geocode_attempted_here.csv")))
    google_cache_path = Path(str(geocoding_cfg.get("google_cache_file", "data/geocode_cache_google.csv")))
    google_attempt_log_path = Path(str(geocoding_cfg.get("google_attempt_log_file", "data/geocode_attempted_google.csv")))

    cache_df = _combined_geocode_cache(census_cache_path, here_cache_path, google_cache_path)
    merged_df = merge_service_with_geocodes(merged_input_df, cache_df)

    if "receipt_latitude" in merged_df.columns and "receipt_longitude" in merged_df.columns:
        receipt_mask = merged_df["receipt_latitude"].notna() & merged_df["receipt_longitude"].notna()
        merged_df.loc[receipt_mask, "latitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_latitude"], errors="coerce")
        merged_df.loc[receipt_mask, "longitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_longitude"], errors="coerce")
        for source_col, target_col in [
            ("receipt_matched_address", "matched_address"),
            ("receipt_match_indicator", "match_indicator"),
            ("receipt_match_type", "match_type"),
            ("receipt_census_state_fips", "census_state_fips"),
            ("receipt_census_county_fips", "census_county_fips"),
            ("receipt_census_tract", "census_tract"),
            ("receipt_census_block", "census_block"),
            ("receipt_geocoded_date", "geocoded_date"),
        ]:
            if source_col in merged_df.columns:
                merged_df.loc[receipt_mask, target_col] = merged_df.loc[receipt_mask, source_col]
        if "receipt_source" in merged_df.columns:
            merged_df.loc[receipt_mask, "source"] = merged_df.loc[receipt_mask, "receipt_source"].fillna("receipt_lookup")
        else:
            merged_df.loc[receipt_mask, "source"] = "receipt_lookup"

    failed_mask = merged_df["source"].astype(str).eq("failed")
    if failed_mask.any():
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            raw_path = tmp_dir / "service_runtime_raw.csv"
            geocoded_path = tmp_dir / "service_runtime_geocoded.csv"
            report_path = tmp_dir / "service_runtime_geocode_report.json"
            merged_input_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
            census = CensusBatchGeocoder(
                cache_path=census_cache_path,
                log_path=Path(str(geocoding_cfg.get("census_daily_log_file", "data/geocode_daily_log_us_census.json"))),
                daily_limit=int(geocoding_cfg.get("daily_limit", 10000)),
                timeout=int(geocoding_cfg.get("timeout", 120)),
                batch_size=int(geocoding_cfg.get("batch_size", 1000)),
            )
            try:
                census.run_for_service_file(
                    service_path=raw_path,
                    merged_output_path=geocoded_path,
                    report_path=report_path,
                )
            except Exception:
                pass

        cache_df = _combined_geocode_cache(census_cache_path, here_cache_path, google_cache_path)
        merged_df = merge_service_with_geocodes(merged_input_df, cache_df)
        if "receipt_latitude" in merged_df.columns and "receipt_longitude" in merged_df.columns:
            receipt_mask = merged_df["receipt_latitude"].notna() & merged_df["receipt_longitude"].notna()
            merged_df.loc[receipt_mask, "latitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_latitude"], errors="coerce")
            merged_df.loc[receipt_mask, "longitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_longitude"], errors="coerce")
            merged_df.loc[receipt_mask, "source"] = merged_df.get("receipt_source", pd.Series(index=merged_df.index)).fillna("receipt_lookup")
        failed_mask = merged_df["source"].astype(str).eq("failed")

    here_api_key = str(geocoding_cfg.get("here_api_key", "")).strip()
    if failed_mask.any() and here_api_key:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            unmatched_path = tmp_dir / "service_runtime_unmatched_here.csv"
            unmatched_df = merged_input_df.iloc[failed_mask.to_numpy()].copy()
            unmatched_df.to_csv(unmatched_path, index=False, encoding="utf-8-sig")
            here = HereGeocoder(
                api_key=here_api_key,
                cache_path=here_cache_path,
                attempt_log_path=here_attempt_log_path,
                monthly_limit=int(geocoding_cfg.get("here_monthly_limit", 10000)),
                sleep_sec=float(geocoding_cfg.get("here_sleep_sec", 0.05)),
                min_query_score=float(geocoding_cfg.get("here_min_query_score", 0.7)),
                min_field_score=float(geocoding_cfg.get("here_min_field_score", 0.7)),
            )
            here.run_for_unmatched(
                service_path=unmatched_path,
                census_cache_path=census_cache_path,
                run_date=None,
                ignore_attempt_log_once=True,
            )

        cache_df = _combined_geocode_cache(census_cache_path, here_cache_path, google_cache_path)
        merged_df = merge_service_with_geocodes(merged_input_df, cache_df)
        if "receipt_latitude" in merged_df.columns and "receipt_longitude" in merged_df.columns:
            receipt_mask = merged_df["receipt_latitude"].notna() & merged_df["receipt_longitude"].notna()
            merged_df.loc[receipt_mask, "latitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_latitude"], errors="coerce")
            merged_df.loc[receipt_mask, "longitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_longitude"], errors="coerce")
            merged_df.loc[receipt_mask, "source"] = merged_df.get("receipt_source", pd.Series(index=merged_df.index)).fillna("receipt_lookup")
        failed_mask = merged_df["source"].astype(str).eq("failed")

    use_pattern_retry = bool(geocoding_cfg.get("us_pattern_retry_before_google", True))
    if failed_mask.any() and here_api_key and use_pattern_retry:
        unmatched_df = merged_input_df.iloc[failed_mask.to_numpy()].copy()
        if "address_key" in merged_df.columns:
            unmatched_df["address_key"] = merged_df.loc[failed_mask, "address_key"].to_numpy()
        elif "address_key" not in unmatched_df.columns:
            unmatched_df["address_key"] = unmatched_df.apply(
                lambda row: build_address_key(
                    row.get("ADDRESS_LINE1_INFO", ""),
                    row.get("CITY_NAME", ""),
                    row.get("STATE_NAME", ""),
                    row.get("POSTAL_CODE", ""),
                    row.get("COUNTRY_NAME", ""),
                ),
                axis=1,
            )
        unmatched_df = unmatched_df.drop_duplicates(subset=["address_key"], keep="first") if "address_key" in unmatched_df.columns else unmatched_df
        here = HereGeocoder(
            api_key=here_api_key,
            cache_path=here_cache_path,
            attempt_log_path=here_attempt_log_path,
            monthly_limit=int(geocoding_cfg.get("here_monthly_limit", 10000)),
            sleep_sec=float(geocoding_cfg.get("here_sleep_sec", 0.05)),
            min_query_score=float(geocoding_cfg.get("here_min_query_score", 0.7)),
            min_field_score=float(geocoding_cfg.get("here_min_field_score", 0.7)),
        )
        here_cache_df = load_geocode_cache(here_cache_path)
        here_attempt_df = pd.read_csv(here_attempt_log_path, encoding="utf-8-sig", low_memory=False) if here_attempt_log_path.exists() else pd.DataFrame()
        run_month = pd.Timestamp.today().strftime("%Y-%m")
        if not here_attempt_df.empty and "attempted_date" in here_attempt_df.columns:
            monthly_used = int(here_attempt_df["attempted_date"].astype(str).str.startswith(run_month).sum())
        else:
            monthly_used = 0
        monthly_limit = int(geocoding_cfg.get("here_monthly_limit", 10000))
        monthly_remaining = max(monthly_limit - monthly_used, 0)
        new_here_rows: list[dict[str, object]] = []
        new_attempt_rows: list[dict[str, object]] = []
        attempted_count = 0
        cached_keys = set(here_cache_df["address_key"].astype(str)) if "address_key" in here_cache_df.columns else set()
        for _, row in unmatched_df.iterrows():
            if attempted_count >= monthly_remaining:
                break
            address_key = str(row.get("address_key", "")).strip()
            if not address_key or address_key in cached_keys:
                continue
            variants = build_us_geocode_query_variants(row)
            if not variants:
                continue
            result, attempts = here.geocode_query_variants(address_key, variants)
            attempted_count += len(attempts)
            new_attempt_rows.extend(attempts)
            if result is not None:
                new_here_rows.append(result)
                cached_keys.add(address_key)

        if new_here_rows:
            merged_here_cache = pd.concat([here_cache_df, pd.DataFrame(new_here_rows)], ignore_index=True)
            merged_here_cache = merged_here_cache.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            here_cache_path.parent.mkdir(parents=True, exist_ok=True)
            merged_here_cache.to_csv(here_cache_path, index=False, encoding="utf-8-sig")
        if new_attempt_rows:
            merged_attempt_df = pd.concat([here_attempt_df, pd.DataFrame(new_attempt_rows)], ignore_index=True)
            if "address_key" in merged_attempt_df.columns and "variant" in merged_attempt_df.columns:
                merged_attempt_df = merged_attempt_df.drop_duplicates(subset=["address_key", "variant"], keep="last").reset_index(drop=True)
            elif "address_key" in merged_attempt_df.columns:
                merged_attempt_df = merged_attempt_df.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            here_attempt_log_path.parent.mkdir(parents=True, exist_ok=True)
            merged_attempt_df.to_csv(here_attempt_log_path, index=False, encoding="utf-8-sig")

        if new_here_rows:
            cache_df = _combined_geocode_cache(census_cache_path, here_cache_path, google_cache_path)
            merged_df = merge_service_with_geocodes(merged_input_df, cache_df)
            if "receipt_latitude" in merged_df.columns and "receipt_longitude" in merged_df.columns:
                receipt_mask = merged_df["receipt_latitude"].notna() & merged_df["receipt_longitude"].notna()
                merged_df.loc[receipt_mask, "latitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_latitude"], errors="coerce")
                merged_df.loc[receipt_mask, "longitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_longitude"], errors="coerce")
                merged_df.loc[receipt_mask, "source"] = merged_df.get("receipt_source", pd.Series(index=merged_df.index)).fillna("receipt_lookup")
            failed_mask = merged_df["source"].astype(str).eq("failed")

    google_api_key = str(geocoding_cfg.get("google_api_key", "")).strip()
    if failed_mask.any() and google_api_key:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            unmatched_path = tmp_dir / "service_runtime_unmatched.csv"
            unmatched_df = merged_input_df.iloc[failed_mask.to_numpy()].copy()
            unmatched_df.to_csv(unmatched_path, index=False, encoding="utf-8-sig")
            google = GoogleGeocoder(
                api_key=google_api_key,
                cache_path=google_cache_path,
                attempt_log_path=google_attempt_log_path,
                monthly_limit=int(geocoding_cfg.get("google_monthly_limit", 10000)),
                sleep_sec=float(geocoding_cfg.get("google_sleep_sec", 0.05)),
            )
            google.run_for_unmatched(
                service_path=unmatched_path,
                census_cache_path=census_cache_path,
                run_date=None,
                ignore_attempt_log_once=True,
            )

        cache_df = _combined_geocode_cache(census_cache_path, here_cache_path, google_cache_path)
        merged_df = merge_service_with_geocodes(merged_input_df, cache_df)
        if "receipt_latitude" in merged_df.columns and "receipt_longitude" in merged_df.columns:
            receipt_mask = merged_df["receipt_latitude"].notna() & merged_df["receipt_longitude"].notna()
            merged_df.loc[receipt_mask, "latitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_latitude"], errors="coerce")
            merged_df.loc[receipt_mask, "longitude"] = pd.to_numeric(merged_df.loc[receipt_mask, "receipt_longitude"], errors="coerce")
            merged_df.loc[receipt_mask, "source"] = merged_df.get("receipt_source", pd.Series(index=merged_df.index)).fillna("receipt_lookup")

    merged_df = merged_df.drop(
        columns=[col for col in merged_df.columns if col.startswith("receipt_")],
        errors="ignore",
    )

    return merged_df


def _prepare_service_df_for_atlanta(geocoded_df: pd.DataFrame) -> pd.DataFrame:
    df = prod._normalize_text(
        geocoded_df.copy(),
        [
            "STRATEGIC_CITY_NAME",
            "POSTAL_CODE",
            "GSFS_RECEIPT_NO",
            "SVC_ENGINEER_CODE",
            "SVC_ENGINEER_NAME",
            "SVC_CENTER_TYPE",
            "SERVICE_PRODUCT_GROUP_CODE",
            "SERVICE_PRODUCT_CODE",
            "RECEIPT_DETAIL_SYMPTOM_CODE",
        ],
    )
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "PROMISE_DATE" in df.columns:
        df["service_date"] = pd.to_datetime(df["PROMISE_DATE"].astype(str), format="%Y%m%d", errors="coerce")
    elif "PROMISE_TIMESTAMP" in df.columns:
        df["service_date"] = pd.to_datetime(df["PROMISE_TIMESTAMP"], errors="coerce").dt.normalize()
    elif "REPAIR_END_DATE_YYYYMMDD" in df.columns:
        df["service_date"] = pd.to_datetime(df["REPAIR_END_DATE_YYYYMMDD"].astype(str), format="%Y%m%d", errors="coerce")
    if "STRATEGIC_CITY_NAME" not in df.columns:
        df["STRATEGIC_CITY_NAME"] = prod.ATLANTA_CITY
    else:
        missing_city_mask = df["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq("")
        df.loc[missing_city_mask, "STRATEGIC_CITY_NAME"] = prod.ATLANTA_CITY
    df = df[df["STRATEGIC_CITY_NAME"] == prod.ATLANTA_CITY].copy()
    df = df[~df["SVC_CENTER_TYPE"].isin(prod.EXCLUDED_CENTER_TYPES)].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    if "service_date" in df.columns:
        df = df[df["service_date"].notna()].copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.zfill(5)
    return df


def build_runtime_atlanta_inputs(
    queried_service_df: pd.DataFrame,
    profile_file: Path = DEFAULT_PROFILE_FILE,
    symptom_file: Path = DEFAULT_SYMPTOM_FILE,
    config_file: Path = DEFAULT_CONFIG_FILE,
) -> RuntimeAtlantaPrepResult:
    config = _load_config(config_file)
    normalized_raw_df = _normalize_service_columns(queried_service_df)
    geocoded_df = _merge_service_geocodes(normalized_raw_df, config)
    service_df = _prepare_service_df_for_atlanta(geocoded_df)

    region_zip_path = DEFAULT_REGION_ZIP_PATH if DEFAULT_REGION_ZIP_PATH.exists() else FALLBACK_REGION_ZIP_PATH
    region_zip_df = pd.read_csv(region_zip_path, encoding="utf-8-sig")
    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    engineer_region_df = pd.read_csv(DEFAULT_ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_geocode_df = pd.read_csv(DEFAULT_HOME_GEOCODE_PATH, encoding="utf-8-sig")
    if DEFAULT_HEAVY_REPAIR_LOOKUP_PATH.exists():
        heavy_lookup_df = pd.read_csv(DEFAULT_HEAVY_REPAIR_LOOKUP_PATH, encoding="utf-8-sig")
    else:
        heavy_lookup_df = prod._build_heavy_repair_lookup(symptom_file)

    service_enriched_df = prod._enrich_service_df(service_df, heavy_lookup_df)
    service_enriched_df["service_date_key"] = service_enriched_df["service_date"].dt.strftime("%Y-%m-%d")
    service_enriched_df = service_enriched_df.merge(
        region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates(),
        on="POSTAL_CODE",
        how="left",
    )
    service_enriched_df = service_enriched_df[service_enriched_df["region_seq"].notna()].copy()
    service_enriched_df["region_seq"] = pd.to_numeric(service_enriched_df["region_seq"], errors="coerce").astype(int)

    engineer_region_df["SVC_CENTER_TYPE"] = engineer_region_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    engineer_region_df = engineer_region_df[engineer_region_df["SVC_CENTER_TYPE"] == prod.DMS_CENTER_TYPE].copy()
    engineer_name_col = "SVC_ENGINEER_NAME" if "SVC_ENGINEER_NAME" in engineer_region_df.columns else "Name"
    engineer_name_lookup = (
        engineer_region_df[["SVC_ENGINEER_CODE", engineer_name_col]]
        .dropna(subset=["SVC_ENGINEER_CODE"])
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
        .rename(columns={engineer_name_col: "lookup_engineer_name"})
    )
    service_enriched_df = service_enriched_df.merge(engineer_name_lookup, on="SVC_ENGINEER_CODE", how="left")
    if "SVC_ENGINEER_NAME" not in service_enriched_df.columns:
        service_enriched_df["SVC_ENGINEER_NAME"] = service_enriched_df["lookup_engineer_name"]
    else:
        missing_name_mask = service_enriched_df["SVC_ENGINEER_NAME"].astype(str).str.strip().eq("")
        service_enriched_df.loc[missing_name_mask, "SVC_ENGINEER_NAME"] = service_enriched_df.loc[missing_name_mask, "lookup_engineer_name"]
    service_enriched_df = service_enriched_df.drop(columns=["lookup_engineer_name"], errors="ignore")
    if "SVC_CENTER_TYPE" in home_geocode_df.columns:
        home_geocode_df["SVC_CENTER_TYPE"] = home_geocode_df["SVC_CENTER_TYPE"].astype(str).str.upper()

    return RuntimeAtlantaPrepResult(
        queried_service_df=normalized_raw_df,
        geocoded_service_df=geocoded_df,
        region_zip_df=region_zip_df,
        engineer_region_df=engineer_region_df,
        home_geocode_df=home_geocode_df,
        service_filtered_df=service_df,
        service_enriched_df=service_enriched_df,
    )
