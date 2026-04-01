from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .census_geocoder import CensusBatchGeocoder, load_geocode_cache, merge_service_with_geocodes
from .google_geocoder import GoogleGeocoder
from . import production_atlanta as prod


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
    df = raw_df.copy()
    rename_map = {
        "SERVICE_CENTER_TYPE": "SVC_CENTER_TYPE",
        "DETAIL_SYMPTOM_CODE": "RECEIPT_DETAIL_SYMPTOM_CODE",
        "SERVICE_PRODUCT_NAME": "SVC_PRODUCT_NAME",
        "SERVICE_PRODUCT_GROUP_NAME": "SVC_PRODUCT_GROUP_NAME",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    for col in [
        "STRATEGIC_CITY_NAME",
        "POSTAL_CODE",
        "GSFS_RECEIPT_NO",
        "SVC_ENGINEER_CODE",
        "SVC_ENGINEER_NAME",
        "SVC_CENTER_TYPE",
        "SERVICE_PRODUCT_GROUP_CODE",
        "SERVICE_PRODUCT_CODE",
        "RECEIPT_DETAIL_SYMPTOM_CODE",
        "STATE_NAME",
        "CITY_NAME",
        "COUNTRY_NAME",
        "ADDRESS_LINE1_INFO",
    ]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if "SVC_ENGINEER_NAME" not in df.columns and "SVC_ENGINEER_CODE" in df.columns:
        df["SVC_ENGINEER_NAME"] = df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    elif "SVC_ENGINEER_NAME" in df.columns and "SVC_ENGINEER_CODE" in df.columns:
        missing_name_mask = df["SVC_ENGINEER_NAME"].astype(str).str.strip().eq("")
        df.loc[missing_name_mask, "SVC_ENGINEER_NAME"] = df.loc[missing_name_mask, "SVC_ENGINEER_CODE"].astype(str).str.strip()
    if "POSTAL_CODE" in df.columns:
        df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    if "GSFS_RECEIPT_NO" in df.columns:
        sort_cols = [col for col in ["PROMISE_DATE", "PROMISE_TIMESTAMP", "REPAIR_END_DATE_YYYYMMDD", "GSFS_RECEIPT_NO"] if col in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        df = df.drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return df


def _merge_service_geocodes(raw_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    geocoding_cfg = config.get("geocoding", {})
    census_cache_path = Path(str(geocoding_cfg.get("census_cache_file", "data/geocode_cache_us_census.csv")))
    google_cache_path = Path(str(geocoding_cfg.get("google_cache_file", "data/geocode_cache_google.csv")))
    google_attempt_log_path = Path(str(geocoding_cfg.get("google_attempt_log_file", "data/geocode_attempted_google.csv")))

    cache_df = pd.concat(
        [load_geocode_cache(census_cache_path), load_geocode_cache(google_cache_path)],
        ignore_index=True,
    ).drop_duplicates(subset=["address_key"], keep="first")
    merged_df = merge_service_with_geocodes(raw_df, cache_df)

    failed_mask = merged_df["source"].astype(str).eq("failed")
    if failed_mask.any():
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            raw_path = tmp_dir / "service_runtime_raw.csv"
            geocoded_path = tmp_dir / "service_runtime_geocoded.csv"
            report_path = tmp_dir / "service_runtime_geocode_report.json"
            raw_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
            census = CensusBatchGeocoder(
                cache_path=census_cache_path,
                log_path=Path(str(geocoding_cfg.get("census_daily_log_file", "data/geocode_daily_log_us_census.json"))),
                daily_limit=int(geocoding_cfg.get("daily_limit", 10000)),
                timeout=int(geocoding_cfg.get("timeout", 120)),
                batch_size=int(geocoding_cfg.get("batch_size", 1000)),
            )
            census.run_for_service_file(
                service_path=raw_path,
                merged_output_path=geocoded_path,
                report_path=report_path,
            )

        cache_df = pd.concat(
            [load_geocode_cache(census_cache_path), load_geocode_cache(google_cache_path)],
            ignore_index=True,
        ).drop_duplicates(subset=["address_key"], keep="first")
        merged_df = merge_service_with_geocodes(raw_df, cache_df)
        failed_mask = merged_df["source"].astype(str).eq("failed")

    google_api_key = str(geocoding_cfg.get("google_api_key", "")).strip()
    if failed_mask.any() and google_api_key:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            unmatched_path = tmp_dir / "service_runtime_unmatched.csv"
            raw_df.loc[failed_mask].to_csv(unmatched_path, index=False, encoding="utf-8-sig")
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

        cache_df = pd.concat(
            [load_geocode_cache(census_cache_path), load_geocode_cache(google_cache_path)],
            ignore_index=True,
        ).drop_duplicates(subset=["address_key"], keep="first")
        merged_df = merge_service_with_geocodes(raw_df, cache_df)

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
        home_geocode_df = home_geocode_df[home_geocode_df["SVC_CENTER_TYPE"] == prod.DMS_CENTER_TYPE].copy()

    return RuntimeAtlantaPrepResult(
        queried_service_df=normalized_raw_df,
        geocoded_service_df=geocoded_df,
        region_zip_df=region_zip_df,
        engineer_region_df=engineer_region_df,
        home_geocode_df=home_geocode_df,
        service_filtered_df=service_df,
        service_enriched_df=service_enriched_df,
    )
