from __future__ import annotations

import itertools
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .area_map import get_latest_geocoded_service_file
from .census_geocoder import load_geocode_cache, merge_service_with_geocodes
from .google_geocoder import GoogleGeocoder
from .region_sweep import _assign_city_regions


DEFAULT_PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
DEFAULT_SERVICE_FILE = Path("260310/input/Service_202603181109_geocoded.csv")
DEFAULT_SYMPTOM_FILE = Path("data/Notification_Symptom_mapping_20241120_3depth.xlsx")
DEFAULT_PRODUCTION_INPUT_DIR = Path("260310/production_input")
DEFAULT_PRODUCTION_OUTPUT_DIR = Path("260310/production_output")
ATLANTA_CITY = "Atlanta, GA"
EXCLUDED_CENTER_TYPES = {"MAJOR DEALER", "REGIONAL DEALER"}
TV_PRODUCT_GROUP = "TV"
REF_PRODUCT_GROUP = "REF"
HEAVY_REPAIR_SHEET = "3depth 기준 중수리 증상"
DMS_CENTER_TYPE = "DMS"
DMS2_CENTER_TYPE = "DMS2"
FLOATING_REGION_NAME = "Atlanta Floating DMS2"
MANUAL_DMS_REGION_OVERRIDES = {
    "AI102448": 1,
    "AI103264": 2,
    "AI103317": 3,
}
DMS2_ANCHOR_REGION_OVERRIDES = {
    "40324200": 1,
    "JMOTOS2": 3,
}


@dataclass
class AtlantaProductionPrepResult:
    region_zip_path: Path
    engineer_region_path: Path
    home_geocode_path: Path
    heavy_repair_lookup_path: Path
    service_filtered_path: Path
    service_enriched_path: Path
    region_workload_summary_path: Path
    profile_copy_path: Path


def _normalize_text(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype(str).str.strip()
    return df


def _load_profile_sheets(profile_file: Path) -> dict[str, pd.DataFrame]:
    excel = pd.ExcelFile(profile_file)
    sheets = {sheet_name: pd.read_excel(profile_file, sheet_name=sheet_name) for sheet_name in excel.sheet_names}
    return sheets


def _load_service_df(service_file: Path | None) -> pd.DataFrame:
    resolved = service_file if service_file and service_file.exists() else get_latest_geocoded_service_file()
    if resolved is None or not resolved.exists():
        raise FileNotFoundError("No valid geocoded service file was found.")
    df = pd.read_csv(resolved, encoding="utf-8-sig", low_memory=False)
    df = _normalize_text(
        df,
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
    if "REPAIR_END_DATE_YYYYMMDD" in df.columns:
        df["service_date"] = pd.to_datetime(df["REPAIR_END_DATE_YYYYMMDD"].astype(str), format="%Y%m%d", errors="coerce")
    df = df[df["STRATEGIC_CITY_NAME"] == ATLANTA_CITY].copy()
    df = df[~df["SVC_CENTER_TYPE"].isin(EXCLUDED_CENTER_TYPES)].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.zfill(5)
    return df


def _build_region_zip_df(service_df: pd.DataFrame, region_count: int = 3) -> pd.DataFrame:
    assigned_df = _assign_city_regions(service_df, ATLANTA_CITY, region_count).copy()
    region_zip_df = (
        assigned_df[["POSTAL_CODE", "region_id", "region_seq"]]
        .drop_duplicates()
        .sort_values(["region_seq", "POSTAL_CODE"])
        .reset_index(drop=True)
    )
    region_zip_df["new_region_name"] = region_zip_df["region_seq"].apply(lambda n: f"Atlanta New Region {int(n)}")
    return region_zip_df


def _pick_best_dms_assignment(engineer_overlap_df: pd.DataFrame) -> pd.DataFrame:
    dms_df = engineer_overlap_df[engineer_overlap_df["SVC_CENTER_TYPE"] == DMS_CENTER_TYPE].copy()
    engineers = sorted(dms_df["SVC_ENGINEER_CODE"].unique().tolist())
    regions = sorted(dms_df["region_seq"].unique().tolist())
    if len(engineers) != 15 or len(regions) != 3:
        raise RuntimeError(f"Expected 15 DMS engineers and 3 regions, got {len(engineers)} engineers and {len(regions)} regions.")
    score_lookup = {
        (str(row["SVC_ENGINEER_CODE"]), int(row["region_seq"])): (
            int(row["zip_overlap_count"]),
            float(row["zip_overlap_ratio"]),
        )
        for _, row in dms_df.iterrows()
    }

    best_rows: list[dict] | None = None
    best_score: tuple[int, float] | None = None
    region1, region2, region3 = [int(r) for r in regions]
    engineer_set = set(engineers)
    for region1_group in itertools.combinations(engineers, 5):
        remaining_after_region1 = sorted(engineer_set.difference(region1_group))
        for region2_group in itertools.combinations(remaining_after_region1, 5):
            region3_group = sorted(set(remaining_after_region1).difference(region2_group))
            rows: list[dict] = []
            total_overlap = 0
            total_ratio = 0.0
            for engineer_code, region_seq in (
                [(engineer, region1) for engineer in region1_group]
                + [(engineer, region2) for engineer in region2_group]
                + [(engineer, region3) for engineer in region3_group]
            ):
                overlap_count, overlap_ratio = score_lookup.get((str(engineer_code), int(region_seq)), (0, 0.0))
                total_overlap += int(overlap_count)
                total_ratio += float(overlap_ratio)
                rows.append(
                    {
                        "SVC_ENGINEER_CODE": engineer_code,
                        "assigned_region_seq": int(region_seq),
                        "zip_overlap_count": int(overlap_count),
                        "zip_overlap_ratio": round(float(overlap_ratio), 4),
                    }
                )
            score = (int(total_overlap), round(float(total_ratio), 6))
            if best_score is None or score > best_score:
                best_score = score
                best_rows = rows

    if best_rows is None:
        raise RuntimeError("Failed to build fixed 5-per-region DMS allocation for Atlanta.")
    return pd.DataFrame(best_rows)


def _apply_manual_dms_region_overrides(dms_assignment_df: pd.DataFrame) -> pd.DataFrame:
    if dms_assignment_df.empty:
        return dms_assignment_df
    updated_df = dms_assignment_df.copy()
    for engineer_code, region_seq in MANUAL_DMS_REGION_OVERRIDES.items():
        mask = updated_df["SVC_ENGINEER_CODE"].astype(str) == str(engineer_code)
        if mask.any():
            updated_df.loc[mask, "assigned_region_seq"] = int(region_seq)
    region_counts = (
        pd.to_numeric(updated_df["assigned_region_seq"], errors="coerce")
        .dropna()
        .astype(int)
        .value_counts()
        .to_dict()
    )
    if region_counts != {1: 5, 2: 5, 3: 5}:
        raise RuntimeError(f"Manual DMS overrides broke the 5/5/5 allocation: {region_counts}")
    return updated_df


def _build_engineer_region_df(
    zip_df: pd.DataFrame,
    slot_df: pd.DataFrame,
    product_df: pd.DataFrame,
    region_zip_df: pd.DataFrame,
    service_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    zip_city = zip_df[zip_df["STRATEGIC_CITY_NAME"] == ATLANTA_CITY].copy()
    zip_city["POSTAL_CODE"] = zip_city["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    zip_city = _normalize_text(zip_city, ["SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE"])

    slot_city = slot_df[slot_df["STRATEGIC_CITY_NAME"] == ATLANTA_CITY].copy() if "STRATEGIC_CITY_NAME" in slot_df.columns else slot_df.copy()
    slot_city = _normalize_text(slot_city, ["SVC_ENGINEER_CODE", "Name", "STRATEGIC_CITY_NAME"])
    product_city = product_df[product_df["STRATEGIC_CITY_NAME"] == ATLANTA_CITY].copy() if "STRATEGIC_CITY_NAME" in product_df.columns else product_df.copy()
    product_city = _normalize_text(product_city, ["SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "AREA_PRODUCT_FLAG", "SVC_CENTER_TYPE"])

    region_zip_set = region_zip_df[["POSTAL_CODE", "region_seq"]].drop_duplicates().copy()
    overlap_df = (
        zip_city[["SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE", "POSTAL_CODE"]]
        .drop_duplicates()
        .merge(region_zip_set, on="POSTAL_CODE", how="left")
        .groupby(["SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE", "region_seq"])
        .agg(zip_overlap_count=("POSTAL_CODE", "nunique"))
        .reset_index()
    )
    total_zip_df = (
        zip_city.groupby("SVC_ENGINEER_CODE")
        .agg(original_zip_count=("POSTAL_CODE", "nunique"))
        .reset_index()
    )
    overlap_df = overlap_df.merge(total_zip_df, on="SVC_ENGINEER_CODE", how="left")
    overlap_df["zip_overlap_ratio"] = overlap_df["zip_overlap_count"] / overlap_df["original_zip_count"].replace(0, 1)

    dms_assignment_df = _pick_best_dms_assignment(overlap_df)
    dms_meta_df = (
        zip_city[zip_city["SVC_CENTER_TYPE"] == DMS_CENTER_TYPE][["SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE"]]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .reset_index(drop=True)
    )
    dms_assignment_df = dms_assignment_df.merge(dms_meta_df, on="SVC_ENGINEER_CODE", how="left")
    dms_assignment_df = _apply_manual_dms_region_overrides(dms_assignment_df)

    service_enriched_df = service_df.copy()
    service_enriched_df["service_time_min"] = pd.to_numeric(service_enriched_df.get("service_time_min", 45), errors="coerce").fillna(45)
    region_workload_df = (
        service_enriched_df.merge(region_zip_df[["POSTAL_CODE", "region_seq"]], on="POSTAL_CODE", how="left")
        .groupby("region_seq")
        .agg(
            total_service_time_min=("service_time_min", "sum"),
            heavy_repair_count=("is_heavy_repair", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
        )
        .reset_index()
        .sort_values(["total_service_time_min", "heavy_repair_count", "service_count"], ascending=[False, False, False])
        .reset_index(drop=True)
    )

    dms2_df = (
        zip_city[zip_city["SVC_CENTER_TYPE"] == DMS2_CENTER_TYPE][["SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE"]]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .reset_index(drop=True)
    )
    ranked_regions = region_workload_df["region_seq"].astype(int).tolist()
    dms2_assignment_df = dms2_df.copy()
    dms2_assignment_df["assigned_region_seq"] = pd.NA
    dms2_assignment_df["assigned_region_name"] = FLOATING_REGION_NAME
    dms2_assignment_df["zip_overlap_count"] = pd.NA
    dms2_assignment_df["zip_overlap_ratio"] = pd.NA
    dms2_assignment_df["preferred_region_rank_1"] = ranked_regions[0] if len(ranked_regions) >= 1 else pd.NA
    dms2_assignment_df["preferred_region_rank_2"] = ranked_regions[1] if len(ranked_regions) >= 2 else pd.NA
    dms2_assignment_df["preferred_region_rank_3"] = ranked_regions[2] if len(ranked_regions) >= 3 else pd.NA
    dms2_assignment_df["anchor_region_seq"] = dms2_assignment_df["SVC_ENGINEER_CODE"].astype(str).map(DMS2_ANCHOR_REGION_OVERRIDES)
    dms2_assignment_df["anchor_region_name"] = dms2_assignment_df["anchor_region_seq"].apply(
        lambda n: f"Atlanta New Region {int(n)}" if pd.notna(n) else pd.NA
    )

    all_columns = list(dict.fromkeys(dms_assignment_df.columns.tolist() + dms2_assignment_df.columns.tolist()))
    dms_assignment_df = dms_assignment_df.reindex(columns=all_columns).astype(object)
    dms2_assignment_df = dms2_assignment_df.reindex(columns=all_columns).astype(object)
    engineer_region_df = pd.concat([dms_assignment_df, dms2_assignment_df], ignore_index=True, sort=False)
    fixed_mask = engineer_region_df["assigned_region_name"].isna()
    engineer_region_df.loc[fixed_mask, "assigned_region_name"] = engineer_region_df.loc[fixed_mask, "assigned_region_seq"].apply(
        lambda n: f"Atlanta New Region {int(n)}"
    )
    engineer_region_df = engineer_region_df.merge(
        slot_city[["SVC_ENGINEER_CODE", "Name"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"]),
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    engineer_region_df["normalized_slot"] = 8
    engineer_region_df = engineer_region_df.merge(
        (
            product_city[
                (product_city["SERVICE_PRODUCT_GROUP_CODE"] == REF_PRODUCT_GROUP)
                & product_city["AREA_PRODUCT_FLAG"].isin(["Y", "N"])
            ][["SVC_ENGINEER_CODE", "AREA_PRODUCT_FLAG"]]
            .drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
            .rename(columns={"AREA_PRODUCT_FLAG": "REF_HEAVY_REPAIR_FLAG"})
        ),
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    engineer_region_df["REF_HEAVY_REPAIR_FLAG"] = engineer_region_df["REF_HEAVY_REPAIR_FLAG"].fillna("Y")
    engineer_region_df["region_sort_seq"] = pd.to_numeric(engineer_region_df["assigned_region_seq"], errors="coerce").fillna(99).astype(int)
    engineer_region_df = engineer_region_df.sort_values(["region_sort_seq", "SVC_CENTER_TYPE", "SVC_ENGINEER_CODE"]).reset_index(drop=True)
    engineer_region_df = engineer_region_df.drop(columns=["region_sort_seq"])
    return engineer_region_df, region_workload_df


def _geocode_home_address_df(address_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    geocoding_cfg = config.get("geocoding", {})
    cache_file = Path(str(geocoding_cfg.get("census_cache_file", "data/geocode_cache_us_census.csv")))
    google_cache_file = Path(str(geocoding_cfg.get("google_cache_file", "data/geocode_cache_google.csv")))
    google_attempt_log_file = Path(str(geocoding_cfg.get("google_attempt_log_file", "data/geocode_attempted_google.csv")))
    google_api_key = geocoding_cfg.get("google_api_key")

    temp_df = address_df.copy()
    temp_df["ADDRESS_LINE1_INFO"] = temp_df["Home Street Address"]
    temp_df["CITY_NAME"] = temp_df["City "]
    temp_df["STATE_NAME"] = temp_df["State"]
    temp_df["POSTAL_CODE"] = temp_df["Zip"].astype(str).str.strip().str.zfill(5)
    temp_df["COUNTRY_NAME"] = "USA"
    temp_df["GSFS_RECEIPT_NO"] = temp_df["SVC_ENGINEER_CODE"]

    cache_df = pd.concat([load_geocode_cache(cache_file), load_geocode_cache(google_cache_file)], ignore_index=True)
    cache_df = cache_df.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
    merged_df = merge_service_with_geocodes(temp_df, cache_df)

    failed_mask = merged_df["source"].astype(str).eq("failed")
    if failed_mask.any() and google_api_key:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            temp_service_path = tmp_dir / "atlanta_engineer_home_address_unmatched.csv"
            temp_df.loc[failed_mask].to_csv(temp_service_path, index=False, encoding="utf-8-sig")
            google = GoogleGeocoder(
                api_key=str(google_api_key),
                cache_path=google_cache_file,
                attempt_log_path=google_attempt_log_file,
                monthly_limit=int(geocoding_cfg.get("google_monthly_limit", 10000)),
                sleep_sec=float(geocoding_cfg.get("google_sleep_sec", 0.05)),
            )
            google.run_for_unmatched(
                service_path=temp_service_path,
                census_cache_path=cache_file,
                run_date=None,
                ignore_attempt_log_once=True,
            )
        cache_df = pd.concat([load_geocode_cache(cache_file), load_geocode_cache(google_cache_file)], ignore_index=True)
        cache_df = cache_df.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
        merged_df = merge_service_with_geocodes(temp_df, cache_df)

    home_df = merged_df[
        [
            "SVC_ENGINEER_CODE",
            "Name",
            "Home Street Address",
            "City ",
            "State",
            "Zip",
            "matched_address",
            "match_indicator",
            "match_type",
            "latitude",
            "longitude",
            "source",
        ]
    ].copy()
    return home_df


def _build_heavy_repair_lookup(symptom_file: Path) -> pd.DataFrame:
    lookup_df = pd.read_excel(symptom_file, sheet_name=HEAVY_REPAIR_SHEET)
    lookup_df = _normalize_text(
        lookup_df,
        ["SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "SYMP_CODE_ONE", "SYMP_CODE_TWO", "SYMP_CODE_THREE"],
    )
    lookup_df = lookup_df[lookup_df["SYMP_CODE_THREE"].ne("")].copy()
    return lookup_df.drop_duplicates(
        subset=["SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "SYMP_CODE_THREE"]
    ).reset_index(drop=True)


def _enrich_service_df(service_df: pd.DataFrame, heavy_lookup_df: pd.DataFrame) -> pd.DataFrame:
    heavy_lookup_key = set(
        heavy_lookup_df.apply(
            lambda row: (
                str(row["SERVICE_PRODUCT_GROUP_CODE"]).strip(),
                str(row["SERVICE_PRODUCT_CODE"]).strip(),
                str(row["SYMP_CODE_THREE"]).strip(),
            ),
            axis=1,
        ).tolist()
    )
    service_df = service_df.copy()
    service_df["is_heavy_repair"] = service_df.apply(
        lambda row: (
            str(row.get("SERVICE_PRODUCT_GROUP_CODE", "")).strip(),
            str(row.get("SERVICE_PRODUCT_CODE", "")).strip(),
            str(row.get("RECEIPT_DETAIL_SYMPTOM_CODE", "")).strip(),
        )
        in heavy_lookup_key,
        axis=1,
    )
    service_df["service_time_min"] = service_df["is_heavy_repair"].map(lambda flag: 100 if bool(flag) else 45)
    service_df["is_tv_job"] = service_df["SERVICE_PRODUCT_GROUP_CODE"].astype(str).str.strip().eq(TV_PRODUCT_GROUP)
    return service_df


def _write_profile_copy(
    sheets: dict[str, pd.DataFrame],
    updated_address_df: pd.DataFrame,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            if sheet_name == "4. Address":
                updated_address_df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)


def build_atlanta_production_inputs(
    profile_file: Path = DEFAULT_PROFILE_FILE,
    service_file: Path = DEFAULT_SERVICE_FILE,
    symptom_file: Path = DEFAULT_SYMPTOM_FILE,
    production_input_dir: Path = DEFAULT_PRODUCTION_INPUT_DIR,
    production_output_dir: Path = DEFAULT_PRODUCTION_OUTPUT_DIR,
    config_file: Path = Path("config.json"),
) -> AtlantaProductionPrepResult:
    sheets = _load_profile_sheets(profile_file)
    zip_df = sheets["1. Zip Coverage"].copy()
    slot_df = sheets["2. Slot"].copy()
    product_df = sheets["3. Product"].copy()
    address_df = sheets["4. Address"].copy()

    zip_df = _normalize_text(zip_df, ["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "AREA_NAME", "SVC_CENTER_TYPE"])
    zip_df["POSTAL_CODE"] = zip_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    slot_df = _normalize_text(slot_df, ["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "Name"])
    product_df = _normalize_text(product_df, ["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "AREA_PRODUCT_FLAG", "SVC_CENTER_TYPE"])
    address_df = _normalize_text(address_df, ["SVC_ENGINEER_CODE", "Name", "Home Street Address", "City ", "State"])
    address_df["Zip"] = address_df["Zip"].astype(str).str.strip().str.zfill(5)

    service_df = _load_service_df(service_file)
    heavy_lookup_df = _build_heavy_repair_lookup(symptom_file)
    service_enriched_df = _enrich_service_df(service_df, heavy_lookup_df)
    region_zip_df = _build_region_zip_df(service_enriched_df, region_count=3)
    engineer_region_df, region_workload_df = _build_engineer_region_df(zip_df, slot_df, product_df, region_zip_df, service_enriched_df)

    atl_engineers = set(engineer_region_df["SVC_ENGINEER_CODE"].astype(str).tolist())
    address_city_df = address_df[address_df["SVC_ENGINEER_CODE"].isin(atl_engineers)].copy()
    config = {} if not config_file.exists() else json.loads(config_file.read_text(encoding="utf-8"))
    home_geocode_df = _geocode_home_address_df(address_city_df, config)
    home_geocode_df = home_geocode_df.merge(
        engineer_region_df[["SVC_ENGINEER_CODE", "assigned_region_seq", "assigned_region_name", "SVC_CENTER_TYPE", "normalized_slot", "REF_HEAVY_REPAIR_FLAG"]],
        on="SVC_ENGINEER_CODE",
        how="left",
    )

    production_input_dir.mkdir(parents=True, exist_ok=True)
    production_output_dir.mkdir(parents=True, exist_ok=True)

    region_zip_path = production_input_dir / "atlanta_fixed_region_zip_3.csv"
    engineer_region_path = production_input_dir / "atlanta_engineer_region_assignment.csv"
    home_geocode_path = production_input_dir / "atlanta_engineer_home_geocoded.csv"
    heavy_repair_lookup_path = production_input_dir / "atlanta_heavy_repair_lookup.csv"
    service_filtered_path = production_input_dir / "atlanta_service_filtered.csv"
    service_enriched_path = production_input_dir / "atlanta_service_enriched.csv"
    region_workload_summary_path = production_output_dir / "atlanta_region_workload_summary.csv"
    profile_copy_path = production_input_dir / f"{profile_file.stem}_production.xlsx"

    region_zip_df.to_csv(region_zip_path, index=False, encoding="utf-8-sig")
    engineer_region_df.to_csv(engineer_region_path, index=False, encoding="utf-8-sig")
    home_geocode_df.to_csv(home_geocode_path, index=False, encoding="utf-8-sig")
    heavy_lookup_df.to_csv(heavy_repair_lookup_path, index=False, encoding="utf-8-sig")
    service_df.to_csv(service_filtered_path, index=False, encoding="utf-8-sig")
    service_enriched_df.to_csv(service_enriched_path, index=False, encoding="utf-8-sig")
    region_workload_df.to_csv(region_workload_summary_path, index=False, encoding="utf-8-sig")
    updated_address_df = address_df.merge(
        home_geocode_df[
            [
                "SVC_ENGINEER_CODE",
                "matched_address",
                "match_indicator",
                "match_type",
                "latitude",
                "longitude",
                "source",
            ]
        ],
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    _write_profile_copy(sheets, updated_address_df, profile_copy_path)

    return AtlantaProductionPrepResult(
        region_zip_path=region_zip_path,
        engineer_region_path=engineer_region_path,
        home_geocode_path=home_geocode_path,
        heavy_repair_lookup_path=heavy_repair_lookup_path,
        service_filtered_path=service_filtered_path,
        service_enriched_path=service_enriched_path,
        region_workload_summary_path=region_workload_summary_path,
        profile_copy_path=profile_copy_path,
    )
