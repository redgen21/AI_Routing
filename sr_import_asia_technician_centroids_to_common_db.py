from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from smart_routing.common_vrp_db import _execute_values_upsert, init_schema, upsert_technician_master


DEFAULT_CONFIG_PATH = Path("config.json")
DEFAULT_COMMON_CONFIG_PATH = Path("config_common_vrp.json")
DEFAULT_PROFILE_PATH = Path("260310/production_input/Asia_DMS_Profile_20260627_production.xlsx")
DEFAULT_OUTPUT_PATH = Path("data/exports/asia_technician_postal_centroids.csv")


def clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    return "" if text.casefold() in {"nan", "none", "nat", "<na>", "<null>"} else text


def clean_postal(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(5) if digits else text


def normalize_center_type(value: Any) -> str:
    text = clean_text(value)
    upper = text.upper()
    if upper == "EXCLUSIVE ASC":
        return "Exclusive ASC"
    if upper == "GENERAL ASC":
        return "General ASC"
    if upper in {"DSC", "DMS", "DMS2", "ASC"}:
        return upper
    return text or "DSC"


def normalize_product_group(value: Any) -> str:
    text = clean_text(value)
    upper = text.upper()
    mapping = {
        "RAC": "ACN",
        "RAC BD": "ACN",
        "REF": "REF",
        "LTV": "TV",
        "PTV": "TV",
        "CRT TV": "TV",
        "COMMERCIAL TV": "TV",
        "LED SIGNAGE": "CS",
        "MNT SIGNAGE": "CS",
        "MNT": "MNT",
        "AIR CARE": "ACL",
    }
    return mapping.get(upper, upper)


def load_service_file(config_path: Path) -> Path:
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    area_cfg = cfg.get("area_map_asia", {}) if isinstance(cfg.get("area_map_asia", {}), dict) else {}
    fallback_area_cfg = cfg.get("area_map", {}) if isinstance(cfg.get("area_map", {}), dict) else {}
    service_file = clean_text(area_cfg.get("service_file")) or clean_text(fallback_area_cfg.get("service_file"))
    if not service_file:
        raise ValueError("Missing area_map_asia.service_file in config.json")
    path = Path(service_file)
    if not path.exists():
        raise FileNotFoundError(f"Missing service file: {path}")
    return path


def build_postal_centroids(service_df: pd.DataFrame) -> pd.DataFrame:
    working = service_df.copy()
    working["POSTAL_CODE"] = working.get("POSTAL_CODE", pd.Series(index=working.index)).map(clean_postal)
    working["latitude"] = pd.to_numeric(working.get("latitude"), errors="coerce")
    working["longitude"] = pd.to_numeric(working.get("longitude"), errors="coerce")
    working = working[working["POSTAL_CODE"].ne("") & working["latitude"].notna() & working["longitude"].notna()].copy()
    if working.empty:
        return pd.DataFrame(columns=["POSTAL_CODE", "postal_latitude", "postal_longitude", "postal_service_count"])
    return (
        working.groupby("POSTAL_CODE", as_index=False)
        .agg(
            postal_latitude=("latitude", "mean"),
            postal_longitude=("longitude", "mean"),
            postal_service_count=("POSTAL_CODE", "size"),
        )
    )


def build_city_centroids(service_df: pd.DataFrame) -> dict[str, tuple[float, float, int]]:
    working = service_df.copy()
    working["latitude"] = pd.to_numeric(working.get("latitude"), errors="coerce")
    working["longitude"] = pd.to_numeric(working.get("longitude"), errors="coerce")
    city_columns = [col for col in ["STRATEGIC_CITY_NAME", "CITY_NAME", "translated_city"] if col in working.columns]
    rows: list[dict[str, Any]] = []
    for col in city_columns:
        tmp = working[[col, "latitude", "longitude"]].copy()
        tmp["city_key"] = tmp[col].map(clean_text).str.casefold()
        tmp = tmp[tmp["city_key"].ne("") & tmp["latitude"].notna() & tmp["longitude"].notna()]
        if not tmp.empty:
            rows.extend(tmp[["city_key", "latitude", "longitude"]].to_dict("records"))
    if not rows:
        return {}
    city_df = pd.DataFrame(rows)
    grouped = (
        city_df.groupby("city_key", as_index=False)
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"), count=("city_key", "size"))
    )
    return {
        str(row["city_key"]): (float(row["latitude"]), float(row["longitude"]), int(row["count"]))
        for _, row in grouped.iterrows()
    }


def build_technician_rows(profile_path: Path, service_path: Path) -> pd.DataFrame:
    zip_df = pd.read_excel(profile_path, sheet_name="1. Zip Coverage")
    slot_df = pd.read_excel(profile_path, sheet_name="2. Slot")
    address_df = pd.read_excel(profile_path, sheet_name="4. Address")
    service_df = pd.read_csv(service_path, encoding="utf-8-sig", low_memory=False)

    postal_centroids = build_postal_centroids(service_df)
    city_centroids = build_city_centroids(service_df)

    zip_df = zip_df.rename(
        columns={
            "SVC_ENGINEER_CODE": "employee_code",
            "POSTAL_CODE": "postal_code",
            "STRATEGIC_CITY_NAME": "coverage_city",
            "SVC_CENTER_TYPE": "coverage_center_type",
        }
    )
    zip_df["employee_code"] = zip_df["employee_code"].map(clean_text).str.upper()
    zip_df["postal_code"] = zip_df["postal_code"].map(clean_postal)
    zip_df["coverage_city"] = zip_df["coverage_city"].map(clean_text)
    zip_df = zip_df[zip_df["employee_code"].ne("") & zip_df["postal_code"].ne("")].copy()
    zip_centroid_df = zip_df.merge(
        postal_centroids,
        left_on="postal_code",
        right_on="POSTAL_CODE",
        how="left",
    )

    engineer_centroids = (
        zip_centroid_df[zip_centroid_df["postal_latitude"].notna() & zip_centroid_df["postal_longitude"].notna()]
        .groupby("employee_code", as_index=False)
        .agg(
            centroid_latitude=("postal_latitude", "mean"),
            centroid_longitude=("postal_longitude", "mean"),
            centroid_postal_count=("postal_code", "nunique"),
            centroid_service_count=("postal_service_count", "sum"),
        )
    )

    coverage_summary = (
        zip_df.groupby("employee_code", as_index=False)
        .agg(
            coverage_postal_count=("postal_code", "nunique"),
            coverage_postal_codes=("postal_code", lambda s: ",".join(sorted(set(map(str, s))))),
        )
    )

    slot_df = slot_df.rename(
        columns={
            "SVC_ENGINEER_CODE": "employee_code",
            "Name": "employee_name",
            "SVC_CENTER_TYPE": "center_type",
            "Slot": "slot_count",
            "STRATEGIC_CITY_NAME": "strategic_city_name_profile",
        }
    )
    slot_df["employee_code"] = slot_df["employee_code"].map(clean_text).str.upper()
    slot_df["employee_name"] = slot_df["employee_name"].map(clean_text)
    slot_df["center_type"] = slot_df["center_type"].map(normalize_center_type)
    slot_df["strategic_city_name_profile"] = slot_df["strategic_city_name_profile"].map(clean_text)
    slot_df["slot_count"] = pd.to_numeric(slot_df["slot_count"], errors="coerce").fillna(8).astype(int)
    slot_df = slot_df.drop_duplicates(subset=["employee_code"], keep="first").copy()

    address_df = address_df.rename(
        columns={
            "SVC_ENGINEER_CODE": "employee_code",
            "Name": "address_employee_name",
            "Home Street Address": "home_address",
            "City ": "home_city",
            "State": "home_state",
            "Zip": "home_postal_code",
            "latitude": "address_latitude",
            "longitude": "address_longitude",
        }
    )
    address_df["employee_code"] = address_df["employee_code"].map(clean_text).str.upper()
    for col in ["address_employee_name", "home_address", "home_city", "home_state", "home_postal_code"]:
        if col not in address_df.columns:
            address_df[col] = ""
        address_df[col] = address_df[col].map(clean_text)
    address_df["home_postal_code"] = address_df["home_postal_code"].map(clean_postal)
    address_df["address_latitude"] = pd.to_numeric(address_df.get("address_latitude"), errors="coerce")
    address_df["address_longitude"] = pd.to_numeric(address_df.get("address_longitude"), errors="coerce")
    address_df = address_df.drop_duplicates(subset=["employee_code"], keep="first").copy()

    output = (
        slot_df.merge(address_df, on="employee_code", how="left")
        .merge(engineer_centroids, on="employee_code", how="left")
        .merge(coverage_summary, on="employee_code", how="left")
    )

    rows: list[dict[str, Any]] = []
    for _, row in output.iterrows():
        city_name = clean_text(row.get("strategic_city_name_profile")) or clean_text(row.get("home_city"))
        city_key = city_name.casefold()
        centroid_lat = pd.to_numeric(pd.Series([row.get("centroid_latitude")]), errors="coerce").iloc[0]
        centroid_lng = pd.to_numeric(pd.Series([row.get("centroid_longitude")]), errors="coerce").iloc[0]
        address_lat = pd.to_numeric(pd.Series([row.get("address_latitude")]), errors="coerce").iloc[0]
        address_lng = pd.to_numeric(pd.Series([row.get("address_longitude")]), errors="coerce").iloc[0]
        source = "postal_service_centroid"
        if pd.isna(centroid_lat) or pd.isna(centroid_lng):
            city_fallback = city_centroids.get(city_key)
            if city_fallback:
                centroid_lat, centroid_lng = city_fallback[0], city_fallback[1]
                source = "city_service_centroid"
            elif pd.notna(address_lat) and pd.notna(address_lng):
                centroid_lat, centroid_lng = float(address_lat), float(address_lng)
                source = "address_fallback"
            else:
                source = "missing"

        coverage_postal_count = pd.to_numeric(pd.Series([row.get("coverage_postal_count")]), errors="coerce").iloc[0]
        centroid_postal_count = pd.to_numeric(pd.Series([row.get("centroid_postal_count")]), errors="coerce").iloc[0]
        rows.append(
            {
                "subsidiary_name": "LGEID",
                "strategic_city_name": "INDONESIA",
                "technician_city": city_name,
                "employee_code": clean_text(row.get("employee_code")),
                "employee_name": clean_text(row.get("employee_name")) or clean_text(row.get("address_employee_name")),
                "center_type": normalize_center_type(row.get("center_type")),
                "home_address": f"Postal coverage centroid - {city_name}",
                "home_city": city_name,
                "home_state": clean_text(row.get("home_state")) or "Indonesia",
                "home_country": "Indonesia",
                "home_postal_code": clean_text(row.get("coverage_postal_codes")).split(",")[0] if clean_text(row.get("coverage_postal_codes")) else clean_postal(row.get("home_postal_code")),
                "home_latitude": float(centroid_lat) if pd.notna(centroid_lat) else None,
                "home_longitude": float(centroid_lng) if pd.notna(centroid_lng) else None,
                "slot_count": int(row.get("slot_count", 8)),
                "active_flag": True,
                "priority_group": "A",
                "max_home_to_job_min": None,
                "centroid_source": source,
                "coverage_postal_count": int(coverage_postal_count) if pd.notna(coverage_postal_count) else 0,
                "centroid_postal_count": int(centroid_postal_count) if pd.notna(centroid_postal_count) else 0,
                "coverage_postal_codes": clean_text(row.get("coverage_postal_codes")),
            }
        )
    return pd.DataFrame(rows)


def build_capability_rows(profile_path: Path) -> pd.DataFrame:
    product_df = pd.read_excel(profile_path, sheet_name="3. Product")
    rename_map = {
        "SVC_ENGINEER_CODE": "employee_code",
        "SERVICE_PRODUCT_GROUP_CODE": "product_group_code",
        "SERVICE_PRODUCT_CODE": "product_code",
        "REPAIR_FLAG": "repair_flag",
        "AREA_PRODUCT_FLAG": "area_product_flag",
        "STRATEGIC_CITY_NAME": "technician_city",
        "SVC_CENTER_TYPE": "center_type",
    }
    product_df = product_df.rename(columns=rename_map).copy()
    for col in ["employee_code", "product_group_code", "product_code", "repair_flag", "area_product_flag", "technician_city", "center_type"]:
        if col not in product_df.columns:
            product_df[col] = ""
        product_df[col] = product_df[col].map(clean_text)
    product_df["employee_code"] = product_df["employee_code"].str.upper()
    product_df["product_group_code"] = product_df["product_group_code"].map(normalize_product_group)
    # Asia product master has inconsistent product-code detail. Keep the import
    # group-only until the proper Asia product-code master is available.
    product_df["product_code"] = ""
    product_df = product_df[product_df["employee_code"].ne("") & product_df["product_group_code"].ne("")].copy()
    product_df["repair_allowed"] = product_df["repair_flag"].str.upper().eq("T")
    product_df["heavy_repair_allowed"] = ~(
        product_df["product_group_code"].eq("REF")
        & product_df["area_product_flag"].str.upper().eq("N")
    )
    output = product_df[
        [
            "employee_code",
            "product_group_code",
            "product_code",
            "repair_allowed",
            "heavy_repair_allowed",
            "technician_city",
            "center_type",
        ]
    ].drop_duplicates(subset=["employee_code", "product_group_code", "product_code"])
    output.insert(0, "strategic_city_name", "INDONESIA")
    output.insert(0, "subsidiary_name", "LGEID")
    output["priority_score"] = 100
    output["effective_start_date"] = None
    output["effective_end_date"] = None
    return output.reset_index(drop=True)


def add_missing_service_group_capabilities(
    capability_df: pd.DataFrame,
    technician_df: pd.DataFrame,
    service_path: Path,
) -> pd.DataFrame:
    service_df = pd.read_csv(service_path, encoding="utf-8-sig", low_memory=False)
    if "SERVICE_PRODUCT_GROUP_CODE" not in service_df.columns:
        return capability_df
    requested_groups = {
        normalize_product_group(value)
        for value in service_df["SERVICE_PRODUCT_GROUP_CODE"].dropna().astype(str).tolist()
        if clean_text(value)
    }
    covered_groups = {
        clean_text(value).upper()
        for value in capability_df.get("product_group_code", pd.Series(dtype=str)).tolist()
        if clean_text(value)
    }
    missing_groups = sorted(group for group in requested_groups if group and group not in covered_groups)
    if not missing_groups:
        return capability_df
    employee_codes = sorted(
        {
            clean_text(value).upper()
            for value in technician_df.get("employee_code", pd.Series(dtype=str)).tolist()
            if clean_text(value)
        }
    )
    fallback_rows = [
        {
            "subsidiary_name": "LGEID",
            "strategic_city_name": "INDONESIA",
            "employee_code": employee_code,
            "product_group_code": group,
            "product_code": "",
            "repair_allowed": True,
            "heavy_repair_allowed": True,
            "technician_city": "",
            "center_type": "",
            "priority_score": 100,
            "effective_start_date": None,
            "effective_end_date": None,
        }
        for employee_code in employee_codes
        for group in missing_groups
    ]
    if not fallback_rows:
        return capability_df
    output = pd.concat([capability_df, pd.DataFrame(fallback_rows)], ignore_index=True)
    return output.drop_duplicates(
        subset=["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"],
        keep="first",
    ).reset_index(drop=True)


def build_region_rows(profile_path: Path, service_path: Path) -> pd.DataFrame:
    zip_df = pd.read_excel(profile_path, sheet_name="1. Zip Coverage")
    service_df = pd.read_csv(service_path, encoding="utf-8-sig", low_memory=False)
    postal_centroids = build_postal_centroids(service_df)
    zip_df = zip_df.rename(
        columns={
            "POSTAL_CODE": "postal_code",
            "AREA_NAME": "region_name",
            "STRATEGIC_CITY_NAME": "technician_city",
            "SVC_ENGINEER_CODE": "employee_code",
            "SVC_CENTER_TYPE": "center_type",
        }
    )
    for col in ["postal_code", "region_name", "technician_city", "employee_code", "center_type"]:
        if col not in zip_df.columns:
            zip_df[col] = ""
        zip_df[col] = zip_df[col].map(clean_text)
    zip_df["postal_code"] = zip_df["postal_code"].map(clean_postal)
    zip_df = zip_df[zip_df["postal_code"].ne("")].copy()
    if zip_df.empty:
        return pd.DataFrame()

    primary = (
        zip_df.sort_values(["postal_code", "region_name", "employee_code"])
        .drop_duplicates(subset=["postal_code"], keep="first")
        .copy()
    )
    region_names = sorted(primary["region_name"].dropna().astype(str).unique().tolist())
    region_seq_lookup = {name: idx + 1 for idx, name in enumerate(region_names)}
    primary["region_seq"] = primary["region_name"].map(region_seq_lookup).fillna(0).astype(int)
    primary = primary.merge(
        postal_centroids.rename(columns={"POSTAL_CODE": "postal_code"}),
        on="postal_code",
        how="left",
    )
    output = pd.DataFrame(
        {
            "subsidiary_name": "LGEID",
            "strategic_city_name": "INDONESIA",
            "postal_code": primary["postal_code"],
            "region_seq": primary["region_seq"],
            "region_name": primary["region_name"].where(primary["region_name"].ne(""), primary["technician_city"]),
            "region_center_latitude": primary["postal_latitude"],
            "region_center_longitude": primary["postal_longitude"],
            "technician_city": primary["technician_city"],
            "employee_code": primary["employee_code"],
            "center_type": primary["center_type"].map(normalize_center_type),
        }
    )
    return output.drop_duplicates(subset=["subsidiary_name", "strategic_city_name", "postal_code"]).reset_index(drop=True)


def import_technician_rows(df: pd.DataFrame, common_config_path: Path) -> int:
    saved = 0
    for _, row in df.iterrows():
        if pd.isna(row.get("home_latitude")) or pd.isna(row.get("home_longitude")):
            continue
        saved += upsert_technician_master(row.to_dict(), config_path=common_config_path)
    return saved


def import_capability_rows(df: pd.DataFrame, common_config_path: Path) -> int:
    columns = [
        "subsidiary_name",
        "strategic_city_name",
        "employee_code",
        "product_group_code",
        "product_code",
        "repair_allowed",
        "heavy_repair_allowed",
        "priority_score",
        "effective_start_date",
        "effective_end_date",
    ]
    if df.empty:
        return 0
    rows = [tuple(row.get(col) for col in columns) for _, row in df.iterrows()]
    return _execute_values_upsert(
        "common_technician_capability_master",
        columns,
        rows,
        ["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"],
        ["repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date"],
        config_path=common_config_path,
    )


def import_region_rows(df: pd.DataFrame, common_config_path: Path) -> int:
    columns = [
        "subsidiary_name",
        "strategic_city_name",
        "postal_code",
        "region_seq",
        "region_name",
        "region_center_latitude",
        "region_center_longitude",
    ]
    if df.empty:
        return 0
    db_df = df.copy()
    db_df["region_center_latitude"] = pd.to_numeric(db_df["region_center_latitude"], errors="coerce")
    db_df["region_center_longitude"] = pd.to_numeric(db_df["region_center_longitude"], errors="coerce")
    db_df["region_center_latitude"] = db_df["region_center_latitude"].where(db_df["region_center_latitude"].notna(), None)
    db_df["region_center_longitude"] = db_df["region_center_longitude"].where(db_df["region_center_longitude"].notna(), None)
    rows = [tuple(row.get(col) for col in columns) for _, row in db_df.iterrows()]
    return _execute_values_upsert(
        "common_region_master",
        columns,
        rows,
        ["subsidiary_name", "strategic_city_name", "postal_code"],
        ["region_seq", "region_name", "region_center_latitude", "region_center_longitude"],
        config_path=common_config_path,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Asia technician postal centroid home locations into common VRP DB.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--common-config", default=str(DEFAULT_COMMON_CONFIG_PATH))
    parser.add_argument("--profile-file", default=str(DEFAULT_PROFILE_PATH))
    parser.add_argument("--service-file", default="")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--apply", action="store_true", help="Write rows to common_technician_master. Without this, only preview/export is created.")
    args = parser.parse_args()

    config_path = Path(args.config)
    common_config_path = Path(args.common_config)
    profile_path = Path(args.profile_file)
    service_path = Path(args.service_file) if args.service_file else load_service_file(config_path)
    if not profile_path.exists():
        raise FileNotFoundError(f"Missing profile file: {profile_path}")

    df = build_technician_rows(profile_path, service_path)
    capability_df = build_capability_rows(profile_path)
    capability_df = add_missing_service_group_capabilities(capability_df, df, service_path)
    region_df = build_region_rows(profile_path, service_path)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    capability_output_path = output_path.with_name(output_path.stem.replace("technician_postal_centroids", "technician_capabilities") + output_path.suffix)
    region_output_path = output_path.with_name(output_path.stem.replace("technician_postal_centroids", "postal_regions") + output_path.suffix)
    capability_df.to_csv(capability_output_path, index=False, encoding="utf-8-sig")
    region_df.to_csv(region_output_path, index=False, encoding="utf-8-sig")

    source_counts = df["centroid_source"].value_counts(dropna=False).to_dict() if "centroid_source" in df.columns else {}
    print(f"profile_file={profile_path}")
    print(f"service_file={service_path}")
    print(f"output={output_path}")
    print(f"capability_output={capability_output_path}")
    print(f"region_output={region_output_path}")
    print(f"technicians={len(df)}")
    print(f"capabilities={len(capability_df)}")
    print(f"regions={len(region_df)}")
    print(f"centroid_sources={source_counts}")
    missing_df = df[df["home_latitude"].isna() | df["home_longitude"].isna()].copy()
    print(f"missing_coordinates={len(missing_df)}")
    if not missing_df.empty:
        print(missing_df[["employee_code", "employee_name", "technician_city", "centroid_source"]].to_string(index=False))

    preview_cols = [
        "employee_code",
        "employee_name",
        "technician_city",
        "center_type",
        "home_latitude",
        "home_longitude",
        "centroid_source",
        "coverage_postal_count",
        "centroid_postal_count",
    ]
    print(df[preview_cols].head(20).to_string(index=False))

    if args.apply:
        init_schema(common_config_path)
        saved_technicians = import_technician_rows(df, common_config_path)
        saved_capabilities = import_capability_rows(capability_df, common_config_path)
        saved_regions = import_region_rows(region_df, common_config_path)
        print(f"saved_technicians={saved_technicians}")
        print(f"saved_capabilities={saved_capabilities}")
        print(f"saved_regions={saved_regions}")
    else:
        print("dry_run=true; add --apply to save rows to common_technician_master, common_technician_capability_master, and common_region_master")


if __name__ == "__main__":
    main()
