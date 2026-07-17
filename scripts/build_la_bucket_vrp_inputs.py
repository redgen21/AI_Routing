from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smart_routing.common_vrp_runtime import _load_fallback_heavy_repair_rules


BASE_CITY = "Los Angeles, CA"
BUCKET_SIM_DRAFT_XLSX = Path("260310/LA Bucket Sim_Draft.xlsx")
SCENARIOS = {
    "area_type_clusters": {
        "city_name": "Los Angeles, CA - Area Type Clusters",
        "region_source": Path("260310/production_input/los_angeles_fixed_region_zip_6_area_type.csv"),
        "region_seed": Path("260310/production_input/los_angeles_area_type_clusters_region_seed.csv"),
    },
    "bucket_sim_draft": {
        "city_name": "Los Angeles, CA - Bucket Sim Draft",
        "region_source": Path("260310/input/fixed_region_maps/fixed_region_postal_los_angeles_ca_bucket_sim_draft.csv"),
        "region_seed": Path("260310/production_input/los_angeles_bucket_sim_draft_region_seed.csv"),
    },
}


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _zip(value: object) -> str:
    text = _clean_text(value).replace(".0", "")
    return text.zfill(5) if text else ""


def _scenario_slug(value: str) -> str:
    return value.lower().replace(" ", "_").replace(",", "").replace("-", "").replace("__", "_")


def _apply_bucket_sim_draft_area_names(df: pd.DataFrame) -> pd.DataFrame:
    if not BUCKET_SIM_DRAFT_XLSX.exists():
        return df
    draft_df = pd.read_excel(BUCKET_SIM_DRAFT_XLSX, dtype=str)
    if "Zip_Code" not in draft_df.columns or "AREA NAME" not in draft_df.columns:
        return df

    area_lookup = draft_df[["Zip_Code", "AREA NAME"]].copy()
    area_lookup["POSTAL_CODE"] = area_lookup["Zip_Code"].map(_zip)
    area_lookup["AREA_NAME_FROM_DRAFT"] = area_lookup["AREA NAME"].map(_clean_text)
    area_lookup = area_lookup[
        area_lookup["POSTAL_CODE"].ne("") & area_lookup["AREA_NAME_FROM_DRAFT"].ne("")
    ].drop_duplicates(subset=["POSTAL_CODE"], keep="first")
    if area_lookup.empty:
        return df

    working = df.copy()
    working["POSTAL_CODE"] = working["POSTAL_CODE"].map(_zip)
    working = working.drop(columns=["AREA_NAME_FROM_DRAFT"], errors="ignore")
    working = working.merge(area_lookup[["POSTAL_CODE", "AREA_NAME_FROM_DRAFT"]], on="POSTAL_CODE", how="left")
    in_bucket = working["AREA_NAME_FROM_DRAFT"].fillna("").astype(str).str.strip().ne("")
    working.loc[in_bucket, "AREA_NAME"] = working.loc[in_bucket, "AREA_NAME_FROM_DRAFT"]
    working.loc[in_bucket, "new_region_name"] = working.loc[in_bucket, "AREA_NAME_FROM_DRAFT"]
    working.loc[in_bucket, "area_type"] = "OVERLAP"
    working.loc[~in_bucket, "AREA_NAME"] = "Outside Bucket Sim Draft"
    working.loc[~in_bucket, "new_region_name"] = "Outside Bucket Sim Draft"
    working.loc[~in_bucket, "area_type"] = "DMS2_EXCLUSIVE"

    area_names = sorted(working.loc[in_bucket, "AREA_NAME"].dropna().astype(str).unique().tolist())
    seq_lookup = {name: idx + 1 for idx, name in enumerate(area_names)}
    outside_seq = len(seq_lookup) + 1
    working["region_seq"] = working["AREA_NAME"].map(seq_lookup).fillna(outside_seq).astype(int)
    working["region_id"] = working["region_seq"]
    return working.drop(columns=["AREA_NAME_FROM_DRAFT"], errors="ignore")


def _bucket_sim_draft_technician_areas() -> dict[str, str]:
    """Return the DMS technician-to-Bucket-area mapping maintained in the draft."""
    if not BUCKET_SIM_DRAFT_XLSX.exists():
        return {}
    draft_df = pd.read_excel(BUCKET_SIM_DRAFT_XLSX, dtype=str)
    technician_area_column = "AREA NAME.1" if "AREA NAME.1" in draft_df.columns else "AREA NAME"
    required = {"SVC_ENGINEER_CODE", technician_area_column}
    if not required.issubset(draft_df.columns):
        return {}
    mapping_df = draft_df[["SVC_ENGINEER_CODE", technician_area_column]].copy()
    mapping_df["employee_code"] = mapping_df["SVC_ENGINEER_CODE"].map(_clean_text)
    mapping_df["preferred_region_name"] = mapping_df[technician_area_column].map(_clean_text)
    mapping_df = mapping_df[
        mapping_df["employee_code"].ne("") & mapping_df["preferred_region_name"].ne("")
    ].drop_duplicates(subset=["employee_code"], keep="first")
    return dict(zip(mapping_df["employee_code"], mapping_df["preferred_region_name"]))


def _write_scenario_region_seed() -> dict[str, pd.DataFrame]:
    region_frames: dict[str, pd.DataFrame] = {}
    for scenario_key, scenario in SCENARIOS.items():
        source = scenario["region_source"]
        seed_path = scenario["region_seed"]
        if source.exists():
            df = pd.read_csv(source, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
        elif seed_path.exists():
            df = pd.read_csv(seed_path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
        else:
            raise FileNotFoundError(
                f"Missing region source: {source}. Also missing fallback seed: {seed_path}"
            )
        df["POSTAL_CODE"] = df["POSTAL_CODE"].map(_zip)
        df["STRATEGIC_CITY_NAME"] = scenario["city_name"]
        if scenario_key == "bucket_sim_draft":
            df = _apply_bucket_sim_draft_area_names(df)
        if "area_type" in df.columns:
            df["area_type"] = df["area_type"].fillna("").astype(str).str.strip().str.upper()
        df.to_csv(source, index=False, encoding="utf-8-sig")
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(seed_path, index=False, encoding="utf-8-sig")
        region_frames[scenario_key] = df
    return region_frames


def _load_la_jobs(service_path: Path) -> pd.DataFrame:
    if not service_path.exists():
        raise FileNotFoundError(f"Missing service file: {service_path}")
    df = pd.read_csv(service_path, encoding="utf-8-sig", low_memory=False, dtype={"POSTAL_CODE": str})
    df = df[df["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq(BASE_CITY)].copy()
    df["PROMISE_DATE"] = df["PROMISE_DATE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df["promise_dt"] = pd.to_datetime(df["PROMISE_DATE"], format="%Y%m%d", errors="coerce")
    df = df[df["promise_dt"].notna() & df["promise_dt"].dt.weekday.lt(5)].copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(_zip)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    df = df.sort_values(["PROMISE_DATE", "GSFS_RECEIPT_NO"]).drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first")
    return df.reset_index(drop=True)


def _load_dms_technicians(profile_path: Path, la_jobs_df: pd.DataFrame) -> pd.DataFrame:
    address_df = pd.read_excel(profile_path, sheet_name="4. Address")
    dms_jobs = la_jobs_df[la_jobs_df["SVC_CENTER_TYPE"].astype(str).str.strip().str.upper().eq("DMS")].copy()
    address_df["SVC_ENGINEER_CODE"] = address_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    city_label_mask = (
        address_df.get("City ", pd.Series("", index=address_df.index)).fillna("").astype(str).str.strip().eq(BASE_CITY)
        | address_df.get("State", pd.Series("", index=address_df.index)).fillna("").astype(str).str.strip().eq(BASE_CITY)
    )
    merged = (
        address_df[city_label_mask & address_df["SVC_ENGINEER_CODE"].ne("")]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
        .copy()
    )
    merged["home_latitude"] = pd.to_numeric(merged.get("latitude"), errors="coerce")
    merged["home_longitude"] = pd.to_numeric(merged.get("longitude"), errors="coerce")
    fallback = (
        dms_jobs
        .groupby("SVC_ENGINEER_CODE")
        .agg(
            fallback_name=("SVC_ENGINEER_NAME", lambda s: _clean_text(s.mode().iloc[0]) if not s.mode().empty else ""),
            fallback_latitude=("latitude", "mean"),
            fallback_longitude=("longitude", "mean"),
            fallback_city=("CITY_NAME", lambda s: _clean_text(s.mode().iloc[0]) if not s.mode().empty else ""),
            fallback_state=("STATE_NAME", lambda s: _clean_text(s.mode().iloc[0]) if not s.mode().empty else "CA"),
            fallback_postal_code=("POSTAL_CODE", lambda s: _zip(s.mode().iloc[0]) if not s.mode().empty else ""),
        )
        .reset_index()
    )
    merged = merged.merge(fallback, on="SVC_ENGINEER_CODE", how="left")
    if "Name" not in merged.columns:
        merged["Name"] = ""
    merged["Name"] = merged["Name"].fillna("").astype(str).str.strip()
    merged.loc[merged["Name"].eq(""), "Name"] = merged.loc[merged["Name"].eq(""), "fallback_name"]
    for col in ["Home Street Address", "City ", "State", "Zip"]:
        if col not in merged.columns:
            merged[col] = pd.NA
    missing_home = merged["home_latitude"].isna() | merged["home_longitude"].isna()
    merged.loc[missing_home, "home_latitude"] = merged.loc[missing_home, "fallback_latitude"]
    merged.loc[missing_home, "home_longitude"] = merged.loc[missing_home, "fallback_longitude"]
    merged.loc[missing_home, "Home Street Address"] = merged.loc[missing_home, "Home Street Address"].fillna("DMS service centroid")
    merged.loc[missing_home, "City "] = merged.loc[missing_home, "City "].fillna(merged.loc[missing_home, "fallback_city"])
    merged.loc[missing_home, "State"] = merged.loc[missing_home, "State"].fillna(merged.loc[missing_home, "fallback_state"])
    merged["Zip"] = merged["Zip"].astype("object")
    merged.loc[missing_home, "Zip"] = merged.loc[missing_home, "Zip"].fillna(merged.loc[missing_home, "fallback_postal_code"])
    remaining_missing = merged["home_latitude"].isna() | merged["home_longitude"].isna()
    if remaining_missing.any():
        dms_jobs = la_jobs_df[la_jobs_df["SVC_CENTER_TYPE"].astype(str).str.strip().str.upper().eq("DMS")]
        city_lat = pd.to_numeric(dms_jobs["latitude"], errors="coerce").mean()
        city_lng = pd.to_numeric(dms_jobs["longitude"], errors="coerce").mean()
        merged.loc[remaining_missing, "home_latitude"] = city_lat
        merged.loc[remaining_missing, "home_longitude"] = city_lng
        merged.loc[remaining_missing, "Home Street Address"] = merged.loc[remaining_missing, "Home Street Address"].fillna("LA DMS service centroid")
        merged.loc[remaining_missing, "City "] = merged.loc[remaining_missing, "City "].fillna("Los Angeles")
        merged.loc[remaining_missing, "State"] = merged.loc[remaining_missing, "State"].fillna("CA")
    merged = merged.dropna(subset=["home_latitude", "home_longitude"]).copy()
    return pd.DataFrame(
        {
            "employee_code": merged["SVC_ENGINEER_CODE"].astype(str).str.strip(),
            "employee_name": merged["Name"].astype(str).str.strip(),
            "center_type": "DMS",
            "home_address": merged.get("Home Street Address", "").fillna("").astype(str).str.strip(),
            "home_city": merged.get("City ", "").fillna("").astype(str).str.strip(),
            "home_state": merged.get("State", "").fillna("").astype(str).str.strip(),
            "home_country": "USA",
            "home_postal_code": merged.get("Zip", "").map(_zip),
            "home_latitude": merged["home_latitude"],
            "home_longitude": merged["home_longitude"],
        }
    ).drop_duplicates(subset=["employee_code"])


def _build_dms2_technicians(la_jobs_df: pd.DataFrame) -> pd.DataFrame:
    dms2_df = la_jobs_df[la_jobs_df["SVC_CENTER_TYPE"].astype(str).str.strip().str.upper().eq("DMS2")].copy()
    dms2_df = dms2_df[dms2_df["SVC_ENGINEER_CODE"].astype(str).str.strip().ne("")]
    grouped = (
        dms2_df.groupby(["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME"], dropna=False)
        .agg(
            home_latitude=("latitude", "mean"),
            home_longitude=("longitude", "mean"),
            job_count=("GSFS_RECEIPT_NO", "nunique"),
            home_city=("CITY_NAME", lambda s: _clean_text(s.mode().iloc[0]) if not s.mode().empty else ""),
            home_state=("STATE_NAME", lambda s: _clean_text(s.mode().iloc[0]) if not s.mode().empty else "CA"),
            home_postal_code=("POSTAL_CODE", lambda s: _zip(s.mode().iloc[0]) if not s.mode().empty else ""),
        )
        .reset_index()
    )
    return pd.DataFrame(
        {
            "employee_code": grouped["SVC_ENGINEER_CODE"].astype(str).str.strip(),
            "employee_name": grouped["SVC_ENGINEER_NAME"].fillna("").astype(str).str.strip(),
            "center_type": "DMS2",
            "home_address": "DMS2 service centroid",
            "home_city": grouped["home_city"],
            "home_state": grouped["home_state"].replace("", "CA"),
            "home_country": "USA",
            "home_postal_code": grouped["home_postal_code"],
            "home_latitude": grouped["home_latitude"],
            "home_longitude": grouped["home_longitude"],
            "source_job_count": grouped["job_count"],
        }
    ).drop_duplicates(subset=["employee_code"])


def _technician_upload_frame(tech_df: pd.DataFrame, scenario_city: str, promise_date: str) -> pd.DataFrame:
    out = tech_df.copy()
    out["subsidiary_name"] = "LGEAI"
    out["strategic_city_name"] = scenario_city
    out["PROMISE_DATE"] = promise_date
    out["available"] = True
    out["shift_start"] = "08:00"
    out["shift_end"] = "18:00"
    out["slot_count"] = 6
    out["max_slots"] = 6
    out["max_jobs"] = 6
    out["priority_group"] = "B"
    if "BUCKET SIM DRAFT" not in scenario_city.upper():
        out["preferred_region_name"] = ""
    out["start_location_type"] = "Home"
    out["start_location_address"] = ""
    out["max_minutes"] = 600
    out.loc[out["center_type"].astype(str).str.upper().eq("DMS2"), "max_minutes"] = 24 * 60
    out["max_home_to_job_min"] = out["center_type"].astype(str).str.upper().map(
        lambda center_type: -1 if center_type == "DMS2" else pd.NA
    )
    cols = [
        "subsidiary_name",
        "strategic_city_name",
        "PROMISE_DATE",
        "employee_code",
        "employee_name",
        "center_type",
        "available",
        "shift_start",
        "shift_end",
        "slot_count",
        "max_slots",
        "max_jobs",
        "max_minutes",
        "max_home_to_job_min",
        "priority_group",
        "preferred_region_name",
        "start_location_type",
        "start_location_address",
        "home_address",
        "home_city",
        "home_state",
        "home_country",
        "home_postal_code",
        "home_latitude",
        "home_longitude",
    ]
    return out.reindex(columns=cols)


def _filter_attended_technicians(tech_df: pd.DataFrame, day_df: pd.DataFrame) -> pd.DataFrame:
    if tech_df.empty or day_df.empty:
        return tech_df.iloc[0:0].copy()
    attended_codes = {
        _clean_text(code)
        for code in day_df.loc[
            day_df["SVC_CENTER_TYPE"].astype(str).str.strip().str.upper().isin({"DMS", "DMS2"}),
            "SVC_ENGINEER_CODE",
        ].tolist()
        if _clean_text(code)
    }
    if not attended_codes:
        return tech_df.iloc[0:0].copy()
    return tech_df[tech_df["employee_code"].astype(str).str.strip().isin(attended_codes)].copy()


def _heavy_repair_mask(df: pd.DataFrame) -> pd.Series:
    rules = _load_fallback_heavy_repair_rules()
    exact_keys = set(
        zip(
            rules["product_group_code"].astype(str).str.strip().str.upper(),
            rules["product_code"].astype(str).str.strip().str.upper(),
            rules["detailed_symptom_code"].astype(str).str.strip().str.upper(),
        )
    )
    group_keys = set(
        zip(
            rules["product_group_code"].astype(str).str.strip().str.upper(),
            rules["detailed_symptom_code"].astype(str).str.strip().str.upper(),
        )
    )

    def _is_heavy(row: pd.Series) -> bool:
        product_group = _clean_text(row.get("SERVICE_PRODUCT_GROUP_CODE")).upper()
        product_code = _clean_text(row.get("SERVICE_PRODUCT_CODE")).upper()
        symptom = _clean_text(row.get("RECEIPT_DETAIL_SYMPTOM_CODE")).upper()
        candidates = [symptom]
        if symptom:
            candidates.extend([symptom[:5], symptom[:3]])
        return any(
            (product_group, product_code, candidate) in exact_keys
            or (product_group, candidate) in group_keys
            for candidate in candidates
            if candidate
        )

    return df.apply(_is_heavy, axis=1)


def _job_upload_frame(day_df: pd.DataFrame, region_df: pd.DataFrame, scenario_city: str) -> pd.DataFrame:
    region_cols = ["POSTAL_CODE", "region_seq", "new_region_name", "AREA_NAME", "area_type"]
    lookup = region_df[[col for col in region_cols if col in region_df.columns]].drop_duplicates(subset=["POSTAL_CODE"])
    merged = day_df.merge(lookup, on="POSTAL_CODE", how="left")
    merged["STRATEGIC_CITY_NAME"] = scenario_city
    merged["CITY_NAME"] = scenario_city
    merged["fixed"] = False
    merged["reschedule"] = False
    merged["job_slot_count"] = 1
    merged.loc[_heavy_repair_mask(merged), "job_slot_count"] = 2
    cols = [
        "SUBSIDIARY_NAME",
        "STRATEGIC_CITY_NAME",
        "SVC_ENGINEER_CODE",
        "SVC_ENGINEER_NAME",
        "SVC_CENTER_TYPE",
        "SERVICE_PRODUCT_GROUP_CODE",
        "SERVICE_PRODUCT_CODE",
        "RECEIPT_DETAIL_SYMPTOM_CODE",
        "GSFS_RECEIPT_NO",
        "PROMISE_DATE",
        "CITY_NAME",
        "STATE_NAME",
        "COUNTRY_NAME",
        "POSTAL_CODE",
        "ADDRESS_LINE1_INFO",
        "latitude",
        "longitude",
        "fixed",
        "reschedule",
        "job_slot_count",
        "region_seq",
        "new_region_name",
        "AREA_NAME",
        "area_type",
    ]
    return merged.reindex(columns=cols)


def _write_inputs(service_path: Path, profile_path: Path, output_dir: Path) -> dict[str, Any]:
    region_frames = _write_scenario_region_seed()
    la_jobs_df = _load_la_jobs(service_path)
    dms_tech_df = _load_dms_technicians(profile_path, la_jobs_df)
    dms2_tech_df = _build_dms2_technicians(la_jobs_df)
    bucket_technician_areas = _bucket_sim_draft_technician_areas()
    dms_tech_df["preferred_region_name"] = dms_tech_df["employee_code"].map(bucket_technician_areas).fillna("")
    dms2_tech_df["preferred_region_name"] = ""
    tech_df = pd.concat([dms_tech_df, dms2_tech_df], ignore_index=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, Any]] = []
    technician_master_path = output_dir / "la_scenario_technician_master.csv"
    tech_master = []
    for scenario_key, scenario in SCENARIOS.items():
        scenario_city = str(scenario["city_name"])
        for _, row in tech_df.iterrows():
            row_dict = row.to_dict()
            row_dict["subsidiary_name"] = "LGEAI"
            row_dict["strategic_city_name"] = scenario_city
            row_dict["active_flag"] = True
            row_dict["priority_group"] = "B"
            row_dict["max_home_to_job_min"] = -1 if str(row_dict.get("center_type", "")).upper() == "DMS2" else None
            row_dict["max_minutes"] = 24 * 60 if str(row_dict.get("center_type", "")).upper() == "DMS2" else 300
            tech_master.append(row_dict)
    pd.DataFrame(tech_master).to_csv(technician_master_path, index=False, encoding="utf-8-sig")

    for scenario_key, scenario in SCENARIOS.items():
        scenario_city = str(scenario["city_name"])
        scenario_dir = output_dir / scenario_key
        scenario_dir.mkdir(parents=True, exist_ok=True)
        region_df = region_frames[scenario_key]
        for promise_date, day_df in la_jobs_df.groupby("PROMISE_DATE", sort=True):
            jobs_out = _job_upload_frame(day_df.copy(), region_df, scenario_city)
            attended_tech_df = _filter_attended_technicians(tech_df, day_df)
            tech_out = _technician_upload_frame(attended_tech_df, scenario_city, str(promise_date))
            jobs_path = scenario_dir / f"jobs_{promise_date}_{scenario_key}.csv"
            tech_path = scenario_dir / f"technicians_{promise_date}_{scenario_key}.csv"
            jobs_out.to_csv(jobs_path, index=False, encoding="utf-8-sig")
            tech_out.to_csv(tech_path, index=False, encoding="utf-8-sig")

            area_summary = (
                jobs_out.assign(area_type=jobs_out["area_type"].fillna("").replace("", "UNMAPPED"))
                .groupby("area_type")
                .agg(service_count=("GSFS_RECEIPT_NO", "nunique"), zip_count=("POSTAL_CODE", "nunique"))
                .reset_index()
            )
            for _, area_row in area_summary.iterrows():
                summary_rows.append(
                    {
                        "scenario": scenario_key,
                        "strategic_city_name": scenario_city,
                        "promise_date": promise_date,
                        "area_type": area_row["area_type"],
                        "service_count": int(area_row["service_count"]),
                        "zip_count": int(area_row["zip_count"]),
                        "job_file": str(jobs_path),
                        "technician_file": str(tech_path),
                    }
                )
    summary_df = pd.DataFrame(summary_rows)
    summary_path = output_dir / "la_bucket_test_file_summary.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return {
        "la_jobs": len(la_jobs_df),
        "dates": sorted(la_jobs_df["PROMISE_DATE"].astype(str).unique().tolist()),
        "dms_technicians": len(dms_tech_df),
        "dms2_technicians": len(dms2_tech_df),
        "summary_path": summary_path,
        "technician_master_path": technician_master_path,
    }


def _update_db(config_path: Path, technician_master_path: Path) -> None:
    from smart_routing.common_vrp_db import get_db_connection, seed_default_masters, upsert_technician_master

    scenario_city_names = [BASE_CITY] + [str(scenario["city_name"]) for scenario in SCENARIOS.values()]
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            for city_name in scenario_city_names:
                cur.execute(
                    """
                    delete from common_technician_capability_master
                    where subsidiary_name = %s and strategic_city_name = %s
                    """,
                    ("LGEAI", city_name),
                )
                cur.execute(
                    """
                    delete from common_technician_master
                    where subsidiary_name = %s and strategic_city_name = %s
                    """,
                    ("LGEAI", city_name),
                )
        conn.commit()

    seed_default_masters(config_path)
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            for city_name in scenario_city_names:
                cur.execute(
                    """
                    delete from common_technician_capability_master c
                    where c.subsidiary_name = %s
                      and c.strategic_city_name = %s
                      and not exists (
                          select 1
                          from common_technician_master m
                          where m.subsidiary_name = c.subsidiary_name
                            and m.strategic_city_name = c.strategic_city_name
                            and m.employee_code = c.employee_code
                      )
                    """,
                    ("LGEAI", city_name),
                )
        conn.commit()

    tech_df = pd.read_csv(technician_master_path, encoding="utf-8-sig", low_memory=False)
    for _, row in tech_df.iterrows():
        upsert_technician_master(row.to_dict(), config_path=config_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-file", default="260310/input/Service_202607071543_normalized_geocoded.csv")
    parser.add_argument("--profile-file", default="260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")
    parser.add_argument("--output-dir", default="260310/la bucket test")
    parser.add_argument("--config", default="config_common_vrp.json")
    parser.add_argument("--update-db", action="store_true")
    args = parser.parse_args()

    result = _write_inputs(Path(args.service_file), Path(args.profile_file), Path(args.output_dir))
    print(f"LA weekday jobs: {result['la_jobs']}")
    print(f"Dates: {', '.join(result['dates'])}")
    print(f"DMS technicians: {result['dms_technicians']}")
    print(f"DMS2 technicians: {result['dms2_technicians']}")
    print(f"Summary: {result['summary_path']}")
    print(f"Technician master: {result['technician_master_path']}")
    if args.update_db:
        _update_db(Path(args.config), Path(result["technician_master_path"]))
        print("DB seed/update completed.")


if __name__ == "__main__":
    main()
