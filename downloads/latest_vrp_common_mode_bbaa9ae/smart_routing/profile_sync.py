from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
SERVICE_FILE = Path("260310/Service_202603181109.csv")
INPUT_DIR = Path("260310/input")
OUTPUT_DIR = Path("260310/output")
ACTIVE_CENTER_TYPES = {"DMS"}
DEFAULT_SLOT = 7


@dataclass
class ProfileSyncResult:
    updated_zip_df: pd.DataFrame
    updated_slot_df: pd.DataFrame
    unmatched_service_sm_df: pd.DataFrame
    summary_df: pd.DataFrame
    zip_output_path: Path
    slot_output_path: Path
    unmatched_output_path: Path
    summary_output_path: Path


def _read_service_csv(service_file: Path) -> pd.DataFrame:
    for encoding in ["cp1252", "latin1", "cp949", "utf-8-sig"]:
        try:
            return pd.read_csv(service_file, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("service_csv", b"", 0, 1, f"Unable to decode {service_file}")


def _normalize_text(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype(str).str.strip()
    return df


def _build_primary_area_master(zip_df: pd.DataFrame) -> pd.DataFrame:
    area_counts_df = (
        zip_df.groupby(
            ["POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE", "AREA_CODE", "AREA_NAME", "SHIP_TO", "DEPARTMENT_NAME"]
        )
        .agg(assignment_count=("SVC_ENGINEER_CODE", "size"))
        .reset_index()
        .sort_values(
            ["POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE", "assignment_count", "AREA_NAME"],
            ascending=[True, True, True, False, True],
        )
    )
    primary_area_df = area_counts_df.drop_duplicates(
        subset=["POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"],
        keep="first",
    ).copy()
    return primary_area_df[
        ["SHIP_TO", "DEPARTMENT_NAME", "AREA_CODE", "AREA_NAME", "POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"]
    ]


def build_updated_profile(
    profile_file: Path = PROFILE_FILE,
    service_file: Path = SERVICE_FILE,
    input_dir: Path = INPUT_DIR,
    output_dir: Path = OUTPUT_DIR,
) -> ProfileSyncResult:
    zip_df = pd.read_excel(profile_file, sheet_name="1. Zip Coverage")
    slot_df = pd.read_excel(profile_file, sheet_name="2. Slot")
    service_df = _read_service_csv(service_file)

    zip_df = _normalize_text(
        zip_df,
        ["SHIP_TO", "DEPARTMENT_NAME", "SVC_ENGINEER_CODE", "AREA_CODE", "AREA_NAME", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"],
    )
    slot_df = _normalize_text(slot_df, ["Ship To Code", "SVC_ENGINEER_CODE", "Name", "STRATEGIC_CITY_NAME"])
    service_df = _normalize_text(
        service_df,
        ["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE", "POSTAL_CODE"],
    )

    zip_df["POSTAL_CODE"] = zip_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    service_df["POSTAL_CODE"] = service_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)

    active_service_df = service_df[
        service_df["SVC_CENTER_TYPE"].isin(ACTIVE_CENTER_TYPES)
        & service_df["SVC_ENGINEER_CODE"].ne("")
        & service_df["POSTAL_CODE"].ne("")
    ].copy()

    area_master_df = _build_primary_area_master(zip_df)

    service_sm_postal_df = active_service_df[
        ["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"]
    ].drop_duplicates()

    updated_zip_df = area_master_df.merge(
        service_sm_postal_df,
        on=["POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"],
        how="inner",
    )
    updated_zip_df = updated_zip_df[
        ["SHIP_TO", "DEPARTMENT_NAME", "SVC_ENGINEER_CODE", "AREA_CODE", "AREA_NAME", "POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"]
    ].drop_duplicates()
    updated_zip_df = updated_zip_df.sort_values(
        ["STRATEGIC_CITY_NAME", "AREA_NAME", "POSTAL_CODE", "SVC_ENGINEER_CODE"]
    ).reset_index(drop=True)

    active_sm_df = active_service_df[
        ["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE"]
    ].drop_duplicates()
    active_sm_df = active_sm_df.sort_values(
        ["SVC_ENGINEER_CODE", "STRATEGIC_CITY_NAME", "SVC_ENGINEER_NAME", "SVC_CENTER_TYPE"]
    ).drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")

    slot_active_df = slot_df[slot_df["SVC_ENGINEER_CODE"].isin(set(active_sm_df["SVC_ENGINEER_CODE"]))].copy()
    missing_slot_sm_df = active_sm_df[~active_sm_df["SVC_ENGINEER_CODE"].isin(set(slot_df["SVC_ENGINEER_CODE"]))].copy()
    missing_slot_sm_df = missing_slot_sm_df.rename(columns={"SVC_ENGINEER_NAME": "Name"})
    missing_slot_sm_df["Ship To Code"] = ""
    missing_slot_sm_df["Slot"] = DEFAULT_SLOT
    missing_slot_sm_df = missing_slot_sm_df[["Ship To Code", "SVC_ENGINEER_CODE", "Name", "Slot", "STRATEGIC_CITY_NAME"]]

    updated_slot_df = pd.concat(
        [slot_active_df[["Ship To Code", "SVC_ENGINEER_CODE", "Name", "Slot", "STRATEGIC_CITY_NAME"]], missing_slot_sm_df],
        ignore_index=True,
    )
    updated_slot_df = updated_slot_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
    updated_slot_df = updated_slot_df.sort_values(["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE"]).reset_index(drop=True)

    matched_service_sms = set(updated_zip_df["SVC_ENGINEER_CODE"])
    unmatched_service_sm_df = active_sm_df[~active_sm_df["SVC_ENGINEER_CODE"].isin(matched_service_sms)].copy()
    unmatched_service_sm_df = unmatched_service_sm_df.sort_values(["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE"]).reset_index(drop=True)

    summary_rows = [
        {"metric": "active_service_sm_count", "value": int(active_sm_df["SVC_ENGINEER_CODE"].nunique())},
        {"metric": "updated_zip_row_count", "value": int(len(updated_zip_df))},
        {"metric": "updated_zip_sm_count", "value": int(updated_zip_df["SVC_ENGINEER_CODE"].nunique())},
        {"metric": "primary_area_master_postal_count", "value": int(area_master_df["POSTAL_CODE"].nunique())},
        {"metric": "updated_slot_row_count", "value": int(len(updated_slot_df))},
        {"metric": "added_slot_default_7_count", "value": int(len(missing_slot_sm_df))},
        {"metric": "removed_zip_engineer_count", "value": int(zip_df[~zip_df["SVC_ENGINEER_CODE"].isin(set(active_sm_df["SVC_ENGINEER_CODE"]))]["SVC_ENGINEER_CODE"].nunique())},
        {"metric": "removed_slot_engineer_count", "value": int(slot_df[~slot_df["SVC_ENGINEER_CODE"].isin(set(active_sm_df["SVC_ENGINEER_CODE"]))]["SVC_ENGINEER_CODE"].nunique())},
        {"metric": "unmatched_service_sm_count", "value": int(len(unmatched_service_sm_df))},
    ]
    summary_df = pd.DataFrame(summary_rows)

    base_name = service_file.stem
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_output_path = input_dir / f"Zip_Coverage_updated_{base_name}.csv"
    slot_output_path = input_dir / f"Slot_updated_{base_name}.csv"
    unmatched_output_path = output_dir / f"unmatched_service_sm_{base_name}.csv"
    summary_output_path = output_dir / f"profile_sync_summary_{base_name}.csv"

    updated_zip_df.to_csv(zip_output_path, index=False, encoding="utf-8-sig")
    updated_slot_df.to_csv(slot_output_path, index=False, encoding="utf-8-sig")
    unmatched_service_sm_df.to_csv(unmatched_output_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_output_path, index=False, encoding="utf-8-sig")

    return ProfileSyncResult(
        updated_zip_df=updated_zip_df,
        updated_slot_df=updated_slot_df,
        unmatched_service_sm_df=unmatched_service_sm_df,
        summary_df=summary_df,
        zip_output_path=zip_output_path,
        slot_output_path=slot_output_path,
        unmatched_output_path=unmatched_output_path,
        summary_output_path=summary_output_path,
    )
