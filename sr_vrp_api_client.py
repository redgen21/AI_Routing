from __future__ import annotations

import colorsys
import json
import uuid
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster

import smart_routing.live_atlanta_runtime as live_runtime
from smart_routing.area_map import load_city_map_data
from smart_routing.live_atlanta_runtime import _load_config as _load_runtime_config
from smart_routing.live_atlanta_runtime import _merge_service_geocodes
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient
from smart_routing.vrp_api_client import (
    build_payload_from_service_frame,
    get_routing_job_result,
    get_routing_job_status,
    submit_routing_job,
)


st.set_page_config(page_title="Smart Routing API Client", layout="wide")

ROUTING_MODE = "na_general"
DEFAULT_SERVER_URL = "http://20.51.244.68:8055"
NETWORK_URL = "http://10.233.84.33:8503"
CONFIG_PATH = Path("config.json")
INPUT_STORE_PATH = Path("data/atlanta_input_store.parquet")
MASTER_PATH = Path("data/All_In_One_Master.xlsx")
PROFILE_PATH = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
DEFAULT_STRATEGIC_CITY = "Atlanta, GA"
DEFAULT_STATE = "GA"
DEFAULT_COUNTRY = "USA"
INPUT_REQUIRED_COLUMNS = [
    "SVC_ENGINEER_CODE",
    "SVC_ENGINEER_NAME",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "RECEIPT_DETAIL_SYMPTOM_CODE",
    "GSFS_RECEIPT_NO",
    "PROMISE_DATE",
    "CITY_NAME",
    "POSTAL_CODE",
    "ADDRESS_LINE1_INFO",
]
STORE_COLUMNS = [
    "record_id",
    "input_source",
    "SVC_ENGINEER_CODE",
    "SVC_ENGINEER_NAME",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "RECEIPT_DETAIL_SYMPTOM_CODE",
    "GSFS_RECEIPT_NO",
    "PROMISE_DATE",
    "CITY_NAME",
    "POSTAL_CODE",
    "ADDRESS_LINE1_INFO",
    "fixed",
    "STRATEGIC_CITY_NAME",
    "STATE_NAME",
    "COUNTRY_NAME",
    "latitude",
    "longitude",
    "matched_address",
    "match_indicator",
    "match_type",
    "source",
    "created_at",
    "updated_at",
]


def _coerce_bool_value(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "y", "yes", "t"}:
        return True
    if text in {"false", "0", "n", "no", "f", ""}:
        return False
    return bool(text)


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    return series.map(_coerce_bool_value).astype(bool)


def _empty_store_df() -> pd.DataFrame:
    return pd.DataFrame(columns=STORE_COLUMNS)


def _load_input_store() -> pd.DataFrame:
    if not INPUT_STORE_PATH.exists():
        return _empty_store_df()
    df = pd.read_parquet(INPUT_STORE_PATH)
    for col in STORE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df["fixed"] = _coerce_bool_series(df["fixed"])
    return df[STORE_COLUMNS].copy()


def _save_input_store(df: pd.DataFrame) -> None:
    INPUT_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_df = df.copy()
    for col in STORE_COLUMNS:
        if col not in save_df.columns:
            save_df[col] = pd.NA
    save_df["fixed"] = _coerce_bool_series(save_df["fixed"])
    save_df = save_df[STORE_COLUMNS].copy()
    save_df.to_parquet(INPUT_STORE_PATH, index=False)


@st.cache_data(show_spinner=False)
def _load_master_df(master_path: str) -> pd.DataFrame:
    df = pd.read_excel(master_path)
    required_cols = [
        "Product Group Name",
        "Product Group Code",
        "Product Name",
        "Product Code",
        "Symptom Name",
        "Symptom Code",
        "Symtom Type Name",
        "Symtom Type Code",
        "Detailed Symptom Name",
        "Detailed Symptom Code",
    ]
    df = df[required_cols].dropna(subset=["Product Group Code", "Product Code", "Detailed Symptom Code"]).copy()
    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()
    return df.drop_duplicates().reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _load_engineer_options(profile_path: str) -> pd.DataFrame:
    df = pd.read_excel(profile_path, sheet_name="2. Slot")
    required_cols = ["STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE", "SVC_ENGINEER_CODE", "Name"]
    df = df[required_cols].copy()
    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()
    df["SVC_CENTER_TYPE"] = df["SVC_CENTER_TYPE"].str.upper()
    df = df[
        df["STRATEGIC_CITY_NAME"].eq(DEFAULT_STRATEGIC_CITY)
        & df["SVC_CENTER_TYPE"].eq("DMS")
        & df["SVC_ENGINEER_CODE"].ne("")
    ].copy()
    df = df.drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
    return df.rename(columns={"Name": "SVC_ENGINEER_NAME"}).sort_values(["SVC_ENGINEER_NAME", "SVC_ENGINEER_CODE"]).reset_index(drop=True)


def _normalize_promise_date(value: str) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    return digits if len(digits) == 8 else ""


def _prepare_input_df(raw_df: pd.DataFrame, input_source: str, existing_df: pd.DataFrame, *, allow_existing_receipt: str = "") -> tuple[pd.DataFrame, list[str]]:
    working = raw_df.copy()
    missing = [col for col in INPUT_REQUIRED_COLUMNS if col not in working.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    working = working[INPUT_REQUIRED_COLUMNS].copy()
    fixed_series = _coerce_bool_series(
        raw_df["fixed"] if "fixed" in raw_df.columns else pd.Series(False, index=raw_df.index)
    )
    for col in INPUT_REQUIRED_COLUMNS:
        working[col] = working[col].astype(str).str.strip()
        working[col] = working[col].replace(
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
    working["PROMISE_DATE"] = working["PROMISE_DATE"].map(_normalize_promise_date)
    if working["PROMISE_DATE"].eq("").any():
        raise ValueError("PROMISE_DATE must be in YYYYMMDD format.")
    working["POSTAL_CODE"] = working["POSTAL_CODE"].str.replace(r"\.0+$", "", regex=True).str.zfill(5)
    working["fixed"] = fixed_series.loc[working.index].astype(bool)
    working["STRATEGIC_CITY_NAME"] = DEFAULT_STRATEGIC_CITY
    working["STATE_NAME"] = DEFAULT_STATE
    working["COUNTRY_NAME"] = DEFAULT_COUNTRY

    duplicate_alerts: list[str] = []
    dup_in_input = working["GSFS_RECEIPT_NO"].duplicated(keep=False)
    if dup_in_input.any():
        duplicate_values = sorted(working.loc[dup_in_input, "GSFS_RECEIPT_NO"].astype(str).unique().tolist())
        raise ValueError(f"Duplicate GSFS_RECEIPT_NO in input: {', '.join(duplicate_values)}")

    existing_receipts = existing_df["GSFS_RECEIPT_NO"].astype(str).tolist() if not existing_df.empty else []
    if allow_existing_receipt:
        existing_receipts = [value for value in existing_receipts if value != allow_existing_receipt]
    duplicate_mask = working["GSFS_RECEIPT_NO"].astype(str).isin(existing_receipts)
    if duplicate_mask.any():
        duplicate_values = sorted(working.loc[duplicate_mask, "GSFS_RECEIPT_NO"].astype(str).unique().tolist())
        duplicate_alerts = duplicate_values
        working = working.loc[~duplicate_mask].copy()

    if working.empty:
        return pd.DataFrame(columns=INPUT_REQUIRED_COLUMNS + ["STRATEGIC_CITY_NAME", "STATE_NAME", "COUNTRY_NAME"]), duplicate_alerts

    timestamp = pd.Timestamp.now().isoformat()
    working["record_id"] = [uuid.uuid4().hex for _ in range(len(working))]
    working["input_source"] = input_source
    working["created_at"] = timestamp
    working["updated_at"] = timestamp
    return working, duplicate_alerts


def _geocode_input_df(input_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if input_df.empty:
        return _empty_store_df(), pd.DataFrame()
    config = _load_runtime_config()
    geocoded_df = _merge_service_geocodes(input_df.copy(), config)
    geocoded_df["latitude"] = pd.to_numeric(geocoded_df.get("latitude"), errors="coerce")
    geocoded_df["longitude"] = pd.to_numeric(geocoded_df.get("longitude"), errors="coerce")
    failed_df = geocoded_df[geocoded_df["latitude"].isna() | geocoded_df["longitude"].isna()].copy()
    success_df = geocoded_df[geocoded_df["latitude"].notna() & geocoded_df["longitude"].notna()].copy()
    for col in STORE_COLUMNS:
        if col not in success_df.columns:
            success_df[col] = pd.NA
    return success_df[STORE_COLUMNS].copy(), failed_df


def _build_store_display_df(store_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return store_df.copy()
    product_lookup = master_df[
        [
            "Product Group Code",
            "Product Group Name",
            "Product Code",
            "Product Name",
        ]
    ].drop_duplicates()
    detail_lookup = master_df[
        [
            "Product Group Code",
            "Product Group Name",
            "Product Code",
            "Product Name",
            "Detailed Symptom Code",
            "Detailed Symptom Name",
            "Symptom Name",
            "Symtom Type Name",
        ]
    ].drop_duplicates()
    merged = store_df.merge(
        product_lookup,
        left_on=["SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE"],
        right_on=["Product Group Code", "Product Code"],
        how="left",
    )
    merged = merged.drop(columns=["Product Group Code", "Product Code"], errors="ignore")
    detail_merged = store_df.merge(
        detail_lookup,
        left_on=["SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "RECEIPT_DETAIL_SYMPTOM_CODE"],
        right_on=["Product Group Code", "Product Code", "Detailed Symptom Code"],
        how="left",
        suffixes=("", "_detail"),
    )
    for col in ["Detailed Symptom Name", "Symptom Name", "Symtom Type Name"]:
        if col in detail_merged.columns:
            merged[col] = detail_merged[col]
    return merged


def _build_service_frame_for_payload(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return pd.DataFrame()
    payload_df = store_df.copy()
    payload_df["SVC_CENTER_TYPE"] = "DMS"
    payload_df["fixed"] = _coerce_bool_series(
        payload_df["fixed"] if "fixed" in payload_df.columns else pd.Series(False, index=payload_df.index)
    )
    return payload_df


def _build_runtime_from_saved_inputs(store_df: pd.DataFrame) -> live_runtime.RuntimeAtlantaPrepResult:
    normalized_raw_df = live_runtime._normalize_service_columns(store_df)
    service_df = live_runtime._prepare_service_df_for_atlanta(normalized_raw_df)

    region_zip_path = live_runtime.DEFAULT_REGION_ZIP_PATH if live_runtime.DEFAULT_REGION_ZIP_PATH.exists() else live_runtime.FALLBACK_REGION_ZIP_PATH
    region_zip_df = pd.read_csv(region_zip_path, encoding="utf-8-sig")
    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    engineer_region_df = pd.read_csv(live_runtime.DEFAULT_ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_geocode_df = pd.read_csv(live_runtime.DEFAULT_HOME_GEOCODE_PATH, encoding="utf-8-sig")
    if live_runtime.DEFAULT_HEAVY_REPAIR_LOOKUP_PATH.exists():
        heavy_lookup_df = pd.read_csv(live_runtime.DEFAULT_HEAVY_REPAIR_LOOKUP_PATH, encoding="utf-8-sig")
    else:
        heavy_lookup_df = live_runtime.prod._build_heavy_repair_lookup(live_runtime.DEFAULT_SYMPTOM_FILE)

    service_enriched_df = live_runtime.prod._enrich_service_df(service_df, heavy_lookup_df)
    service_enriched_df["service_date_key"] = service_enriched_df["service_date"].dt.strftime("%Y-%m-%d")
    service_enriched_df = service_enriched_df.merge(
        region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates(),
        on="POSTAL_CODE",
        how="left",
    )
    service_enriched_df = service_enriched_df[service_enriched_df["region_seq"].notna()].copy()
    service_enriched_df["region_seq"] = pd.to_numeric(service_enriched_df["region_seq"], errors="coerce").astype(int)

    engineer_region_df["SVC_CENTER_TYPE"] = engineer_region_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    engineer_region_df = engineer_region_df[engineer_region_df["SVC_CENTER_TYPE"] == live_runtime.prod.DMS_CENTER_TYPE].copy()
    engineer_name_col = "SVC_ENGINEER_NAME" if "SVC_ENGINEER_NAME" in engineer_region_df.columns else "Name"
    engineer_name_lookup = (
        engineer_region_df[["SVC_ENGINEER_CODE", engineer_name_col]]
        .dropna(subset=["SVC_ENGINEER_CODE"])
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first")
        .rename(columns={engineer_name_col: "lookup_engineer_name"})
    )
    home_geocode_df = home_geocode_df.merge(engineer_name_lookup, on="SVC_ENGINEER_CODE", how="left")
    if "SVC_ENGINEER_NAME" not in home_geocode_df.columns:
        home_geocode_df["SVC_ENGINEER_NAME"] = home_geocode_df["lookup_engineer_name"]
    else:
        missing_name_mask = home_geocode_df["SVC_ENGINEER_NAME"].astype(str).str.strip().eq("")
        home_geocode_df.loc[missing_name_mask, "SVC_ENGINEER_NAME"] = home_geocode_df.loc[missing_name_mask, "lookup_engineer_name"]
    home_geocode_df = home_geocode_df.drop(columns=["lookup_engineer_name"], errors="ignore")

    return live_runtime.RuntimeAtlantaPrepResult(
        queried_service_df=store_df.copy(),
        geocoded_service_df=normalized_raw_df.copy(),
        region_zip_df=region_zip_df,
        engineer_region_df=engineer_region_df,
        home_geocode_df=home_geocode_df,
        service_filtered_df=service_df,
        service_enriched_df=service_enriched_df,
    )


def _read_uploaded_service_csv(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    return pd.read_csv(
        uploaded_file,
        encoding="utf-8-sig",
        keep_default_na=False,
        sep=None,
        engine="python",
    )


def _master_row_from_codes(master_df: pd.DataFrame, group_code: str, product_code: str, detail_code: str) -> pd.Series | None:
    matched = master_df[
        master_df["Product Group Code"].astype(str).eq(str(group_code))
        & master_df["Product Code"].astype(str).eq(str(product_code))
        & master_df["Detailed Symptom Code"].astype(str).eq(str(detail_code))
    ]
    if matched.empty:
        return None
    return matched.iloc[0]


@st.dialog("Direct Input", width="large")
def _direct_input_dialog(master_df: pd.DataFrame, store_df: pd.DataFrame, edit_record_id: str = "") -> None:
    engineer_df = _load_engineer_options(str(PROFILE_PATH))
    if engineer_df.empty:
        st.error("No Atlanta DMS engineers found in 2. Slot profile.")
        return
    edit_record = None
    if edit_record_id and edit_record_id != "__new__":
        matched = store_df[store_df["record_id"].astype(str) == str(edit_record_id)]
        if not matched.empty:
            edit_record = matched.iloc[0]

    default_row = None
    if edit_record is not None:
        default_row = _master_row_from_codes(
            master_df,
            str(edit_record.get("SERVICE_PRODUCT_GROUP_CODE", "")),
            str(edit_record.get("SERVICE_PRODUCT_CODE", "")),
            str(edit_record.get("RECEIPT_DETAIL_SYMPTOM_CODE", "")),
        )

    default_receipt = str(edit_record.get("GSFS_RECEIPT_NO", "")) if edit_record is not None else ""
    default_promise = str(edit_record.get("PROMISE_DATE", "")) if edit_record is not None else ""
    default_city = str(edit_record.get("CITY_NAME", "")) if edit_record is not None else ""
    default_postal = str(edit_record.get("POSTAL_CODE", "")) if edit_record is not None else ""
    default_address = str(edit_record.get("ADDRESS_LINE1_INFO", "")) if edit_record is not None else ""
    default_fixed = _coerce_bool_value(edit_record.get("fixed", False)) if edit_record is not None else False
    default_promise_date = pd.to_datetime(default_promise, format="%Y%m%d", errors="coerce")
    promise_date_value = st.date_input(
        "PROMISE_DATE",
        value=default_promise_date.date() if pd.notna(default_promise_date) else None,
        format="YYYY/MM/DD",
    )
    receipt_no = st.text_input("GSFS_RECEIPT_NO", value=default_receipt)
    engineer_options = engineer_df.assign(engineer_label=engineer_df["SVC_ENGINEER_NAME"] + " (" + engineer_df["SVC_ENGINEER_CODE"] + ")")
    engineer_labels = engineer_options["engineer_label"].tolist()
    default_engineer_label = engineer_labels[0] if engineer_labels else ""
    if edit_record is not None and not engineer_options.empty:
        matched_engineer = engineer_options[engineer_options["SVC_ENGINEER_CODE"].astype(str) == str(edit_record.get("SVC_ENGINEER_CODE", ""))]
        if not matched_engineer.empty:
            default_engineer_label = str(matched_engineer.iloc[0]["engineer_label"])
    selected_engineer_label = st.selectbox(
        "SVC_ENGINEER_NAME",
        engineer_labels,
        index=engineer_labels.index(default_engineer_label) if default_engineer_label in engineer_labels else 0,
    )
    selected_engineer_row = engineer_options[engineer_options["engineer_label"].astype(str) == str(selected_engineer_label)].head(1)
    if selected_engineer_row.empty:
        st.error("Unable to resolve selected engineer.")
        return
    selected_engineer_row = selected_engineer_row.iloc[0]

    group_names = sorted(master_df["Product Group Name"].dropna().astype(str).unique().tolist())
    default_group_name = str(default_row["Product Group Name"]) if default_row is not None else (group_names[0] if group_names else "")
    selected_group_name = st.selectbox("Product Group Name", group_names, index=group_names.index(default_group_name) if default_group_name in group_names else 0)
    group_df = master_df[master_df["Product Group Name"].astype(str) == str(selected_group_name)].copy()

    product_names = sorted(group_df["Product Name"].dropna().astype(str).unique().tolist())
    default_product_name = str(default_row["Product Name"]) if default_row is not None and str(default_row["Product Group Name"]) == str(selected_group_name) else (product_names[0] if product_names else "")
    selected_product_name = st.selectbox("Product Name", product_names, index=product_names.index(default_product_name) if default_product_name in product_names else 0)
    product_df = group_df[group_df["Product Name"].astype(str) == str(selected_product_name)].copy()

    symptom_names = ["None"] + sorted(product_df["Symptom Name"].dropna().astype(str).unique().tolist())
    default_symptom_name = (
        str(default_row["Symptom Name"])
        if default_row is not None and str(default_row["Product Name"]) == str(selected_product_name)
        else "None"
    )
    selected_symptom_name = st.selectbox(
        "Symptom Name",
        symptom_names,
        index=symptom_names.index(default_symptom_name) if default_symptom_name in symptom_names else 0,
    )

    selected_detail_row = None
    if selected_symptom_name == "None":
        selected_symptom_type_name = st.selectbox(
            "Symtom Type Name",
            ["None"],
            index=0,
        )
        selected_detailed_name = st.selectbox(
            "Detailed Symptom Name",
            ["None"],
            index=0,
        )
    else:
        symptom_df = product_df[product_df["Symptom Name"].astype(str) == str(selected_symptom_name)].copy()
        symptom_type_names = ["None"] + sorted(symptom_df["Symtom Type Name"].dropna().astype(str).unique().tolist())
        default_symptom_type_name = (
            str(default_row["Symtom Type Name"])
            if default_row is not None and str(default_row["Symptom Name"]) == str(selected_symptom_name)
            else "None"
        )
        selected_symptom_type_name = st.selectbox(
            "Symtom Type Name",
            symptom_type_names,
            index=symptom_type_names.index(default_symptom_type_name) if default_symptom_type_name in symptom_type_names else 0,
        )
        if selected_symptom_type_name == "None":
            selected_detailed_name = st.selectbox(
                "Detailed Symptom Name",
                ["None"],
                index=0,
            )
        else:
            symptom_type_df = symptom_df[symptom_df["Symtom Type Name"].astype(str) == str(selected_symptom_type_name)].copy()
            detailed_names = ["None"] + sorted(symptom_type_df["Detailed Symptom Name"].dropna().astype(str).unique().tolist())
            default_detailed_name = (
                str(default_row["Detailed Symptom Name"])
                if default_row is not None and str(default_row["Symtom Type Name"]) == str(selected_symptom_type_name)
                else "None"
            )
            selected_detailed_name = st.selectbox(
                "Detailed Symptom Name",
                detailed_names,
                index=detailed_names.index(default_detailed_name) if default_detailed_name in detailed_names else 0,
            )
            if selected_detailed_name != "None":
                selected_detail_row = symptom_type_df[
                    symptom_type_df["Detailed Symptom Name"].astype(str) == str(selected_detailed_name)
                ].head(1)
                if selected_detail_row.empty:
                    st.error("Unable to resolve selected product/symptom combination.")
                    return
                selected_detail_row = selected_detail_row.iloc[0]
    city_name = st.text_input("CITY_NAME", value=default_city)
    postal_code = st.text_input("POSTAL_CODE", value=default_postal)
    address_line1 = st.text_input("ADDRESS_LINE1_INFO", value=default_address)
    fixed = st.checkbox("Fixed Assignment", value=default_fixed)

    save_col, cancel_col = st.columns(2)
    if save_col.button("Save", type="primary", width="stretch"):
        candidate_df = pd.DataFrame(
            [
                {
                    "SVC_ENGINEER_CODE": str(selected_engineer_row["SVC_ENGINEER_CODE"]).strip(),
                    "SVC_ENGINEER_NAME": str(selected_engineer_row["SVC_ENGINEER_NAME"]).strip(),
                    "SERVICE_PRODUCT_GROUP_CODE": str(group_df["Product Group Code"].iloc[0]).strip(),
                    "SERVICE_PRODUCT_CODE": str(selected_detail_row["Product Code"]).strip() if selected_detail_row is not None else str(product_df["Product Code"].iloc[0]).strip(),
                    "RECEIPT_DETAIL_SYMPTOM_CODE": str(selected_detail_row["Detailed Symptom Code"]).strip() if selected_detail_row is not None else "",
                    "GSFS_RECEIPT_NO": str(receipt_no).strip(),
                    "PROMISE_DATE": pd.Timestamp(promise_date_value).strftime("%Y%m%d"),
                    "CITY_NAME": str(city_name).strip(),
                    "POSTAL_CODE": str(postal_code).strip(),
                    "ADDRESS_LINE1_INFO": str(address_line1).strip(),
                    "fixed": bool(fixed),
                }
            ]
        )
        try:
            prepared_df, duplicate_receipts = _prepare_input_df(
                candidate_df,
                "direct_input",
                store_df,
                allow_existing_receipt=str(edit_record.get("GSFS_RECEIPT_NO", "")) if edit_record is not None else "",
            )
            if duplicate_receipts:
                st.error(f"Duplicate GSFS_RECEIPT_NO already exists: {', '.join(duplicate_receipts)}")
                return
            if prepared_df.empty:
                st.error("No valid input row to save.")
                return
            if edit_record is not None:
                prepared_df.loc[:, "record_id"] = str(edit_record["record_id"])
                prepared_df.loc[:, "created_at"] = str(edit_record.get("created_at", pd.Timestamp.now().isoformat()))
            geocoded_df, failed_df = _geocode_input_df(prepared_df)
            if not failed_df.empty:
                st.error("Address error. Google geocoding could not resolve the address.")
                st.dataframe(failed_df[["GSFS_RECEIPT_NO", "ADDRESS_LINE1_INFO", "CITY_NAME", "POSTAL_CODE"]], width="stretch", hide_index=True)
                return
            next_store_df = store_df.copy()
            if edit_record is not None:
                next_store_df = next_store_df[next_store_df["record_id"].astype(str) != str(edit_record["record_id"])].copy()
            next_store_df = pd.concat([next_store_df, geocoded_df], ignore_index=True)
            _save_input_store(next_store_df)
            st.session_state["input_store_refresh_nonce"] = uuid.uuid4().hex
            st.session_state["input_dialog_record_id"] = None
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    if cancel_col.button("Close", width="stretch"):
        st.session_state["input_dialog_record_id"] = None
        st.rerun()


@st.cache_data(show_spinner=False)
def _load_client_config(config_path: str = str(CONFIG_PATH)) -> dict:
    path = Path(config_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


@st.cache_resource(show_spinner=False)
def get_route_client() -> OSRMTripClient:
    routing_cfg = _load_client_config().get("routing", {})
    city_osrm_urls = routing_cfg.get("city_osrm_urls", {}) if isinstance(routing_cfg.get("city_osrm_urls", {}), dict) else {}
    osrm_url = str(city_osrm_urls.get(DEFAULT_STRATEGIC_CITY, "http://20.51.244.68:5002")).strip().rstrip("/")
    osrm_profile = str(routing_cfg.get("osrm_profile", "driving")).strip() or "driving"
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=osrm_url,
            mode="osrm",
            osrm_profile=osrm_profile,
            cache_file=Path("data/cache/osrm_trip_cache_atlanta_vrp_api_client.csv"),
            fallback_osrm_url=None,
        )
    )


def _popup(content: str, width: int = 360) -> folium.Popup:
    wrapped = (
        f"<div style='min-width:{width}px;max-width:{width}px;white-space:normal;"
        "line-height:1.4;font-size:13px;'>"
        f"{content}</div>"
    )
    return folium.Popup(wrapped, max_width=width + 40)


def _render_folium_map(map_obj: folium.Map, height: int = 760) -> None:
    st.iframe(map_obj.get_root().render(), height=height)


def _generate_color_map(labels: list[str]) -> dict[str, str]:
    color_map: dict[str, str] = {}
    hue = 0.11
    golden_ratio = 0.618033988749895
    for label in sorted({str(v).strip() for v in labels if str(v).strip()}):
        hue = (hue + golden_ratio) % 1.0
        rgb = colorsys.hsv_to_rgb(hue, 0.68, 0.92)
        color_map[label] = "#{:02x}{:02x}{:02x}".format(int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return color_map


def _region_color_map() -> dict[str, str]:
    return {
        "Atlanta New Region 1": "#db4437",
        "Atlanta New Region 2": "#0f9d58",
        "Atlanta New Region 3": "#4285f4",
    }


def _build_region_layers(region_zip_df: pd.DataFrame, service_df: pd.DataFrame):
    city_data = load_city_map_data(DEFAULT_STRATEGIC_CITY)
    zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_df = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates().copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.zfill(5)
    merged = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")
    if service_df.empty or "POSTAL_CODE" not in service_df.columns:
        postal_counts = pd.Series(dtype=int)
    else:
        postal_counts = service_df["POSTAL_CODE"].astype(str).str.zfill(5).value_counts()
    merged["service_count"] = merged["POSTAL_CODE"].map(postal_counts).fillna(0).astype(int)
    region_layer = (
        merged.dropna(subset=["new_region_name"])
        .dissolve(by="new_region_name", as_index=False, aggfunc="first")[["new_region_name", "region_seq", "geometry"]]
        .sort_values("region_seq")
        .reset_index(drop=True)
    )
    return merged, region_layer


def _build_route_groups(schedule_df: pd.DataFrame):
    route_groups: list[dict] = []
    if schedule_df.empty:
        return route_groups
    for engineer_code, group in schedule_df.groupby("assigned_sm_code", dropna=True):
        group = group.sort_values("visit_seq").reset_index(drop=True)
        start_coord = None
        return_to_home = _coerce_bool_value(group.iloc[0].get("return_to_home", False))
        if pd.notna(group.iloc[0].get("home_start_longitude")) and pd.notna(group.iloc[0].get("home_start_latitude")):
            start_coord = (float(group.iloc[0]["home_start_longitude"]), float(group.iloc[0]["home_start_latitude"]))
        stop_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in group.iterrows()]
        coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
        if return_to_home and start_coord is not None and stop_coords:
            coord_chain = coord_chain + [start_coord]
        route_payload = get_route_client().build_ordered_route(tuple(coord_chain), preserve_first=start_coord is not None)
        route_groups.append(
            {
                "engineer_code": str(engineer_code),
                "engineer_name": str(group["assigned_sm_name"].iloc[0]),
                "center_type": str(group.get("assigned_center_type", pd.Series([""])).iloc[0]).strip().upper()
                if "assigned_center_type" in group.columns
                else "",
                "route_payload": route_payload,
                "scheduled_rows": group.to_dict("records"),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "home_coord": start_coord,
            }
        )
    return route_groups


def _build_region_staffing_view(service_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"}
    if service_df.empty or not required_cols.issubset(service_df.columns):
        return pd.DataFrame(columns=["region", "dms_count", "dms2_count", "dms_service_count", "dms2_service_count", "service_count"])
    staffing_df = service_df[["new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"]].dropna(
        subset=["new_region_name", "assigned_sm_code"]
    ).copy()
    staffing_df["assigned_center_type"] = staffing_df["assigned_center_type"].astype(str).str.upper()
    rows: list[dict[str, object]] = []
    for region_name, group in staffing_df.groupby("new_region_name", dropna=False):
        rows.append(
            {
                "region": str(region_name),
                "dms_count": int(group.loc[group["assigned_center_type"] == "DMS", "assigned_sm_code"].astype(str).nunique()),
                "dms2_count": int(group.loc[group["assigned_center_type"] == "DMS2", "assigned_sm_code"].astype(str).nunique()),
                "dms_service_count": int(group.loc[group["assigned_center_type"] == "DMS", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "dms2_service_count": int(group.loc[group["assigned_center_type"] == "DMS2", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
            }
        )
    return pd.DataFrame(rows).sort_values("region").reset_index(drop=True)


def _build_pre_result_service_view(service_df: pd.DataFrame) -> pd.DataFrame:
    if service_df.empty:
        return pd.DataFrame()
    preview_df = service_df.copy()
    preview_df["assigned_sm_code"] = preview_df.get("SVC_ENGINEER_CODE", pd.Series(index=preview_df.index)).astype(str)
    preview_df["assigned_sm_name"] = preview_df.get("SVC_ENGINEER_NAME", pd.Series(index=preview_df.index)).astype(str)
    preview_df["assigned_center_type"] = preview_df.get("SVC_CENTER_TYPE", pd.Series(index=preview_df.index)).astype(str)
    return preview_df


def _build_preview_route_groups(service_df: pd.DataFrame, home_df: pd.DataFrame):
    if service_df.empty:
        return []
    preview_df = _build_pre_result_service_view(service_df)
    home_lookup = (
        home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .rename(columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"})
    )
    preview_df = preview_df.merge(home_lookup, on="assigned_sm_code", how="left")
    preview_df["visit_seq"] = (
        preview_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False).cumcount() + 1
        if {"service_date_key", "assigned_sm_code"}.issubset(preview_df.columns)
        else range(1, len(preview_df) + 1)
    )
    preview_df["visit_start_time"] = ""
    preview_df["visit_end_time"] = ""
    preview_df["assigned_region_name"] = preview_df.get("new_region_name", pd.Series(index=preview_df.index))
    preview_df["return_to_home"] = bool((st.session_state.get("vrp_payload") or {}).get("options", {}).get("return_to_home", False))
    return _build_route_groups(preview_df)


def build_map(region_name: str, display_service_df: pd.DataFrame, home_df: pd.DataFrame, route_groups: list[dict], region_zip_df: pd.DataFrame):
    zip_layer, region_layer = _build_region_layers(region_zip_df, display_service_df)
    region_colors = _region_color_map()
    engineer_colors = _generate_color_map([group["engineer_code"] for group in route_groups])

    if region_name != "ALL":
        zip_layer = zip_layer[zip_layer["new_region_name"] == region_name].copy()
        region_layer = region_layer[region_layer["new_region_name"] == region_name].copy()
        display_service_df = display_service_df[display_service_df["new_region_name"] == region_name].copy()
        home_df = home_df[home_df["assigned_region_name"] == region_name].copy()

    if not region_layer.empty:
        center_points = region_layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
        center_lat = float(center_points.y.mean())
        center_lon = float(center_points.x.mean())
    else:
        center_lat, center_lon = 33.7490, -84.3880

    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles="CartoDB positron")
    folium.GeoJson(
        data=zip_layer.to_json(),
        name="ZIP Coverage",
        style_function=lambda feature: {
            "color": "#c5c9cf" if int(feature["properties"].get("service_count", 0) or 0) == 0 else "#9aa0a6",
            "weight": 0.5 if int(feature["properties"].get("service_count", 0) or 0) == 0 else 0.8,
            "fillColor": "#eceff3" if int(feature["properties"].get("service_count", 0) or 0) == 0 else region_colors.get(feature["properties"].get("new_region_name", ""), "#dddddd"),
            "fillOpacity": 0.05 if int(feature["properties"].get("service_count", 0) or 0) == 0 else 0.12,
        },
        tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "new_region_name", "service_count"], aliases=["ZIP", "Region", "Service Count"]),
    ).add_to(fmap)
    folium.GeoJson(
        data=region_layer.to_json(),
        name="Production Regions",
        style_function=lambda feature: {
            "color": region_colors.get(feature["properties"].get("new_region_name", ""), "#333333"),
            "weight": 3,
            "fillColor": "none",
            "fillOpacity": 0.0,
        },
        tooltip=folium.GeoJsonTooltip(fields=["new_region_name"], aliases=["Region"]),
    ).add_to(fmap)

    if route_groups:
        route_layer = folium.FeatureGroup(name="Assigned Routes").add_to(fmap)
        for group in route_groups:
            engineer_color = engineer_colors.get(group["engineer_code"], "#111827")
            group_center_type = str(group.get("center_type", "")).upper()
            geometry = group["route_payload"]["geometry"]
            if geometry:
                folium.PolyLine(
                    locations=geometry,
                    color=engineer_color,
                    weight=3,
                    opacity=0.85,
                    popup=_popup(
                        f"<b>Engineer</b>: {group['engineer_name']}<br>"
                        f"<b>Engineer Code</b>: {group['engineer_code']}<br>"
                        f"<b>Service Count</b>: {group['service_count']} | "
                        f"<b>Distance</b>: {group['route_payload']['distance_km']:.2f} km | "
                        f"<b>Duration</b>: {group['route_payload']['duration_min']:.2f} min",
                        width=420,
                    ),
                ).add_to(route_layer)
            if group["home_coord"] is not None:
                home_lon, home_lat = group["home_coord"]
                home_bg = "#111111" if group_center_type == "DMS2" else "#ffffff"
                home_fg = "#ffffff" if group_center_type == "DMS2" else engineer_color
                folium.Marker(
                    location=[home_lat, home_lon],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:10px;font-weight:700;color:{home_fg};"
                            f"background:{home_bg};border:2px solid {engineer_color};border-radius:12px;"
                            "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                        )
                    ),
                    popup=_popup(f"<b>Home Start</b>: {group['engineer_name']}<br><b>Engineer Code</b>: {group['engineer_code']}", width=280),
                ).add_to(route_layer)
            for row in group["scheduled_rows"]:
                seq = int(row.get("visit_seq", 0))
                center_type = str(row.get("assigned_center_type", "")).strip().upper()
                marker_bg = "#111111" if center_type == "DMS2" else "#ffffff"
                marker_fg = "#ffffff" if center_type == "DMS2" else engineer_color
                changed_text = ""
                if "changed" in row:
                    changed_text = f"<b>Changed</b>: {'Y' if bool(row.get('changed', False)) else 'N'}<br>"
                popup_html = (
                    f"<b>Engineer</b>: {row.get('assigned_sm_name', '')}<br>"
                    f"<b>Engineer Code</b>: {row.get('assigned_sm_code', '')} | "
                    f"<b>Center Type</b>: {center_type} | "
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                    f"<b>Seq</b>: {seq}<br>"
                    f"{changed_text}"
                    f"<b>Home Region</b>: {row.get('assigned_region_name', '')}<br>"
                    f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')}<br>"
                    f"<b>Start</b>: {row.get('visit_start_time', '')} | "
                    f"<b>End</b>: {row.get('visit_end_time', '')}"
                )
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:11px;font-weight:700;color:{marker_fg};"
                            f"background:{marker_bg};border:2px solid {engineer_color};border-radius:12px;"
                            "width:22px;height:22px;line-height:18px;text-align:center;\">"
                            f"{seq}</div>"
                        )
                    ),
                    popup=_popup(popup_html, width=460),
                ).add_to(route_layer)
    else:
        point_cluster = MarkerCluster(name="Service Points").add_to(fmap)
        for _, row in display_service_df.iterrows():
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                continue
            folium.CircleMarker(
                location=[float(row["latitude"]), float(row["longitude"])],
                radius=4,
                color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                weight=1,
                fill=True,
                fill_color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                fill_opacity=0.75,
                popup=_popup(
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                    f"<b>Region</b>: {row.get('new_region_name', '')}<br>"
                    f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')}",
                    width=420,
                ),
            ).add_to(point_cluster)

    home_group = folium.FeatureGroup(name="Engineer Homes").add_to(fmap)
    for _, row in home_df.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue
        code = str(row.get("SVC_ENGINEER_CODE", ""))
        border_color = engineer_colors.get(code, "#444444")
        folium.Marker(
            location=[float(row["latitude"]), float(row["longitude"])],
            icon=folium.DivIcon(
                html=(
                    f"<div style=\"font-size:10px;font-weight:700;color:{border_color};"
                    f"background:#fff;border:2px solid {border_color};border-radius:12px;"
                    "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                )
            ),
            popup=_popup(
                f"<b>Engineer</b>: {row.get('Name', '')}<br>"
                f"<b>Engineer Code</b>: {row.get('SVC_ENGINEER_CODE', '')}<br>"
                f"<b>Assigned Region</b>: {row.get('assigned_region_name', '')}",
                width=440,
            ),
        ).add_to(home_group)

    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


def _build_engineer_options(assignment_df: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    if assignment_df.empty:
        return ["ALL"], {}
    engineer_df = assignment_df[["assigned_sm_code", "assigned_sm_name"]].drop_duplicates().copy()
    engineer_df["assigned_sm_code"] = engineer_df["assigned_sm_code"].astype(str).str.strip()
    engineer_df["assigned_sm_name"] = engineer_df["assigned_sm_name"].astype(str).str.strip()
    name_counts = engineer_df["assigned_sm_name"].value_counts()
    labels = ["ALL"]
    label_to_code: dict[str, str] = {}
    for _, row in engineer_df.sort_values(["assigned_sm_name", "assigned_sm_code"]).iterrows():
        code = str(row["assigned_sm_code"])
        name = str(row["assigned_sm_name"])
        label = name if int(name_counts.get(name, 0)) <= 1 else f"{name} ({code})"
        labels.append(label)
        label_to_code[label] = code
    return labels, label_to_code


def _build_result_frames(result_payload: dict, runtime_state: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    assignments_df = pd.DataFrame(result_payload.get("assignments", []))
    if assignments_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    service_df = runtime_state["service_df"].copy()
    home_df = runtime_state["home_df"].copy()
    engineers_df = runtime_state["engineer_region_df"].copy()
    assignment_cols = ["salesforce_id", "receipt_no", "employee_code", "sequence", "planned_start", "planned_end", "changed"]
    merged = service_df.merge(
        assignments_df[assignment_cols].rename(columns={"receipt_no": "GSFS_RECEIPT_NO"}),
        on="GSFS_RECEIPT_NO",
        how="inner",
    )
    engineer_lookup = engineers_df[["SVC_ENGINEER_CODE", "Name", "SVC_CENTER_TYPE", "assigned_region_seq", "assigned_region_name"]].drop_duplicates(
        subset=["SVC_ENGINEER_CODE"]
    )
    merged = merged.merge(
        engineer_lookup.rename(
            columns={
                "SVC_ENGINEER_CODE": "assigned_sm_code",
                "Name": "assigned_sm_name",
                "SVC_CENTER_TYPE": "assigned_center_type",
                "assigned_region_seq": "assigned_region_seq",
                "assigned_region_name": "assigned_region_name",
            }
        ),
        left_on="employee_code",
        right_on="assigned_sm_code",
        how="left",
    )
    home_lookup = home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"]).rename(
        columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"}
    )
    merged = merged.merge(home_lookup, on="assigned_sm_code", how="left")
    merged["visit_seq"] = pd.to_numeric(merged["sequence"], errors="coerce").fillna(0).astype(int)
    merged["visit_start_time"] = pd.to_datetime(merged["planned_start"], errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["visit_end_time"] = pd.to_datetime(merged["planned_end"], errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["travel_time_from_prev_min"] = pd.NA
    merged["assigned_sm_name"] = merged["assigned_sm_name"].fillna(merged["employee_code"])
    merged["changed"] = merged.get("changed", False).fillna(False)
    merged["return_to_home"] = bool((st.session_state.get("vrp_payload") or {}).get("options", {}).get("return_to_home", False))
    schedule_df = merged.sort_values(["service_date_key", "assigned_sm_code", "visit_seq"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _build_actual_frames(runtime_state: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    service_df = runtime_state["service_df"].copy()
    home_df = runtime_state["home_df"].copy()
    if service_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    actual_df = service_df.copy()
    actual_df["assigned_sm_code"] = actual_df.get("SVC_ENGINEER_CODE", pd.Series(index=actual_df.index)).astype(str)
    actual_df["assigned_sm_name"] = actual_df.get("SVC_ENGINEER_NAME", pd.Series(index=actual_df.index)).astype(str)
    actual_df["assigned_center_type"] = actual_df.get("SVC_CENTER_TYPE", pd.Series(index=actual_df.index)).astype(str)
    home_lookup = home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"]).rename(
        columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"}
    )
    actual_df = actual_df.merge(home_lookup, on="assigned_sm_code", how="left")
    actual_df["visit_seq"] = actual_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False).cumcount() + 1
    actual_df["visit_start_time"] = ""
    actual_df["visit_end_time"] = ""
    actual_df["travel_time_from_prev_min"] = pd.NA
    actual_df["return_to_home"] = bool((st.session_state.get("vrp_payload") or {}).get("options", {}).get("return_to_home", False))
    schedule_df = actual_df.sort_values(["service_date_key", "assigned_sm_code", "visit_seq"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _routing_status_progress(status_value: str) -> tuple[float, str]:
    status = str(status_value or "").strip().lower()
    if status == "queued":
        return 0.2, "Routing request queued."
    if status == "running":
        return 0.6, "Smart Routing is running."
    if status == "completed":
        return 1.0, "Smart Routing completed."
    if status == "failed":
        return 1.0, "Smart Routing failed."
    return 0.0, "Routing request not submitted."


def _reset_vrp_result_view(default_compare_mode: str = "Smart Routing") -> None:
    st.session_state["vrp_compare_mode"] = default_compare_mode
    st.session_state.pop("preview_date", None)
    st.session_state.pop("preview_region", None)
    st.session_state.pop("preview_engineer", None)


@st.fragment(run_every="5s")
def _auto_poll_routing_status() -> None:
    server_url = str(st.session_state.get("smart_routing_server_url", "")).strip()
    job_id = str(st.session_state.get("vrp_job_id", "")).strip()
    current_status_payload = st.session_state.get("vrp_job_status") or {}
    current_status = str(current_status_payload.get("status", "")).strip().lower()
    if not server_url or not job_id or current_status not in {"queued", "running"}:
        return
    try:
        latest_status = get_routing_job_status(server_url, job_id)
        st.session_state["vrp_job_status"] = latest_status
        latest_state = str(latest_status.get("status", "")).strip().lower()
        if latest_state == "completed":
            st.session_state["vrp_job_result"] = get_routing_job_result(server_url, job_id)
            _reset_vrp_result_view()
            st.rerun()
        if latest_state == "failed":
            st.session_state["vrp_job_result"] = None
            st.rerun()
    except Exception:
        return


def _render_input_manager(server_url: str) -> None:
    st.session_state["smart_routing_server_url"] = server_url
    master_df = _load_master_df(str(MASTER_PATH))
    store_df = _load_input_store()
    display_store_df = _build_store_display_df(store_df, master_df)

    input_tab, list_tab = st.tabs(["Input", "Saved List"])

    with input_tab:
        source_mode = st.radio("Input Source", ["Upload CSV", "Direct Input"], horizontal=True)
        if source_mode == "Upload CSV":
            uploaded_file = st.file_uploader("Upload Service CSV", type=["csv"])
            if st.button("Save Uploaded Rows", width="stretch", type="primary"):
                if uploaded_file is None:
                    st.warning("Upload a CSV file first.")
                else:
                    try:
                        raw_df = _read_uploaded_service_csv(uploaded_file)
                        prepared_df, duplicate_receipts = _prepare_input_df(raw_df, "csv_upload", store_df)
                        if prepared_df.empty:
                            if duplicate_receipts:
                                st.warning(f"Skipped duplicates: {', '.join(duplicate_receipts)}")
                            else:
                                st.warning("No new rows to save.")
                        else:
                            geocoded_df, failed_df = _geocode_input_df(prepared_df)
                            if not geocoded_df.empty:
                                next_store_df = geocoded_df.copy() if store_df.empty else pd.concat([store_df, geocoded_df], ignore_index=True)
                                _save_input_store(next_store_df)
                            if duplicate_receipts:
                                st.warning(f"Skipped duplicates: {', '.join(duplicate_receipts)}")
                            if not failed_df.empty:
                                st.error("Address error. Google geocoding could not resolve some uploaded addresses.")
                                st.dataframe(
                                    failed_df[["GSFS_RECEIPT_NO", "ADDRESS_LINE1_INFO", "CITY_NAME", "POSTAL_CODE"]],
                                    width="stretch",
                                    hide_index=True,
                                )
                            if not geocoded_df.empty:
                                st.success(f"Saved {len(geocoded_df)} new rows to parquet.")
                            st.session_state["input_store_refresh_nonce"] = uuid.uuid4().hex
                            if not geocoded_df.empty:
                                st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
        else:
            if st.button("Open Direct Input", width="stretch", type="primary"):
                st.session_state["input_dialog_record_id"] = "__new__"
        dialog_record_id = st.session_state.get("input_dialog_record_id")
        if dialog_record_id is not None:
            _direct_input_dialog(master_df, store_df, str(dialog_record_id))

        available_dates = sorted(store_df["PROMISE_DATE"].dropna().astype(str).unique().tolist()) if not store_df.empty else []
        selected_promise_date = st.selectbox(
            "PROMISE_DATE to Build Payload",
            options=available_dates,
            index=0 if available_dates else None,
            key="payload_promise_date",
            placeholder="Select PROMISE_DATE",
        )
        selected_count = int(store_df.loc[store_df["PROMISE_DATE"].astype(str) == str(selected_promise_date), "GSFS_RECEIPT_NO"].astype(str).nunique()) if selected_promise_date else 0
        st.caption(f"Saved rows for selected PROMISE_DATE: {selected_count}")
        return_to_home = st.checkbox("Return To Home", value=bool(st.session_state.get("vrp_return_to_home", True)), key="vrp_return_to_home")
        if st.button("Build Payload", width="stretch"):
            if not selected_promise_date:
                st.warning("Select PROMISE_DATE first.")
            else:
                service_input_df = store_df[store_df["PROMISE_DATE"].astype(str) == str(selected_promise_date)].copy()
                if service_input_df.empty:
                    st.warning("No saved rows for the selected PROMISE_DATE.")
                else:
                    with st.spinner("Preparing routing payload..."):
                        payload_source_df = _build_service_frame_for_payload(service_input_df)
                        runtime = _build_runtime_from_saved_inputs(payload_source_df)
                        planning_date = (
                            str(runtime.service_enriched_df["service_date_key"].dropna().astype(str).min())
                            if not runtime.service_enriched_df.empty and "service_date_key" in runtime.service_enriched_df.columns
                            else f"{str(selected_promise_date)[:4]}-{str(selected_promise_date)[4:6]}-{str(selected_promise_date)[6:8]}"
                        )
                        payload = build_payload_from_service_frame(
                            runtime.service_enriched_df,
                            runtime.engineer_region_df,
                            runtime.home_geocode_df,
                            planning_date=planning_date,
                            request_id=f"ROUTE-{planning_date}",
                            mode=ROUTING_MODE,
                            return_to_home=bool(return_to_home),
                        )
                        st.session_state["vrp_payload"] = payload
                        st.session_state["vrp_runtime"] = {
                            "input_label": f"Saved input rows for {selected_promise_date}",
                            "rendered_sql": "",
                            "service_df": runtime.service_enriched_df,
                            "region_zip_df": runtime.region_zip_df,
                            "engineer_region_df": runtime.engineer_region_df,
                            "home_df": runtime.home_geocode_df,
                        }
                        st.session_state["vrp_job_id"] = ""
                        st.session_state["vrp_job_submit"] = None
                        st.session_state["vrp_job_status"] = None
                        st.session_state["vrp_job_result"] = None
                    st.success("Payload prepared.")

        payload = st.session_state.get("vrp_payload")
        if payload:
            st.caption(
                f"Prepared payload with {len(payload.get('technicians', []))} technicians and {len(payload.get('jobs', []))} jobs."
            )
            current_job_id = str(st.session_state.get("vrp_job_id", "")).strip()
            if current_job_id:
                st.caption(f"Current Job ID: {current_job_id}")
            with st.expander("Payload Preview", expanded=False):
                st.json(payload)

            request_col, check_col = st.columns(2)
            if request_col.button("Request Routing", width="stretch"):
                with st.spinner("Submitting Smart Routing job..."):
                    response = submit_routing_job(server_url, payload)
                    st.session_state["vrp_job_submit"] = response
                    st.session_state["vrp_job_id"] = response.get("job_id", "")
                    st.session_state["vrp_job_status"] = {
                        "job_id": response.get("job_id", ""),
                        "status": str(response.get("status", "queued")).strip().lower(),
                    }
                    st.session_state["vrp_job_result"] = None
                    _reset_vrp_result_view()
                st.success(f"Submitted job {st.session_state.get('vrp_job_id', '')}")
            if check_col.button("Check Routing Result", width="stretch"):
                job_id = str(st.session_state.get("vrp_job_id", "")).strip()
                if not job_id:
                    st.warning("Submit a job first.")
                else:
                    with st.spinner("Fetching job status..."):
                        latest_status = get_routing_job_status(server_url, job_id)
                        st.session_state["vrp_job_status"] = latest_status
                        latest_state = str(latest_status.get("status", "")).strip().lower()
                        if latest_state == "completed":
                            st.session_state["vrp_job_result"] = get_routing_job_result(server_url, job_id)
                            _reset_vrp_result_view()
                            st.success("Smart Routing completed. Displaying the latest result.")
                            st.rerun()
                        elif latest_state == "failed":
                            st.session_state["vrp_job_result"] = None
                    st.success("Status updated.")

        status_payload = st.session_state.get("vrp_job_status")
        progress_value, progress_text = _routing_status_progress(status_payload.get("status", "") if status_payload else "")
        st.progress(progress_value)
        st.caption(progress_text)
        _auto_poll_routing_status()
        if status_payload:
            current_status = str(status_payload.get("status", "")).strip().lower()
            if current_status and current_status != "completed":
                st.caption(f"Smart Routing job status: {current_status}. Auto-checking every 5 seconds.")
                error_message = str(status_payload.get("error_message", "")).strip()
                if current_status == "failed" and error_message:
                    st.error(f"Server error: {error_message}")
            elif current_status == "completed":
                st.caption("Smart Routing job completed.")
                view_options = ["Actual", "Smart Routing"]
                st.radio("Assignment View", view_options, horizontal=True, key="vrp_compare_mode")

    with list_tab:
        if display_store_df.empty:
            st.session_state.pop("saved_input_table", None)
            st.info("No saved input rows yet.")
        else:
            saved_dates = sorted(display_store_df["PROMISE_DATE"].dropna().astype(str).unique().tolist(), reverse=True)
            selected_list_date = st.selectbox(
                "Filter by PROMISE_DATE",
                options=["ALL"] + saved_dates,
                index=0,
                key="saved_list_promise_date",
            )
            filtered_display_df = display_store_df.copy()
            if selected_list_date != "ALL":
                filtered_display_df = filtered_display_df[filtered_display_df["PROMISE_DATE"].astype(str) == str(selected_list_date)].copy()
            filtered_display_df = filtered_display_df.sort_values(
                ["PROMISE_DATE", "GSFS_RECEIPT_NO"],
                ascending=[False, True],
                na_position="last",
            ).reset_index(drop=True)
            list_cols = [
                "SVC_ENGINEER_NAME",
                "SVC_ENGINEER_CODE",
                "GSFS_RECEIPT_NO",
                "fixed",
                "PROMISE_DATE",
                "Product Group Name",
                "Product Name",
                "Detailed Symptom Name",
                "CITY_NAME",
                "POSTAL_CODE",
                "ADDRESS_LINE1_INFO",
                "latitude",
                "longitude",
                "input_source",
            ]
            dataframe_state = st.dataframe(
                filtered_display_df[list_cols],
                width="stretch",
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="saved_input_table",
            )
            selected_rows = list(getattr(getattr(dataframe_state, "selection", None), "rows", []) or [])
            if selected_rows:
                selected_idx = int(selected_rows[0])
                if 0 <= selected_idx < len(filtered_display_df):
                    selected_record_id = str(filtered_display_df.iloc[selected_idx]["record_id"])
                    selected_receipt = str(filtered_display_df.iloc[selected_idx]["GSFS_RECEIPT_NO"])
                    st.caption(f"Selected receipt: {selected_receipt}")
                    if st.button("Edit Selected Row", width="stretch"):
                        st.session_state["input_dialog_record_id"] = selected_record_id
                        st.rerun()


def main() -> None:
    st.title("Smart Routing API Client")
    st.caption(f"Network URL: {NETWORK_URL}")
    left_col, right_col = st.columns([1, 2.2])

    with left_col:
        server_url = DEFAULT_SERVER_URL
        _render_input_manager(server_url)

    with right_col:
        runtime_state = st.session_state.get("vrp_runtime")
        if runtime_state is None:
            st.info("Build a payload first, then submit and refresh the Smart Routing job.")
            return

        current_job_id = str(st.session_state.get("vrp_job_id", "")).strip()
        latest_status_payload = st.session_state.get("vrp_job_status") or {}
        latest_status_value = str(latest_status_payload.get("status", "")).strip().lower()
        if current_job_id and (not latest_status_payload or latest_status_value in {"queued", "running"}):
            try:
                refreshed_status = get_routing_job_status(server_url, current_job_id)
                st.session_state["vrp_job_status"] = refreshed_status
                latest_status_payload = refreshed_status
                latest_status_value = str(refreshed_status.get("status", "")).strip().lower()
                if latest_status_value == "completed" and not st.session_state.get("vrp_job_result"):
                    st.session_state["vrp_job_result"] = get_routing_job_result(server_url, current_job_id)
                    _reset_vrp_result_view()
            except Exception:
                pass

        if runtime_state.get("rendered_sql"):
            with st.expander("Executed SQL", expanded=False):
                st.code(runtime_state["rendered_sql"], language="sql")

        current_status_payload = st.session_state.get("vrp_job_status") or {}
        current_status = str(current_status_payload.get("status", "")).strip().lower()
        result_payload = st.session_state.get("vrp_job_result")
        if current_job_id and current_status == "completed" and not result_payload:
            try:
                st.session_state["vrp_job_result"] = get_routing_job_result(server_url, current_job_id)
                _reset_vrp_result_view()
                result_payload = st.session_state.get("vrp_job_result")
            except Exception:
                result_payload = None

        if not result_payload:
            preview_df = _build_pre_result_service_view(runtime_state["service_df"])
            compare_mode = st.session_state.get("vrp_compare_mode", "Actual")
            if compare_mode == "Smart Routing":
                st.info("Smart Routing result is not ready yet. The current map shows the loaded service points until the job completes.")
            else:
                st.info("The current map shows the loaded service points.")
            available_dates = sorted(preview_df["service_date_key"].dropna().astype(str).unique().tolist()) if "service_date_key" in preview_df.columns else []
            available_regions = ["ALL"] + sorted(preview_df["new_region_name"].dropna().astype(str).unique().tolist()) if "new_region_name" in preview_df.columns else ["ALL"]
            preview_engineer_options, preview_engineer_label_to_code = _build_engineer_options(preview_df)
            preview_col1, preview_col2, preview_col3 = st.columns(3)
            preview_date = preview_col1.selectbox("Date", options=available_dates, index=0 if available_dates else None, key="preview_date")
            preview_region = preview_col2.selectbox("Region", options=available_regions, index=0, key="preview_region")
            preview_engineer_label = preview_col3.selectbox("Engineer", options=preview_engineer_options, index=0, key="preview_engineer")
            preview_engineer_code = preview_engineer_label_to_code.get(preview_engineer_label, "ALL")
            if preview_date:
                preview_df = preview_df[preview_df["service_date_key"].astype(str) == str(preview_date)].copy()
            preview_home = runtime_state["home_df"].copy()
            if preview_region != "ALL":
                preview_df = preview_df[preview_df["new_region_name"].astype(str) == str(preview_region)].copy()
                preview_home = preview_home[preview_home["assigned_region_name"].astype(str) == str(preview_region)].copy()
            if preview_engineer_code != "ALL":
                preview_df = preview_df[preview_df["SVC_ENGINEER_CODE"].astype(str) == str(preview_engineer_code)].copy()
                preview_home = preview_home[preview_home["SVC_ENGINEER_CODE"].astype(str) == str(preview_engineer_code)].copy()
            preview_route_groups = _build_preview_route_groups(preview_df, preview_home)
            preview_map = build_map(preview_region, preview_df, preview_home, preview_route_groups, runtime_state["region_zip_df"])
            _render_folium_map(preview_map, height=760)
            preview_schedule_df = pd.DataFrame()
            for group in preview_route_groups:
                preview_schedule_df = pd.concat([preview_schedule_df, pd.DataFrame(group["scheduled_rows"])], ignore_index=True)
            preview_cols = [
                "service_date_key",
                "SVC_ENGINEER_NAME",
                "SVC_ENGINEER_CODE",
                "GSFS_RECEIPT_NO",
                "fixed",
                "visit_seq",
                "visit_start_time",
                "visit_end_time",
                "SERVICE_PRODUCT_GROUP_CODE",
                "SERVICE_PRODUCT_CODE",
                "SVC_CENTER_TYPE",
                "new_region_name",
            ]
            preview_source_df = preview_schedule_df if not preview_schedule_df.empty else preview_df
            preview_cols = [col for col in preview_cols if col in preview_source_df.columns]
            if preview_cols:
                st.subheader("Loaded Service Points")
                st.dataframe(preview_source_df[preview_cols], width="stretch", hide_index=True)
                st.download_button(
                    "Download Loaded Service CSV",
                    data=_to_csv_bytes(preview_source_df),
                    file_name="loaded_service_points.csv",
                    mime="text/csv",
                    width="stretch",
                )
            return

        assignment_df, schedule_df = _build_result_frames(result_payload, runtime_state)
        if assignment_df.empty or schedule_df.empty:
            st.warning("The job completed but returned no routed assignments.")
            return

        compare_mode = st.session_state.get("vrp_compare_mode", "Actual")
        if compare_mode == "Actual":
            assignment_df, schedule_df = _build_actual_frames(runtime_state)

        if compare_mode == "Smart Routing":
            changed_count = int(pd.to_numeric(assignment_df.get("changed", False), errors="coerce").fillna(0).astype(bool).sum())
            total_count = int(len(assignment_df))
            st.caption(f"Changed assignments: {changed_count} / {total_count}")

        available_dates = sorted(schedule_df["service_date_key"].dropna().astype(str).unique().tolist())
        available_regions = ["ALL"] + sorted(schedule_df["new_region_name"].dropna().astype(str).unique().tolist())
        engineer_options, engineer_label_to_code = _build_engineer_options(assignment_df)

        filter_col1, filter_col2, filter_col3 = st.columns(3)
        selected_date = filter_col1.selectbox("Date", options=available_dates, index=0)
        selected_region = filter_col2.selectbox("Region", options=available_regions, index=0)
        selected_engineer_label = filter_col3.selectbox("Engineer", options=engineer_options, index=0)
        selected_engineer_code = engineer_label_to_code.get(selected_engineer_label, "ALL")

        filtered_assignment = assignment_df[assignment_df["service_date_key"].astype(str) == str(selected_date)].copy()
        filtered_schedule = schedule_df[schedule_df["service_date_key"].astype(str) == str(selected_date)].copy()
        filtered_home = runtime_state["home_df"].copy()
        if selected_region != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["new_region_name"].astype(str) == str(selected_region)].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"].astype(str) == str(selected_region)].copy()
            filtered_home = filtered_home[filtered_home["assigned_region_name"].astype(str) == str(selected_region)].copy()
        if selected_engineer_code != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
            filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == str(selected_engineer_code)].copy()

        route_groups = _build_route_groups(filtered_schedule)
        service_count = int(filtered_assignment["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
        engineer_count = int(filtered_assignment["assigned_sm_code"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
        dms_engineer_count = 0
        dms2_engineer_count = 0
        if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns:
            center_types = filtered_assignment["assigned_center_type"].astype(str).str.upper()
            dms_engineer_count = int(filtered_assignment.loc[center_types == "DMS", "assigned_sm_code"].astype(str).nunique())
            dms2_engineer_count = int(filtered_assignment.loc[center_types == "DMS2", "assigned_sm_code"].astype(str).nunique())

        route_distance_series = pd.Series([float(group["route_payload"]["distance_km"]) for group in route_groups], dtype=float)
        route_duration_series = pd.Series([float(group["route_payload"]["duration_min"]) for group in route_groups], dtype=float)
        avg_distance = float(route_distance_series.mean()) if not route_distance_series.empty else 0.0
        avg_duration = float(route_duration_series.mean()) if not route_duration_series.empty else 0.0

        jobs_per_engineer = (
            filtered_assignment.groupby("assigned_sm_code", dropna=True)["GSFS_RECEIPT_NO"].nunique()
            if not filtered_assignment.empty
            else pd.Series(dtype=float)
        )
        jobs_std = float(jobs_per_engineer.std(ddof=0)) if not jobs_per_engineer.empty else 0.0

        staffing_df = _build_region_staffing_view(filtered_assignment)
        engineer_summary_rows: list[dict[str, object]] = []
        route_group_by_code = {str(group["engineer_code"]): group for group in route_groups}
        if not filtered_assignment.empty:
            for engineer_code, group in filtered_assignment.groupby("assigned_sm_code", dropna=True):
                route_group = route_group_by_code.get(str(engineer_code))
                engineer_summary_rows.append(
                    {
                        "Engineer": str(group["assigned_sm_name"].iloc[0]) if "assigned_sm_name" in group.columns and not group.empty else str(engineer_code),
                        "job_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                        "route_distance_km": round(float(route_group["route_payload"]["distance_km"]), 2) if route_group else 0.0,
                        "route_duration_min": round(float(route_group["route_payload"]["duration_min"]), 2) if route_group else 0.0,
                    }
                )
        engineer_summary_df = pd.DataFrame(engineer_summary_rows).sort_values(["job_count", "Engineer"], ascending=[False, True]) if engineer_summary_rows else pd.DataFrame()
        with left_col:
            metric_col1, metric_col2 = st.columns(2)
            metric_col1.metric("Service Count", service_count)
            metric_col2.metric("Assigned Engineer Count", f"{engineer_count} (DMS {dms_engineer_count}, DMS2 {dms2_engineer_count})")
            metric_col3, metric_col4 = st.columns(2)
            metric_col3.metric("Average Distance (km)", f"{avg_distance:.2f}")
            metric_col4.metric("Average Duration (min)", f"{avg_duration:.2f}")
            st.metric("Jobs per Engineer Std", f"{jobs_std:.2f}")
            if not staffing_df.empty:
                st.markdown("**Regional Staffing / Jobs**")
                st.dataframe(staffing_df, width="stretch", hide_index=True)
            if not engineer_summary_df.empty:
                st.markdown("**Engineer Summary**")
                st.dataframe(engineer_summary_df, width="stretch", hide_index=True)

        map_obj = build_map(selected_region, filtered_assignment, filtered_home, route_groups, runtime_state["region_zip_df"])
        _render_folium_map(map_obj, height=760)

        st.subheader("Selected Schedule")
        display_cols = [
            "service_date_key",
            "assigned_sm_name",
            "assigned_sm_code",
            "GSFS_RECEIPT_NO",
            "fixed",
            "changed",
            "visit_seq",
            "visit_start_time",
            "visit_end_time",
            "SERVICE_PRODUCT_GROUP_CODE",
            "SERVICE_PRODUCT_CODE",
            "assigned_center_type",
            "new_region_name",
        ]
        display_cols = [col for col in display_cols if col in filtered_schedule.columns]
        st.dataframe(filtered_schedule[display_cols], width="stretch", hide_index=True)
        st.download_button(
            "Download Assignment CSV",
            data=_to_csv_bytes(filtered_schedule),
            file_name=f"{st.session_state.get('vrp_job_id', 'vrp_job')}_schedule.csv",
            mime="text/csv",
            width="stretch",
        )

        unassigned_df = pd.DataFrame(result_payload.get("unassigned", []))
        if not unassigned_df.empty:
            st.subheader("Unassigned")
            st.dataframe(unassigned_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
