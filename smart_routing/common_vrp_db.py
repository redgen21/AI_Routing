from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from .area_map import get_latest_geocoded_service_file
from .census_geocoder import normalize_postal_code
from .live_atlanta_runtime import _load_config as _load_runtime_config
from .live_atlanta_runtime import _merge_service_geocodes


COMMON_CONFIG_PATH = Path("config_common_vrp.json")
PROFILE_PATH = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
DEFAULT_REGION_ZIP_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3.csv")
DEFAULT_HEAVY_REPAIR_LOOKUP_PATH = Path("260310/production_input/atlanta_heavy_repair_lookup.csv")
DEFAULT_SYMPTOM_FILE = Path("data/Notification_Symptom_mapping_20241120_3depth.xlsx")


def _clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _geocode_technician_home_df(home_df: pd.DataFrame) -> pd.DataFrame:
    if home_df.empty:
        return home_df.copy()
    geocode_input = home_df.copy()
    config = _load_runtime_config()
    geocoded_df = _merge_service_geocodes(geocode_input, config)
    geocoded_df["latitude"] = pd.to_numeric(geocoded_df.get("latitude"), errors="coerce")
    geocoded_df["longitude"] = pd.to_numeric(geocoded_df.get("longitude"), errors="coerce")
    return geocoded_df


def load_common_config(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing common config: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def get_db_connection(config_path: Path = COMMON_CONFIG_PATH):
    cfg = load_common_config(config_path).get("database", {})
    return psycopg2.connect(
        host=str(cfg.get("host", "localhost")),
        port=int(cfg.get("port", 5432)),
        dbname=str(cfg.get("dbname", "VRP_DB")),
        user=str(cfg.get("user", "vrp_agent")),
        password=str(cfg.get("password", "")),
    )


SCHEMA_SQL = """
create table if not exists common_routing_config_master (
    subsidiary_name text not null,
    strategic_city_name text not null,
    distance_backend text,
    assignment_distance_backend text,
    osrm_url text,
    osrm_profile text,
    effective_service_per_sm integer,
    target_sm_per_region integer,
    service_time_per_job_min integer,
    max_work_min_per_sm_day integer,
    max_travel_min_per_sm_day integer,
    max_travel_km_per_sm_day integer,
    timezone_offset text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name)
);

create table if not exists common_region_master (
    subsidiary_name text not null,
    strategic_city_name text not null,
    postal_code text not null,
    region_seq integer not null,
    region_name text not null,
    region_center_latitude double precision,
    region_center_longitude double precision,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, postal_code)
);

create table if not exists common_technician_master (
    subsidiary_name text not null,
    strategic_city_name text not null,
    employee_code text not null,
    employee_name text not null,
    center_type text,
    home_address text,
    home_city text,
    home_state text,
    home_country text,
    home_postal_code text,
    home_latitude double precision,
    home_longitude double precision,
    active_flag boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, employee_code)
);

create table if not exists common_technician_capability_master (
    subsidiary_name text not null,
    strategic_city_name text not null,
    employee_code text not null,
    product_group_code text not null,
    product_code text not null,
    repair_allowed boolean not null default true,
    heavy_repair_allowed boolean not null default true,
    priority_score integer,
    effective_start_date date,
    effective_end_date date,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, employee_code, product_group_code, product_code)
);

create table if not exists common_heavy_repair_rule_master (
    product_group_code text not null,
    product_code text not null,
    detailed_symptom_code text not null,
    created_at timestamptz not null default now(),
    primary key (product_group_code, product_code, detailed_symptom_code)
);

create table if not exists common_job_input (
    record_id text not null,
    subsidiary_name text not null,
    strategic_city_name text not null,
    svc_engineer_code text,
    svc_engineer_name text,
    service_product_group_code text,
    service_product_code text,
    receipt_detail_symptom_code text,
    gsfs_receipt_no text not null,
    promise_date text not null,
    city_name text,
    state_name text,
    country_name text,
    postal_code text,
    address_line1_info text,
    latitude double precision,
    longitude double precision,
    source text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (record_id),
    unique (subsidiary_name, strategic_city_name, gsfs_receipt_no)
);

create table if not exists common_request_technician_input (
    record_id text not null,
    subsidiary_name text not null,
    strategic_city_name text not null,
    promise_date text not null,
    employee_code text not null,
    employee_name text not null,
    center_type text,
    shift_start text,
    shift_end text,
    slot_count integer,
    max_jobs integer,
    available boolean not null default true,
    start_location_type text,
    start_location_address text,
    source text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (record_id),
    unique (subsidiary_name, strategic_city_name, promise_date, employee_code)
);

create table if not exists common_routing_request (
    request_id text not null,
    subsidiary_name text not null,
    strategic_city_name text not null,
    promise_date text not null,
    routing_job_id text,
    routing_status text,
    payload_json text,
    status_json text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (request_id)
);

create table if not exists common_routing_result (
    request_id text not null,
    routing_job_id text,
    result_json text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (request_id)
);
"""


def init_schema(config_path: Path = COMMON_CONFIG_PATH) -> None:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute(
                """
                alter table if exists common_request_technician_input
                add column if not exists promise_date text
                """
            )
            cur.execute(
                """
                alter table if exists common_request_technician_input
                add column if not exists source text
                """
            )
            cur.execute(
                """
                do $$
                begin
                    if exists (
                        select 1
                        from information_schema.table_constraints
                        where table_name = 'common_request_technician_input'
                          and constraint_name = 'common_request_technician_inp_subsidiary_name_strategic_cit_key'
                    ) then
                        alter table common_request_technician_input
                        drop constraint common_request_technician_inp_subsidiary_name_strategic_cit_key;
                    end if;
                end $$;
                """
            )
            cur.execute(
                """
                do $$
                begin
                    if not exists (
                        select 1
                        from information_schema.table_constraints
                        where table_name = 'common_request_technician_input'
                          and constraint_name = 'common_request_technician_input_context_employee_key'
                    ) then
                        alter table common_request_technician_input
                        add constraint common_request_technician_input_context_employee_key
                        unique (subsidiary_name, strategic_city_name, promise_date, employee_code);
                    end if;
                end $$;
                """
            )
        conn.commit()


def _fetch_df(query: str, params: tuple[Any, ...] = (), config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    with get_db_connection(config_path) as conn:
        return pd.read_sql_query(query, conn, params=params)


def _execute_values_upsert(
    table_name: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    conflict_cols: list[str],
    update_cols: list[str],
    config_path: Path = COMMON_CONFIG_PATH,
) -> int:
    if not rows:
        return 0
    insert_cols = ", ".join(columns)
    conflict_expr = ", ".join(conflict_cols)
    if update_cols:
        update_expr = ", ".join([f"{col}=excluded.{col}" for col in update_cols] + ["updated_at=now()"])
        conflict_sql = f"on conflict ({conflict_expr}) do update set {update_expr}"
    else:
        conflict_sql = f"on conflict ({conflict_expr}) do nothing"
    sql = f"""
        insert into {table_name} ({insert_cols})
        values %s
        {conflict_sql}
    """
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    return len(rows)


def list_contexts(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, list[str]]:
    df = _fetch_df(
        """
        select distinct subsidiary_name, strategic_city_name
        from common_routing_config_master
        order by subsidiary_name, strategic_city_name
        """,
        config_path=config_path,
    )
    return {
        "subsidiaries": sorted(df["subsidiary_name"].dropna().astype(str).unique().tolist()),
        "cities": sorted(df["strategic_city_name"].dropna().astype(str).unique().tolist()),
    }


def get_routing_config(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any] | None:
    df = _fetch_df(
        """
        select *
        from common_routing_config_master
        where subsidiary_name = %s and strategic_city_name = %s
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for key in ["created_at", "updated_at"]:
        if key in row and pd.notna(row[key]):
            row[key] = str(row[key])
    return row


def upsert_routing_config(config_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH) -> int:
    columns = [
        "subsidiary_name",
        "strategic_city_name",
        "distance_backend",
        "assignment_distance_backend",
        "osrm_url",
        "osrm_profile",
        "effective_service_per_sm",
        "target_sm_per_region",
        "service_time_per_job_min",
        "max_work_min_per_sm_day",
        "max_travel_min_per_sm_day",
        "max_travel_km_per_sm_day",
        "timezone_offset",
    ]
    row = tuple(config_row.get(col) for col in columns)
    return _execute_values_upsert(
        "common_routing_config_master",
        columns,
        [row],
        ["subsidiary_name", "strategic_city_name"],
        [col for col in columns if col not in {"subsidiary_name", "strategic_city_name"}],
        config_path=config_path,
    )


def list_engineers(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_technician_master
        where subsidiary_name = %s and strategic_city_name = %s
        order by employee_name, employee_code
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )


def list_capabilities(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_technician_capability_master
        where subsidiary_name = %s and strategic_city_name = %s
        order by employee_code, product_group_code, product_code
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )


def list_heavy_repair_rules(config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_heavy_repair_rule_master
        order by product_group_code, product_code, detailed_symptom_code
        """,
        config_path=config_path,
    )


def upsert_routing_request(request_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH) -> int:
    columns = [
        "request_id",
        "subsidiary_name",
        "strategic_city_name",
        "promise_date",
        "routing_job_id",
        "routing_status",
        "payload_json",
        "status_json",
    ]
    row = tuple(request_row.get(col) for col in columns)
    return _execute_values_upsert(
        "common_routing_request",
        columns,
        [row],
        ["request_id"],
        [col for col in columns if col != "request_id"],
        config_path=config_path,
    )


def get_routing_request(request_id: str, config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any] | None:
    df = _fetch_df(
        """
        select *
        from common_routing_request
        where request_id = %s
        """,
        (request_id,),
        config_path=config_path,
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for key in ["created_at", "updated_at"]:
        if key in row and pd.notna(row[key]):
            row[key] = str(row[key])
    return row


def get_latest_routing_request(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any] | None:
    df = _fetch_df(
        """
        select *
        from common_routing_request
        where subsidiary_name = %s and strategic_city_name = %s and promise_date = %s
        order by updated_at desc, created_at desc
        limit 1
        """,
        (subsidiary_name, strategic_city_name, promise_date),
        config_path=config_path,
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for key in ["created_at", "updated_at"]:
        if key in row and pd.notna(row[key]):
            row[key] = str(row[key])
    return row


def upsert_routing_result(result_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH) -> int:
    columns = ["request_id", "routing_job_id", "result_json"]
    row = tuple(result_row.get(col) for col in columns)
    return _execute_values_upsert(
        "common_routing_result",
        columns,
        [row],
        ["request_id"],
        [col for col in columns if col != "request_id"],
        config_path=config_path,
    )


def get_routing_result(request_id: str, config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any] | None:
    df = _fetch_df(
        """
        select *
        from common_routing_result
        where request_id = %s
        """,
        (request_id,),
        config_path=config_path,
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for key in ["created_at", "updated_at"]:
        if key in row and pd.notna(row[key]):
            row[key] = str(row[key])
    return row


def list_regions(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_region_master
        where subsidiary_name = %s and strategic_city_name = %s
        order by region_seq, postal_code
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )


def _seed_routing_config(config_path: Path = COMMON_CONFIG_PATH) -> None:
    cfg = load_common_config(config_path)
    seed = cfg.get("routing_seed", {})
    defaults = cfg.get("defaults", {})
    strategic_city_name = defaults.get("strategic_city_name", "Atlanta, GA")
    city_osrm_urls = seed.get("city_osrm_urls", {}) or {}
    resolved_osrm_url = city_osrm_urls.get(str(strategic_city_name), seed.get("osrm_url"))
    upsert_routing_config(
        {
            "subsidiary_name": defaults.get("subsidiary_name", "LGEAI"),
            "strategic_city_name": strategic_city_name,
            "distance_backend": seed.get("distance_backend"),
            "assignment_distance_backend": seed.get("assignment_distance_backend"),
            "osrm_url": resolved_osrm_url,
            "osrm_profile": seed.get("osrm_profile"),
            "effective_service_per_sm": seed.get("effective_service_per_sm"),
            "target_sm_per_region": seed.get("target_sm_per_region"),
            "service_time_per_job_min": seed.get("service_time_per_job_min"),
            "max_work_min_per_sm_day": seed.get("max_work_min_per_sm_day"),
            "max_travel_min_per_sm_day": seed.get("max_travel_min_per_sm_day"),
            "max_travel_km_per_sm_day": seed.get("max_travel_km_per_sm_day"),
            "timezone_offset": seed.get("timezone_offset", "-04:00"),
        },
        config_path=config_path,
    )


def _seed_technician_master(config_path: Path = COMMON_CONFIG_PATH) -> None:
    slot_df = pd.read_excel(PROFILE_PATH, sheet_name="2. Slot")
    address_df = pd.read_excel(PROFILE_PATH, sheet_name="4. Address")
    slot_df = slot_df.rename(columns={"Name": "employee_name", "SVC_ENGINEER_CODE": "employee_code", "SVC_CENTER_TYPE": "center_type"})
    slot_df["strategic_city_name"] = slot_df["STRATEGIC_CITY_NAME"].astype(str).str.strip()
    slot_df["employee_code"] = slot_df["employee_code"].astype(str).str.strip()
    slot_df["employee_name"] = slot_df["employee_name"].astype(str).str.strip()
    address_df = address_df.rename(
        columns={
            "SVC_ENGINEER_CODE": "employee_code",
            "Name": "employee_name",
            "Home Street Address": "home_address",
            "City ": "home_city",
            "State": "home_state",
            "Zip": "home_postal_code",
        }
    )
    address_df["employee_code"] = address_df["employee_code"].astype(str).str.strip()
    merged = slot_df.merge(address_df, on=["employee_code", "employee_name"], how="left")
    merged["subsidiary_name"] = "LGEAI"
    merged["home_country"] = "USA"
    home_input_rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        employee_code = _clean_text(row.get("employee_code"))
        home_address = _clean_text(row.get("home_address", ""))
        home_city = _clean_text(row.get("home_city", ""))
        home_state = _clean_text(row.get("home_state", ""))
        home_postal_code = normalize_postal_code(row.get("home_postal_code"))
        if employee_code and any([home_address, home_city, home_state, home_postal_code]):
            home_input_rows.append(
                {
                    "GSFS_RECEIPT_NO": employee_code,
                    "ADDRESS_LINE1_INFO": home_address,
                    "CITY_NAME": home_city,
                    "STATE_NAME": home_state,
                    "COUNTRY_NAME": "USA",
                    "POSTAL_CODE": home_postal_code,
                }
            )
    geocoded_home_lookup: dict[str, tuple[float | None, float | None]] = {}
    if home_input_rows:
        geocoded_home_df = _geocode_technician_home_df(pd.DataFrame(home_input_rows))
        geocoded_home_lookup = {
            str(row["GSFS_RECEIPT_NO"]).strip(): (
                float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
                float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
            )
            for _, row in geocoded_home_df.iterrows()
        }
    rows = []
    for _, row in merged.iterrows():
        employee_code = _clean_text(row["employee_code"])
        home_postal_code = normalize_postal_code(row.get("home_postal_code"))
        home_latitude, home_longitude = geocoded_home_lookup.get(employee_code, (None, None))
        rows.append(
            (
                row["subsidiary_name"],
                _clean_text(row["strategic_city_name"]),
                employee_code,
                _clean_text(row["employee_name"]),
                _clean_text(row.get("center_type", "")).upper(),
                _clean_text(row.get("home_address", "")),
                _clean_text(row.get("home_city", "")),
                _clean_text(row.get("home_state", "")),
                "USA",
                home_postal_code,
                home_latitude,
                home_longitude,
                True,
            )
        )
    _execute_values_upsert(
        "common_technician_master",
        [
            "subsidiary_name",
            "strategic_city_name",
            "employee_code",
            "employee_name",
            "center_type",
            "home_address",
            "home_city",
            "home_state",
            "home_country",
            "home_postal_code",
            "home_latitude",
            "home_longitude",
            "active_flag",
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "employee_code"],
        ["employee_name", "center_type", "home_address", "home_city", "home_state", "home_country", "home_postal_code", "home_latitude", "home_longitude", "active_flag"],
        config_path=config_path,
    )


def _seed_technician_capabilities(config_path: Path = COMMON_CONFIG_PATH) -> None:
    product_df = pd.read_excel(PROFILE_PATH, sheet_name="3. Product")
    product_df["subsidiary_name"] = "LGEAI"
    product_df["strategic_city_name"] = product_df["STRATEGIC_CITY_NAME"].astype(str).str.strip()
    product_df["employee_code"] = product_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    product_df["product_group_code"] = product_df["SERVICE_PRODUCT_GROUP_CODE"].astype(str).str.strip()
    product_df["product_code"] = product_df["SERVICE_PRODUCT_CODE"].astype(str).str.strip()
    product_df["repair_allowed"] = product_df["REPAIR_FLAG"].astype(str).str.upper().eq("T")
    product_df["heavy_repair_allowed"] = ~(
        product_df["product_group_code"].eq("REF")
        & product_df["AREA_PRODUCT_FLAG"].astype(str).str.upper().eq("N")
    )
    rows = []
    for _, row in product_df.drop_duplicates(subset=["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"]).iterrows():
        rows.append(
            (
                row["subsidiary_name"],
                row["strategic_city_name"],
                row["employee_code"],
                row["product_group_code"],
                row["product_code"],
                bool(row["repair_allowed"]),
                bool(row["heavy_repair_allowed"]),
                100,
                None,
                None,
            )
        )
    _execute_values_upsert(
        "common_technician_capability_master",
        [
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
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"],
        ["repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date"],
        config_path=config_path,
    )


def _seed_region_master(config_path: Path = COMMON_CONFIG_PATH) -> None:
    defaults = load_common_config(config_path).get("defaults", {})
    region_df = pd.read_csv(DEFAULT_REGION_ZIP_PATH, encoding="utf-8-sig")
    region_df["POSTAL_CODE"] = region_df["POSTAL_CODE"].astype(str).str.zfill(5)
    centers: dict[int, tuple[float | None, float | None]] = {}
    latest_service = get_latest_geocoded_service_file()
    if latest_service and latest_service.exists():
        service_df = pd.read_csv(latest_service, encoding="utf-8-sig", low_memory=False)
        if {"POSTAL_CODE", "latitude", "longitude"}.issubset(service_df.columns):
            service_df["POSTAL_CODE"] = service_df["POSTAL_CODE"].astype(str).str.replace(r"\.0+$", "", regex=True).str.zfill(5)
            service_df["latitude"] = pd.to_numeric(service_df["latitude"], errors="coerce")
            service_df["longitude"] = pd.to_numeric(service_df["longitude"], errors="coerce")
            merged = service_df.merge(region_df[["POSTAL_CODE", "region_seq"]].drop_duplicates(), on="POSTAL_CODE", how="inner")
            center_df = merged.dropna(subset=["latitude", "longitude", "region_seq"]).groupby("region_seq").agg(
                region_center_latitude=("latitude", "mean"),
                region_center_longitude=("longitude", "mean"),
            )
            centers = {
                int(idx): (float(row["region_center_latitude"]), float(row["region_center_longitude"]))
                for idx, row in center_df.reset_index().set_index("region_seq").iterrows()
            }
    rows = []
    for _, row in region_df.drop_duplicates(subset=["POSTAL_CODE"]).iterrows():
        center = centers.get(int(row["region_seq"]), (None, None))
        rows.append(
            (
                defaults.get("subsidiary_name", "LGEAI"),
                defaults.get("strategic_city_name", "Atlanta, GA"),
                str(row["POSTAL_CODE"]).zfill(5),
                int(row["region_seq"]),
                str(row["new_region_name"]),
                center[0],
                center[1],
            )
        )
    _execute_values_upsert(
        "common_region_master",
        [
            "subsidiary_name",
            "strategic_city_name",
            "postal_code",
            "region_seq",
            "region_name",
            "region_center_latitude",
            "region_center_longitude",
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "postal_code"],
        ["region_seq", "region_name", "region_center_latitude", "region_center_longitude"],
        config_path=config_path,
    )


def _seed_heavy_repair_rules(config_path: Path = COMMON_CONFIG_PATH) -> None:
    if DEFAULT_HEAVY_REPAIR_LOOKUP_PATH.exists():
        lookup_df = pd.read_csv(DEFAULT_HEAVY_REPAIR_LOOKUP_PATH, encoding="utf-8-sig")
    else:
        lookup_df = pd.read_excel(DEFAULT_SYMPTOM_FILE)
    cols = ["SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "SYMP_CODE_THREE"]
    lookup_df = lookup_df[cols].dropna(subset=["SYMP_CODE_THREE"]).drop_duplicates()
    rows = [
        (
            str(row["SERVICE_PRODUCT_GROUP_CODE"]).strip(),
            str(row["SERVICE_PRODUCT_CODE"]).strip(),
            str(row["SYMP_CODE_THREE"]).strip(),
        )
        for _, row in lookup_df.iterrows()
        if str(row["SYMP_CODE_THREE"]).strip()
    ]
    _execute_values_upsert(
        "common_heavy_repair_rule_master",
        ["product_group_code", "product_code", "detailed_symptom_code"],
        rows,
        ["product_group_code", "product_code", "detailed_symptom_code"],
        [],
        config_path=config_path,
    )


def seed_default_masters(config_path: Path = COMMON_CONFIG_PATH) -> None:
    init_schema(config_path)
    _seed_routing_config(config_path)
    _seed_technician_master(config_path)
    _seed_technician_capabilities(config_path)
    _seed_region_master(config_path)
    _seed_heavy_repair_rules(config_path)
