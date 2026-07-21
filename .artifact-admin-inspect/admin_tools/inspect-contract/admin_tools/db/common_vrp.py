"""Minimal Common VRP DB administration repository.

This module is release-local: it deliberately has no dependency on the
application runtime package. It owns only the administrative DB contracts used
by reset and seed/import commands.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from admin_tools.db.data_catalog import na_data_path
from admin_tools.db.heavy_repair import load_heavy_repair_rules


COMMON_CONFIG_PATH = Path(os.environ.get("COMMON_VRP_CONFIG_PATH", "config/common_vrp.prod.json")).resolve()
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


SCHEMA_SQL = """
create table if not exists common_routing_config_master (
    subsidiary_name text not null, strategic_city_name text not null,
    distance_backend text, assignment_distance_backend text, osrm_url text, osrm_profile text,
    effective_service_per_sm integer, target_sm_per_region integer, service_time_per_job_min integer,
    max_work_min_per_sm_day integer, max_travel_min_per_sm_day integer, max_travel_km_per_sm_day integer,
    max_single_leg_min integer, max_home_to_job_min integer, long_leg_penalty_start_min integer,
    long_leg_penalty_multiplier numeric, timezone_offset text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name)
);
create table if not exists common_region_master (
    subsidiary_name text not null, strategic_city_name text not null, postal_code text not null,
    region_seq integer not null, region_name text not null, area_type text,
    region_center_latitude double precision, region_center_longitude double precision,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, postal_code)
);
create table if not exists common_technician_master (
    subsidiary_name text not null, strategic_city_name text not null, employee_code text not null,
    employee_name text not null, center_type text, home_address text, home_city text, home_state text,
    home_country text, home_postal_code text, home_latitude double precision, home_longitude double precision,
    active_flag boolean not null default true, priority_group text not null default 'B', max_home_to_job_min integer,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, employee_code)
);
create table if not exists common_technician_capability_master (
    subsidiary_name text not null, strategic_city_name text not null, employee_code text not null,
    product_group_code text not null, product_code text not null, repair_allowed boolean not null default true,
    heavy_repair_allowed boolean not null default true, priority_score integer, effective_start_date date,
    effective_end_date date, created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, employee_code, product_group_code, product_code)
);
create table if not exists common_heavy_repair_rule_master (
    product_group_code text not null, product_code text not null, detailed_symptom_code text not null,
    created_at timestamptz not null default now(),
    primary key (product_group_code, product_code, detailed_symptom_code)
);
create table if not exists common_job_input (
    record_id text not null, subsidiary_name text not null, strategic_city_name text not null,
    svc_engineer_code text, svc_engineer_name text, service_product_group_code text, service_product_code text,
    receipt_detail_symptom_code text, gsfs_receipt_no text not null, promise_date text not null, city_name text,
    state_name text, country_name text, postal_code text, address_line1_info text,
    fixed boolean not null default false, reschedule boolean not null default false, job_slot_count integer not null default 1,
    latitude double precision, longitude double precision, source text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (record_id), unique (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no)
);
create table if not exists common_request_technician_input (
    record_id text not null, subsidiary_name text not null, strategic_city_name text not null, promise_date text not null,
    employee_code text not null, employee_name text not null, center_type text, shift_start text, shift_end text,
    slot_count integer, priority_group text not null default 'B', preferred_region_name text, max_minutes integer,
    max_jobs integer, available boolean not null default true, start_location_type text, start_location_address text,
    source text, created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (record_id), unique (subsidiary_name, strategic_city_name, promise_date, employee_code)
);
create table if not exists common_routing_request (
    request_id text not null, subsidiary_name text not null, strategic_city_name text not null, promise_date text not null,
    routing_job_id text, routing_status text, payload_json text, status_json text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key (request_id)
);
create table if not exists common_routing_result (
    request_id text not null, routing_job_id text, result_json text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key (request_id)
);
create table if not exists common_avoid_area (
    avoid_area_id text not null, subsidiary_name text not null, strategic_city_name text not null,
    area_name text not null, description text, geometry_json text not null, active_flag boolean not null default true,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key (avoid_area_id)
);
create table if not exists common_geocode_cache (
    address_key text not null, source_bucket text not null, address_line1 text, city text, state text, postal_code text,
    country_name text, matched_address text, match_indicator text, match_type text, longitude double precision,
    latitude double precision, tiger_line_id text, tiger_line_side text, census_state_fips text, census_county_fips text,
    census_tract text, census_block text, geocoded_date date, source text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (address_key, source_bucket)
);
create table if not exists common_geocode_attempt_log (
    address_key text not null, source_bucket text not null, attempted_date date not null, status text, source text,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (address_key, source_bucket, attempted_date)
);
create table if not exists common_geocode_daily_log (
    run_date date not null, source_bucket text not null, used_count integer not null default 0,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key (run_date, source_bucket)
);
"""


def _clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat", "<na>", "<null>"} else text


def normalize_postal_code(value: Any) -> str:
    text = _clean_text(value)
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(character for character in text if character.isdigit())
    return digits.zfill(5) if digits else text


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "y", "yes", "t"}:
        return True
    if text in {"false", "0", "n", "no", "f", ""}:
        return False
    return default


def _priority_group(value: Any, default: str = "B") -> str:
    text = _clean_text(value).upper()
    if text in {"A", "HIGH", "P3", "PRIORITY 3", "3"}:
        return "A"
    if text in {"C", "LOW", "P1", "PRIORITY 1", "1"}:
        return "C"
    if text in {"B", "MEDIUM", "MID", "P2", "PRIORITY 2", "2"}:
        return "B"
    return default


def load_common_config(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing common config: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("database"), dict):
        raise ValueError("Common config requires a database object.")
    return payload


def get_db_connection(config_path: Path = COMMON_CONFIG_PATH):
    database = load_common_config(config_path)["database"]
    return psycopg2.connect(
        host=str(database.get("host", "localhost")), port=int(database.get("port", 5432)),
        dbname=str(database.get("dbname", "")), user=str(database.get("user", "")),
        password=str(database.get("password", "")), connect_timeout=10,
    )


def init_schema(config_path: Path = COMMON_CONFIG_PATH) -> None:
    """Create the administration-required Common VRP tables and compatible columns."""
    with get_db_connection(config_path) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)
            for statement in (
                "alter table if exists common_routing_config_master add column if not exists max_single_leg_min integer",
                "alter table if exists common_routing_config_master add column if not exists max_home_to_job_min integer",
                "alter table if exists common_routing_config_master add column if not exists long_leg_penalty_start_min integer",
                "alter table if exists common_routing_config_master add column if not exists long_leg_penalty_multiplier numeric",
                "alter table if exists common_region_master add column if not exists area_type text",
                "alter table if exists common_request_technician_input add column if not exists promise_date text",
                "alter table if exists common_request_technician_input add column if not exists source text",
                "alter table if exists common_request_technician_input add column if not exists priority_group text not null default 'B'",
                "alter table if exists common_request_technician_input add column if not exists max_minutes integer",
                "alter table if exists common_request_technician_input add column if not exists preferred_region_name text",
            ):
                cursor.execute(statement)
            cursor.execute(
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
        connection.commit()


def _safe_identifiers(values: list[str]) -> None:
    if not values or any(not _IDENTIFIER.fullmatch(value) for value in values):
        raise ValueError("Administrative upsert received an invalid SQL identifier.")


def _execute_values_upsert(
    table_name: str, columns: list[str], rows: list[tuple[Any, ...]], conflict_cols: list[str],
    update_cols: list[str], config_path: Path = COMMON_CONFIG_PATH, *, connection: Any | None = None,
) -> int:
    if not rows:
        return 0
    _safe_identifiers([table_name, *columns, *conflict_cols, *update_cols])
    update_sql = ", ".join([f"{column}=excluded.{column}" for column in update_cols] + ["updated_at=now()"])
    conflict_sql = (
        f"on conflict ({', '.join(conflict_cols)}) do update set {update_sql}"
        if update_cols else f"on conflict ({', '.join(conflict_cols)}) do nothing"
    )
    statement = f"insert into {table_name} ({', '.join(columns)}) values %s {conflict_sql}"
    if connection is not None:
        with connection.cursor() as cursor:
            execute_values(cursor, statement, rows)
        return len(rows)
    with get_db_connection(config_path) as owned_connection:
        with owned_connection.cursor() as cursor:
            execute_values(cursor, statement, rows)
        owned_connection.commit()
    return len(rows)


def upsert_technician_master(
    technician_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH, *, connection: Any | None = None,
) -> int:
    subsidiary_name = _clean_text(technician_row.get("subsidiary_name"))
    strategic_city_name = _clean_text(technician_row.get("strategic_city_name"))
    employee_code = _clean_text(technician_row.get("employee_code"))
    employee_name = _clean_text(technician_row.get("employee_name")) or employee_code
    if not subsidiary_name or not strategic_city_name or not employee_code:
        raise ValueError("subsidiary_name, strategic_city_name, and employee_code are required.")
    latitude = pd.to_numeric(pd.Series([technician_row.get("home_latitude")]), errors="coerce").iloc[0]
    longitude = pd.to_numeric(pd.Series([technician_row.get("home_longitude")]), errors="coerce").iloc[0]
    row = (
        subsidiary_name, strategic_city_name, employee_code, employee_name,
        _clean_text(technician_row.get("center_type")).upper() or "DMS",
        _clean_text(technician_row.get("home_address")), _clean_text(technician_row.get("home_city")),
        _clean_text(technician_row.get("home_state")), _clean_text(technician_row.get("home_country")) or "USA",
        normalize_postal_code(technician_row.get("home_postal_code")),
        float(latitude) if pd.notna(latitude) else None, float(longitude) if pd.notna(longitude) else None,
        _coerce_bool(technician_row.get("active_flag", True), True),
        _priority_group(technician_row.get("priority_group", "B")),
        int(pd.to_numeric(pd.Series([technician_row.get("max_home_to_job_min")]), errors="coerce").iloc[0])
        if pd.notna(pd.to_numeric(pd.Series([technician_row.get("max_home_to_job_min")]), errors="coerce").iloc[0]) else None,
    )
    columns = [
        "subsidiary_name", "strategic_city_name", "employee_code", "employee_name", "center_type",
        "home_address", "home_city", "home_state", "home_country", "home_postal_code", "home_latitude",
        "home_longitude", "active_flag", "priority_group", "max_home_to_job_min",
    ]
    return _execute_values_upsert(
        "common_technician_master", columns, [row], columns[:3], columns[3:], config_path, connection=connection,
    )


def upsert_routing_config(
    config_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH, *, connection: Any | None = None,
) -> int:
    columns = [
        "subsidiary_name", "strategic_city_name", "distance_backend", "assignment_distance_backend", "osrm_url",
        "osrm_profile", "effective_service_per_sm", "target_sm_per_region", "service_time_per_job_min",
        "max_work_min_per_sm_day", "max_travel_min_per_sm_day", "max_travel_km_per_sm_day", "max_single_leg_min",
        "max_home_to_job_min", "long_leg_penalty_start_min", "long_leg_penalty_multiplier", "timezone_offset",
    ]
    return _execute_values_upsert(
        "common_routing_config_master", columns, [tuple(config_row.get(column) for column in columns)], columns[:2],
        columns[2:], config_path, connection=connection,
    )


def _seed_routing_config(config_path: Path, *, connection: Any | None = None) -> None:
    cfg = load_common_config(config_path)
    seed = cfg.get("routing_seed", {}) if isinstance(cfg.get("routing_seed"), dict) else {}
    defaults = cfg.get("defaults", {}) if isinstance(cfg.get("defaults"), dict) else {}
    city_urls = seed.get("city_osrm_urls", {}) if isinstance(seed.get("city_osrm_urls"), dict) else {}
    city_overrides = seed.get("city_overrides", {}) if isinstance(seed.get("city_overrides"), dict) else {}
    context = cfg.get("context_seed", {}) if isinstance(cfg.get("context_seed"), dict) else {}
    subsidiaries = context.get("subsidiaries", {}) if isinstance(context.get("subsidiaries"), dict) else {}
    contexts: list[tuple[str, str]] = []
    if subsidiaries:
        for subsidiary, cities in subsidiaries.items():
            for city in list(cities or []):
                if _clean_text(subsidiary) and _clean_text(city):
                    contexts.append((_clean_text(subsidiary), _clean_text(city)))
    else:
        cities = [defaults.get("strategic_city_name", "Atlanta, GA"), *city_urls.keys(), *city_overrides.keys()]
        contexts = [(_clean_text(defaults.get("subsidiary_name", "LGEAI")), _clean_text(city)) for city in cities]
    contexts = [(subsidiary, city) for subsidiary, city in dict.fromkeys(contexts) if subsidiary and city]
    if bool(context.get("replace", False)) and contexts:
        allowed_by_subsidiary: dict[str, list[str]] = {}
        for subsidiary, city in contexts:
            allowed_by_subsidiary.setdefault(subsidiary, []).append(city)
        if connection is not None:
            with connection.cursor() as cursor:
                for subsidiary, cities in allowed_by_subsidiary.items():
                    cursor.execute(
                        "delete from common_routing_config_master where subsidiary_name = %s "
                        "and not (strategic_city_name = any(%s))",
                        (subsidiary, cities),
                    )
        else:
            with get_db_connection(config_path) as owned_connection:
                with owned_connection.cursor() as cursor:
                    for subsidiary, cities in allowed_by_subsidiary.items():
                        cursor.execute(
                            "delete from common_routing_config_master where subsidiary_name = %s "
                            "and not (strategic_city_name = any(%s))",
                            (subsidiary, cities),
                        )
                owned_connection.commit()
    for subsidiary, city in contexts:
        if not subsidiary or not city:
            continue
        values = dict(seed)
        values.update(city_overrides.get(city, {}))
        values["subsidiary_name"] = subsidiary
        values["strategic_city_name"] = city
        values["osrm_url"] = city_urls.get(city, values.get("osrm_url"))
        values["timezone_offset"] = values.get("timezone_offset", "-04:00")
        upsert_routing_config(values, config_path, connection=connection)


def _profile_path() -> Path:
    return na_data_path("profile_production")


def _seed_technician_master(config_path: Path, *, connection: Any | None = None) -> None:
    profile = _profile_path()
    slots = pd.read_excel(profile, sheet_name="2. Slot").rename(
        columns={"Name": "employee_name", "SVC_ENGINEER_CODE": "employee_code", "SVC_CENTER_TYPE": "center_type"}
    )
    addresses = pd.read_excel(profile, sheet_name="4. Address").rename(
        columns={"SVC_ENGINEER_CODE": "employee_code", "Name": "employee_name", "Home Street Address": "home_address",
                 "City ": "home_city", "State": "home_state", "Zip": "home_postal_code",
                 "latitude": "home_latitude", "longitude": "home_longitude"}
    )
    for frame, columns in ((slots, ["employee_code", "employee_name", "center_type", "STRATEGIC_CITY_NAME"]),
                           (addresses, ["employee_code", "employee_name", "home_address", "home_city", "home_state", "home_postal_code"])):
        for column in columns:
            if column not in frame.columns:
                frame[column] = ""
    merged = slots.merge(addresses, on=["employee_code", "employee_name"], how="left", suffixes=("", "_address"))
    for _, row in merged.iterrows():
        upsert_technician_master(
            {
                "subsidiary_name": "LGEAI", "strategic_city_name": _clean_text(row.get("STRATEGIC_CITY_NAME")),
                "employee_code": _clean_text(row.get("employee_code")), "employee_name": _clean_text(row.get("employee_name")),
                "center_type": _clean_text(row.get("center_type")).upper(), "home_address": row.get("home_address"),
                "home_city": row.get("home_city"), "home_state": row.get("home_state"), "home_country": "USA",
                "home_postal_code": row.get("home_postal_code"), "home_latitude": row.get("home_latitude"),
                "home_longitude": row.get("home_longitude"), "active_flag": True, "priority_group": "B",
            }, config_path, connection=connection,
        )


def _seed_technician_capabilities(config_path: Path, *, connection: Any | None = None) -> None:
    products = pd.read_excel(_profile_path(), sheet_name="3. Product")
    required = ["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "REPAIR_FLAG", "AREA_PRODUCT_FLAG"]
    for column in required:
        if column not in products.columns:
            products[column] = ""
    rows = []
    for _, value in products.iterrows():
        employee = _clean_text(value["SVC_ENGINEER_CODE"])
        group = _clean_text(value["SERVICE_PRODUCT_GROUP_CODE"])
        product = _clean_text(value["SERVICE_PRODUCT_CODE"])
        city = _clean_text(value["STRATEGIC_CITY_NAME"])
        if employee and group and city:
            rows.append(("LGEAI", city, employee, group, product,
                         _clean_text(value["REPAIR_FLAG"]).upper() == "T",
                         not (group == "REF" and _clean_text(value["AREA_PRODUCT_FLAG"]).upper() == "N"), 100, None, None))
    _execute_values_upsert(
        "common_technician_capability_master",
        ["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code", "repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date"],
        list(dict.fromkeys(rows)), ["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"],
        ["repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date"],
        config_path, connection=connection,
    )


def _region_seed_specs(config_path: Path) -> list[dict[str, Any]]:
    cfg = load_common_config(config_path)
    defaults = cfg.get("defaults", {}) if isinstance(cfg.get("defaults"), dict) else {}
    configured = cfg.get("region_seed_files")
    if not isinstance(configured, list) or not configured:
        configured = [{"subsidiary_name": defaults.get("subsidiary_name", "LGEAI"),
                       "strategic_city_name": defaults.get("strategic_city_name", "Atlanta, GA"),
                       "file": str(na_data_path("region_seed_dir") / "atlanta_fixed_region_zip_3.csv")}]
    return [spec for spec in configured if isinstance(spec, dict) and _clean_text(spec.get("file") or spec.get("path"))]


def _seed_region_master(config_path: Path, *, connection: Any | None = None) -> None:
    defaults = load_common_config(config_path).get("defaults", {})
    for spec in _region_seed_specs(config_path):
        source = Path(_clean_text(spec.get("file") or spec.get("path")))
        if not source.is_absolute() and not source.exists():
            source = na_data_path("region_seed_dir") / source.name
        if not source.exists():
            continue
        subsidiary = _clean_text(spec.get("subsidiary_name")) or _clean_text(defaults.get("subsidiary_name")) or "LGEAI"
        city = _clean_text(spec.get("strategic_city_name")) or _clean_text(defaults.get("strategic_city_name")) or "Atlanta, GA"
        frame = pd.read_csv(source, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
        if not {"POSTAL_CODE", "region_seq"}.issubset(frame.columns):
            continue
        if "STRATEGIC_CITY_NAME" in frame.columns:
            selected = frame[frame["STRATEGIC_CITY_NAME"].map(_clean_text).eq(city)]
            if not selected.empty:
                frame = selected.copy()
        frame["POSTAL_CODE"] = frame["POSTAL_CODE"].map(normalize_postal_code)
        frame["region_seq"] = pd.to_numeric(frame["region_seq"], errors="coerce")
        frame = frame[frame["POSTAL_CODE"].ne("") & frame["region_seq"].notna()].drop_duplicates("POSTAL_CODE")
        rows = []
        prefix = _clean_text(spec.get("region_name_prefix"))
        for _, value in frame.iterrows():
            names = [_clean_text(value.get(column)) for column in ("new_region_name", "region_name", "AREA_NAME")]
            name = (
                f"{prefix} {int(value['region_seq'])}"
                if prefix else next((candidate for candidate in names if candidate), f"Region {int(value['region_seq'])}")
            )
            latitude = pd.to_numeric(pd.Series([value.get("region_center_latitude", value.get("latitude"))]), errors="coerce").iloc[0]
            longitude = pd.to_numeric(pd.Series([value.get("region_center_longitude", value.get("longitude"))]), errors="coerce").iloc[0]
            rows.append((subsidiary, city, value["POSTAL_CODE"], int(value["region_seq"]), name,
                         _clean_text(value.get("area_type")) or None, float(latitude) if pd.notna(latitude) else None,
                         float(longitude) if pd.notna(longitude) else None))
        columns = ["subsidiary_name", "strategic_city_name", "postal_code", "region_seq", "region_name", "area_type", "region_center_latitude", "region_center_longitude"]
        conflict = ["subsidiary_name", "strategic_city_name", "postal_code"]
        updates = ["region_seq", "region_name", "area_type", "region_center_latitude", "region_center_longitude"]
        if connection is not None:
            with connection.cursor() as cursor:
                cursor.execute("delete from common_region_master where subsidiary_name = %s and strategic_city_name = %s", (subsidiary, city))
            _execute_values_upsert("common_region_master", columns, rows, conflict, updates, config_path, connection=connection)
        else:
            with get_db_connection(config_path) as owned_connection:
                with owned_connection.cursor() as cursor:
                    cursor.execute("delete from common_region_master where subsidiary_name = %s and strategic_city_name = %s", (subsidiary, city))
                _execute_values_upsert("common_region_master", columns, rows, conflict, updates, config_path, connection=owned_connection)
                owned_connection.commit()


def _seed_heavy_repair_rules(config_path: Path, *, connection: Any | None = None) -> None:
    rules = load_heavy_repair_rules()
    _execute_values_upsert(
        "common_heavy_repair_rule_master", ["product_group_code", "product_code", "detailed_symptom_code"],
        [tuple(row) for row in rules.itertuples(index=False, name=None)], ["product_group_code", "product_code", "detailed_symptom_code"], [], config_path, connection=connection,
    )


def seed_default_masters(
    config_path: Path = COMMON_CONFIG_PATH, *, connection: Any | None = None, initialize_schema: bool = True,
) -> None:
    if initialize_schema:
        if connection is not None:
            raise ValueError("initialize_schema must be false when using an existing transaction connection.")
        init_schema(config_path)
    options = load_common_config(config_path).get("master_seed", {})
    _seed_routing_config(config_path, connection=connection)
    if not isinstance(options, dict) or bool(options.get("technician_master", True)):
        _seed_technician_master(config_path, connection=connection)
    _seed_technician_capabilities(config_path, connection=connection)
    _seed_region_master(config_path, connection=connection)
    _seed_heavy_repair_rules(config_path, connection=connection)
