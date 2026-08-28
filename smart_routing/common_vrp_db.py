from __future__ import annotations

import json
import os
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from .area_map import get_latest_geocoded_service_file
from .census_geocoder import normalize_postal_code
from .data_catalog import na_data_path
from .live_atlanta_runtime import _load_config as _load_runtime_config
from .live_atlanta_runtime import _merge_service_geocodes


warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMMON_CONFIG_PATH = Path(
    os.environ.get("COMMON_VRP_CONFIG_PATH", str(PROJECT_ROOT / "config" / "common_vrp.prod.json"))
).resolve()
GEOCODE_CACHE_RETENTION_DAYS = 7
GEOCODE_ATTEMPT_RETENTION_DAYS = 400


def _active_profile_path() -> Path:
    """Resolve the selected catalog at call time, after environment setup."""
    return na_data_path("profile_production")


def _default_region_zip_path() -> Path:
    """Resolve the selected catalog at call time, after environment setup."""
    return na_data_path("region_seed_dir") / "atlanta_fixed_region_zip_3.csv"


def _default_heavy_repair_lookup_path() -> Path:
    """Resolve after an admin CLI has selected its data catalog."""
    return na_data_path("heavy_repair_lookup")


def _default_symptom_path() -> Path:
    """Resolve after an admin CLI has selected its data catalog."""
    return na_data_path("symptom_mapping")


def _clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


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


def _coerce_priority_group_label(value: Any, default: str = "B") -> str:
    if pd.isna(value):
        return default
    text = str(value).strip().upper()
    if text in {"A", "HIGH", "P3", "PRIORITY 3", "3"}:
        return "A"
    if text in {"C", "LOW", "P1", "PRIORITY 1", "1"}:
        return "C"
    if text in {"B", "MEDIUM", "MID", "P2", "PRIORITY 2", "2"}:
        return "B"
    return default


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
    region_policy text,
    region_plan_id text,
    region_plan_revision integer,
    region_plan_checksum char(64),
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
    max_single_leg_min integer,
    max_home_to_job_min integer,
    long_leg_penalty_start_min integer,
    long_leg_penalty_multiplier numeric,
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
    area_type text,
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
    priority_group text not null default 'B',
    max_home_to_job_min integer,
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
    fixed boolean not null default false,
    reschedule boolean not null default false,
    job_slot_count integer not null default 1,
    latitude double precision,
    longitude double precision,
    source text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (record_id),
    unique (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no)
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
    priority_group text not null default 'B',
    preferred_region_name text,
    max_minutes integer,
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

create table if not exists common_avoid_area (
    avoid_area_id text not null,
    subsidiary_name text not null,
    strategic_city_name text not null,
    area_name text not null,
    description text,
    geometry_json text not null,
    active_flag boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (avoid_area_id)
);

create table if not exists common_geocode_cache (
    address_key text not null,
    source_bucket text not null,
    address_line1 text,
    city text,
    state text,
    postal_code text,
    country_name text,
    matched_address text,
    match_indicator text,
    match_type text,
    longitude double precision,
    latitude double precision,
    tiger_line_id text,
    tiger_line_side text,
    census_state_fips text,
    census_county_fips text,
    census_tract text,
    census_block text,
    geocoded_date date,
    source text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (address_key, source_bucket)
);

create table if not exists common_geocode_attempt_log (
    address_key text not null,
    source_bucket text not null,
    attempted_date date not null,
    status text,
    source text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (address_key, source_bucket, attempted_date)
);

create table if not exists common_geocode_daily_log (
    run_date date not null,
    source_bucket text not null,
    used_count integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (run_date, source_bucket)
);
"""


GEOCODE_ATTEMPT_LOG_MIGRATION_SQL = """
alter table if exists common_geocode_attempt_log
add column if not exists source_bucket text;

update common_geocode_attempt_log
set source_bucket = case
    when lower(coalesce(source, '')) like '%here%' then 'here'
    when lower(coalesce(source, '')) like '%google%' then 'google'
    when lower(coalesce(source, '')) like '%census%' then 'census'
    else 'default'
end
where source_bucket is null or btrim(source_bucket) = '';

alter table if exists common_geocode_attempt_log
alter column source_bucket set not null;

do $$
declare
    current_pk_name text;
    current_pk_columns text[];
begin
    select c.conname,
           array_agg(a.attname::text order by k.ordinality)
    into current_pk_name, current_pk_columns
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    join unnest(c.conkey) with ordinality as k(attnum, ordinality) on true
    join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
    where t.relname = 'common_geocode_attempt_log'
      and n.nspname = current_schema()
      and c.contype = 'p'
    group by c.conname;

    if current_pk_name is not null
       and current_pk_columns <> array['address_key', 'source_bucket', 'attempted_date']::text[] then
        execute format(
            'alter table common_geocode_attempt_log drop constraint %I',
            current_pk_name
        );
        current_pk_name := null;
    end if;

    if current_pk_name is null then
        alter table common_geocode_attempt_log
        add constraint common_geocode_attempt_log_pkey
        primary key (address_key, source_bucket, attempted_date);
    end if;
end $$;
"""


def init_schema(config_path: Path = COMMON_CONFIG_PATH) -> None:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute(GEOCODE_ATTEMPT_LOG_MIGRATION_SQL)
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists max_single_leg_min integer
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists max_home_to_job_min integer
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists long_leg_penalty_start_min integer
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists long_leg_penalty_multiplier numeric
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists region_policy text
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists region_plan_id text
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists region_plan_revision integer
                """
            )
            cur.execute(
                """
                alter table if exists common_routing_config_master
                add column if not exists region_plan_checksum char(64)
                """
            )
            cur.execute(
                """
                alter table if exists common_region_master
                add column if not exists area_type text
                """
            )
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
                alter table if exists common_request_technician_input
                add column if not exists priority_group text not null default 'B'
                """
            )
            cur.execute(
                """
                alter table if exists common_request_technician_input
                add column if not exists max_minutes integer
                """
            )
            cur.execute(
                """
                alter table if exists common_request_technician_input
                add column if not exists preferred_region_name text
                """
            )
            cur.execute(
                """
                alter table if exists common_request_technician_input
                alter column priority_group drop default
                """
            )
            cur.execute(
                """
                alter table if exists common_request_technician_input
                alter column priority_group type text
                using case
                    when upper(priority_group::text) in ('A', '3', 'P3', 'PRIORITY 3') then 'A'
                    when upper(priority_group::text) in ('C', '1', 'P1', 'PRIORITY 1') then 'C'
                    else 'B'
                end,
                alter column priority_group set default 'B'
                """
            )
            cur.execute(
                """
                alter table if exists common_technician_master
                add column if not exists max_home_to_job_min integer
                """
            )
            cur.execute(
                """
                alter table if exists common_technician_master
                add column if not exists priority_group text not null default 'B'
                """
            )
            cur.execute(
                """
                alter table if exists common_technician_master
                alter column priority_group drop default
                """
            )
            cur.execute(
                """
                alter table if exists common_technician_master
                alter column priority_group type text
                using case
                    when upper(priority_group::text) in ('A', '3', 'P3', 'PRIORITY 3') then 'A'
                    when upper(priority_group::text) in ('C', '1', 'P1', 'PRIORITY 1') then 'C'
                    else 'B'
                end,
                alter column priority_group set default 'B'
                """
            )
            cur.execute(
                """
                alter table if exists common_job_input
                add column if not exists fixed boolean not null default false
                """
            )
            cur.execute(
                """
                alter table if exists common_job_input
                add column if not exists reschedule boolean not null default false
                """
            )
            cur.execute(
                """
                alter table if exists common_job_input
                add column if not exists job_slot_count integer not null default 1
                """
            )
            cur.execute(
                """
                do $$
                begin
                    if exists (
                        select 1
                        from information_schema.columns
                        where table_name = 'common_job_input'
                          and column_name = 'two_slot_job'
                    ) then
                        update common_job_input
                        set job_slot_count = case when two_slot_job then greatest(job_slot_count, 2) else job_slot_count end;
                    end if;
                end $$;
                """
            )
            cur.execute(
                """
                alter table if exists common_job_input
                drop column if exists two_slot_job
                """
            )
            cur.execute(
                """
                create index if not exists common_geocode_cache_updated_at_idx
                on common_geocode_cache (updated_at)
                """
            )
            cur.execute(
                """
                create index if not exists common_geocode_attempt_log_updated_at_idx
                on common_geocode_attempt_log (updated_at)
                """
            )
            cur.execute(
                """
                delete from common_geocode_cache
                where updated_at < now() - interval '7 days'
                """
            )
            cur.execute(
                """
                delete from common_geocode_attempt_log
                where attempted_date < current_date - (%s || ' days')::interval
                """,
                (GEOCODE_ATTEMPT_RETENTION_DAYS,),
            )
            cur.execute(
                """
                delete from common_geocode_daily_log
                where updated_at < now() - interval '7 days'
                """
            )
            cur.execute(
                """
                do $$
                declare
                    old_constraint_name text;
                begin
                    select c.conname
                    into old_constraint_name
                    from pg_constraint c
                    join pg_class t on t.oid = c.conrelid
                    join pg_namespace n on n.oid = t.relnamespace
                    where t.relname = 'common_job_input'
                      and n.nspname = current_schema()
                      and c.contype = 'u'
                      and (
                          select array_agg(att.attname::text order by u.ord)
                          from unnest(c.conkey) with ordinality as u(attnum, ord)
                          join pg_attribute att on att.attrelid = c.conrelid and att.attnum = u.attnum
                      ) = array['subsidiary_name', 'strategic_city_name', 'gsfs_receipt_no']::text[];
                    if old_constraint_name is not null then
                        execute format('alter table common_job_input drop constraint %I', old_constraint_name);
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
                        where table_name = 'common_job_input'
                          and constraint_name = 'common_job_input_context_date_receipt_key'
                    ) then
                        alter table common_job_input
                        add constraint common_job_input_context_date_receipt_key
                        unique (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no);
                    end if;
                end $$;
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
    *,
    connection: Any | None = None,
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
    if connection is not None:
        with connection.cursor() as cur:
            execute_values(cur, sql, rows)
        return len(rows)
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    return len(rows)


GEOCODE_CACHE_COLUMNS = [
    "address_key",
    "address_line1",
    "city",
    "state",
    "postal_code",
    "country_name",
    "matched_address",
    "match_indicator",
    "match_type",
    "longitude",
    "latitude",
    "tiger_line_id",
    "tiger_line_side",
    "census_state_fips",
    "census_county_fips",
    "census_tract",
    "census_block",
    "geocoded_date",
    "source",
]


def _source_bucket_from_path(path: Path | str) -> str:
    text = str(path).lower()
    if "here" in text:
        return "here"
    if "google" in text:
        return "google"
    if "census" in text:
        return "census"
    return "default"


def cleanup_geocode_cache(
    retention_days: int = GEOCODE_CACHE_RETENTION_DAYS,
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    attempt_retention_days: int = GEOCODE_ATTEMPT_RETENTION_DAYS,
) -> None:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from common_geocode_cache where updated_at < now() - (%s || ' days')::interval",
                (int(retention_days),),
            )
            cur.execute(
                "delete from common_geocode_attempt_log "
                "where attempted_date < current_date - (%s || ' days')::interval",
                (int(attempt_retention_days),),
            )
            cur.execute(
                "delete from common_geocode_daily_log where updated_at < now() - (%s || ' days')::interval",
                (int(retention_days),),
            )
        conn.commit()


def load_geocode_cache_df(path: Path | str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    bucket = _source_bucket_from_path(path)
    df = _fetch_df(
        f"""
        select {", ".join(GEOCODE_CACHE_COLUMNS)}
        from common_geocode_cache
        where source_bucket = %s
          and updated_at >= now() - interval '7 days'
        order by updated_at desc
        """,
        (bucket,),
        config_path=config_path,
    )
    return df


def upsert_geocode_cache_df(path: Path | str, df: pd.DataFrame, config_path: Path = COMMON_CONFIG_PATH) -> int:
    if df.empty:
        return 0
    bucket = _source_bucket_from_path(path)
    working = df.copy()
    for col in GEOCODE_CACHE_COLUMNS:
        if col not in working.columns:
            working[col] = None
    working = working[working["address_key"].astype(str).str.strip() != ""].copy()
    if working.empty:
        return 0
    for col in ["longitude", "latitude"]:
        working[col] = pd.to_numeric(working[col], errors="coerce")
    working["geocoded_date"] = pd.to_datetime(working["geocoded_date"], errors="coerce").dt.date
    working = working.where(pd.notna(working), None)
    working["source_bucket"] = bucket
    columns = ["address_key", "source_bucket"] + [col for col in GEOCODE_CACHE_COLUMNS if col != "address_key"]
    rows = [tuple(row.get(col) for col in columns) for _, row in working.iterrows()]
    return _execute_values_upsert(
        "common_geocode_cache",
        columns,
        rows,
        ["address_key", "source_bucket"],
        [col for col in columns if col not in {"address_key", "source_bucket"}],
        config_path=config_path,
    )


def load_geocode_daily_log(source_bucket: str = "census", config_path: Path = COMMON_CONFIG_PATH) -> dict[str, int]:
    df = _fetch_df(
        """
        select run_date, used_count
        from common_geocode_daily_log
        where source_bucket = %s and updated_at >= now() - interval '7 days'
        """,
        (source_bucket,),
        config_path=config_path,
    )
    if df.empty:
        return {}
    return {str(pd.to_datetime(row["run_date"]).date()): int(row["used_count"] or 0) for _, row in df.iterrows()}


def increment_geocode_daily_log(
    run_date: str,
    added_count: int,
    source_bucket: str = "census",
    config_path: Path = COMMON_CONFIG_PATH,
) -> None:
    if int(added_count) <= 0:
        return
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into common_geocode_daily_log (run_date, source_bucket, used_count)
                values (%s, %s, %s)
                on conflict (run_date, source_bucket)
                do update set used_count = common_geocode_daily_log.used_count + excluded.used_count,
                              updated_at = now()
                """,
                (run_date, source_bucket, int(added_count)),
            )
        conn.commit()


def load_geocode_attempt_log_df(path: Path | str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    bucket = _source_bucket_from_path(path)
    df = _fetch_df(
        """
        select address_key, attempted_date, status, source
        from common_geocode_attempt_log
        where source_bucket = %s
          and attempted_date >= current_date - (%s || ' days')::interval
        order by updated_at desc
        """,
        (bucket, GEOCODE_ATTEMPT_RETENTION_DAYS),
        config_path=config_path,
    )
    return df


def upsert_geocode_attempt_log_df(path: Path | str, df: pd.DataFrame, config_path: Path = COMMON_CONFIG_PATH) -> int:
    if df.empty:
        return 0
    bucket = _source_bucket_from_path(path)
    working = df.copy()
    for col in ["address_key", "attempted_date", "status", "source"]:
        if col not in working.columns:
            working[col] = None
    working = working[working["address_key"].astype(str).str.strip() != ""].copy()
    if working.empty:
        return 0
    working["attempted_date"] = pd.to_datetime(working["attempted_date"], errors="coerce").dt.date
    working = working[working["attempted_date"].notna()].copy()
    if working.empty:
        return 0
    working = working.where(pd.notna(working), None)
    working["source_bucket"] = bucket
    columns = ["address_key", "source_bucket", "attempted_date", "status", "source"]
    rows = [tuple(row.get(col) for col in columns) for _, row in working.iterrows()]
    return _execute_values_upsert(
        "common_geocode_attempt_log",
        columns,
        rows,
        ["address_key", "source_bucket", "attempted_date"],
        ["status", "source"],
        config_path=config_path,
    )


def list_contexts(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any]:
    df = _fetch_df(
        """
        select distinct subsidiary_name, strategic_city_name
        from common_routing_config_master
        order by subsidiary_name, strategic_city_name
        """,
        config_path=config_path,
    )
    cities_by_subsidiary: dict[str, list[str]] = {}
    if not df.empty:
        for subsidiary_name, group in df.dropna(subset=["subsidiary_name", "strategic_city_name"]).groupby("subsidiary_name"):
            cities_by_subsidiary[str(subsidiary_name)] = sorted(
                group["strategic_city_name"].dropna().astype(str).unique().tolist()
            )
    return {
        "subsidiaries": sorted(df["subsidiary_name"].dropna().astype(str).unique().tolist()),
        "cities": sorted(df["strategic_city_name"].dropna().astype(str).unique().tolist()),
        "cities_by_subsidiary": cities_by_subsidiary,
    }


def get_active_region_plan_snapshot(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    """Read the additive plan tables directly; runtime ZIP never imports admin_tools."""
    runtime_config = load_common_config(config_path)
    environment = str(runtime_config.get("environment", "")).strip().lower()
    # Atlanta_6area has only been approved for Development verification.
    # Production promotion is not implemented; a config-only bypass would
    # incorrectly turn restored/manual rows into an approved runtime policy.
    if environment == "production":
        raise RuntimeError("REGION_PLAN_RUNTIME_DISABLED_IN_PRODUCTION")
    try:
        active_df = _fetch_df(
            """
            select p.*, c.context_status, c.source_strategic_city_name,
                   a.activation_revision, a.preview_digest
            from common_region_plan_activation a
            join common_region_plan p using (subsidiary_name, strategic_city_name, plan_id)
            join common_city_context c using (subsidiary_name, strategic_city_name)
            where a.subsidiary_name = %s and a.strategic_city_name = %s and a.active_flag = true
            """,
            (subsidiary_name, strategic_city_name), config_path=config_path,
        )
    except Exception as exc:
        raise RuntimeError("REGION_PLAN_RUNTIME_REPOSITORY_UNAVAILABLE") from exc
    if active_df.empty:
        raise RuntimeError("ACTIVE_REGION_PLAN_NOT_FOUND")
    plan = active_df.iloc[0].to_dict()
    plan_id = str(plan.get("plan_id", "")).strip()
    if not plan_id:
        raise RuntimeError("ACTIVE_REGION_PLAN_NOT_FOUND")
    params = (subsidiary_name, strategic_city_name, plan_id)
    try:
        regions = _fetch_df("select * from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by region_seq", params, config_path=config_path)
        postals = _fetch_df(
            """select p.*, r.region_name from common_region_plan_postal p join common_region_plan_region r
               on (r.subsidiary_name,r.strategic_city_name,r.plan_id,r.region_seq)=(p.subsidiary_name,p.strategic_city_name,p.plan_id,p.region_seq)
               where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s order by p.postal_code""",
            params, config_path=config_path,
        )
        technicians = _fetch_df("select * from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by employee_code", params, config_path=config_path)
        overflow = _fetch_df("select * from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by postal_code", params, config_path=config_path)
    except Exception as exc:
        raise RuntimeError("REGION_PLAN_RUNTIME_REPOSITORY_UNAVAILABLE") from exc
    return {
        "enabled": True,
        "status": str(plan.get("plan_status", "")),
        "context_status": str(plan.get("context_status", "")),
        "source_strategic_city_name": str(plan.get("source_strategic_city_name", "")).strip(),
        "plan_id": plan_id,
        "revision": plan.get("revision"),
        "policy_version": str(plan.get("policy_version", "")),
        "checksum": str(plan.get("bundle_sha256", "")),
        "activation_revision": plan.get("activation_revision"),
        "regions": regions.where(pd.notna(regions), None).to_dict("records"),
        "postals": postals.where(pd.notna(postals), None).to_dict("records"),
        "technicians": technicians.where(pd.notna(technicians), None).to_dict("records"),
        "boundary_overflow": overflow.where(pd.notna(overflow), None).to_dict("records"),
    }


def list_active_region_plan_contexts(config_path: Path = COMMON_CONFIG_PATH) -> list[dict[str, str]]:
    """List Development plan contexts without exposing plan membership."""
    runtime_config = load_common_config(config_path)
    if str(runtime_config.get("environment", "")).strip().lower() == "production":
        return []
    try:
        rows = _fetch_df(
            """select a.subsidiary_name, a.strategic_city_name
               from common_region_plan_activation a
               join common_region_plan p using (subsidiary_name, strategic_city_name, plan_id)
               join common_city_context c using (subsidiary_name, strategic_city_name)
               where a.active_flag = true and p.plan_status = 'active'
                 and coalesce(c.context_status, 'active') = 'active'
               order by a.subsidiary_name, a.strategic_city_name""",
            config_path=config_path,
        )
    except Exception as exc:
        raise RuntimeError("REGION_PLAN_RUNTIME_REPOSITORY_UNAVAILABLE") from exc
    return rows.where(pd.notna(rows), "").to_dict("records")


def region_plan_operation(
    operation: str,
    payload: dict[str, Any],
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    """Runtime has read-only plan access; admin writes remain a separate artifact."""
    subsidiary_name = str(payload.get("subsidiary_name", "LGEAI"))
    strategic_city_name = str(payload.get("strategic_city_name", payload.get("city_key", "Atlanta_6area")))
    if operation in {"active", "get", "list"}:
        try:
            snapshot = get_active_region_plan_snapshot(subsidiary_name, strategic_city_name, config_path=config_path)
        except RuntimeError as exc:
            if operation == "list" and str(exc) == "ACTIVE_REGION_PLAN_NOT_FOUND":
                return {"plans": []}
            raise
        if operation == "list":
            # Keep list safe and small; full membership is available only from
            # the selected active plan read endpoint.
            return {"plans": [{key: snapshot.get(key) for key in ("plan_id", "revision", "policy_version", "checksum", "activation_revision", "status", "context_status")}]}
        return snapshot
    raise RuntimeError("REGION_PLAN_OPERATION_UNAVAILABLE_IN_RUNTIME")


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


def list_region_plan_options(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    """List active Area Map Plans available to one operational city.

    Older imports used names such as ``Atlanta_6area`` as the Region Plan
    context key.  The new client treats ``Atlanta, GA`` as the physical city,
    but this compatibility query still exposes those plans when their city
    context declares Atlanta as the source city.
    """
    try:
        # Canonical Area Plan inventory is keyed by subsidiary + operational
        # city + plan.  The legacy storage city is only used to join existing
        # Region/ZIP/Technician detail tables.
        return _fetch_df(
            """
            select ap.plan_id,
                   ap.plan_revision,
                   ap.checksum,
                   ap.plan_status as lifecycle,
                   ap.legacy_storage_city_name as plan_storage_city_name,
                   ap.city_name as source_strategic_city_name,
                   ap.legacy_storage_city_name
              from common_area_plan ap
             where ap.subsidiary_name = %s
               and ap.city_name = %s
               and ap.plan_status = 'active'
             order by ap.updated_at desc, ap.plan_id
            """,
            (subsidiary_name, strategic_city_name),
            config_path=config_path,
        )
    except Exception:
        pass
    try:
        return _fetch_df(
            """
            select rp.routing_plan_id as plan_id,
                   rp.revision as plan_revision,
                   rp.policy_version,
                   rp.source_sha256 as checksum,
                   rs.source_sha256 as region_set_source_sha256,
                   rp.plan_status as lifecycle,
                   rp.strategic_city_name as plan_storage_city_name,
                   rp.source_strategic_city_name
              from common_routing_plan rp
              join common_region_set rs
                on rs.subsidiary_name = rp.subsidiary_name
               and rs.source_strategic_city_name = rp.source_strategic_city_name
               and rs.region_set_id = rp.region_set_id
             where rp.subsidiary_name = %s
               and (rp.strategic_city_name = %s or rp.source_strategic_city_name = %s)
               and rp.plan_status = 'active'
             order by case when rp.strategic_city_name = %s then 0 else 1 end,
                      rp.updated_at desc, rp.routing_plan_id
            """,
            (subsidiary_name, strategic_city_name, strategic_city_name, strategic_city_name),
            config_path=config_path,
        )
    except Exception:
        try:
            return _fetch_df(
                """
            select p.plan_id,
                   p.revision as plan_revision,
                   p.policy_version,
                   p.bundle_sha256 as checksum,
                   p.fixed_region_sha256 as region_set_source_sha256,
                   p.plan_status as lifecycle,
                   p.strategic_city_name as plan_storage_city_name,
                   c.source_strategic_city_name
              from common_region_plan p
              left join common_city_context c
                on c.subsidiary_name = p.subsidiary_name
               and c.strategic_city_name = p.strategic_city_name
             where p.subsidiary_name = %s
               and (p.strategic_city_name = %s or c.source_strategic_city_name = %s)
               and p.plan_status = 'active'
             order by case when p.strategic_city_name = %s then 0 else 1 end,
                      p.updated_at desc, p.plan_id
            """,
            (subsidiary_name, strategic_city_name, strategic_city_name, strategic_city_name),
            config_path=config_path,
            )
        except Exception:
            return pd.DataFrame()


def list_region_set_options(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    """List reusable Region Sets and the Routing Plans that reference them."""
    try:
        return _fetch_df(
            """
            select rs.region_set_id,
                   rs.region_set_name,
                   rs.region_count,
                   rp.routing_plan_id as plan_id,
                   rp.strategic_city_name as plan_storage_city_name,
                   rp.source_strategic_city_name,
                   rp.policy_version,
                   rp.overlap_policy,
                   rp.plan_status as lifecycle,
                   rp.revision as plan_revision,
                   rp.source_sha256 as checksum
              from common_region_set rs
              join common_routing_plan rp
                on rp.subsidiary_name = rs.subsidiary_name
               and rp.source_strategic_city_name = rs.source_strategic_city_name
               and rp.region_set_id = rs.region_set_id
             where rs.subsidiary_name = %s
               and (rs.source_strategic_city_name = %s
                    or rp.strategic_city_name = %s)
               and rp.plan_status = 'active'
             order by rs.region_set_name, rp.updated_at desc, rp.routing_plan_id
            """,
            (subsidiary_name, strategic_city_name, strategic_city_name),
            config_path=config_path,
        )
    except Exception:
        # Development databases reconciled before the normalized schema use
        # the compatibility tables until the schema migration is run.
        plans = list_region_plan_options(subsidiary_name, strategic_city_name, config_path=config_path)
        if plans.empty:
            return pd.DataFrame()
        plans = plans.copy()
        plans["region_set_id"] = plans["region_set_source_sha256"].map(
            lambda value: "rs_" + str(value)[:24] if str(value).strip() else ""
        )
        plans["region_set_name"] = plans["plan_storage_city_name"].astype(str) + " Region Set"
        plans["region_count"] = pd.NA
        return plans


def _region_plan_storage_reference(
    subsidiary_name: str,
    strategic_city_name: str,
    plan_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any] | None:
    options = list_region_plan_options(subsidiary_name, strategic_city_name, config_path=config_path)
    if options.empty or "plan_id" not in options.columns:
        return None
    matched = options[options["plan_id"].astype(str).eq(str(plan_id).strip())].head(1)
    if matched.empty:
        return None
    return matched.iloc[0].to_dict()


def _load_region_plan_snapshot(
    subsidiary_name: str,
    requested_city_name: str,
    plan_reference: dict[str, Any],
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    storage_city = str(plan_reference.get("plan_storage_city_name", requested_city_name)).strip()
    plan_id = str(plan_reference.get("plan_id", "")).strip()
    if not storage_city or not plan_id:
        raise RuntimeError("CONFIGURED_REGION_PLAN_NOT_FOUND")
    params = (subsidiary_name, storage_city, plan_id)
    try:
        regions = _fetch_df(
            "select * from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by region_seq",
            params,
            config_path=config_path,
        )
        postals = _fetch_df(
            """select p.*, r.region_name from common_region_plan_postal p join common_region_plan_region r
               on (r.subsidiary_name,r.strategic_city_name,r.plan_id,r.region_seq)=(p.subsidiary_name,p.strategic_city_name,p.plan_id,p.region_seq)
               where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s order by p.postal_code""",
            params,
            config_path=config_path,
        )
        technicians = _fetch_df(
            "select * from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by employee_code",
            params,
            config_path=config_path,
        )
        overflow = _fetch_df(
            "select * from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by postal_code",
            params,
            config_path=config_path,
        )
    except Exception as exc:
        raise RuntimeError("REGION_PLAN_RUNTIME_REPOSITORY_UNAVAILABLE") from exc
    return {
        "enabled": True,
        "status": str(plan_reference.get("lifecycle", plan_reference.get("plan_status", ""))),
        "context_status": "active" if str(plan_reference.get("lifecycle", "")).lower() == "active" else "",
        "source_strategic_city_name": str(plan_reference.get("source_strategic_city_name") or requested_city_name).strip(),
        "configured_city_name": requested_city_name,
        "plan_storage_city_name": storage_city,
        "plan_id": plan_id,
        "revision": plan_reference.get("plan_revision", plan_reference.get("revision")),
        "policy_version": str(plan_reference.get("policy_version", "")),
        "checksum": str(plan_reference.get("checksum", "")),
        "activation_revision": None,
        "regions": regions.where(pd.notna(regions), None).to_dict("records"),
        "postals": postals.where(pd.notna(postals), None).to_dict("records"),
        "technicians": technicians.where(pd.notna(technicians), None).to_dict("records"),
        "boundary_overflow": overflow.where(pd.notna(overflow), None).to_dict("records"),
    }


def get_configured_region_plan_snapshot(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any] | None:
    """Resolve the plan selected in the city routing config.

    Empty configuration preserves the pre-existing active-plan behavior.  Once
    a plan is selected, routing uses that immutable plan rather than whichever
    plan happens to be active in the legacy activation table.
    """
    try:
        config_row = get_routing_config(subsidiary_name, strategic_city_name, config_path=config_path) or {}
    except Exception:
        # Preserve the legacy active-plan path and testability when a city has
        # no reachable database yet.  A configured plan is still fail-closed
        # below once the config row can be read.
        return None
    plan_id = _clean_text(config_row.get("region_plan_id"))
    if not plan_id:
        return None
    reference = _region_plan_storage_reference(
        subsidiary_name,
        strategic_city_name,
        plan_id,
        config_path=config_path,
    )
    if reference is None:
        raise RuntimeError("CONFIGURED_REGION_PLAN_NOT_FOUND")
    if str(reference.get("lifecycle", "")).strip().casefold() != "active":
        raise RuntimeError("CONFIGURED_REGION_PLAN_NOT_ACTIVE")
    expected_revision = config_row.get("region_plan_revision")
    if expected_revision is not None and str(expected_revision).strip() and pd.notna(expected_revision):
        if int(expected_revision) != int(reference.get("plan_revision")):
            raise RuntimeError("CONFIGURED_REGION_PLAN_REVISION_MISMATCH")
    expected_checksum = _clean_text(config_row.get("region_plan_checksum"))
    actual_checksum = _clean_text(reference.get("checksum"))
    if expected_checksum and actual_checksum and expected_checksum != actual_checksum:
        raise RuntimeError("CONFIGURED_REGION_PLAN_CHECKSUM_MISMATCH")
    return _load_region_plan_snapshot(
        subsidiary_name,
        strategic_city_name,
        reference,
        config_path=config_path,
    )


def upsert_routing_config(
    config_row: dict[str, Any],
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> int:
    config_row = dict(config_row)
    if any(key in config_row for key in ("region_plan_id", "region_plan_revision", "region_plan_checksum")):
        environment = str(load_common_config(config_path).get("environment", "")).strip().lower()
        if environment not in {"development", "dev"}:
            raise ValueError("REGION_PLAN_SELECTION_DEVELOPMENT_ONLY")
    plan_id = _clean_text(config_row.get("region_plan_id"))
    if plan_id:
        reference = _region_plan_storage_reference(
            _clean_text(config_row.get("subsidiary_name")),
            _clean_text(config_row.get("strategic_city_name")),
            plan_id,
            config_path=config_path,
        )
        if reference is None:
            raise ValueError("Selected Region Plan was not found for the city.")
        plan_revision = reference.get("plan_revision")
        plan_checksum = _clean_text(reference.get("checksum"))
        supplied_revision = config_row.get("region_plan_revision")
        if supplied_revision is not None and str(supplied_revision).strip() and pd.notna(supplied_revision):
            if int(supplied_revision) != int(plan_revision):
                raise ValueError("Selected Region Plan revision is stale.")
        supplied_checksum = _clean_text(config_row.get("region_plan_checksum"))
        if supplied_checksum and plan_checksum and supplied_checksum != plan_checksum:
            raise ValueError("Selected Region Plan checksum is stale.")
        config_row["region_plan_revision"] = plan_revision
        config_row["region_plan_checksum"] = plan_checksum or None
    elif any(key in config_row for key in ("region_plan_id", "region_plan_revision", "region_plan_checksum")):
        config_row["region_plan_revision"] = None
        config_row["region_plan_checksum"] = None
    columns = [
        "subsidiary_name",
        "strategic_city_name",
        "region_policy",
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
        "max_single_leg_min",
        "max_home_to_job_min",
        "long_leg_penalty_start_min",
        "long_leg_penalty_multiplier",
        "timezone_offset",
    ]
    if any(key in config_row for key in ("region_plan_id", "region_plan_revision", "region_plan_checksum")):
        columns[2:2] = ["region_plan_id", "region_plan_revision", "region_plan_checksum"]
    row = tuple(config_row.get(col) for col in columns)
    return _execute_values_upsert(
        "common_routing_config_master",
        columns,
        [row],
        ["subsidiary_name", "strategic_city_name"],
        [col for col in columns if col not in {"subsidiary_name", "strategic_city_name"}],
        config_path=config_path,
        connection=connection,
    )


def list_engineers(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    base_sql = """
        select *
        from common_technician_master
        where subsidiary_name = %s and strategic_city_name = %s
        order by employee_name, employee_code
    """
    try:
        # The lateral lookup binds only the Plan selected in the city config.
        # It also understands legacy plans stored under a target context such
        # as Atlanta_6area while the operational city is Atlanta, GA.
        return _fetch_df(
            """
            select t.*,
                   cfg.region_plan_id,
                   rpt.assigned_region_seq,
                   r.region_id as assigned_region_id,
                   r.region_name as assigned_region_name,
                   r.required_center_type as assigned_region_center_type
              from common_technician_master t
              left join common_routing_config_master cfg
                on cfg.subsidiary_name=t.subsidiary_name
               and cfg.strategic_city_name=t.strategic_city_name
              left join lateral (
                    select p.strategic_city_name as plan_storage_city_name
                      from common_region_plan p
                      left join common_city_context c
                        on c.subsidiary_name=p.subsidiary_name
                       and c.strategic_city_name=p.strategic_city_name
                     where p.subsidiary_name=t.subsidiary_name
                       and p.plan_id=cfg.region_plan_id
                       and p.plan_status='active'
                       and (p.strategic_city_name=t.strategic_city_name
                            or c.source_strategic_city_name=t.strategic_city_name)
                     order by case when p.strategic_city_name=t.strategic_city_name then 0 else 1 end,
                              p.updated_at desc
                     limit 1
              ) plan_ref on true
              left join common_region_plan_technician rpt
                on rpt.subsidiary_name=t.subsidiary_name
               and rpt.strategic_city_name=plan_ref.plan_storage_city_name
               and rpt.plan_id=cfg.region_plan_id
               and rpt.employee_code=t.employee_code
              left join common_region_plan_region r
                on r.subsidiary_name=rpt.subsidiary_name
               and r.strategic_city_name=rpt.strategic_city_name
               and r.plan_id=rpt.plan_id
               and r.region_seq=rpt.assigned_region_seq
             where t.subsidiary_name=%s and t.strategic_city_name=%s
             order by t.employee_name, t.employee_code
            """,
            (subsidiary_name, strategic_city_name),
            config_path=config_path,
        )
    except Exception:
        return _fetch_df(base_sql, (subsidiary_name, strategic_city_name), config_path=config_path)


def upsert_technician_master(
    technician_row: dict[str, Any],
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> int:
    subsidiary_name = _clean_text(technician_row.get("subsidiary_name"))
    strategic_city_name = _clean_text(technician_row.get("strategic_city_name"))
    employee_code = _clean_text(technician_row.get("employee_code"))
    employee_name = _clean_text(technician_row.get("employee_name")) or employee_code
    if not subsidiary_name or not strategic_city_name or not employee_code:
        raise ValueError("subsidiary_name, strategic_city_name, and employee_code are required.")
    region_assignment_requested = bool(_clean_text(technician_row.get("region_plan_id"))) or (
        "assigned_region_seq" in technician_row
        and pd.notna(pd.to_numeric(pd.Series([technician_row.get("assigned_region_seq")]), errors="coerce").iloc[0])
    )
    if connection is None and region_assignment_requested:
        environment = str(load_common_config(config_path).get("environment", "")).strip().lower()
        if environment not in {"development", "dev"}:
            raise ValueError("REGION_ASSIGNMENT_DEVELOPMENT_ONLY")
        # Profile and Plan assignment must commit together when the client
        # edits a Technician Master row with a Region selected.
        with get_db_connection(config_path) as managed_connection:
            return upsert_technician_master(
                technician_row,
                config_path=config_path,
                connection=managed_connection,
            )

    home_address = _clean_text(technician_row.get("home_address"))
    home_city = _clean_text(technician_row.get("home_city"))
    home_state = _clean_text(technician_row.get("home_state"))
    home_country = _clean_text(technician_row.get("home_country")) or "USA"
    home_postal_code = normalize_postal_code(technician_row.get("home_postal_code"))
    home_latitude = pd.to_numeric(pd.Series([technician_row.get("home_latitude")]), errors="coerce").iloc[0]
    home_longitude = pd.to_numeric(pd.Series([technician_row.get("home_longitude")]), errors="coerce").iloc[0]
    priority_group = _coerce_priority_group_label(technician_row.get("priority_group", "B"))
    max_home_to_job_min = pd.to_numeric(pd.Series([technician_row.get("max_home_to_job_min")]), errors="coerce").iloc[0]

    if pd.isna(home_latitude) or pd.isna(home_longitude):
        if any([home_address, home_city, home_state, home_postal_code]):
            geocode_input = pd.DataFrame(
                [
                    {
                        "GSFS_RECEIPT_NO": employee_code,
                        "ADDRESS_LINE1_INFO": home_address,
                        "CITY_NAME": home_city,
                        "STATE_NAME": home_state,
                        "COUNTRY_NAME": home_country,
                        "POSTAL_CODE": home_postal_code,
                    }
                ]
            )
            geocoded_df = _geocode_technician_home_df(geocode_input)
            if not geocoded_df.empty:
                first_row = geocoded_df.iloc[0]
                home_latitude = pd.to_numeric(pd.Series([first_row.get("latitude")]), errors="coerce").iloc[0]
                home_longitude = pd.to_numeric(pd.Series([first_row.get("longitude")]), errors="coerce").iloc[0]

    row = (
        subsidiary_name,
        strategic_city_name,
        employee_code,
        employee_name,
        _clean_text(technician_row.get("center_type", "")).upper() or "DMS",
        home_address,
        home_city,
        home_state,
        home_country,
        home_postal_code,
        float(home_latitude) if pd.notna(home_latitude) else None,
        float(home_longitude) if pd.notna(home_longitude) else None,
        bool(technician_row.get("active_flag", True)),
        priority_group,
        int(max_home_to_job_min) if pd.notna(max_home_to_job_min) else None,
    )
    saved = _execute_values_upsert(
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
            "priority_group",
            "max_home_to_job_min",
        ],
        [row],
        ["subsidiary_name", "strategic_city_name", "employee_code"],
        [
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
            "priority_group",
            "max_home_to_job_min",
        ],
        config_path=config_path,
        connection=connection,
    )
    if region_assignment_requested:
        upsert_region_plan_technician_assignment(
            {
                **technician_row,
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "employee_code": employee_code,
            },
            config_path=config_path,
            connection=connection,
        )
    return saved


def upsert_region_plan_technician_assignment(
    assignment: dict[str, Any],
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> int:
    """Save a technician's Region assignment for one configured Plan.

    Technician profile columns remain in ``common_technician_master``.  This
    child row is deliberately Plan-scoped so changing from 3-area to 6-area
    does not overwrite the technician's profile or another Plan's assignment.
    """
    subsidiary_name = _clean_text(assignment.get("subsidiary_name"))
    city_name = _clean_text(assignment.get("strategic_city_name"))
    plan_id = _clean_text(assignment.get("region_plan_id"))
    employee_code = _clean_text(assignment.get("employee_code"))
    if not all((subsidiary_name, city_name, plan_id, employee_code)):
        raise ValueError("subsidiary_name, strategic_city_name, region_plan_id, and employee_code are required.")
    reference = _region_plan_storage_reference(subsidiary_name, city_name, plan_id, config_path=config_path)
    if reference is None:
        raise ValueError("Selected Region Plan was not found for the city.")
    storage_city = _clean_text(reference.get("plan_storage_city_name")) or city_name
    region_seq = pd.to_numeric(pd.Series([assignment.get("assigned_region_seq")]), errors="coerce").iloc[0]
    if pd.isna(region_seq) or int(region_seq) <= 0:
        raise ValueError("A Region must be selected for the technician.")
    region_seq = int(region_seq)
    if connection is None:
        with get_db_connection(config_path) as managed_connection:
            return upsert_region_plan_technician_assignment(
                assignment,
                config_path=config_path,
                connection=managed_connection,
            )
    conn = connection
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """select required_center_type from common_region_plan_region
                   where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and region_seq=%s""",
                (subsidiary_name, storage_city, plan_id, region_seq),
            )
            region_row = cur.fetchone()
            if region_row is None:
                raise ValueError("Selected Region does not belong to the selected Region Plan.")
            required_center = _clean_text(region_row[0]).upper()
            supplied_center = _clean_text(assignment.get("center_type")).upper()
            if required_center and supplied_center and required_center != supplied_center:
                raise ValueError(f"Technician center type {supplied_center} does not match Region center type {required_center}.")
            cur.execute(
                """select policy_mode from common_region_plan_technician
                   where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and employee_code=%s""",
                (subsidiary_name, storage_city, plan_id, employee_code),
            )
            existing = cur.fetchone()
            policy_mode = _clean_text(assignment.get("policy_mode")) or _clean_text(existing[0] if existing else "") or "active_roster_type_hard_region_soft/v1"
            cur.execute(
                """insert into common_region_plan_technician
                   (subsidiary_name, strategic_city_name, plan_id, employee_code,
                    assigned_region_seq, policy_mode, active_flag)
                   values (%s,%s,%s,%s,%s,%s,%s)
                   on conflict (subsidiary_name, strategic_city_name, plan_id, employee_code)
                   do update set assigned_region_seq=excluded.assigned_region_seq,
                                 policy_mode=excluded.policy_mode,
                                 active_flag=excluded.active_flag""",
                (subsidiary_name, storage_city, plan_id, employee_code, region_seq, policy_mode, _coerce_bool(assignment.get("active_flag", True), default=True)),
            )
            saved = int(cur.rowcount or 0)
        conn.commit()
    return saved


def list_configured_region_plan_regions(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    config_row = get_routing_config(subsidiary_name, strategic_city_name, config_path=config_path) or {}
    plan_id = _clean_text(config_row.get("region_plan_id"))
    if not plan_id:
        return pd.DataFrame()
    reference = _region_plan_storage_reference(subsidiary_name, strategic_city_name, plan_id, config_path=config_path)
    if reference is None:
        return pd.DataFrame()
    return _fetch_df(
        """select region_seq, region_id, region_name, required_center_type, source_territory
           from common_region_plan_region
           where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
           order by region_seq""",
        (subsidiary_name, _clean_text(reference.get("plan_storage_city_name")) or strategic_city_name, plan_id),
        config_path=config_path,
    )


def list_configured_region_plan_postals(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    """Return ZIP-to-Region coverage for the configured or active Area Plan."""
    config_row = get_routing_config(subsidiary_name, strategic_city_name, config_path=config_path) or {}
    plan_id = _clean_text(config_row.get("region_plan_id"))
    storage_city = strategic_city_name
    if plan_id:
        reference = _region_plan_storage_reference(subsidiary_name, strategic_city_name, plan_id, config_path=config_path)
        if reference is not None:
            storage_city = _clean_text(reference.get("plan_storage_city_name")) or strategic_city_name
        else:
            plan_id = ""
    if not plan_id:
        # The routing runtime itself falls back to the active plan when City
        # Config has not yet been saved.  The result map must use the same
        # fallback rather than silently rendering an empty Area layer.
        try:
            active = get_active_region_plan_snapshot(
                subsidiary_name, strategic_city_name, config_path=config_path,
            )
        except RuntimeError:
            active = {}
        plan_id = _clean_text(active.get("plan_id"))
    if not plan_id:
        return pd.DataFrame(columns=["postal_code", "region_seq", "region_name", "area_type"])
    return _fetch_df(
        """select p.postal_code, p.region_seq, r.region_name, p.area_type
             from common_region_plan_postal p
             join common_region_plan_region r
               on (r.subsidiary_name, r.strategic_city_name, r.plan_id, r.region_seq)
                = (p.subsidiary_name, p.strategic_city_name, p.plan_id, p.region_seq)
            where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s
            order by p.postal_code, p.region_seq""",
        (subsidiary_name, storage_city, plan_id),
        config_path=config_path,
    )


def delete_technician_master(
    subsidiary_name: str,
    strategic_city_name: str,
    employee_code: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> int:
    subsidiary_name = _clean_text(subsidiary_name)
    strategic_city_name = _clean_text(strategic_city_name)
    employee_code = _clean_text(employee_code)
    if not subsidiary_name or not strategic_city_name or not employee_code:
        raise ValueError("subsidiary_name, strategic_city_name, and employee_code are required.")
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from common_region_plan_technician
                where employee_code = %s
                  and subsidiary_name = %s
                  and (strategic_city_name = %s or exists (
                      select 1 from common_city_context c
                       where c.subsidiary_name = common_region_plan_technician.subsidiary_name
                         and c.strategic_city_name = common_region_plan_technician.strategic_city_name
                         and c.source_strategic_city_name = %s
                  ))
                """,
                (employee_code, subsidiary_name, strategic_city_name, strategic_city_name),
            )
            cur.execute(
                """
                delete from common_technician_capability_master
                where subsidiary_name = %s and strategic_city_name = %s and employee_code = %s
                """,
                (subsidiary_name, strategic_city_name, employee_code),
            )
            cur.execute(
                """
                delete from common_request_technician_input
                where subsidiary_name = %s and strategic_city_name = %s and employee_code = %s
                """,
                (subsidiary_name, strategic_city_name, employee_code),
            )
            cur.execute(
                """
                delete from common_technician_master
                where subsidiary_name = %s and strategic_city_name = %s and employee_code = %s
                """,
                (subsidiary_name, strategic_city_name, employee_code),
            )
            deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted


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


def list_jobs(subsidiary_name: str, strategic_city_name: str, config_path: Path = COMMON_CONFIG_PATH) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_job_input
        where subsidiary_name = %s and strategic_city_name = %s
        order by promise_date desc, gsfs_receipt_no
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )


def upsert_jobs(rows: list[dict[str, Any]], config_path: Path = COMMON_CONFIG_PATH) -> int:
    columns = [
        "record_id",
        "subsidiary_name",
        "strategic_city_name",
        "svc_engineer_code",
        "svc_engineer_name",
        "service_product_group_code",
        "service_product_code",
        "receipt_detail_symptom_code",
        "gsfs_receipt_no",
        "promise_date",
        "city_name",
        "state_name",
        "country_name",
        "postal_code",
        "address_line1_info",
        "fixed",
        "reschedule",
        "job_slot_count",
        "latitude",
        "longitude",
        "source",
    ]
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        normalized["fixed"] = _coerce_bool(normalized.get("fixed", False), default=False)
        normalized["reschedule"] = _coerce_bool(normalized.get("reschedule", False), default=False)
        numeric_slot = pd.to_numeric(pd.Series([normalized.get("job_slot_count")]), errors="coerce").iloc[0]
        if pd.isna(numeric_slot):
            normalized["job_slot_count"] = 2 if _coerce_bool(normalized.get("two_slot_job", False), default=False) else 1
        else:
            normalized["job_slot_count"] = max(1, int(numeric_slot))
        normalized_rows.append(normalized)
    value_rows = [tuple(row.get(col) for col in columns) for row in normalized_rows]
    if not value_rows:
        return 0
    delete_keys = [
        (
            _clean_text(row.get("record_id")),
            _clean_text(row.get("subsidiary_name")),
            _clean_text(row.get("strategic_city_name")),
            _clean_text(row.get("promise_date")),
            _clean_text(row.get("gsfs_receipt_no")),
        )
        for row in normalized_rows
    ]
    insert_cols = ", ".join(columns)
    update_cols = [col for col in columns if col not in {"subsidiary_name", "strategic_city_name", "promise_date", "gsfs_receipt_no"}]
    update_expr = ", ".join([f"{col}=excluded.{col}" for col in update_cols] + ["updated_at=now()"])
    insert_sql = f"""
        insert into common_job_input ({insert_cols})
        values %s
        on conflict (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no)
        do update set {update_expr}
    """
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                delete from common_job_input existing
                using (values %s) as incoming(record_id, subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no)
                where existing.subsidiary_name = incoming.subsidiary_name
                  and existing.strategic_city_name = incoming.strategic_city_name
                  and (
                    (
                      incoming.promise_date <> ''
                      and incoming.gsfs_receipt_no <> ''
                      and existing.promise_date = incoming.promise_date
                      and existing.gsfs_receipt_no = incoming.gsfs_receipt_no
                    )
                    or (incoming.record_id <> '' and existing.record_id = incoming.record_id)
                  )
                """,
                delete_keys,
            )
            execute_values(cur, insert_sql, value_rows)
        conn.commit()
    return len(value_rows)


def delete_job(
    subsidiary_name: str,
    strategic_city_name: str,
    record_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> int:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from common_job_input
                where subsidiary_name = %s and strategic_city_name = %s and record_id = %s
                """,
                (subsidiary_name, strategic_city_name, record_id),
            )
            deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted


def list_request_technicians(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    return _fetch_df(
        """
        select *
        from common_request_technician_input
        where subsidiary_name = %s and strategic_city_name = %s and promise_date = %s
        order by employee_name, employee_code
        """,
        (subsidiary_name, strategic_city_name, promise_date),
        config_path=config_path,
    )


def replace_request_technicians(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    rows: list[dict[str, Any]],
    config_path: Path = COMMON_CONFIG_PATH,
) -> int:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from common_request_technician_input
                where subsidiary_name = %s and strategic_city_name = %s and promise_date = %s
                """,
                (subsidiary_name, strategic_city_name, promise_date),
            )
        conn.commit()

    columns = [
        "record_id",
        "subsidiary_name",
        "strategic_city_name",
        "promise_date",
        "employee_code",
        "employee_name",
        "center_type",
        "shift_start",
        "shift_end",
        "slot_count",
        "priority_group",
        "preferred_region_name",
        "max_minutes",
        "max_jobs",
        "available",
        "start_location_type",
        "start_location_address",
        "source",
    ]
    normalized_rows: list[tuple[Any, ...]] = []
    for row in rows:
        working = dict(row)
        working["subsidiary_name"] = subsidiary_name
        working["strategic_city_name"] = strategic_city_name
        working["promise_date"] = promise_date
        working["priority_group"] = _coerce_priority_group_label(working.get("priority_group", "B"))
        normalized_rows.append(tuple(working.get(col) for col in columns))
    return _execute_values_upsert(
        "common_request_technician_input",
        columns,
        normalized_rows,
        ["record_id"],
        [col for col in columns if col != "record_id"],
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


def delete_routing_requests_for_date(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    keep_request_id: str | None = None,
    config_path: Path = COMMON_CONFIG_PATH,
) -> None:
    params: list[Any] = [subsidiary_name, strategic_city_name, str(promise_date)]
    keep_clause = ""
    if keep_request_id:
        keep_clause = "and request_id <> %s"
        params.append(str(keep_request_id))

    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                delete from common_routing_result
                where request_id in (
                    select request_id
                    from common_routing_request
                    where subsidiary_name = %s
                      and strategic_city_name = %s
                      and promise_date = %s
                      {keep_clause}
                )
                """,
                tuple(params),
            )
            cur.execute(
                f"""
                delete from common_routing_request
                where subsidiary_name = %s
                  and strategic_city_name = %s
                  and promise_date = %s
                  {keep_clause}
                """,
                tuple(params),
            )
        conn.commit()


def delete_routing_result(request_id: str, config_path: Path = COMMON_CONFIG_PATH) -> None:
    request_id = str(request_id or "").strip()
    if not request_id:
        return
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from common_routing_result
                where request_id = %s
                """,
                (request_id,),
            )
        conn.commit()


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


def list_routing_request_dates(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> list[str]:
    df = _fetch_df(
        """
        select distinct promise_date
        from common_routing_request
        where subsidiary_name = %s and strategic_city_name = %s
        order by promise_date desc
        """,
        (subsidiary_name, strategic_city_name),
        config_path=config_path,
    )
    if df.empty:
        return []
    return [str(value).strip() for value in df["promise_date"].dropna().astype(str).tolist() if str(value).strip()]


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
    """Return postal membership from the selected/active Area Plan only.

    Legacy ``common_region_master`` rows remain available for migration and
    audit, but they must not silently drive the client map after Region Plan
    v2 is enabled.
    """

    snapshot = get_configured_region_plan_snapshot(
        subsidiary_name, strategic_city_name, config_path=config_path
    )
    if snapshot is None:
        try:
            snapshot = get_active_region_plan_snapshot(
                subsidiary_name, strategic_city_name, config_path=config_path
            )
        except RuntimeError:
            snapshot = None
            options = list_region_plan_options(
                subsidiary_name, strategic_city_name, config_path=config_path
            )
            if not options.empty:
                active = options[
                    options["lifecycle"].astype(str).str.casefold().eq("active")
                ].head(1)
                if not active.empty:
                    snapshot = _load_region_plan_snapshot(
                        subsidiary_name,
                        strategic_city_name,
                        active.iloc[0].to_dict(),
                        config_path=config_path,
                    )
    if not snapshot:
        return pd.DataFrame(
            columns=["postal_code", "region_seq", "region_name", "area_type"]
        )
    rows = snapshot.get("postals") or []
    if not isinstance(rows, list):
        return pd.DataFrame(
            columns=["postal_code", "region_seq", "region_name", "area_type"]
        )
    return pd.DataFrame(rows)


def list_avoid_areas(
    subsidiary_name: str,
    strategic_city_name: str,
    active_only: bool = False,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    params: list[Any] = [subsidiary_name, strategic_city_name]
    active_clause = ""
    if active_only:
        active_clause = "and active_flag = true"
    return _fetch_df(
        f"""
        select *
        from common_avoid_area
        where subsidiary_name = %s and strategic_city_name = %s
          {active_clause}
        order by active_flag desc, updated_at desc, area_name
        """,
        tuple(params),
        config_path=config_path,
    )


def upsert_avoid_area(area_row: dict[str, Any], config_path: Path = COMMON_CONFIG_PATH) -> int:
    avoid_area_id = _clean_text(area_row.get("avoid_area_id")) or _clean_text(area_row.get("id"))
    if not avoid_area_id:
        import uuid

        avoid_area_id = uuid.uuid4().hex
    subsidiary_name = _clean_text(area_row.get("subsidiary_name"))
    strategic_city_name = _clean_text(area_row.get("strategic_city_name"))
    area_name = _clean_text(area_row.get("area_name")) or "Avoid Area"
    geometry = area_row.get("geometry")
    geometry_json = area_row.get("geometry_json")
    if geometry_json is None and geometry is not None:
        geometry_json = json.dumps(geometry, ensure_ascii=False)
    geometry_json = _clean_text(geometry_json)
    if not subsidiary_name or not strategic_city_name or not geometry_json:
        raise ValueError("subsidiary_name, strategic_city_name, and geometry are required.")

    return _execute_values_upsert(
        "common_avoid_area",
        [
            "avoid_area_id",
            "subsidiary_name",
            "strategic_city_name",
            "area_name",
            "description",
            "geometry_json",
            "active_flag",
        ],
        [
            (
                avoid_area_id,
                subsidiary_name,
                strategic_city_name,
                area_name,
                _clean_text(area_row.get("description")),
                geometry_json,
                _coerce_bool(area_row.get("active_flag", True), default=True),
            )
        ],
        ["avoid_area_id"],
        ["subsidiary_name", "strategic_city_name", "area_name", "description", "geometry_json", "active_flag"],
        config_path=config_path,
    )


def delete_avoid_area(
    subsidiary_name: str,
    strategic_city_name: str,
    avoid_area_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> int:
    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from common_avoid_area
                where subsidiary_name = %s and strategic_city_name = %s and avoid_area_id = %s
                """,
                (subsidiary_name, strategic_city_name, avoid_area_id),
            )
            deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted


def _seed_routing_config(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> None:
    cfg = load_common_config(config_path)
    seed = cfg.get("routing_seed", {})
    defaults = cfg.get("defaults", {})
    city_osrm_urls = seed.get("city_osrm_urls", {}) or {}
    city_overrides = seed.get("city_overrides", {}) or {}
    context_seed = cfg.get("context_seed", {}) if isinstance(cfg.get("context_seed", {}), dict) else {}
    context_subsidiaries = context_seed.get("subsidiaries", {}) if isinstance(context_seed.get("subsidiaries", {}), dict) else {}

    context_rows: list[tuple[str, str]] = []
    if context_subsidiaries:
        for subsidiary_name, city_names in context_subsidiaries.items():
            for city_name in list(city_names or []):
                clean_subsidiary = _clean_text(subsidiary_name)
                clean_city = _clean_text(city_name)
                if clean_subsidiary and clean_city:
                    context_rows.append((clean_subsidiary, clean_city))
    else:
        strategic_city_names = [str(defaults.get("strategic_city_name", "Atlanta, GA"))]
        strategic_city_names.extend(str(city_name) for city_name in city_osrm_urls.keys())
        strategic_city_names.extend(str(city_name) for city_name in city_overrides.keys())
        context_rows = [
            (_clean_text(defaults.get("subsidiary_name", "LGEAI")), _clean_text(city_name))
            for city_name in dict.fromkeys(name for name in strategic_city_names if str(name).strip())
        ]

    context_rows = list(dict.fromkeys(row for row in context_rows if row[0] and row[1]))
    if bool(context_seed.get("replace", False)) and context_rows:
        configured_subsidiaries = sorted({subsidiary_name for subsidiary_name, _ in context_rows})
        if connection is not None:
            with connection.cursor() as cur:
                for subsidiary_name in configured_subsidiaries:
                    allowed_cities = [city for sub, city in context_rows if sub == subsidiary_name]
                    cur.execute(
                        """
                        delete from common_routing_config_master
                        where subsidiary_name = %s
                          and not (strategic_city_name = any(%s))
                        """,
                        (subsidiary_name, allowed_cities),
                    )
        else:
            with get_db_connection(config_path) as conn:
                with conn.cursor() as cur:
                    for subsidiary_name in configured_subsidiaries:
                        allowed_cities = [city for sub, city in context_rows if sub == subsidiary_name]
                        cur.execute(
                            """
                            delete from common_routing_config_master
                            where subsidiary_name = %s
                              and not (strategic_city_name = any(%s))
                            """,
                            (subsidiary_name, allowed_cities),
                        )
                conn.commit()

    for subsidiary_name, strategic_city_name in context_rows:
        resolved_osrm_url = city_osrm_urls.get(str(strategic_city_name), seed.get("osrm_url"))
        city_seed = dict(seed)
        city_seed.pop("city_osrm_urls", None)
        city_seed.pop("city_overrides", None)
        city_seed.pop("context_seed", None)
        override = city_overrides.get(str(strategic_city_name), {})
        if isinstance(override, dict):
            city_seed.update(override)
        upsert_routing_config(
            {
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "region_policy": city_seed.get("region_policy"),
                "distance_backend": city_seed.get("distance_backend"),
                "assignment_distance_backend": city_seed.get("assignment_distance_backend"),
                "osrm_url": resolved_osrm_url,
                "osrm_profile": city_seed.get("osrm_profile"),
                "effective_service_per_sm": city_seed.get("effective_service_per_sm"),
                "target_sm_per_region": city_seed.get("target_sm_per_region"),
                "service_time_per_job_min": city_seed.get("service_time_per_job_min"),
                "max_work_min_per_sm_day": city_seed.get("max_work_min_per_sm_day"),
                "max_travel_min_per_sm_day": city_seed.get("max_travel_min_per_sm_day"),
                "max_travel_km_per_sm_day": city_seed.get("max_travel_km_per_sm_day"),
                "max_single_leg_min": city_seed.get("max_single_leg_min"),
                "max_home_to_job_min": city_seed.get("max_home_to_job_min"),
                "long_leg_penalty_start_min": city_seed.get("long_leg_penalty_start_min"),
                "long_leg_penalty_multiplier": city_seed.get("long_leg_penalty_multiplier"),
                "timezone_offset": city_seed.get("timezone_offset", "-04:00"),
            },
            config_path=config_path,
            connection=connection,
        )


def _seed_technician_master(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> None:
    profile_path = _active_profile_path()
    slot_df = pd.read_excel(profile_path, sheet_name="2. Slot")
    address_df = pd.read_excel(profile_path, sheet_name="4. Address")
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
                "B",
                None,
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
            "priority_group",
            "max_home_to_job_min",
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "employee_code"],
        ["employee_name", "center_type", "home_address", "home_city", "home_state", "home_country", "home_postal_code", "home_latitude", "home_longitude", "active_flag", "priority_group", "max_home_to_job_min"],
        config_path=config_path,
        connection=connection,
    )


def _seed_technician_capabilities(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> None:
    product_df = pd.read_excel(_active_profile_path(), sheet_name="3. Product")
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
        connection=connection,
    )


def _region_seed_specs(config_path: Path = COMMON_CONFIG_PATH) -> list[dict[str, Any]]:
    cfg = load_common_config(config_path)
    defaults = cfg.get("defaults", {}) if isinstance(cfg.get("defaults", {}), dict) else {}
    specs = cfg.get("region_seed_files")
    if not isinstance(specs, list) or not specs:
        specs = [
            {
                "subsidiary_name": defaults.get("subsidiary_name", "LGEAI"),
                "strategic_city_name": defaults.get("strategic_city_name", "Atlanta, GA"),
                "file": str(_default_region_zip_path()),
            }
        ]

    normalized_specs: list[dict[str, Any]] = []
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        seed_file = _clean_text(spec.get("file") or spec.get("path"))
        if not seed_file:
            continue
        normalized_specs.append(
            {
                "subsidiary_name": _clean_text(spec.get("subsidiary_name")) or defaults.get("subsidiary_name", "LGEAI"),
                "strategic_city_name": _clean_text(spec.get("strategic_city_name"))
                or defaults.get("strategic_city_name", "Atlanta, GA"),
                "file": seed_file,
                "region_name_prefix": _clean_text(spec.get("region_name_prefix")),
            }
        )
    return normalized_specs


def _region_center_lookup(region_df: pd.DataFrame) -> dict[int, tuple[float | None, float | None]]:
    centers: dict[int, tuple[float | None, float | None]] = {}
    if {"latitude", "longitude", "region_seq"}.issubset(region_df.columns):
        region_df["latitude"] = pd.to_numeric(region_df["latitude"], errors="coerce")
        region_df["longitude"] = pd.to_numeric(region_df["longitude"], errors="coerce")
        center_df = region_df.dropna(subset=["latitude", "longitude", "region_seq"]).groupby("region_seq").agg(
            region_center_latitude=("latitude", "mean"),
            region_center_longitude=("longitude", "mean"),
        )
        if not center_df.empty:
            return {
                int(idx): (float(row["region_center_latitude"]), float(row["region_center_longitude"]))
                for idx, row in center_df.reset_index().set_index("region_seq").iterrows()
            }

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
    return centers


def _region_name_for_seed(row: pd.Series, prefix: str = "") -> str:
    if prefix:
        return f"{prefix} {int(row['region_seq'])}"
    for col in ["new_region_name", "region_name", "AREA_NAME"]:
        if col in row.index:
            value = _clean_text(row.get(col))
            if value:
                return value
    return f"Region {int(row['region_seq'])}"


def _replace_region_master_rows(
    subsidiary_name: str,
    strategic_city_name: str,
    rows: list[tuple[Any, ...]],
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> int:
    if connection is not None:
        with connection.cursor() as cur:
            cur.execute(
                """
                delete from common_region_master
                where subsidiary_name = %s and strategic_city_name = %s
                """,
                (subsidiary_name, strategic_city_name),
            )
    else:
        with get_db_connection(config_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    delete from common_region_master
                    where subsidiary_name = %s and strategic_city_name = %s
                    """,
                    (subsidiary_name, strategic_city_name),
                )
            conn.commit()
    return _execute_values_upsert(
        "common_region_master",
        [
            "subsidiary_name",
            "strategic_city_name",
            "postal_code",
            "region_seq",
            "region_name",
            "area_type",
            "region_center_latitude",
            "region_center_longitude",
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "postal_code"],
        ["region_seq", "region_name", "area_type", "region_center_latitude", "region_center_longitude"],
        config_path=config_path,
        connection=connection,
    )


def _seed_region_master(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> None:
    for spec in _region_seed_specs(config_path):
        seed_path = Path(spec["file"])
        if not seed_path.is_absolute() and not seed_path.exists():
            seed_path = na_data_path("region_seed_dir") / seed_path.name
        if not seed_path.exists():
            continue
        subsidiary_name = str(spec["subsidiary_name"])
        strategic_city_name = str(spec["strategic_city_name"])
        region_df = pd.read_csv(seed_path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
        if "POSTAL_CODE" not in region_df.columns or "region_seq" not in region_df.columns:
            continue
        if "STRATEGIC_CITY_NAME" in region_df.columns:
            city_mask = region_df["STRATEGIC_CITY_NAME"].map(_clean_text).eq(strategic_city_name)
            if city_mask.any():
                region_df = region_df[city_mask].copy()
        region_df["POSTAL_CODE"] = region_df["POSTAL_CODE"].astype(str).str.replace(r"\.0+$", "", regex=True).str.strip().str.zfill(5)
        region_df["region_seq"] = pd.to_numeric(region_df["region_seq"], errors="coerce")
        region_df = region_df[region_df["POSTAL_CODE"].ne("") & region_df["region_seq"].notna()].copy()
        if region_df.empty:
            continue
        region_df["region_seq"] = region_df["region_seq"].astype(int)
        centers = _region_center_lookup(region_df)
        region_name_prefix = str(spec.get("region_name_prefix") or "")
        rows = []
        for _, row in region_df.drop_duplicates(subset=["POSTAL_CODE"]).iterrows():
            center = centers.get(int(row["region_seq"]), (None, None))
            rows.append(
                (
                    subsidiary_name,
                    strategic_city_name,
                    str(row["POSTAL_CODE"]).zfill(5),
                    int(row["region_seq"]),
                    _region_name_for_seed(row, region_name_prefix),
                    _clean_text(row.get("area_type")) or None,
                    center[0],
                    center[1],
                )
            )
        _replace_region_master_rows(
            subsidiary_name,
            strategic_city_name,
            rows,
            config_path=config_path,
            connection=connection,
        )


def _seed_heavy_repair_rules(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
) -> None:
    heavy_repair_lookup_path = _default_heavy_repair_lookup_path()
    if heavy_repair_lookup_path.exists():
        lookup_df = pd.read_csv(heavy_repair_lookup_path, encoding="utf-8-sig")
    else:
        lookup_df = pd.read_excel(_default_symptom_path())
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
        connection=connection,
    )


def seed_default_masters(
    config_path: Path = COMMON_CONFIG_PATH,
    *,
    connection: Any | None = None,
    initialize_schema: bool = True,
) -> None:
    cfg = load_common_config(config_path)
    seed_options = cfg.get("master_seed", {}) if isinstance(cfg.get("master_seed", {}), dict) else {}
    if initialize_schema:
        if connection is not None:
            raise ValueError("initialize_schema must be false when using an existing transaction connection.")
        init_schema(config_path)
    _seed_routing_config(config_path, connection=connection)
    if bool(seed_options.get("technician_master", True)):
        _seed_technician_master(config_path, connection=connection)
    _seed_technician_capabilities(config_path, connection=connection)
    _seed_region_master(config_path, connection=connection)
    _seed_heavy_repair_rules(config_path, connection=connection)
