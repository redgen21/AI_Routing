-- Incremental Production -> Development input-data copy.
--
-- Run this file against vrp_db_dev.  It copies only rows with a PROMISE_DATE
-- on/after 2026-07-23 that do not already exist in Development.  Existing
-- Development rows are never updated or deleted.
--
-- The runner creates the dblink extension in vrp_db_dev when it is absent.
-- Supply source_dsn without putting a password in this file, for example:
--   export VRP_PROD_DSN='host=localhost dbname=vrp_db user=vrp_agent password=...'
--   psql -h localhost -U vrp_agent -d vrp_db_dev \
--     -v source_dsn="$VRP_PROD_DSN" \
--     -f /home/csda/AI_Routing/admin_tools/db/runners/sync_vrp_db_jobs_technicians_since_20260723.sql
--
-- Configure the Development credential once in ~/.pgpass (mode 0600):
--   localhost:5432:vrp_db_dev:vrp_agent:<development-db-password>
-- Thereafter psql uses it automatically and never prompts for a password.

\set ON_ERROR_STOP on
\pset pager off

\if :{?source_dsn}
\else
  \echo 'source_dsn is required. No data was changed.'
  \quit
\endif

create extension if not exists dblink;

begin;

select dblink_connect('vrp_prod_sync', :'source_dsn');

create temporary table stage_jobs (
    record_id text,
    subsidiary_name text,
    strategic_city_name text,
    svc_engineer_code text,
    svc_engineer_name text,
    service_product_group_code text,
    service_product_code text,
    receipt_detail_symptom_code text,
    gsfs_receipt_no text,
    promise_date text,
    city_name text,
    state_name text,
    country_name text,
    postal_code text,
    address_line1_info text,
    fixed boolean,
    reschedule boolean,
    job_slot_count integer,
    latitude double precision,
    longitude double precision,
    source text,
    created_at timestamptz,
    updated_at timestamptz
) on commit drop;

insert into stage_jobs
select *
from dblink(
    'vrp_prod_sync',
    $$
      select record_id, subsidiary_name, strategic_city_name,
             svc_engineer_code, svc_engineer_name,
             service_product_group_code, service_product_code,
             receipt_detail_symptom_code, gsfs_receipt_no, promise_date,
             city_name, state_name, country_name, postal_code, address_line1_info,
             fixed, reschedule, job_slot_count, latitude, longitude, source,
             created_at, updated_at
        from common_job_input
       where replace(promise_date, '-', '') >= '20260723'
    $$
) as src(
    record_id text, subsidiary_name text, strategic_city_name text,
    svc_engineer_code text, svc_engineer_name text,
    service_product_group_code text, service_product_code text,
    receipt_detail_symptom_code text, gsfs_receipt_no text, promise_date text,
    city_name text, state_name text, country_name text, postal_code text,
    address_line1_info text, fixed boolean, reschedule boolean,
    job_slot_count integer, latitude double precision, longitude double precision,
    source text, created_at timestamptz, updated_at timestamptz
);

create temporary table stage_technicians (
    record_id text,
    subsidiary_name text,
    strategic_city_name text,
    promise_date text,
    employee_code text,
    employee_name text,
    center_type text,
    shift_start text,
    shift_end text,
    slot_count integer,
    priority_group text,
    preferred_region_name text,
    max_minutes integer,
    max_jobs integer,
    available boolean,
    start_location_type text,
    start_location_address text,
    source text,
    created_at timestamptz,
    updated_at timestamptz
) on commit drop;

insert into stage_technicians
select *
from dblink(
    'vrp_prod_sync',
    $$
      select record_id, subsidiary_name, strategic_city_name, promise_date,
             employee_code, employee_name, center_type, shift_start, shift_end,
             slot_count, priority_group, preferred_region_name, max_minutes,
             max_jobs, available, start_location_type, start_location_address,
             source, created_at, updated_at
        from common_request_technician_input
       where replace(promise_date, '-', '') >= '20260723'
    $$
) as src(
    record_id text, subsidiary_name text, strategic_city_name text,
    promise_date text, employee_code text, employee_name text, center_type text,
    shift_start text, shift_end text, slot_count integer, priority_group text,
    preferred_region_name text, max_minutes integer, max_jobs integer,
    available boolean, start_location_type text, start_location_address text,
    source text, created_at timestamptz, updated_at timestamptz
);

with inserted as (
    insert into common_job_input (
        record_id, subsidiary_name, strategic_city_name,
        svc_engineer_code, svc_engineer_name,
        service_product_group_code, service_product_code,
        receipt_detail_symptom_code, gsfs_receipt_no, promise_date,
        city_name, state_name, country_name, postal_code, address_line1_info,
        fixed, reschedule, job_slot_count, latitude, longitude, source,
        created_at, updated_at
    )
    select s.*
      from stage_jobs s
     where not exists (
        select 1 from common_job_input d
         where d.subsidiary_name = s.subsidiary_name
           and d.strategic_city_name = s.strategic_city_name
           and d.promise_date = s.promise_date
           and d.gsfs_receipt_no = s.gsfs_receipt_no
     )
    on conflict do nothing
    returning 1
)
select count(*) as inserted_new_jobs from inserted;

with inserted as (
    insert into common_request_technician_input (
        record_id, subsidiary_name, strategic_city_name, promise_date,
        employee_code, employee_name, center_type, shift_start, shift_end,
        slot_count, priority_group, preferred_region_name, max_minutes,
        max_jobs, available, start_location_type, start_location_address,
        source, created_at, updated_at
    )
    select s.*
      from stage_technicians s
     where not exists (
        select 1 from common_request_technician_input d
         where d.subsidiary_name = s.subsidiary_name
           and d.strategic_city_name = s.strategic_city_name
           and d.promise_date = s.promise_date
           and d.employee_code = s.employee_code
     )
    on conflict do nothing
    returning 1
)
select count(*) as inserted_new_technicians from inserted;

select dblink_disconnect('vrp_prod_sync');

commit;
