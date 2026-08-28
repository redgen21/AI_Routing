-- Read-only Production vs Development input comparison for Home-based routing.
--
-- Run against vrp_db_dev.  This script never inserts, updates, or deletes a
-- persistent row.  It reads Production through dblink and uses only temporary
-- tables in this psql session.
--
-- Required psql variable:
--   source_dsn : Production DB connection string (do not put its password here)
-- Optional variables (defaults below):
--   subsidiary=LGEAI  city='Atlanta, GA'  promise_date=20260821
--
-- Example:
--   psql -d vrp_db_dev -v source_dsn="$VRP_PROD_DSN" \
--     -v subsidiary=LGEAI -v city='Atlanta, GA' -v promise_date=20260821 \
--     -f admin_tools/db/runners/compare_prod_dev_home_routing_inputs.sql

\set ON_ERROR_STOP on
\pset pager off

\if :{?source_dsn}
\else
  \echo 'source_dsn is required. No database data was changed.'
  \quit
\endif
\if :{?subsidiary}
\else
  \set subsidiary 'LGEAI'
\endif
\if :{?city}
\else
  \set city 'Atlanta, GA'
\endif
\if :{?promise_date}
\else
  \set promise_date '20260821'
\endif

do $$
begin
  if not exists (select 1 from pg_extension where extname = 'dblink') then
    raise exception 'dblink extension is required in vrp_db_dev. This comparison did not change any routing data.';
  end if;
end $$;

-- PostgreSQL permits no CREATE TEMP TABLE in a READ ONLY transaction on this
-- server.  The statements below create only session-local temporary tables;
-- there are no INSERT/UPDATE/DELETE statements against persistent tables.
begin;
select dblink_connect('vrp_prod_compare', :'source_dsn');

create temporary table prod_job (
  subsidiary_name text, strategic_city_name text, promise_date text,
  gsfs_receipt_no text, fixed boolean, reschedule boolean, svc_engineer_code text,
  job_slot_count integer, latitude double precision, longitude double precision,
  postal_code text, service_product_group_code text, service_product_code text,
  receipt_detail_symptom_code text
) on commit drop;
insert into prod_job
select * from dblink('vrp_prod_compare', format($remote$
  select subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no,
         fixed, reschedule, svc_engineer_code, job_slot_count, latitude, longitude,
         postal_code, service_product_group_code, service_product_code, receipt_detail_symptom_code
    from common_job_input
   where subsidiary_name = %L and strategic_city_name = %L
     and replace(promise_date, '-', '') = %L
$remote$, :'subsidiary', :'city', :'promise_date')) as r(subsidiary_name text, strategic_city_name text, promise_date text,
         gsfs_receipt_no text, fixed boolean, reschedule boolean, svc_engineer_code text,
         job_slot_count integer, latitude double precision, longitude double precision,
         postal_code text, service_product_group_code text, service_product_code text,
         receipt_detail_symptom_code text);

create temporary table prod_technician (
  subsidiary_name text, strategic_city_name text, promise_date text, employee_code text,
  employee_name text, center_type text, available boolean, slot_count integer,
  max_minutes integer, max_jobs integer, shift_start text, shift_end text,
  priority_group text, preferred_region_name text, start_location_type text,
  start_location_address text
) on commit drop;
insert into prod_technician
select * from dblink('vrp_prod_compare', format($remote$
  select subsidiary_name, strategic_city_name, promise_date, employee_code,
         employee_name, center_type, available, slot_count, max_minutes, max_jobs,
         shift_start, shift_end, priority_group, preferred_region_name,
         start_location_type, start_location_address
    from common_request_technician_input
   where subsidiary_name = %L and strategic_city_name = %L
     and replace(promise_date, '-', '') = %L
$remote$, :'subsidiary', :'city', :'promise_date')) as r(subsidiary_name text, strategic_city_name text, promise_date text,
         employee_code text, employee_name text, center_type text, available boolean,
         slot_count integer, max_minutes integer, max_jobs integer, shift_start text,
         shift_end text, priority_group text, preferred_region_name text,
         start_location_type text, start_location_address text);

create temporary table prod_master (
  subsidiary_name text, strategic_city_name text, employee_code text,
  employee_name text, center_type text, active_flag boolean, home_postal_code text,
  home_latitude double precision, home_longitude double precision,
  priority_group text, max_home_to_job_min integer
) on commit drop;
insert into prod_master
select * from dblink('vrp_prod_compare', format($remote$
  select subsidiary_name, strategic_city_name, employee_code, employee_name,
         center_type, active_flag, home_postal_code, home_latitude, home_longitude,
         priority_group, max_home_to_job_min
    from common_technician_master
   where subsidiary_name = %L and strategic_city_name = %L
$remote$, :'subsidiary', :'city')) as r(subsidiary_name text, strategic_city_name text, employee_code text,
         employee_name text, center_type text, active_flag boolean, home_postal_code text,
         home_latitude double precision, home_longitude double precision,
         priority_group text, max_home_to_job_min integer);

create temporary table prod_capability (
  subsidiary_name text, strategic_city_name text, employee_code text,
  product_group_code text, product_code text, repair_allowed boolean,
  heavy_repair_allowed boolean, priority_score integer,
  effective_start_date date, effective_end_date date
) on commit drop;
insert into prod_capability
select * from dblink('vrp_prod_compare', format($remote$
  select subsidiary_name, strategic_city_name, employee_code, product_group_code,
         product_code, repair_allowed, heavy_repair_allowed, priority_score,
         effective_start_date, effective_end_date
    from common_technician_capability_master
   where subsidiary_name = %L and strategic_city_name = %L
$remote$, :'subsidiary', :'city')) as r(subsidiary_name text, strategic_city_name text, employee_code text,
         product_group_code text, product_code text, repair_allowed boolean,
         heavy_repair_allowed boolean, priority_score integer,
         effective_start_date date, effective_end_date date);

-- Production has an older routing-config schema.  Read its row as JSON so a
-- Development-only column (for example region_policy) is reported as missing
-- instead of aborting the complete comparison.
create temporary table prod_config (config_json jsonb) on commit drop;
insert into prod_config
select r.config_json::jsonb from dblink('vrp_prod_compare', format($remote$
  select to_jsonb(c)::text
    from common_routing_config_master c
   where subsidiary_name = %L and strategic_city_name = %L
$remote$, :'subsidiary', :'city')) as r(config_json text);

-- The request payload is closer to the Solver boundary than the source input
-- tables.  Read the newest request for this date from each environment and
-- compare its exact options and ordered arrays below.
create temporary table prod_request (
  request_id text,
  payload_json jsonb,
  updated_at timestamptz
) on commit drop;
insert into prod_request
select r.request_id, r.payload_json::jsonb, r.updated_at
from dblink('vrp_prod_compare', format($remote$
  select request_id, payload_json, updated_at
    from common_routing_request
   where subsidiary_name = %L and strategic_city_name = %L
     and replace(promise_date, '-', '') = %L
   order by updated_at desc, created_at desc
   limit 1
$remote$, :'subsidiary', :'city', :'promise_date')) as r(
  request_id text, payload_json text, updated_at timestamptz
);

\echo '=== Scope and aggregate routing inputs ==='
with p as (
  select count(*) jobs, count(*) filter (where fixed) fixed_jobs,
         count(*) filter (where reschedule) reschedule_jobs, coalesce(sum(job_slot_count), 0) slots
  from prod_job where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
), d as (
  select count(*) jobs, count(*) filter (where fixed) fixed_jobs,
         count(*) filter (where reschedule) reschedule_jobs, coalesce(sum(job_slot_count), 0) slots
  from common_job_input where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
), pt as (
  select count(*) technicians, count(*) filter (where available) available_technicians,
         coalesce(sum(slot_count) filter (where available), 0) available_slots
  from prod_technician where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
), dt as (
  select count(*) technicians, count(*) filter (where available) available_technicians,
         coalesce(sum(slot_count) filter (where available), 0) available_slots
  from common_request_technician_input where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
)
select p.jobs production_jobs, d.jobs development_jobs, p.fixed_jobs production_fixed,
       d.fixed_jobs development_fixed, p.reschedule_jobs production_reschedule,
       d.reschedule_jobs development_reschedule, p.slots production_job_slots,
       d.slots development_job_slots, pt.technicians production_technicians,
       dt.technicians development_technicians, pt.available_technicians production_available,
       dt.available_technicians development_available, pt.available_slots production_available_slots,
       dt.available_slots development_available_slots
from p cross join d cross join pt cross join dt;

\echo '=== Job differences (missing rows or solver-relevant fields) ==='
with p as (
  select * from prod_job where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
), d as (
  select * from common_job_input where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
)
select coalesce(p.gsfs_receipt_no, d.gsfs_receipt_no) receipt_no,
       case when p.gsfs_receipt_no is null then 'development_only'
            when d.gsfs_receipt_no is null then 'production_only' else 'different' end as difference_type,
       concat_ws(', ',
         case when p.fixed is distinct from d.fixed then 'fixed' end,
         case when p.reschedule is distinct from d.reschedule then 'reschedule' end,
         case when p.svc_engineer_code is distinct from d.svc_engineer_code then 'svc_engineer_code' end,
         case when p.job_slot_count is distinct from d.job_slot_count then 'job_slot_count' end,
         case when p.latitude is distinct from d.latitude or p.longitude is distinct from d.longitude then 'coordinates' end,
         case when p.postal_code is distinct from d.postal_code then 'postal_code' end,
         case when p.service_product_group_code is distinct from d.service_product_group_code
                or p.service_product_code is distinct from d.service_product_code then 'product' end,
         case when p.receipt_detail_symptom_code is distinct from d.receipt_detail_symptom_code then 'symptom' end
       ) changed_fields,
       p.fixed production_fixed, d.fixed development_fixed,
       p.reschedule production_reschedule, d.reschedule development_reschedule,
       p.svc_engineer_code production_engineer, d.svc_engineer_code development_engineer,
       p.job_slot_count production_slots, d.job_slot_count development_slots,
       concat_ws(',', p.latitude, p.longitude) production_lon_lat,
       concat_ws(',', d.latitude, d.longitude) development_lon_lat
from p full outer join d using (gsfs_receipt_no)
where p.gsfs_receipt_no is null or d.gsfs_receipt_no is null
   or p.fixed is distinct from d.fixed or p.reschedule is distinct from d.reschedule
   or p.svc_engineer_code is distinct from d.svc_engineer_code
   or p.job_slot_count is distinct from d.job_slot_count
   or p.latitude is distinct from d.latitude or p.longitude is distinct from d.longitude
   or p.postal_code is distinct from d.postal_code
   or p.service_product_group_code is distinct from d.service_product_group_code
   or p.service_product_code is distinct from d.service_product_code
   or p.receipt_detail_symptom_code is distinct from d.receipt_detail_symptom_code
order by receipt_no;

\echo '=== Daily technician roster differences ==='
with p as (
  select * from prod_technician where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
), d as (
  select * from common_request_technician_input where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
)
select coalesce(p.employee_code, d.employee_code) employee_code,
       case when p.employee_code is null then 'development_only'
            when d.employee_code is null then 'production_only' else 'different' end as difference_type,
       concat_ws(', ',
         case when p.available is distinct from d.available then 'available' end,
         case when p.slot_count is distinct from d.slot_count then 'slot_count' end,
         case when p.max_minutes is distinct from d.max_minutes then 'max_minutes' end,
         case when p.max_jobs is distinct from d.max_jobs then 'max_jobs' end,
         case when p.center_type is distinct from d.center_type then 'center_type' end,
         case when p.shift_start is distinct from d.shift_start or p.shift_end is distinct from d.shift_end then 'shift' end,
         case when p.priority_group is distinct from d.priority_group then 'priority_group' end
       ) changed_fields,
       p.available production_available, d.available development_available,
       p.slot_count production_slots, d.slot_count development_slots,
       p.max_minutes production_max_minutes, d.max_minutes development_max_minutes
from p full outer join d using (employee_code)
where p.employee_code is null or d.employee_code is null
   or p.available is distinct from d.available or p.slot_count is distinct from d.slot_count
   or p.max_minutes is distinct from d.max_minutes or p.max_jobs is distinct from d.max_jobs
   or p.center_type is distinct from d.center_type or p.shift_start is distinct from d.shift_start
   or p.shift_end is distinct from d.shift_end or p.priority_group is distinct from d.priority_group
order by employee_code;

\echo '=== Technician Home/master differences for the daily roster ==='
with roster as (
  select employee_code from prod_technician where subsidiary_name = :'subsidiary'
    and strategic_city_name = :'city' and replace(promise_date, '-', '') = :'promise_date'
  union
  select employee_code from common_request_technician_input where subsidiary_name = :'subsidiary'
    and strategic_city_name = :'city' and replace(promise_date, '-', '') = :'promise_date'
), p as (
  select m.* from prod_master m join roster r using (employee_code)
  where m.subsidiary_name = :'subsidiary' and m.strategic_city_name = :'city'
), d as (
  select m.* from common_technician_master m join roster r using (employee_code)
  where m.subsidiary_name = :'subsidiary' and m.strategic_city_name = :'city'
)
select coalesce(p.employee_code, d.employee_code) employee_code,
       case when p.employee_code is null then 'development_only'
            when d.employee_code is null then 'production_only' else 'different' end as difference_type,
       concat_ws(', ',
         case when p.active_flag is distinct from d.active_flag then 'active_flag' end,
         case when p.home_latitude is distinct from d.home_latitude or p.home_longitude is distinct from d.home_longitude then 'home_coordinates' end,
         case when p.home_postal_code is distinct from d.home_postal_code then 'home_postal_code' end,
         case when p.max_home_to_job_min is distinct from d.max_home_to_job_min then 'max_home_to_job_min' end
       ) changed_fields,
       concat_ws(',', p.home_latitude, p.home_longitude) production_home_lat_lon,
       concat_ws(',', d.home_latitude, d.home_longitude) development_home_lat_lon
from p full outer join d using (employee_code)
where p.employee_code is null or d.employee_code is null
   or p.active_flag is distinct from d.active_flag
   or p.home_latitude is distinct from d.home_latitude or p.home_longitude is distinct from d.home_longitude
   or p.home_postal_code is distinct from d.home_postal_code
   or p.max_home_to_job_min is distinct from d.max_home_to_job_min
order by employee_code;

\echo '=== Capability row-count differences for the daily roster ==='
with roster as (
  select employee_code from prod_technician where subsidiary_name = :'subsidiary'
    and strategic_city_name = :'city' and replace(promise_date, '-', '') = :'promise_date'
  union
  select employee_code from common_request_technician_input where subsidiary_name = :'subsidiary'
    and strategic_city_name = :'city' and replace(promise_date, '-', '') = :'promise_date'
), p as (
  select c.employee_code, count(*) capability_rows from prod_capability c join roster r using (employee_code)
  where c.subsidiary_name = :'subsidiary' and c.strategic_city_name = :'city' group by c.employee_code
), d as (
  select c.employee_code, count(*) capability_rows from common_technician_capability_master c join roster r using (employee_code)
  where c.subsidiary_name = :'subsidiary' and c.strategic_city_name = :'city' group by c.employee_code
)
select coalesce(p.employee_code, d.employee_code) employee_code,
       coalesce(p.capability_rows, 0) production_capability_rows,
       coalesce(d.capability_rows, 0) development_capability_rows
from p full outer join d using (employee_code)
where coalesce(p.capability_rows, 0) <> coalesce(d.capability_rows, 0)
order by employee_code;

\echo '=== Routing configuration differences (secrets are not queried) ==='
with p as (
  select config_json from prod_config
), d as (
  select to_jsonb(c) as config_json
  from common_routing_config_master c
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
), pairs as (
  select v.config_key, v.production_value, v.development_value
  from p full outer join d on true
  cross join lateral (values
    ('region_policy', p.config_json ->> 'region_policy', d.config_json ->> 'region_policy'),
    ('region_plan_id', p.config_json ->> 'region_plan_id', d.config_json ->> 'region_plan_id'),
    ('region_plan_revision', p.config_json ->> 'region_plan_revision', d.config_json ->> 'region_plan_revision'),
    ('region_plan_checksum', p.config_json ->> 'region_plan_checksum', d.config_json ->> 'region_plan_checksum'),
    ('distance_backend', p.config_json ->> 'distance_backend', d.config_json ->> 'distance_backend'),
    ('assignment_distance_backend', p.config_json ->> 'assignment_distance_backend', d.config_json ->> 'assignment_distance_backend'),
    ('osrm_url', p.config_json ->> 'osrm_url', d.config_json ->> 'osrm_url'),
    ('osrm_profile', p.config_json ->> 'osrm_profile', d.config_json ->> 'osrm_profile'),
    ('effective_service_per_sm', p.config_json ->> 'effective_service_per_sm', d.config_json ->> 'effective_service_per_sm'),
    ('target_sm_per_region', p.config_json ->> 'target_sm_per_region', d.config_json ->> 'target_sm_per_region'),
    ('service_time_per_job_min', p.config_json ->> 'service_time_per_job_min', d.config_json ->> 'service_time_per_job_min'),
    ('max_work_min_per_sm_day', p.config_json ->> 'max_work_min_per_sm_day', d.config_json ->> 'max_work_min_per_sm_day'),
    ('max_travel_min_per_sm_day', p.config_json ->> 'max_travel_min_per_sm_day', d.config_json ->> 'max_travel_min_per_sm_day'),
    ('max_travel_km_per_sm_day', p.config_json ->> 'max_travel_km_per_sm_day', d.config_json ->> 'max_travel_km_per_sm_day'),
    ('max_single_leg_min', p.config_json ->> 'max_single_leg_min', d.config_json ->> 'max_single_leg_min'),
    ('max_home_to_job_min', p.config_json ->> 'max_home_to_job_min', d.config_json ->> 'max_home_to_job_min'),
    ('long_leg_penalty_start_min', p.config_json ->> 'long_leg_penalty_start_min', d.config_json ->> 'long_leg_penalty_start_min'),
    ('long_leg_penalty_multiplier', p.config_json ->> 'long_leg_penalty_multiplier', d.config_json ->> 'long_leg_penalty_multiplier'),
    ('timezone_offset', p.config_json ->> 'timezone_offset', d.config_json ->> 'timezone_offset')
  ) as v(config_key, production_value, development_value)
)
select config_key, production_value, development_value
from pairs
where production_value is distinct from development_value
order by config_key;

\echo '=== Latest stored Solver payload: request identity and array signatures ==='
with p as (
  select request_id, payload_json from prod_request
), d as (
  select request_id, payload_json::jsonb as payload_json
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), ps as (
  select request_id,
         jsonb_array_length(coalesce(payload_json -> 'jobs', '[]'::jsonb)) as jobs,
         jsonb_array_length(coalesce(payload_json -> 'technicians', '[]'::jsonb)) as technicians,
         jsonb_array_length(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) as capabilities,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_jobs_sha,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'technicians', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_technicians_sha,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_capabilities_sha
  from p
), ds as (
  select request_id,
         jsonb_array_length(coalesce(payload_json -> 'jobs', '[]'::jsonb)) as jobs,
         jsonb_array_length(coalesce(payload_json -> 'technicians', '[]'::jsonb)) as technicians,
         jsonb_array_length(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) as capabilities,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_jobs_sha,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'technicians', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_technicians_sha,
         md5(coalesce((select string_agg(item::text, '|' order by ord)
                       from jsonb_array_elements(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) with ordinality x(item, ord)), '')) as ordered_capabilities_sha
  from d
)
select ps.request_id production_request_id, ds.request_id development_request_id,
       ps.jobs production_jobs, ds.jobs development_jobs,
       ps.technicians production_technicians, ds.technicians development_technicians,
       ps.capabilities production_capabilities, ds.capabilities development_capabilities,
       ps.ordered_jobs_sha production_ordered_jobs_sha, ds.ordered_jobs_sha development_ordered_jobs_sha,
       ps.ordered_technicians_sha production_ordered_technicians_sha, ds.ordered_technicians_sha development_ordered_technicians_sha,
       ps.ordered_capabilities_sha production_ordered_capabilities_sha, ds.ordered_capabilities_sha development_ordered_capabilities_sha
from ps full outer join ds on true;

\echo '=== Latest stored Solver payload: options differences ==='
with p as (
  select payload_json -> 'options' as options from prod_request
), d as (
  select payload_json::jsonb -> 'options' as options
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), keys as (
  select key from p cross join lateral jsonb_object_keys(coalesce(p.options, '{}'::jsonb)) key
  union
  select key from d cross join lateral jsonb_object_keys(coalesce(d.options, '{}'::jsonb)) key
)
select keys.key as option_key, p.options ->> keys.key as production_value,
       d.options ->> keys.key as development_value
from keys left join p on true left join d on true
where p.options -> keys.key is distinct from d.options -> keys.key
order by option_key;

\echo '=== Latest stored Solver payload: Job/technician array-order differences ==='
with d_request as (
  select payload_json::jsonb as payload_json
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), p as (
  select ord, item ->> 'receipt_no' as receipt_no
  from prod_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) with ordinality x(item, ord)
), d as (
  select ord, item ->> 'receipt_no' as receipt_no
  from d_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) with ordinality x(item, ord)
)
select coalesce(p.ord, d.ord) as position, p.receipt_no as production_receipt_no,
       d.receipt_no as development_receipt_no
from p full outer join d using (ord)
where p.receipt_no is distinct from d.receipt_no
order by position;

with d_request as (
  select payload_json::jsonb as payload_json
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), p as (
  select ord, item ->> 'employee_code' as employee_code
  from prod_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'technicians', '[]'::jsonb)) with ordinality x(item, ord)
), d as (
  select ord, item ->> 'employee_code' as employee_code
  from d_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'technicians', '[]'::jsonb)) with ordinality x(item, ord)
)
select coalesce(p.ord, d.ord) as position, p.employee_code as production_employee_code,
       d.employee_code as development_employee_code
from p full outer join d using (ord)
where p.employee_code is distinct from d.employee_code
order by position;

\echo '=== Latest stored Solver payload: per-job field differences ==='
with p_request as (
  select payload_json from prod_request
), d_request as (
  select payload_json::jsonb as payload_json
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), p as (
  select item ->> 'receipt_no' as receipt_no, item
  from p_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) x(item)
), d as (
  select item ->> 'receipt_no' as receipt_no, item
  from d_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'jobs', '[]'::jsonb)) x(item)
), job_pairs as (
  select coalesce(p.receipt_no, d.receipt_no) as receipt_no, p.item as production_job, d.item as development_job
  from p full outer join d using (receipt_no)
), field_diffs as (
  select jp.receipt_no, key as field_name,
         jp.production_job -> key as production_value,
         jp.development_job -> key as development_value
  from job_pairs jp
  cross join lateral jsonb_object_keys(coalesce(jp.production_job, '{}'::jsonb) || coalesce(jp.development_job, '{}'::jsonb)) key
  where jp.production_job -> key is distinct from jp.development_job -> key
)
select receipt_no, field_name, production_value, development_value
from field_diffs
order by receipt_no, field_name;

\echo '=== Latest stored Solver payload: per-capability field differences ==='
with p_request as (
  select payload_json from prod_request
), d_request as (
  select payload_json::jsonb as payload_json
  from common_routing_request
  where subsidiary_name = :'subsidiary' and strategic_city_name = :'city'
    and replace(promise_date, '-', '') = :'promise_date'
  order by updated_at desc, created_at desc
  limit 1
), p as (
  select concat_ws('|', item ->> 'employee_code', item ->> 'product_group_code', item ->> 'product_code') as capability_key, item
  from p_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) x(item)
), d as (
  select concat_ws('|', item ->> 'employee_code', item ->> 'product_group_code', item ->> 'product_code') as capability_key, item
  from d_request cross join lateral jsonb_array_elements(coalesce(payload_json -> 'capabilities', '[]'::jsonb)) x(item)
), capability_pairs as (
  select coalesce(p.capability_key, d.capability_key) as capability_key,
         p.item as production_capability, d.item as development_capability
  from p full outer join d using (capability_key)
), field_diffs as (
  select cp.capability_key, key as field_name,
         cp.production_capability -> key as production_value,
         cp.development_capability -> key as development_value
  from capability_pairs cp
  cross join lateral jsonb_object_keys(coalesce(cp.production_capability, '{}'::jsonb) || coalesce(cp.development_capability, '{}'::jsonb)) key
  where cp.production_capability -> key is distinct from cp.development_capability -> key
)
select capability_key, field_name, production_value, development_value
from field_diffs
order by capability_key, field_name;

select dblink_disconnect('vrp_prod_compare');
rollback;
