-- DEVELOPMENT ONLY.
-- Replace legacy Region Plan city labels in the Region Plans v2 Source city
-- selector with the operational city, Los Angeles, CA.
--
-- This script moves ONLY common_technician_master rows. It deliberately does
-- NOT delete or modify capability, Region Plan, ZIP, routing-config, or
-- plan-technician assignment rows.
--
-- It aborts if the same employee has conflicting master attributes across the
-- legacy city labels. Resolve those conflicts manually; do not pick one row
-- arbitrarily.

begin;

do $$
begin
    if current_database() <> 'vrp_db_dev' then
        raise exception 'Refusing target database %; expected vrp_db_dev', current_database();
    end if;
end $$;

create temporary table legacy_la_source_city (
    city_name text primary key,
    source_priority integer not null
) on commit drop;

insert into legacy_la_source_city (city_name, source_priority) values
    ('Los Angeles, CA - Bucket Sim Draft', 1),
    ('Los Angeles, CA - Area Type Clusters', 2),
    ('LA_6area', 3);

-- Preview only: show exactly what is about to be moved/deleted. Capability
-- counts are informational and must remain unchanged after this script.
select m.city_name as legacy_source_city,
       count(t.employee_code) as technician_master_rows,
       count(c.employee_code) as capability_rows_not_touched
  from legacy_la_source_city m
  left join common_technician_master t
    on t.subsidiary_name = 'LGEAI'
   and t.strategic_city_name = m.city_name
  left join common_technician_capability_master c
    on c.subsidiary_name = t.subsidiary_name
   and c.strategic_city_name = t.strategic_city_name
   and c.employee_code = t.employee_code
 group by m.city_name, m.source_priority
 order by m.source_priority;

-- A technician may appear in more than one old city only when the master
-- profile is identical. Stop rather than silently losing a differing address,
-- center type, active state, or capacity setting.
select t.employee_code,
       count(distinct md5(concat_ws('|',
           coalesce(t.employee_name, ''), coalesce(t.center_type, ''),
           coalesce(t.home_address, ''), coalesce(t.home_city, ''),
           coalesce(t.home_state, ''), coalesce(t.home_country, ''),
           coalesce(t.home_postal_code, ''), coalesce(t.home_latitude::text, ''),
           coalesce(t.home_longitude::text, ''), coalesce(t.active_flag::text, ''),
           coalesce(t.priority_group, ''), coalesce(t.max_home_to_job_min::text, '')
       ))) as distinct_master_profiles,
       array_agg(distinct t.strategic_city_name order by t.strategic_city_name) as legacy_source_cities
  from common_technician_master t
  join legacy_la_source_city m on m.city_name = t.strategic_city_name
 where t.subsidiary_name = 'LGEAI'
 group by t.employee_code
having count(distinct md5(concat_ws('|',
           coalesce(t.employee_name, ''), coalesce(t.center_type, ''),
           coalesce(t.home_address, ''), coalesce(t.home_city, ''),
           coalesce(t.home_state, ''), coalesce(t.home_country, ''),
           coalesce(t.home_postal_code, ''), coalesce(t.home_latitude::text, ''),
           coalesce(t.home_longitude::text, ''), coalesce(t.active_flag::text, ''),
           coalesce(t.priority_group, ''), coalesce(t.max_home_to_job_min::text, '')
       ))) > 1
 order by t.employee_code;

do $$
begin
    if exists (
        select 1
          from common_technician_master t
          join legacy_la_source_city m on m.city_name = t.strategic_city_name
         where t.subsidiary_name = 'LGEAI'
         group by t.employee_code
        having count(distinct md5(concat_ws('|',
            coalesce(t.employee_name, ''), coalesce(t.center_type, ''),
            coalesce(t.home_address, ''), coalesce(t.home_city, ''),
            coalesce(t.home_state, ''), coalesce(t.home_country, ''),
            coalesce(t.home_postal_code, ''), coalesce(t.home_latitude::text, ''),
            coalesce(t.home_longitude::text, ''), coalesce(t.active_flag::text, ''),
            coalesce(t.priority_group, ''), coalesce(t.max_home_to_job_min::text, '')
        ))) > 1
    ) then
        raise exception 'LA_LEGACY_ROSTER_CONFLICT: resolve differing employee master rows before migration';
    end if;
end $$;

-- Copy one validated master profile per employee into the operational city.
-- Existing Los Angeles, CA master rows are preserved unchanged.
insert into common_technician_master (
    subsidiary_name, strategic_city_name, employee_code, employee_name,
    center_type, home_address, home_city, home_state, home_country,
    home_postal_code, home_latitude, home_longitude, active_flag,
    priority_group, max_home_to_job_min
)
select distinct on (t.employee_code)
       t.subsidiary_name, 'Los Angeles, CA', t.employee_code, t.employee_name,
       t.center_type, t.home_address, t.home_city, t.home_state, t.home_country,
       t.home_postal_code, t.home_latitude, t.home_longitude, t.active_flag,
       t.priority_group, t.max_home_to_job_min
  from common_technician_master t
  join legacy_la_source_city m on m.city_name = t.strategic_city_name
 where t.subsidiary_name = 'LGEAI'
 order by t.employee_code, m.source_priority
on conflict (subsidiary_name, strategic_city_name, employee_code) do nothing;

-- Delete only the legacy Source city roster entries. No capability delete is
-- present in this script by design.
delete from common_technician_master t
 using legacy_la_source_city m
 where t.subsidiary_name = 'LGEAI'
   and t.strategic_city_name = m.city_name;

-- Postcondition: LA is the only operational Source city created by this
-- reconciliation; capability rows remain in their original city contexts.
select strategic_city_name, count(*) as active_technician_rows
  from common_technician_master
 where subsidiary_name = 'LGEAI'
   and strategic_city_name in (
       'Los Angeles, CA',
       'LA_6area',
       'Los Angeles, CA - Area Type Clusters',
       'Los Angeles, CA - Bucket Sim Draft'
   )
 group by strategic_city_name
 order by strategic_city_name;

commit;
