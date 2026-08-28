-- Development-only cleanup for Technician master rows accidentally stored
-- under legacy Region Plan target-city contexts.  This does NOT delete any
-- Region Plan, postal, or plan-technician assignment rows.
--
-- Run the preview queries first.  The DELETE section removes only rows whose
-- employee_code already exists in the mapped operational city.  Any unmatched
-- or conflicting legacy row remains in place for manual review.

begin;

create temporary table legacy_target_city_map (
    subsidiary_name text not null,
    legacy_city_name text not null,
    operational_city_name text not null,
    primary key (subsidiary_name, legacy_city_name)
) on commit drop;

insert into legacy_target_city_map values
    ('LGEAI', 'Atlanta_3area', 'Atlanta, GA'),
    ('LGEAI', 'Atlanta_6area', 'Atlanta, GA'),
    ('LGEAI', 'Atlanta_6area_new', 'Atlanta, GA'),
    ('LGEAI', 'Atlanta_6area_overlab', 'Atlanta, GA'),
    ('LGEAI', 'LA_6area', 'Los Angeles, CA');

-- Preview: every legacy Technician row must be classified before deletion.
select m.subsidiary_name,
       m.legacy_city_name,
       count(*) as legacy_technician_rows,
       count(s.employee_code) as matching_operational_employee_codes,
       count(*) - count(s.employee_code) as unmatched_rows
  from legacy_target_city_map m
  left join common_technician_master legacy
    on legacy.subsidiary_name = m.subsidiary_name
   and legacy.strategic_city_name = m.legacy_city_name
  left join common_technician_master s
    on s.subsidiary_name = m.subsidiary_name
   and s.strategic_city_name = m.operational_city_name
   and s.employee_code = legacy.employee_code
 group by m.subsidiary_name, m.legacy_city_name
 order by 1, 2;

-- Review any row that cannot safely be deleted by employee-code identity.
select legacy.subsidiary_name,
       legacy.strategic_city_name as legacy_city_name,
       legacy.employee_code,
       legacy.employee_name,
       legacy.center_type
  from common_technician_master legacy
  join legacy_target_city_map m
    on m.subsidiary_name = legacy.subsidiary_name
   and m.legacy_city_name = legacy.strategic_city_name
  left join common_technician_master operational
    on operational.subsidiary_name = m.subsidiary_name
   and operational.strategic_city_name = m.operational_city_name
   and operational.employee_code = legacy.employee_code
 where operational.employee_code is null
 order by 1, 2, 3;

-- Delete only duplicate capability rows first.
delete from common_technician_capability_master legacy
 using legacy_target_city_map m
 where legacy.subsidiary_name = m.subsidiary_name
   and legacy.strategic_city_name = m.legacy_city_name
   and exists (
       select 1
         from common_technician_capability_master operational
        where operational.subsidiary_name = m.subsidiary_name
          and operational.strategic_city_name = m.operational_city_name
          and operational.employee_code = legacy.employee_code
          and operational.product_group_code = legacy.product_group_code
          and operational.product_code = legacy.product_code
   );

-- Delete only duplicate Technician master rows.  Plan-specific technician
-- assignments remain untouched in common_region_plan_technician.
delete from common_technician_master legacy
 using legacy_target_city_map m
 where legacy.subsidiary_name = m.subsidiary_name
   and legacy.strategic_city_name = m.legacy_city_name
   and exists (
       select 1
         from common_technician_master operational
        where operational.subsidiary_name = m.subsidiary_name
          and operational.strategic_city_name = m.operational_city_name
          and operational.employee_code = legacy.employee_code
   );

-- Commits only duplicate rows proven by employee-code identity.  Unmatched
-- legacy rows remain available for follow-up review.
commit;
