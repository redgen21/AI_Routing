-- Broaden only the additive region-plan technician identifier constraint.
-- Existing AI###### values remain valid; master/capability tables are unchanged.
select 1 / case when exists (
    select 1
    from pg_constraint
    where conrelid = 'public.common_region_plan_technician'::regclass
      and conname = 'common_region_plan_technician_employee_code_check'
      and contype = 'c'
      and pg_get_constraintdef(oid) like '%^AI[0-9]{6}$%'
) or exists (
    select 1
    from pg_constraint
    where conrelid = 'public.common_region_plan_technician'::regclass
      and conname = 'common_region_plan_technician_employee_code_source_id_check'
      and contype = 'c'
      and pg_get_constraintdef(oid) like '%^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$%'
) then 1 else 0 end as expected_v001_constraint;

alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_employee_code_source_id_check;

alter table public.common_region_plan_technician
    add constraint common_region_plan_technician_employee_code_source_id_check
    check (employee_code ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$') not valid;

alter table public.common_region_plan_technician
    validate constraint common_region_plan_technician_employee_code_source_id_check;

alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_employee_code_check;
