-- Add area-type-classified region policy without changing existing
-- explicit-workbook plan rows. This is a one-time registry migration.
alter table public.common_region_plan_region
    add column if not exists required_center_type text;

alter table public.common_region_plan_region
    add constraint common_region_plan_region_required_center_type_v004_check
    check (required_center_type is null or required_center_type in ('DMS', 'DMS2')) not valid;

alter table public.common_region_plan_region
    validate constraint common_region_plan_region_required_center_type_v004_check;

alter table public.common_region_plan_technician
    add constraint common_region_plan_technician_policy_mode_v004_check
    check (policy_mode in (
        'assigned_region_boundary_spillover',
        'active_roster_type_hard_region_soft',
        'active_roster_area_type_fallback_region_soft'
    )) not valid;

alter table public.common_region_plan_technician
    validate constraint common_region_plan_technician_policy_mode_v004_check;

alter table public.common_region_plan_technician
    drop constraint common_region_plan_technician_policy_mode_check;
