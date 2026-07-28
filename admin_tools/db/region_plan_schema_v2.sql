-- Common Region Plan Schema v2 reconciliation contract.
--
-- This is intentionally one common installer, not a city/upload migration.
-- The reconciler loads the historical V001 base definition, then applies these
-- additive compatibility rules under one advisory lock and transaction.
-- Do not run V001--V005 individually for Schema v2.

-- Generic synchronous-verification audit fields.  These belong to the common
-- plan record (not to any city-specific workflow) and remain nullable so
-- plans created before synchronous verification can still be reconciled.
alter table public.common_region_plan
    add column if not exists verified_content_sha256 char(64),
    add column if not exists verified_at timestamptz,
    add column if not exists verified_by text;
alter table public.common_region_plan
    drop constraint if exists common_region_plan_verified_content_sha256_v2_check;
alter table public.common_region_plan
    add constraint common_region_plan_verified_content_sha256_v2_check
    check (
      verified_content_sha256 is null
      or verified_content_sha256 ~ '^[0-9a-f]{64}$'
    );

alter table public.common_region_plan_region
    drop constraint if exists common_region_plan_region_region_seq_check;
alter table public.common_region_plan_region
    drop constraint if exists common_region_plan_region_region_seq_positive_check;
alter table public.common_region_plan_region
    add constraint common_region_plan_region_region_seq_positive_check check (region_seq > 0);

alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_employee_code_check;
alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_employee_code_source_id_check;
alter table public.common_region_plan_technician
    add constraint common_region_plan_technician_employee_code_source_id_check
    check (employee_code ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$');

alter table public.common_region_plan_region
    add column if not exists required_center_type text;
alter table public.common_region_plan_region
    drop constraint if exists common_region_plan_region_required_center_type_v004_check;
alter table public.common_region_plan_region
    add constraint common_region_plan_region_required_center_type_v004_check
    check (required_center_type is null or required_center_type in ('DMS', 'DMS2'));

alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_policy_mode_check;
alter table public.common_region_plan_technician
    drop constraint if exists common_region_plan_technician_policy_mode_v004_check;
alter table public.common_region_plan_technician
    add constraint common_region_plan_technician_policy_mode_v004_check check (policy_mode in (
      'assigned_region_boundary_spillover', 'active_roster_type_hard_region_soft',
      'active_roster_area_type_fallback_region_soft'));

-- A common plan can report any positive number of source memberships. Discover
-- and remove historical checks by the columns they constrain, not by a
-- release-specific constraint name.
do $schema_v2$
declare constraint_row record;
begin
  for constraint_row in
    select conname
      from pg_constraint
     where conrelid = 'public.common_region_plan_postal'::regclass
       and contype = 'c'
       and pg_get_constraintdef(oid) like '%source_membership_count%'
  loop
    execute format(
      'alter table public.common_region_plan_postal drop constraint %I',
      constraint_row.conname
    );
  end loop;
end
$schema_v2$;

-- The dynamically discovered historical membership check above also removes
-- this Schema v2 check on a rerun.  Resolution is a separate predicate, so
-- remove its stable name explicitly before recreating it.  Keeping both
-- drops here makes the two constraints safe when a prior reconcile committed
-- successfully but a later invocation is required.
alter table public.common_region_plan_postal
  drop constraint if exists common_region_plan_postal_membership_count_v2_check;
alter table public.common_region_plan_postal
  drop constraint if exists common_region_plan_postal_resolution_v2_check;

alter table public.common_region_plan_postal
  add constraint common_region_plan_postal_membership_count_v2_check
  check (source_membership_count > 0);
alter table public.common_region_plan_postal
  add constraint common_region_plan_postal_resolution_v2_check check (
    (source_membership_count = 1 and region_seq is not null and resolution_status = 'not_required')
    or
    (source_membership_count > 1 and (
      (resolution_status = 'pending' and region_seq is null)
      or (resolution_status = 'resolved' and region_seq is not null)
    ))
  );

-- Permit more than one alternate region per postal. Validate the prospective
-- key before replacing the historical narrower primary key; any unexpected
-- drift fails closed and rolls the entire reconciliation back.
do $schema_v2$
declare current_key_columns text[];
begin
  if exists (
    select 1
      from public.common_region_plan_boundary_overflow
     group by subsidiary_name, strategic_city_name, plan_id, postal_code,
              alternate_region_seq
    having count(*) > 1
  ) then
    raise exception 'SCHEMA_DRIFT_DETECTED: duplicate boundary overflow alternate key';
  end if;

  select array_agg(attribute.attname order by key_column.ordinality)
    into current_key_columns
    from pg_constraint constraint_info
    cross join lateral unnest(constraint_info.conkey)
      with ordinality as key_column(attnum, ordinality)
    join pg_attribute attribute
      on attribute.attrelid = constraint_info.conrelid
     and attribute.attnum = key_column.attnum
   where constraint_info.conrelid =
         'public.common_region_plan_boundary_overflow'::regclass
     and constraint_info.contype = 'p'
   group by constraint_info.oid;

  if current_key_columns is distinct from array[
    'subsidiary_name', 'strategic_city_name', 'plan_id', 'postal_code',
    'alternate_region_seq'
  ]::text[] then
    alter table public.common_region_plan_boundary_overflow
      drop constraint if exists common_region_plan_boundary_overflow_pkey;
    alter table public.common_region_plan_boundary_overflow
      add constraint common_region_plan_boundary_overflow_pkey primary key (
        subsidiary_name, strategic_city_name, plan_id, postal_code,
        alternate_region_seq
      );
  end if;
end
$schema_v2$;

grant select, insert, update on table public.common_city_context to vrp_agent;
grant select, insert, update on table public.common_region_plan to vrp_agent;
grant select, insert, update on table public.common_region_plan_region,
  public.common_region_plan_postal, public.common_region_plan_technician,
  public.common_region_plan_boundary_overflow, public.common_region_plan_activation to vrp_agent;
