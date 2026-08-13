-- Common Region Plan Schema v2 reconciliation contract.
--
-- This is intentionally one common installer, not a city/upload migration.
-- The reconciler loads the historical V001 base definition, then applies these
-- additive compatibility rules under one advisory lock and transaction.
-- Do not run V001--V005 individually for Schema v2.

-- City Config selects one immutable Region Plan.  These fields are additive
-- and are reconciled only by the development-only Region Plan schema command.
alter table public.common_routing_config_master
    add column if not exists region_plan_id text,
    add column if not exists region_plan_revision integer,
    add column if not exists region_plan_checksum char(64);
alter table public.common_routing_config_master
    drop constraint if exists common_routing_config_master_region_plan_checksum_v2_check;
alter table public.common_routing_config_master
    add constraint common_routing_config_master_region_plan_checksum_v2_check
    check (region_plan_checksum is null or region_plan_checksum ~ '^[0-9a-f]{64}$');

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
      'home_distance_only', 'preferred_region_soft',
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

-- Region topology and routing policy are separate concepts.  A Region Set is
-- reusable by multiple Routing Plans; a Routing Plan selects one Region Set
-- and supplies policy/overlap behavior.  The common_region_plan_* tables above
-- remain as the v2 import compatibility surface while new reads and writes
-- migrate to these normalized tables.
create table if not exists public.common_region_set (
    subsidiary_name text not null,
    source_strategic_city_name text not null,
    region_set_id text not null,
    region_set_name text not null,
    region_count integer not null check (region_count > 0),
    source_sha256 char(64),
    membership_sha256 char(64),
    revision integer not null default 1 check (revision > 0),
    status text not null default 'active' check (status in ('draft','active','retired')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, source_strategic_city_name, region_set_id)
);
create table if not exists public.common_region_set_region (
    subsidiary_name text not null,
    source_strategic_city_name text not null,
    region_set_id text not null,
    region_seq integer not null check (region_seq > 0),
    region_id text not null,
    region_name text not null,
    source_territory text,
    required_center_type text,
    area_type text,
    primary key (subsidiary_name, source_strategic_city_name, region_set_id, region_seq),
    unique (subsidiary_name, source_strategic_city_name, region_set_id, region_id),
    foreign key (subsidiary_name, source_strategic_city_name, region_set_id)
      references public.common_region_set(subsidiary_name, source_strategic_city_name, region_set_id)
);
create table if not exists public.common_region_set_postal (
    subsidiary_name text not null,
    source_strategic_city_name text not null,
    region_set_id text not null,
    postal_code text not null,
    region_seq integer not null,
    area_type text,
    membership_rank integer not null default 1,
    is_primary boolean not null default true,
    overflow_allowed boolean not null default false,
    primary key (subsidiary_name, source_strategic_city_name, region_set_id, postal_code, region_seq),
    foreign key (subsidiary_name, source_strategic_city_name, region_set_id, region_seq)
      references public.common_region_set_region(subsidiary_name, source_strategic_city_name, region_set_id, region_seq)
);
create table if not exists public.common_routing_plan (
    subsidiary_name text not null,
    strategic_city_name text not null,
    source_strategic_city_name text not null,
    routing_plan_id text not null,
    region_set_id text not null,
    policy_version text not null,
    overlap_policy text not null default 'registry_default',
    plan_status text not null default 'candidate' check (plan_status in ('candidate','reviewed','active','superseded','retired')),
    revision integer not null default 1 check (revision > 0),
    source_sha256 char(64),
    manifest_sha256 char(64),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, routing_plan_id),
    foreign key (subsidiary_name, source_strategic_city_name, region_set_id)
      references public.common_region_set(subsidiary_name, source_strategic_city_name, region_set_id)
);
create table if not exists public.common_routing_plan_technician (
    subsidiary_name text not null,
    strategic_city_name text not null,
    routing_plan_id text not null,
    employee_code text not null,
    assigned_region_seq integer not null,
    policy_mode text not null,
    active_flag boolean not null default true,
    primary key (subsidiary_name, strategic_city_name, routing_plan_id, employee_code),
    foreign key (subsidiary_name, strategic_city_name, routing_plan_id)
      references public.common_routing_plan(subsidiary_name, strategic_city_name, routing_plan_id)
);
create table if not exists public.common_routing_plan_activation (
    subsidiary_name text not null,
    strategic_city_name text not null,
    activation_revision integer not null,
    routing_plan_id text not null,
    activated_at timestamptz not null default now(),
    activated_by text,
    activation_reference text,
    active_flag boolean not null default true,
    primary key (subsidiary_name, strategic_city_name, activation_revision),
    unique (subsidiary_name, strategic_city_name, routing_plan_id, activation_revision),
    foreign key (subsidiary_name, strategic_city_name, routing_plan_id)
      references public.common_routing_plan(subsidiary_name, strategic_city_name, routing_plan_id)
);

alter table public.common_routing_plan
    add column if not exists source_strategic_city_name text;
update public.common_routing_plan rp
   set source_strategic_city_name = coalesce(
       rp.source_strategic_city_name,
       c.source_strategic_city_name,
       rp.strategic_city_name
   )
  from public.common_city_context c
 where c.subsidiary_name = rp.subsidiary_name
   and c.strategic_city_name = rp.strategic_city_name
   and rp.source_strategic_city_name is null;
update public.common_routing_plan
   set source_strategic_city_name = strategic_city_name
 where source_strategic_city_name is null;
alter table public.common_routing_plan
    alter column source_strategic_city_name set not null;

create index if not exists common_routing_plan_active_idx
    on public.common_routing_plan (subsidiary_name, strategic_city_name, plan_status);
create index if not exists common_region_set_source_idx
    on public.common_region_set (subsidiary_name, source_strategic_city_name, status);

grant select, insert, update on table public.common_region_set,
  public.common_region_set_region, public.common_region_set_postal,
  public.common_routing_plan, public.common_routing_plan_technician,
  public.common_routing_plan_activation to vrp_agent;

-- Backfill one Region Set per distinct fixed-region checksum.  Plans that
-- differ only by routing policy therefore share topology, while their policy
-- and technician assignments remain separate Routing Plans.
insert into public.common_region_set (
    subsidiary_name, source_strategic_city_name, region_set_id, region_set_name,
    region_count, source_sha256, membership_sha256, status
)
select p.subsidiary_name,
       coalesce(c.source_strategic_city_name, p.strategic_city_name),
       'rs_' || left(coalesce(p.fixed_region_sha256, md5(p.plan_id)), 24),
       coalesce(p.strategic_city_name || ' Region Set', p.plan_id),
       count(distinct r.region_seq)::integer,
       p.fixed_region_sha256,
       p.fixed_region_sha256,
       'active'
  from public.common_region_plan p
  left join public.common_city_context c
    on c.subsidiary_name = p.subsidiary_name
   and c.strategic_city_name = p.strategic_city_name
  join public.common_region_plan_region r
    on r.subsidiary_name = p.subsidiary_name
   and r.strategic_city_name = p.strategic_city_name
   and r.plan_id = p.plan_id
 group by p.subsidiary_name, coalesce(c.source_strategic_city_name, p.strategic_city_name),
          p.strategic_city_name, p.plan_id, p.fixed_region_sha256
on conflict (subsidiary_name, source_strategic_city_name, region_set_id) do nothing;

insert into public.common_region_set_region (
    subsidiary_name, source_strategic_city_name, region_set_id, region_seq,
    region_id, region_name, source_territory, required_center_type
)
select p.subsidiary_name,
       coalesce(c.source_strategic_city_name, p.strategic_city_name),
       'rs_' || left(coalesce(p.fixed_region_sha256, md5(p.plan_id)), 24),
       r.region_seq, r.region_id, r.region_name, r.source_territory,
       r.required_center_type
  from public.common_region_plan p
  left join public.common_city_context c
    on c.subsidiary_name = p.subsidiary_name and c.strategic_city_name = p.strategic_city_name
  join public.common_region_plan_region r
    on r.subsidiary_name = p.subsidiary_name and r.strategic_city_name = p.strategic_city_name and r.plan_id = p.plan_id
on conflict do nothing;

insert into public.common_routing_plan (
    subsidiary_name, strategic_city_name, source_strategic_city_name, routing_plan_id, region_set_id,
    policy_version, plan_status, revision, source_sha256, manifest_sha256
)
select p.subsidiary_name, p.strategic_city_name,
       coalesce(c.source_strategic_city_name, p.strategic_city_name),
       p.plan_id,
       'rs_' || left(coalesce(p.fixed_region_sha256, md5(p.plan_id)), 24),
       p.policy_version, p.plan_status, greatest(coalesce(p.revision, 1), 1), p.source_sha256, p.manifest_sha256
  from public.common_region_plan p
  left join public.common_city_context c
    on c.subsidiary_name = p.subsidiary_name and c.strategic_city_name = p.strategic_city_name
on conflict (subsidiary_name, strategic_city_name, routing_plan_id) do nothing;

insert into public.common_region_set_postal (
    subsidiary_name, source_strategic_city_name, region_set_id, postal_code,
    region_seq, area_type, membership_rank, is_primary
)
select p.subsidiary_name,
       coalesce(c.source_strategic_city_name, p.strategic_city_name),
       rp.region_set_id, p.postal_code, p.region_seq, p.area_type,
       1, true
  from public.common_region_plan_postal p
  join public.common_routing_plan rp
    on rp.subsidiary_name = p.subsidiary_name and rp.strategic_city_name = p.strategic_city_name and rp.routing_plan_id = p.plan_id
  left join public.common_city_context c
    on c.subsidiary_name = p.subsidiary_name and c.strategic_city_name = p.strategic_city_name
on conflict do nothing;

insert into public.common_routing_plan_technician (
    subsidiary_name, strategic_city_name, routing_plan_id, employee_code,
    assigned_region_seq, policy_mode, active_flag
)
select subsidiary_name, strategic_city_name, plan_id, employee_code,
       assigned_region_seq, policy_mode, active_flag
  from public.common_region_plan_technician
on conflict do nothing;

insert into public.common_routing_plan_activation (
    subsidiary_name, strategic_city_name, activation_revision,
    routing_plan_id, activated_by, activation_reference, active_flag
)
select a.subsidiary_name, a.strategic_city_name, a.activation_revision,
       a.plan_id, a.activated_by, a.activation_reference, a.active_flag
  from public.common_region_plan_activation a
  join public.common_routing_plan rp
    on rp.subsidiary_name = a.subsidiary_name
   and rp.strategic_city_name = a.strategic_city_name
   and rp.routing_plan_id = a.plan_id
on conflict do nothing;
