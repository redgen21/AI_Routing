-- Canonical Area Plan catalog.
--
-- Public management identity is deliberately limited to:
--   subsidiary -> operational city -> immutable plan
-- The pre-v2 strategic/target city is retained only as an internal bridge to
-- the existing region-plan tables until their consumers have migrated.

create table if not exists public.common_area_plan (
    subsidiary_name text not null,
    city_name text not null,
    plan_id text not null,
    plan_display_name text not null,
    checksum char(64),
    plan_status text not null check (plan_status in ('candidate','reviewed','active','superseded','retired')),
    -- Legacy Region Plans legitimately start at revision 0.
    plan_revision integer not null check (plan_revision >= 0),
    legacy_storage_city_name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, city_name, plan_id),
    unique (subsidiary_name, legacy_storage_city_name, plan_id)
);

create index if not exists common_area_plan_city_inventory_idx
    on public.common_area_plan (subsidiary_name, city_name, plan_status, updated_at desc);

-- Never choose an arbitrary source city when legacy records collide.  The
-- operator must reconcile these records before the catalog can be populated.
do $$
begin
    if exists (
        select 1
          from public.common_region_plan p
          left join public.common_city_context c
            on c.subsidiary_name = p.subsidiary_name
           and c.strategic_city_name = p.strategic_city_name
         group by p.subsidiary_name,
                  coalesce(c.source_strategic_city_name, p.strategic_city_name),
                  p.plan_id
        having count(distinct p.strategic_city_name) > 1
    ) then
        raise exception 'AREA_PLAN_CITY_PLAN_COLLISION: same subsidiary/city/plan maps to multiple legacy storage cities';
    end if;
end $$;

insert into public.common_area_plan (
    subsidiary_name,
    city_name,
    plan_id,
    plan_display_name,
    checksum,
    plan_status,
    plan_revision,
    legacy_storage_city_name,
    created_at,
    updated_at
)
select p.subsidiary_name,
       coalesce(c.source_strategic_city_name, p.strategic_city_name) as city_name,
       p.plan_id,
       p.plan_id as plan_display_name,
       p.bundle_sha256 as checksum,
       p.plan_status,
       p.revision,
       p.strategic_city_name as legacy_storage_city_name,
       p.created_at,
       p.updated_at
  from public.common_region_plan p
  left join public.common_city_context c
    on c.subsidiary_name = p.subsidiary_name
   and c.strategic_city_name = p.strategic_city_name
on conflict (subsidiary_name, legacy_storage_city_name, plan_id)
do update set city_name = excluded.city_name,
              plan_display_name = excluded.plan_display_name,
              checksum = excluded.checksum,
              plan_status = excluded.plan_status,
              plan_revision = excluded.plan_revision,
              updated_at = excluded.updated_at;

grant select, insert, update, delete on table public.common_area_plan to vrp_agent;
