\set ON_ERROR_STOP on
\pset pager off

with target as (
    select
        'LGEAI'::text as subsidiary_name,
        'Atlanta, GA'::text as strategic_city_name,
        'rp2_atlanta_6area_v2_39d96d5830dcbef682a8'::text as plan_id
),
plan as (
    select p.*
      from common_region_plan p
      join target t
        on t.subsidiary_name = p.subsidiary_name
       and t.strategic_city_name = p.strategic_city_name
       and t.plan_id = p.plan_id
),
actual as (
    select
        p.subsidiary_name,
        p.strategic_city_name,
        p.plan_id,
        (select count(*) from common_region_plan_region r
          where (r.subsidiary_name, r.strategic_city_name, r.plan_id)
              = (p.subsidiary_name, p.strategic_city_name, p.plan_id)) as region_rows,
        (select count(*) from common_region_plan_postal z
          where (z.subsidiary_name, z.strategic_city_name, z.plan_id)
              = (p.subsidiary_name, p.strategic_city_name, p.plan_id)) as postal_rows,
        (select count(*) from common_region_plan_boundary_overflow o
          where (o.subsidiary_name, o.strategic_city_name, o.plan_id)
              = (p.subsidiary_name, p.strategic_city_name, p.plan_id)) as overflow_rows,
        (select count(*) from common_region_plan_technician x
          where (x.subsidiary_name, x.strategic_city_name, x.plan_id)
              = (p.subsidiary_name, p.strategic_city_name, p.plan_id)) as technician_rows,
        (select count(*) from common_region_plan_technician x
          where (x.subsidiary_name, x.strategic_city_name, x.plan_id)
              = (p.subsidiary_name, p.strategic_city_name, p.plan_id)
            and x.active_flag) as active_technician_rows,
        exists (
            select 1 from common_area_plan ap
             where ap.subsidiary_name = p.subsidiary_name
               and ap.plan_id = p.plan_id
               and ap.plan_status = 'active'
        ) as area_catalog_active,
        exists (
            select 1 from common_routing_plan rp
             where rp.subsidiary_name = p.subsidiary_name
               and rp.strategic_city_name = p.strategic_city_name
               and rp.routing_plan_id = p.plan_id
               and rp.plan_status = 'active'
        ) as routing_catalog_active
      from plan p
)
select
    p.plan_id,
    p.plan_status,
    p.revision as plan_revision,
    p.source_sha256 as workbook_sha256,
    p.verified_content_sha256 is not null as content_verified,
    p.unique_postal_count as expected_postal_rows,
    a.postal_rows as actual_postal_rows,
    p.technician_count as expected_active_technician_rows,
    a.active_technician_rows as actual_active_technician_rows,
    a.region_rows,
    a.overflow_rows,
    a.technician_rows,
    a.area_catalog_active,
    a.routing_catalog_active,
    (
        p.plan_status = 'active'
        and p.verified_content_sha256 is not null
        and a.region_rows > 0
        and a.postal_rows = p.unique_postal_count
        and a.active_technician_rows = p.technician_count
        and a.area_catalog_active
        and a.routing_catalog_active
    ) as ready_for_vrp_client
from plan p
join actual a using (subsidiary_name, strategic_city_name, plan_id);
