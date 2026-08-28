\set ON_ERROR_STOP on

begin;

do $$
declare
    plan_status text;
begin
    select p.plan_status
      into plan_status
      from common_region_plan p
     where p.subsidiary_name = 'LGEAI'
       and p.strategic_city_name = 'Atlanta, GA'
       and p.plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8'
     for update;

    if plan_status is null then
        raise exception 'Target incomplete Plan was not found.';
    end if;
    if plan_status <> 'candidate' then
        raise exception 'Refusing to delete non-candidate Plan (status=%).', plan_status;
    end if;
end $$;

delete from common_region_plan_boundary_overflow
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_region_plan_postal
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_region_plan_technician
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_region_plan_region
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_routing_plan_technician
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and routing_plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_routing_plan
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and routing_plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_area_plan
 where subsidiary_name = 'LGEAI'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

delete from common_region_plan
 where subsidiary_name = 'LGEAI'
   and strategic_city_name = 'Atlanta, GA'
   and plan_id = 'rp2_atlanta_6area_v2_39d96d5830dcbef682a8';

commit;
