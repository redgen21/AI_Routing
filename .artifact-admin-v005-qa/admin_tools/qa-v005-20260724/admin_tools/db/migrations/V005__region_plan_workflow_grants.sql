-- Grant only the lifecycle permissions required by the region-plan workflow.
-- PostgreSQL GRANT is additive and idempotent; this migration never changes
-- ownership or grants access to legacy master tables owned by vrp_agent.
grant select, update on table public.common_city_context to vrp_agent;

grant select, update on table public.common_region_plan to vrp_agent;

grant select on table
    public.common_region_plan_region,
    public.common_region_plan_postal,
    public.common_region_plan_technician,
    public.common_region_plan_boundary_overflow
to vrp_agent;

grant select, insert, update on table public.common_region_plan_activation to vrp_agent;
