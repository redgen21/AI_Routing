-- Additive compatibility migration: generic region plans are not Atlanta-six-area plans.
-- Apply after V001.  No data is deleted or rewritten.
alter table common_region_plan_region
    drop constraint if exists common_region_plan_region_region_seq_check;
alter table common_region_plan_region
    add constraint common_region_plan_region_region_seq_positive_check
    check (region_seq > 0);
