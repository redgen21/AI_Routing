\set ON_ERROR_STOP on

-- Prerequisite: run V001__atlanta_6area_region_plan.sql once.
-- Development only. Inserts one immutable active plan; reruns fail closed.
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
  IF current_database() <> 'vrp_db_dev' THEN
    RAISE EXCEPTION 'Expected vrp_db_dev, connected to %', current_database();
  END IF;
  IF to_regclass('public.common_region_plan_activation') IS NULL THEN
    RAISE EXCEPTION 'V001 schema is not installed';
  END IF;
END $$;

SELECT pg_advisory_xact_lock(hashtext('LGEAI:Atlanta_6area:active-plan:v2'));
LOCK TABLE common_city_context, common_region_plan, common_region_plan_region,
  common_region_plan_postal, common_region_plan_technician,
  common_region_plan_boundary_overflow, common_region_plan_activation
  IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE a6_region_stage (
  postal_code text,
  strategic_city_name text,
  region_id text,
  region_seq integer,
  area_name text,
  region_name text,
  area_type text
) ON COMMIT DROP;

\copy a6_region_stage(postal_code,strategic_city_name,region_id,region_seq,area_name,region_name,area_type) FROM '/tmp/fixed_region_postal_atlanta_6area.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')

DO $$
BEGIN
  IF (SELECT count(*) FROM a6_region_stage) <> 297
     OR (SELECT count(DISTINCT postal_code) FROM a6_region_stage) <> 297
     OR EXISTS (
       SELECT 1 FROM a6_region_stage
       WHERE postal_code !~ '^[0-9]{5}$'
          OR strategic_city_name <> 'Atlanta_6area'
          OR region_seq NOT BETWEEN 1 AND 6
          OR region_id <> 'atlanta_6area_r' || lpad(region_seq::text,2,'0')
          OR region_name <> 'Atlanta_6area ' || area_name
          OR area_type <> 'DMS'
     ) THEN
    RAISE EXCEPTION 'Atlanta_6area region CSV validation failed';
  END IF;
  IF EXISTS (
    SELECT 1 FROM (VALUES
      ('30028'),('30040'),('30041'),('30107')
    ) v(postal_code)
    LEFT JOIN a6_region_stage s USING (postal_code)
    WHERE s.region_seq IS DISTINCT FROM 3
  ) THEN
    RAISE EXCEPTION 'Ambiguous ZIPs must resolve to Zone 3';
  END IF;
  IF EXISTS (
    SELECT 1 FROM common_city_context
    WHERE subsidiary_name='LGEAI' AND strategic_city_name='Atlanta_6area'
  ) OR EXISTS (
    SELECT 1 FROM common_region_plan
    WHERE subsidiary_name='LGEAI' AND strategic_city_name='Atlanta_6area'
  ) THEN
    RAISE EXCEPTION 'Atlanta_6area versioned plan already exists; no overwrite performed';
  END IF;
  IF (SELECT count(*) FROM common_technician_master
      WHERE subsidiary_name='LGEAI' AND strategic_city_name='Atlanta_6area'
        AND active_flag) <> 14 THEN
    RAISE EXCEPTION 'Expected 14 active Atlanta_6area technician masters';
  END IF;
  IF EXISTS (
    SELECT 1 FROM (VALUES
      ('AI105115'),('AI102448'),('AI102087'),('AI105116'),('AI103146'),
      ('AI102608'),('AI102977'),('AI103128'),('AI103261'),('AI103317'),
      ('AI005576'),('AI103264'),('AI102961'),('AI102315')
    ) v(employee_code)
    LEFT JOIN common_technician_master m
      ON m.subsidiary_name='LGEAI'
     AND m.strategic_city_name='Atlanta_6area'
     AND m.employee_code=v.employee_code
     AND m.active_flag
    WHERE m.employee_code IS NULL
  ) THEN
    RAISE EXCEPTION 'Required Atlanta_6area technician master is missing';
  END IF;
END $$;

INSERT INTO common_city_context (
  subsidiary_name, strategic_city_name, source_strategic_city_name,
  context_version, policy_version, verification_only,
  context_status, activation_revision
) VALUES (
  'LGEAI','Atlanta_6area','Atlanta, GA',
  'atlanta-6area-plan-bundle/v2',
  'own_region_with_approved_boundary_overflow/v2',true,'active',1
);

INSERT INTO common_region_plan (
  subsidiary_name, strategic_city_name, plan_id, schema_version, policy_version,
  verification_only, source_file_name, source_sha256, manifest_sha256,
  bundle_sha256, fixed_region_sha256, boundary_policy_sha256,
  technician_policy_sha256, plan_status, revision,
  membership_input_rows, membership_accepted_rows, membership_rejected_rows,
  unique_postal_count, technician_count, ambiguous_postal_count,
  import_idempotency_key, imported_by, reviewed_by, review_reference, reviewed_at
) VALUES (
  'LGEAI','Atlanta_6area',
  'atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2',
  'atlanta-6area-plan-bundle/v2',
  'own_region_with_approved_boundary_overflow/v2',true,
  'New ATL Buckets.xlsx',
  '19cd5a42ef3f09e120dd84b26cc202deddc77aedcd08dbfc015d1a4144aeaedb',
  'ed94bc247623caa238eba9fda288826281e8f01c44476d0d49b26670c514ca12',
  '94869889f2e8ea60ca528aa59f0562c9f4b38d655c50782bd57cbcc833c2047b',
  '27786ecae987fb8d302ec7f4a0e4f6fa67b92f9c5313e01c4ba71eb3eaf05fc9',
  '6d4ee0f88abb6f034be8790dd012514db6ef5632428d8f9ad8287ff184714ec4',
  '06201871749a3f5048c30b767a3379931d8f777c75aa575b74e3545c2c2379cd',
  'active',2,301,301,0,297,14,4,
  'atlanta6-split-sheet-import-19cd5a42',
  'legacy_sql_development','legacy_sql_development',
  'Atlanta_6area Zone3 boundary decision',now()
);

INSERT INTO common_region_plan_region (
  subsidiary_name, strategic_city_name, plan_id,
  region_seq, region_id, region_name, source_territory
)
SELECT 'LGEAI','Atlanta_6area',
  'atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2',
  region_seq, min(region_id), min(region_name), min(area_name)
FROM a6_region_stage GROUP BY region_seq ORDER BY region_seq;

INSERT INTO common_region_plan_postal (
  subsidiary_name, strategic_city_name, plan_id, postal_code, region_seq,
  area_type, source_membership_count, resolution_status,
  source_region_seqs, resolution_metadata
)
SELECT 'LGEAI','Atlanta_6area',
  'atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2',
  postal_code, region_seq, area_type,
  CASE WHEN postal_code IN ('30028','30040','30041','30107') THEN 2 ELSE 1 END,
  CASE WHEN postal_code IN ('30028','30040','30041','30107') THEN 'resolved' ELSE 'not_required' END,
  CASE WHEN postal_code IN ('30028','30040','30041','30107')
       THEN '[2,3]'::jsonb ELSE jsonb_build_array(region_seq) END,
  CASE WHEN postal_code IN ('30028','30040','30041','30107') THEN
    jsonb_build_object('primary_region','Zone 3','primary_region_seq',3,
      'alternate_region','Zone 2','alternate_region_seq',2,
      'allow_overflow',true,'penalty_cost',4500,'rationale','')
  ELSE NULL END
FROM a6_region_stage ORDER BY postal_code;

INSERT INTO common_region_plan_technician (
  subsidiary_name, strategic_city_name, plan_id,
  employee_code, assigned_region_seq, policy_mode, active_flag
) VALUES
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI105115',6,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102448',1,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102087',1,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI105116',1,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI103146',2,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102608',2,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102977',2,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI103128',3,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI103261',4,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI103317',4,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI005576',4,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI103264',5,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102961',5,'assigned_region_boundary_spillover',true),
('LGEAI','Atlanta_6area','atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2','AI102315',5,'assigned_region_boundary_spillover',true);

INSERT INTO common_region_plan_boundary_overflow (
  subsidiary_name, strategic_city_name, plan_id, postal_code,
  primary_region_seq, alternate_region_seq, allow_overflow,
  penalty_cost, rationale, policy_version
)
SELECT 'LGEAI','Atlanta_6area',
  'atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2',
  postal_code,3,2,true,4500,NULL,
  'own_region_with_approved_boundary_overflow/v2'
FROM (VALUES ('30028'),('30040'),('30041'),('30107')) v(postal_code);

INSERT INTO common_region_plan_activation (
  subsidiary_name, strategic_city_name, activation_revision, plan_id,
  plan_revision, verification_only, active_flag, preview_digest,
  idempotency_key, activated_by, activation_reference
) VALUES (
  'LGEAI','Atlanta_6area',1,
  'atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2',
  1,true,true,
  '7efe2962f385d5479622f151b2a6e119a7d40397a2f32b63d87f82a150399ef2',
  'atlanta6-activate-19cd5a42','legacy_sql_development',
  'Atlanta_6area development verification'
);

DO $$
DECLARE c record;
BEGIN
  SELECT
    (SELECT count(*) FROM common_region_plan_region WHERE strategic_city_name='Atlanta_6area') regions,
    (SELECT count(*) FROM common_region_plan_postal WHERE strategic_city_name='Atlanta_6area') postals,
    (SELECT count(*) FROM common_region_plan_technician WHERE strategic_city_name='Atlanta_6area') technicians,
    (SELECT count(*) FROM common_region_plan_boundary_overflow WHERE strategic_city_name='Atlanta_6area') boundaries
  INTO c;
  IF (c.regions,c.postals,c.technicians,c.boundaries) <> (6,297,14,4) THEN
    RAISE EXCEPTION 'Plan row counts invalid: %', row_to_json(c);
  END IF;
END $$;

COMMIT;

SELECT p.plan_id,p.plan_status,p.revision,c.context_status,c.activation_revision,
  (SELECT count(*) FROM common_region_plan_postal x WHERE x.plan_id=p.plan_id) postal_rows,
  (SELECT count(*) FROM common_region_plan_technician x WHERE x.plan_id=p.plan_id) technician_rows,
  (SELECT count(*) FROM common_region_plan_boundary_overflow x WHERE x.plan_id=p.plan_id) overflow_rows
FROM common_region_plan p
JOIN common_city_context c USING (subsidiary_name,strategic_city_name)
WHERE p.subsidiary_name='LGEAI' AND p.strategic_city_name='Atlanta_6area';
