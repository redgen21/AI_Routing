\set ON_ERROR_STOP on

-- Development-only, additive technician profile copy for the reviewed
-- Atlanta_6area roster.  It intentionally never writes request/attendance
-- input: availability, slots, and shifts are promise-date-specific facts.
--
-- Static master/capability copy and plan assignment are separate transactions.
-- This lets the legacy region import precede V001 activation while ensuring
-- that no assignment is ever written outside the runtime-consumed plan table.

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '45s';

DO $$
BEGIN
    IF current_database() <> 'vrp_db_dev' THEN
        RAISE EXCEPTION 'Expected vrp_db_dev, connected to %', current_database();
    END IF;
    IF to_regclass('public.common_technician_master') IS NULL
       OR to_regclass('public.common_technician_capability_master') IS NULL THEN
        RAISE EXCEPTION 'Required technician master tables do not exist';
    END IF;
END
$$;

SELECT pg_advisory_xact_lock(hashtext('LGEAI:Atlanta_6area:technician-profile:v1'));
LOCK TABLE public.common_technician_master,
           public.common_technician_capability_master
    IN SHARE ROW EXCLUSIVE MODE;

-- A previous interrupted \i run may have preserved these session-local tables.
-- Never use an unqualified DROP here: only this session's temporary schema is
-- eligible for replay cleanup.
DO $$
BEGIN
    IF pg_my_temp_schema() <> 0 THEN
        EXECUTE 'DROP TABLE IF EXISTS pg_temp.a6_assignment_output, '
             || 'pg_temp.a6_assignment_result, pg_temp.a6_expected_region, '
             || 'pg_temp.a6_technician_stage';
    END IF;
END
$$;

CREATE TEMP TABLE a6_technician_stage (
    employee_code text PRIMARY KEY,
    employee_name text NOT NULL,
    assignment_name text NOT NULL,
    assigned_region_seq integer NOT NULL,
    assigned_region_id text NOT NULL
) ON COMMIT PRESERVE ROWS;

CREATE TEMP TABLE a6_expected_region (
    region_seq integer PRIMARY KEY,
    region_id text NOT NULL UNIQUE,
    region_name text NOT NULL UNIQUE
) ON COMMIT PRESERVE ROWS;

CREATE TEMP TABLE a6_assignment_result (
    assignment_ready boolean NOT NULL,
    reason text NOT NULL,
    plan_id text
) ON COMMIT PRESERVE ROWS;

CREATE TEMP TABLE a6_assignment_output (
    plan_id text NOT NULL,
    employee_code text NOT NULL,
    employee_name text NOT NULL,
    assigned_region_seq integer NOT NULL,
    assigned_region_name text NOT NULL,
    policy_mode text NOT NULL
) ON COMMIT PRESERVE ROWS;

INSERT INTO a6_expected_region (region_seq, region_id, region_name) VALUES
    (1, 'atlanta_6area_r01', 'Atlanta_6area Zone 1'),
    (2, 'atlanta_6area_r02', 'Atlanta_6area Zone 2'),
    (3, 'atlanta_6area_r03', 'Atlanta_6area Zone 3'),
    (4, 'atlanta_6area_r04', 'Atlanta_6area Zone 4'),
    (5, 'atlanta_6area_r05', 'Atlanta_6area Zone 5'),
    (6, 'atlanta_6area_r06', 'Atlanta_6area ATL Outer Area');

INSERT INTO a6_technician_stage (
    employee_code, employee_name, assignment_name, assigned_region_seq, assigned_region_id
) VALUES
    ('AI105115', 'Jason Patterson', 'ATL Outer Area', 6, 'atlanta_6area_r06'),
    ('AI102448', 'Gilbert Dupree', 'Zone 1', 1, 'atlanta_6area_r01'),
    ('AI102087', 'Marcus Harmon', 'Zone 1', 1, 'atlanta_6area_r01'),
    ('AI105116', 'Frank Hooks', 'Zone 1', 1, 'atlanta_6area_r01'),
    ('AI103146', 'Arthur Gloster', 'Zone 2', 2, 'atlanta_6area_r02'),
    ('AI102608', 'Sergey Ashihmin', 'Zone 2', 2, 'atlanta_6area_r02'),
    ('AI102977', 'Yu Zheng', 'Zone 2', 2, 'atlanta_6area_r02'),
    ('AI103128', 'Ivan Zimic', 'Zone 3', 3, 'atlanta_6area_r03'),
    ('AI103261', 'Horace Bull', 'Zone 4', 4, 'atlanta_6area_r04'),
    ('AI103317', 'Richard Grant', 'Zone 4', 4, 'atlanta_6area_r04'),
    ('AI005576', 'RUDY EDOLE', 'Zone 4', 4, 'atlanta_6area_r04'),
    ('AI103264', 'Adolph Scarlett', 'Zone 5', 5, 'atlanta_6area_r05'),
    ('AI102961', 'Nicholas Foskin', 'Zone 5', 5, 'atlanta_6area_r05'),
    ('AI102315', 'Winston Perez', 'Zone 5', 5, 'atlanta_6area_r05');

DO $$
DECLARE
    staged_count integer;
    source_count integer;
BEGIN
    SELECT count(*) INTO staged_count FROM a6_technician_stage;
    IF staged_count <> 14 THEN
        RAISE EXCEPTION 'Expected exactly 14 staged technicians, found %', staged_count;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM a6_technician_stage s
        LEFT JOIN a6_expected_region r
          ON r.region_seq = s.assigned_region_seq
         AND r.region_id = s.assigned_region_id
         AND r.region_name = 'Atlanta_6area ' || s.assignment_name
        WHERE s.employee_code !~ '^AI[0-9]{6}$'
           OR s.employee_name = ''
           OR r.region_seq IS NULL
    ) THEN
        RAISE EXCEPTION 'Staged technician ID, name, or assignment is invalid';
    END IF;

    SELECT count(*) INTO source_count
    FROM public.common_technician_master m
    JOIN a6_technician_stage s ON s.employee_code = m.employee_code
    WHERE m.subsidiary_name = 'LGEAI'
      AND m.strategic_city_name = 'Atlanta, GA';
    IF source_count <> 14 THEN
        RAISE EXCEPTION 'Expected 14 matching LGEAI/Atlanta, GA source masters, found %', source_count;
    END IF;

    -- convert_to performs byte-for-byte UTF-8 comparison; do not trim, fold
    -- case, or otherwise normalize the user-supplied names.
    IF EXISTS (
        SELECT 1
        FROM a6_technician_stage s
        LEFT JOIN public.common_technician_master m
          ON m.subsidiary_name = 'LGEAI'
         AND m.strategic_city_name = 'Atlanta, GA'
         AND m.employee_code = s.employee_code
        WHERE m.employee_code IS NULL
           OR m.active_flag IS NOT TRUE
           OR convert_to(m.employee_name, 'UTF8') IS DISTINCT FROM convert_to(s.employee_name, 'UTF8')
    ) THEN
        RAISE EXCEPTION 'Source technician is missing, inactive, or has a non-exact employee name';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM a6_technician_stage s
        LEFT JOIN public.common_technician_capability_master c
          ON c.subsidiary_name = 'LGEAI'
         AND c.strategic_city_name = 'Atlanta, GA'
         AND c.employee_code = s.employee_code
        GROUP BY s.employee_code
        HAVING count(c.employee_code) < 1
    ) THEN
        RAISE EXCEPTION 'Every staged source technician must have at least one capability';
    END IF;

    -- The Atlanta_6area target is an exact, self-contained 14-person profile.
    -- Rows outside the reviewed roster are never deleted or silently retained.
    IF EXISTS (
        SELECT 1
        FROM public.common_technician_master t
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND NOT EXISTS (
              SELECT 1 FROM a6_technician_stage s WHERE s.employee_code = t.employee_code
          )
    ) THEN
        RAISE EXCEPTION 'Target contains a non-staged Atlanta_6area technician master row';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.common_technician_capability_master t
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND NOT EXISTS (
              SELECT 1 FROM a6_technician_stage s WHERE s.employee_code = t.employee_code
          )
    ) THEN
        RAISE EXCEPTION 'Target contains a non-staged Atlanta_6area capability row';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.common_technician_master t
        JOIN public.common_technician_master m
          ON m.subsidiary_name = 'LGEAI'
         AND m.strategic_city_name = 'Atlanta, GA'
         AND m.employee_code = t.employee_code
        JOIN a6_technician_stage s ON s.employee_code = t.employee_code
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND ROW(
              t.employee_name, t.center_type, t.home_address, t.home_city,
              t.home_state, t.home_country, t.home_postal_code, t.home_latitude,
              t.home_longitude, t.active_flag, t.priority_group,
              t.max_home_to_job_min, t.created_at, t.updated_at
          ) IS DISTINCT FROM ROW(
              m.employee_name, m.center_type, m.home_address, m.home_city,
              m.home_state, m.home_country, m.home_postal_code, m.home_latitude,
              m.home_longitude, m.active_flag, m.priority_group,
              m.max_home_to_job_min, m.created_at, m.updated_at
          )
    ) THEN
        RAISE EXCEPTION 'Existing staged target technician master conflicts with source';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.common_technician_capability_master t
        LEFT JOIN public.common_technician_capability_master c
          ON c.subsidiary_name = 'LGEAI'
         AND c.strategic_city_name = 'Atlanta, GA'
         AND c.employee_code = t.employee_code
         AND c.product_group_code = t.product_group_code
         AND c.product_code = t.product_code
        JOIN a6_technician_stage s ON s.employee_code = t.employee_code
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND (
              c.employee_code IS NULL
              OR ROW(
                  t.repair_allowed, t.heavy_repair_allowed, t.priority_score,
                  t.effective_start_date, t.effective_end_date, t.created_at, t.updated_at
              ) IS DISTINCT FROM ROW(
                  c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                  c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
              )
          )
    ) THEN
        RAISE EXCEPTION 'Existing staged target capability conflicts with source';
    END IF;
END
$$;

-- Copy every current source column, including provenance timestamps.  Existing
-- matching rows are untouched; conflicting rows were rejected above.
INSERT INTO public.common_technician_master AS target (
    subsidiary_name, strategic_city_name, employee_code, employee_name,
    center_type, home_address, home_city, home_state, home_country,
    home_postal_code, home_latitude, home_longitude, active_flag,
    priority_group, max_home_to_job_min, created_at, updated_at
)
SELECT
    'LGEAI', 'Atlanta_6area', m.employee_code, m.employee_name,
    m.center_type, m.home_address, m.home_city, m.home_state, m.home_country,
    m.home_postal_code, m.home_latitude, m.home_longitude, m.active_flag,
    m.priority_group, m.max_home_to_job_min, m.created_at, m.updated_at
FROM public.common_technician_master m
JOIN a6_technician_stage s ON s.employee_code = m.employee_code
WHERE m.subsidiary_name = 'LGEAI'
  AND m.strategic_city_name = 'Atlanta, GA'
ON CONFLICT (subsidiary_name, strategic_city_name, employee_code) DO NOTHING;

INSERT INTO public.common_technician_capability_master AS target (
    subsidiary_name, strategic_city_name, employee_code, product_group_code,
    product_code, repair_allowed, heavy_repair_allowed, priority_score,
    effective_start_date, effective_end_date, created_at, updated_at
)
SELECT
    'LGEAI', 'Atlanta_6area', c.employee_code, c.product_group_code,
    c.product_code, c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
    c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
FROM public.common_technician_capability_master c
JOIN a6_technician_stage s ON s.employee_code = c.employee_code
WHERE c.subsidiary_name = 'LGEAI'
  AND c.strategic_city_name = 'Atlanta, GA'
ON CONFLICT (
    subsidiary_name, strategic_city_name, employee_code, product_group_code, product_code
) DO NOTHING;

DO $$
DECLARE
    target_master_count integer;
BEGIN
    SELECT count(*) INTO target_master_count
    FROM public.common_technician_master
    WHERE subsidiary_name = 'LGEAI'
      AND strategic_city_name = 'Atlanta_6area';
    IF target_master_count <> 14 THEN
        RAISE EXCEPTION 'Post-copy target master count is %, expected 14', target_master_count;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.common_technician_master m
        JOIN a6_technician_stage s ON s.employee_code = m.employee_code
        LEFT JOIN public.common_technician_master t
          ON t.subsidiary_name = 'LGEAI'
         AND t.strategic_city_name = 'Atlanta_6area'
         AND t.employee_code = m.employee_code
        WHERE m.subsidiary_name = 'LGEAI'
          AND m.strategic_city_name = 'Atlanta, GA'
          AND (
              t.employee_code IS NULL
              OR ROW(
                  t.employee_name, t.center_type, t.home_address, t.home_city,
                  t.home_state, t.home_country, t.home_postal_code, t.home_latitude,
                  t.home_longitude, t.active_flag, t.priority_group,
                  t.max_home_to_job_min, t.created_at, t.updated_at
              ) IS DISTINCT FROM ROW(
                  m.employee_name, m.center_type, m.home_address, m.home_city,
                  m.home_state, m.home_country, m.home_postal_code, m.home_latitude,
                  m.home_longitude, m.active_flag, m.priority_group,
                  m.max_home_to_job_min, m.created_at, m.updated_at
              )
          )
    ) THEN
        RAISE EXCEPTION 'Post-copy target master values do not exactly equal source';
    END IF;

    IF EXISTS (
        WITH source_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            JOIN a6_technician_stage s ON s.employee_code = c.employee_code
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta, GA'
        ), target_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta_6area'
        )
        SELECT * FROM source_capability
        EXCEPT ALL
        SELECT * FROM target_capability
    ) OR EXISTS (
        WITH source_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            JOIN a6_technician_stage s ON s.employee_code = c.employee_code
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta, GA'
        ), target_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta_6area'
        )
        SELECT * FROM target_capability
        EXCEPT ALL
        SELECT * FROM source_capability
    ) THEN
        RAISE EXCEPTION 'Post-copy target capabilities are not bidirectionally equal to source';
    END IF;
END
$$;

COMMIT;

-- Assignment is deliberately separate.  A missing/inactive/mismatched plan is
-- not repaired here; it results in assignment_ready=false and no assignment DML.
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '45s';

SELECT pg_advisory_xact_lock(hashtext('LGEAI:Atlanta_6area:technician-profile:v1'));
LOCK TABLE public.common_technician_master,
           public.common_technician_capability_master
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    v_plan_id text;
    v_plan_count integer;
BEGIN
    IF current_database() <> 'vrp_db_dev' THEN
        RAISE EXCEPTION 'Expected vrp_db_dev, connected to %', current_database();
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (
            VALUES
                ('common_city_context', 'subsidiary_name'),
                ('common_city_context', 'strategic_city_name'),
                ('common_city_context', 'context_status'),
                ('common_region_plan', 'subsidiary_name'),
                ('common_region_plan', 'strategic_city_name'),
                ('common_region_plan', 'plan_id'),
                ('common_region_plan', 'plan_status'),
                ('common_region_plan', 'verification_only'),
                ('common_region_plan', 'technician_policy_sha256'),
                ('common_region_plan_region', 'region_seq'),
                ('common_region_plan_region', 'region_id'),
                ('common_region_plan_region', 'region_name'),
                ('common_region_plan_technician', 'employee_code'),
                ('common_region_plan_technician', 'assigned_region_seq'),
                ('common_region_plan_technician', 'policy_mode'),
                ('common_region_plan_technician', 'active_flag'),
                ('common_region_plan_activation', 'plan_id'),
                ('common_region_plan_activation', 'active_flag')
        ) AS required_columns(table_name, column_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = required_columns.table_name
              AND c.column_name = required_columns.column_name
        )
    ) THEN
        INSERT INTO a6_assignment_result VALUES (false, 'V001_SCHEMA_UNAVAILABLE', NULL);
        RETURN;
    END IF;

    EXECUTE 'LOCK TABLE public.common_city_context, public.common_region_plan, '
         || 'public.common_region_plan_region, public.common_region_plan_technician, '
         || 'public.common_region_plan_activation IN SHARE ROW EXCLUSIVE MODE';

    SELECT count(*), min(p.plan_id)
      INTO v_plan_count, v_plan_id
      FROM public.common_region_plan_activation a
      JOIN public.common_region_plan p
        ON p.subsidiary_name = a.subsidiary_name
       AND p.strategic_city_name = a.strategic_city_name
       AND p.plan_id = a.plan_id
      JOIN public.common_city_context c
        ON c.subsidiary_name = a.subsidiary_name
       AND c.strategic_city_name = a.strategic_city_name
     WHERE a.subsidiary_name = 'LGEAI'
       AND a.strategic_city_name = 'Atlanta_6area'
       AND a.active_flag IS TRUE
       AND c.context_status = 'active'
       AND p.plan_status = 'active'
       AND p.verification_only IS TRUE
       AND p.plan_id = 'atlanta_6area_v2_78efd085794cda992d4bce738c5395f0174b1b80c3560c7eddb46b47f67d7523'
       AND p.technician_policy_sha256 = 'aa8967d75389ca5f329c7a076b313062c87b6f66529f1ca68b0eec63a74c84a9';

    IF v_plan_count <> 1 THEN
        INSERT INTO a6_assignment_result VALUES (false, 'EXACT_ACTIVE_PLAN_NOT_FOUND', NULL);
        RETURN;
    END IF;

    IF (
        SELECT count(*)
        FROM public.common_region_plan_region r
        WHERE r.subsidiary_name = 'LGEAI'
          AND r.strategic_city_name = 'Atlanta_6area'
          AND r.plan_id = v_plan_id
    ) <> 6
    OR EXISTS (
        SELECT 1
        FROM a6_expected_region e
        LEFT JOIN public.common_region_plan_region r
          ON r.subsidiary_name = 'LGEAI'
         AND r.strategic_city_name = 'Atlanta_6area'
         AND r.plan_id = v_plan_id
         AND r.region_seq = e.region_seq
        WHERE r.region_seq IS NULL
           OR r.region_id IS DISTINCT FROM e.region_id
           OR r.region_name IS DISTINCT FROM e.region_name
    ) THEN
        INSERT INTO a6_assignment_result VALUES (false, 'ACTIVE_PLAN_REGION_CONTRACT_INVALID', v_plan_id);
        RETURN;
    END IF;

    -- Transaction 1 intentionally commits the static copy even when V001 is
    -- not installed.  When assignment is possible, re-lock and revalidate the
    -- complete source/target profile here so a concurrent change between the
    -- two transactions cannot bind a stale or missing technician to the plan.
    IF (
        SELECT count(*)
        FROM public.common_technician_master m
        JOIN a6_technician_stage s ON s.employee_code = m.employee_code
        WHERE m.subsidiary_name = 'LGEAI'
          AND m.strategic_city_name = 'Atlanta, GA'
    ) <> 14
    OR EXISTS (
        SELECT 1
        FROM a6_technician_stage s
        LEFT JOIN public.common_technician_master m
          ON m.subsidiary_name = 'LGEAI'
         AND m.strategic_city_name = 'Atlanta, GA'
         AND m.employee_code = s.employee_code
        WHERE m.employee_code IS NULL
           OR m.active_flag IS NOT TRUE
           OR convert_to(m.employee_name, 'UTF8')
              IS DISTINCT FROM convert_to(s.employee_name, 'UTF8')
    )
    OR (
        SELECT count(*)
        FROM public.common_technician_master t
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
    ) <> 14
    OR EXISTS (
        SELECT 1
        FROM public.common_technician_master m
        JOIN a6_technician_stage s ON s.employee_code = m.employee_code
        LEFT JOIN public.common_technician_master t
          ON t.subsidiary_name = 'LGEAI'
         AND t.strategic_city_name = 'Atlanta_6area'
         AND t.employee_code = m.employee_code
        WHERE m.subsidiary_name = 'LGEAI'
          AND m.strategic_city_name = 'Atlanta, GA'
          AND (
              t.employee_code IS NULL
              OR ROW(
                  t.employee_name, t.center_type, t.home_address, t.home_city,
                  t.home_state, t.home_country, t.home_postal_code, t.home_latitude,
                  t.home_longitude, t.active_flag, t.priority_group,
                  t.max_home_to_job_min, t.created_at, t.updated_at
              ) IS DISTINCT FROM ROW(
                  m.employee_name, m.center_type, m.home_address, m.home_city,
                  m.home_state, m.home_country, m.home_postal_code, m.home_latitude,
                  m.home_longitude, m.active_flag, m.priority_group,
                  m.max_home_to_job_min, m.created_at, m.updated_at
              )
          )
    ) THEN
        RAISE EXCEPTION 'Technician master changed between copy and plan assignment';
    END IF;

    IF EXISTS (
        WITH source_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date,
                   c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            JOIN a6_technician_stage s ON s.employee_code = c.employee_code
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta, GA'
        ), target_capability AS (
            SELECT c.employee_code, c.product_group_code, c.product_code,
                   c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
                   c.effective_start_date, c.effective_end_date,
                   c.created_at, c.updated_at
            FROM public.common_technician_capability_master c
            WHERE c.subsidiary_name = 'LGEAI'
              AND c.strategic_city_name = 'Atlanta_6area'
        )
        (
            SELECT * FROM source_capability
            EXCEPT ALL
            SELECT * FROM target_capability
        )
        UNION ALL
        (
            SELECT * FROM target_capability
            EXCEPT ALL
            SELECT * FROM source_capability
        )
    ) THEN
        RAISE EXCEPTION 'Capabilities changed between copy and plan assignment';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.common_region_plan_technician t
        LEFT JOIN a6_technician_stage s ON s.employee_code = t.employee_code
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND t.plan_id = v_plan_id
          AND (
              s.employee_code IS NULL
              OR t.assigned_region_seq IS DISTINCT FROM s.assigned_region_seq
              OR t.policy_mode IS DISTINCT FROM 'assigned_region_boundary_spillover'
              OR t.active_flag IS NOT TRUE
          )
    ) THEN
        INSERT INTO a6_assignment_result VALUES (false, 'ACTIVE_PLAN_TECHNICIAN_CONFLICT', v_plan_id);
        RETURN;
    END IF;

    INSERT INTO public.common_region_plan_technician AS target (
        subsidiary_name, strategic_city_name, plan_id, employee_code,
        assigned_region_seq, policy_mode
    )
    SELECT
        'LGEAI', 'Atlanta_6area', v_plan_id, s.employee_code,
        s.assigned_region_seq, 'assigned_region_boundary_spillover'
    FROM a6_technician_stage s
    ON CONFLICT (subsidiary_name, strategic_city_name, plan_id, employee_code) DO NOTHING;

    IF (
        SELECT count(*)
        FROM public.common_region_plan_technician t
        WHERE t.subsidiary_name = 'LGEAI'
          AND t.strategic_city_name = 'Atlanta_6area'
          AND t.plan_id = v_plan_id
    ) <> 14
    OR EXISTS (
        SELECT 1
        FROM a6_technician_stage s
        LEFT JOIN public.common_region_plan_technician t
          ON t.subsidiary_name = 'LGEAI'
         AND t.strategic_city_name = 'Atlanta_6area'
         AND t.plan_id = v_plan_id
         AND t.employee_code = s.employee_code
        WHERE t.employee_code IS NULL
           OR t.assigned_region_seq IS DISTINCT FROM s.assigned_region_seq
           OR t.policy_mode IS DISTINCT FROM 'assigned_region_boundary_spillover'
           OR t.active_flag IS NOT TRUE
    ) THEN
        RAISE EXCEPTION 'Post-insert active plan technician assignments are not exact';
    END IF;

    INSERT INTO a6_assignment_result VALUES (true, 'EXACT_ACTIVE_PLAN_ASSIGNMENTS_VERIFIED', v_plan_id);
    INSERT INTO a6_assignment_output (
        plan_id, employee_code, employee_name, assigned_region_seq,
        assigned_region_name, policy_mode
    )
    SELECT
        v_plan_id, s.employee_code, s.employee_name, s.assigned_region_seq,
        e.region_name, 'assigned_region_boundary_spillover'
    FROM a6_technician_stage s
    JOIN a6_expected_region e ON e.region_seq = s.assigned_region_seq
    ORDER BY s.employee_code;
END
$$;

COMMIT;

-- Evidence report.  These SELECTs contain no source home-address details.
SELECT
    14 AS input_rows,
    14 AS accepted_rows,
    0 AS rejected_rows,
    (SELECT count(*) FROM a6_technician_stage) AS staged_rows,
    (SELECT count(*)
       FROM public.common_technician_master
      WHERE subsidiary_name = 'LGEAI' AND strategic_city_name = 'Atlanta, GA'
        AND employee_code IN (SELECT employee_code FROM a6_technician_stage)) AS source_master_rows,
    (SELECT count(*)
       FROM public.common_technician_master
      WHERE subsidiary_name = 'LGEAI' AND strategic_city_name = 'Atlanta_6area') AS target_master_rows,
    (SELECT count(*)
       FROM public.common_technician_capability_master c
      JOIN a6_technician_stage s ON s.employee_code = c.employee_code
      WHERE c.subsidiary_name = 'LGEAI' AND c.strategic_city_name = 'Atlanta, GA') AS source_capability_rows,
    (SELECT count(*)
       FROM public.common_technician_capability_master
      WHERE subsidiary_name = 'LGEAI' AND strategic_city_name = 'Atlanta_6area') AS target_capability_rows;

WITH source_capability AS (
    SELECT c.employee_code, c.product_group_code, c.product_code,
           c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
           c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
    FROM public.common_technician_capability_master c
    JOIN a6_technician_stage s ON s.employee_code = c.employee_code
    WHERE c.subsidiary_name = 'LGEAI' AND c.strategic_city_name = 'Atlanta, GA'
), target_capability AS (
    SELECT c.employee_code, c.product_group_code, c.product_code,
           c.repair_allowed, c.heavy_repair_allowed, c.priority_score,
           c.effective_start_date, c.effective_end_date, c.created_at, c.updated_at
    FROM public.common_technician_capability_master c
    WHERE c.subsidiary_name = 'LGEAI' AND c.strategic_city_name = 'Atlanta_6area'
)
SELECT
    (SELECT count(*) FROM (
        SELECT * FROM source_capability
        EXCEPT ALL
        SELECT * FROM target_capability
    ) AS missing_in_target) AS capability_rows_missing_in_target,
    (SELECT count(*) FROM (
        SELECT * FROM target_capability
        EXCEPT ALL
        SELECT * FROM source_capability
    ) AS unexpected_in_target) AS unexpected_target_capability_rows;

SELECT employee_code, employee_name, assignment_name, assigned_region_seq, assigned_region_id
FROM a6_technician_stage
ORDER BY employee_code;

SELECT assignment_ready, reason, plan_id
FROM a6_assignment_result;

SELECT plan_id, employee_code, employee_name, assigned_region_seq,
       assigned_region_name, policy_mode
FROM a6_assignment_output
ORDER BY employee_code;

DROP TABLE a6_assignment_output,
           a6_assignment_result,
           a6_expected_region,
           a6_technician_stage;
