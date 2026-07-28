\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF current_database() <> 'vrp_db_dev' THEN
        RAISE EXCEPTION 'Expected vrp_db_dev, connected to %', current_database();
    END IF;
    IF to_regclass('public.common_region_master') IS NULL THEN
        RAISE EXCEPTION 'public.common_region_master does not exist';
    END IF;
END
$$;

SELECT pg_advisory_xact_lock(hashtext('LGEAI:Atlanta_6area:common_region_master'));
LOCK TABLE public.common_region_master IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE atlanta_6area_stage (
    postal_code text NOT NULL,
    strategic_city_name text NOT NULL,
    region_id text NOT NULL,
    region_seq_text text NOT NULL,
    area_name text NOT NULL,
    new_region_name text NOT NULL,
    area_type text NOT NULL
) ON COMMIT DROP;

\copy atlanta_6area_stage (postal_code,strategic_city_name,region_id,region_seq_text,area_name,new_region_name,area_type) FROM '/tmp/fixed_region_postal_atlanta_6area_atlanta_6area_new_atl_buckets_20260721_v2.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DO $$
DECLARE
    actual_rows integer;
    actual_postals integer;
BEGIN
    SELECT count(*), count(DISTINCT postal_code)
      INTO actual_rows, actual_postals
      FROM atlanta_6area_stage;

    IF actual_rows <> 297 OR actual_postals <> 297 THEN
        RAISE EXCEPTION 'Expected 297 rows/ZIPs, found % rows and % ZIPs',
            actual_rows, actual_postals;
    END IF;

    IF EXISTS (
        SELECT 1
          FROM atlanta_6area_stage
         WHERE postal_code !~ '^[0-9]{5}$'
            OR btrim(strategic_city_name) <> 'Atlanta_6area'
            OR region_seq_text !~ '^[1-6]$'
            OR btrim(region_id) = ''
            OR btrim(area_name) = ''
            OR btrim(new_region_name) = ''
            OR btrim(area_type) <> 'DMS'
    ) THEN
        RAISE EXCEPTION 'Required fixed-region value is missing or invalid';
    END IF;

    IF EXISTS (
        SELECT postal_code
          FROM atlanta_6area_stage
         GROUP BY postal_code
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'Duplicate postal_code found';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM atlanta_6area_stage
         WHERE region_id <> 'atlanta_6area_r0' || region_seq_text
            OR area_name <> CASE region_seq_text
                WHEN '1' THEN 'Zone 1'
                WHEN '2' THEN 'Zone 2'
                WHEN '3' THEN 'Zone 3'
                WHEN '4' THEN 'Zone 4'
                WHEN '5' THEN 'Zone 5'
                WHEN '6' THEN 'ATL Outer Area'
            END
            OR new_region_name <> 'Atlanta_6area ' || area_name
    ) THEN
        RAISE EXCEPTION 'Region id/name mapping is invalid';
    END IF;

    IF EXISTS (
        WITH expected(region_seq, expected_count) AS (
            VALUES (1,73), (2,42), (3,25), (4,49), (5,65), (6,43)
        ), actual AS (
            SELECT region_seq_text::integer AS region_seq, count(*) AS actual_count
              FROM atlanta_6area_stage
             GROUP BY region_seq_text::integer
        )
        SELECT 1
          FROM expected
          FULL JOIN actual USING (region_seq)
         WHERE expected.expected_count IS DISTINCT FROM actual.actual_count
    ) THEN
        RAISE EXCEPTION 'Region row counts do not match the approved file';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM atlanta_6area_stage
         WHERE postal_code IN ('30028', '30040', '30041', '30107')
           AND NOT (
               region_seq_text = '3'
               AND region_id = 'atlanta_6area_r03'
               AND area_name = 'Zone 3'
               AND new_region_name = 'Atlanta_6area Zone 3'
               AND area_type = 'DMS'
           )
    ) THEN
        RAISE EXCEPTION 'Ambiguous ZIP primary owner is not the approved Zone 3';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM public.common_region_master AS target
         WHERE target.subsidiary_name = 'LGEAI'
           AND target.strategic_city_name = 'Atlanta_6area'
           AND NOT EXISTS (
               SELECT 1
                 FROM atlanta_6area_stage AS source
                WHERE source.postal_code = target.postal_code
           )
    ) THEN
        RAISE EXCEPTION 'Atlanta_6area contains ZIPs outside this file; no rows were deleted';
    END IF;
END
$$;

INSERT INTO public.common_region_master AS target (
    subsidiary_name,
    strategic_city_name,
    postal_code,
    region_seq,
    region_name,
    area_type,
    region_center_latitude,
    region_center_longitude
)
SELECT
    'LGEAI',
    strategic_city_name,
    postal_code,
    region_seq_text::integer,
    new_region_name,
    area_type,
    NULL::double precision,
    NULL::double precision
FROM atlanta_6area_stage
ON CONFLICT (subsidiary_name, strategic_city_name, postal_code)
DO UPDATE SET
    region_seq = EXCLUDED.region_seq,
    region_name = EXCLUDED.region_name,
    area_type = EXCLUDED.area_type,
    updated_at = now()
WHERE (target.region_seq, target.region_name, target.area_type)
      IS DISTINCT FROM
      (EXCLUDED.region_seq, EXCLUDED.region_name, EXCLUDED.area_type);

DO $$
BEGIN
    IF (
        SELECT count(*)
          FROM public.common_region_master
         WHERE subsidiary_name = 'LGEAI'
           AND strategic_city_name = 'Atlanta_6area'
    ) <> 297 THEN
        RAISE EXCEPTION 'Post-upsert Atlanta_6area row count is not 297';
    END IF;
END
$$;

SELECT
    count(*) AS rows,
    count(DISTINCT postal_code) AS unique_postals,
    min(area_type) AS area_type_min,
    max(area_type) AS area_type_max
FROM public.common_region_master
WHERE subsidiary_name = 'LGEAI'
  AND strategic_city_name = 'Atlanta_6area';

SELECT postal_code, region_seq, region_name, area_type
FROM public.common_region_master
WHERE subsidiary_name = 'LGEAI'
  AND strategic_city_name = 'Atlanta_6area'
  AND postal_code IN ('30028', '30040', '30041', '30107')
ORDER BY postal_code;

SELECT region_seq, count(*) AS rows
FROM public.common_region_master
WHERE subsidiary_name = 'LGEAI'
  AND strategic_city_name = 'Atlanta_6area'
GROUP BY region_seq
ORDER BY region_seq;

COMMIT;
