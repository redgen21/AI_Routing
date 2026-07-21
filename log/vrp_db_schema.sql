--
-- PostgreSQL database dump
--

\restrict 3B0sLsRh13Ww25jCIifkI3DLuIMMvSt7gCSZjsEnhkHUFnhSJ1pqvTXbTwgvAWB

-- Dumped from database version 14.23 (Ubuntu 14.23-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.23 (Ubuntu 14.23-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: common_avoid_area; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_avoid_area (
    avoid_area_id text NOT NULL,
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    area_name text NOT NULL,
    description text,
    geometry_json text NOT NULL,
    active_flag boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_geocode_attempt_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_geocode_attempt_log (
    address_key text NOT NULL,
    attempted_date date NOT NULL,
    status text,
    source text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_geocode_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_geocode_cache (
    address_key text NOT NULL,
    source_bucket text NOT NULL,
    address_line1 text,
    city text,
    state text,
    postal_code text,
    country_name text,
    matched_address text,
    match_indicator text,
    match_type text,
    longitude double precision,
    latitude double precision,
    tiger_line_id text,
    tiger_line_side text,
    census_state_fips text,
    census_county_fips text,
    census_tract text,
    census_block text,
    geocoded_date date,
    source text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_geocode_daily_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_geocode_daily_log (
    run_date date NOT NULL,
    source_bucket text NOT NULL,
    used_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_heavy_repair_rule_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_heavy_repair_rule_master (
    product_group_code text NOT NULL,
    product_code text NOT NULL,
    detailed_symptom_code text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_job_input; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_job_input (
    record_id text NOT NULL,
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    svc_engineer_code text,
    svc_engineer_name text,
    service_product_group_code text,
    service_product_code text,
    receipt_detail_symptom_code text,
    gsfs_receipt_no text NOT NULL,
    promise_date text NOT NULL,
    city_name text,
    state_name text,
    country_name text,
    postal_code text,
    address_line1_info text,
    fixed boolean DEFAULT false NOT NULL,
    job_slot_count integer DEFAULT 1 NOT NULL,
    latitude double precision,
    longitude double precision,
    source text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    reschedule boolean DEFAULT false NOT NULL
);


--
-- Name: common_region_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_region_master (
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    postal_code text NOT NULL,
    region_seq integer NOT NULL,
    region_name text NOT NULL,
    region_center_latitude double precision,
    region_center_longitude double precision,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    area_type text
);


--
-- Name: common_request_technician_input; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_request_technician_input (
    record_id text NOT NULL,
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    promise_date text NOT NULL,
    employee_code text NOT NULL,
    employee_name text NOT NULL,
    center_type text,
    shift_start text,
    shift_end text,
    slot_count integer,
    max_jobs integer,
    available boolean DEFAULT true NOT NULL,
    start_location_type text,
    start_location_address text,
    source text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    priority_group text DEFAULT 'B'::text NOT NULL,
    max_minutes integer,
    preferred_region_name text
);


--
-- Name: common_routing_config_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_routing_config_master (
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    distance_backend text,
    assignment_distance_backend text,
    osrm_url text,
    osrm_profile text,
    effective_service_per_sm integer,
    target_sm_per_region integer,
    service_time_per_job_min integer,
    max_work_min_per_sm_day integer,
    max_travel_min_per_sm_day integer,
    max_travel_km_per_sm_day integer,
    timezone_offset text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    max_single_leg_min integer,
    max_home_to_job_min integer,
    long_leg_penalty_start_min integer,
    long_leg_penalty_multiplier numeric
);


--
-- Name: common_routing_request; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_routing_request (
    request_id text NOT NULL,
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    promise_date text NOT NULL,
    routing_job_id text,
    routing_status text,
    payload_json text,
    status_json text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_routing_result; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_routing_result (
    request_id text NOT NULL,
    routing_job_id text,
    result_json text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_technician_capability_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_technician_capability_master (
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    employee_code text NOT NULL,
    product_group_code text NOT NULL,
    product_code text NOT NULL,
    repair_allowed boolean DEFAULT true NOT NULL,
    heavy_repair_allowed boolean DEFAULT true NOT NULL,
    priority_score integer,
    effective_start_date date,
    effective_end_date date,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: common_technician_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.common_technician_master (
    subsidiary_name text NOT NULL,
    strategic_city_name text NOT NULL,
    employee_code text NOT NULL,
    employee_name text NOT NULL,
    center_type text,
    home_address text,
    home_city text,
    home_state text,
    home_country text,
    home_postal_code text,
    home_latitude double precision,
    home_longitude double precision,
    active_flag boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    priority_group text DEFAULT 'B'::text NOT NULL,
    max_home_to_job_min integer
);


--
-- Name: common_avoid_area common_avoid_area_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_avoid_area
    ADD CONSTRAINT common_avoid_area_pkey PRIMARY KEY (avoid_area_id);


--
-- Name: common_geocode_attempt_log common_geocode_attempt_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_geocode_attempt_log
    ADD CONSTRAINT common_geocode_attempt_log_pkey PRIMARY KEY (address_key, attempted_date);


--
-- Name: common_geocode_cache common_geocode_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_geocode_cache
    ADD CONSTRAINT common_geocode_cache_pkey PRIMARY KEY (address_key, source_bucket);


--
-- Name: common_geocode_daily_log common_geocode_daily_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_geocode_daily_log
    ADD CONSTRAINT common_geocode_daily_log_pkey PRIMARY KEY (run_date, source_bucket);


--
-- Name: common_heavy_repair_rule_master common_heavy_repair_rule_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_heavy_repair_rule_master
    ADD CONSTRAINT common_heavy_repair_rule_master_pkey PRIMARY KEY (product_group_code, product_code, detailed_symptom_code);


--
-- Name: common_job_input common_job_input_context_date_receipt_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_job_input
    ADD CONSTRAINT common_job_input_context_date_receipt_key UNIQUE (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no);


--
-- Name: common_job_input common_job_input_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_job_input
    ADD CONSTRAINT common_job_input_pkey PRIMARY KEY (record_id);


--
-- Name: common_job_input common_job_input_subsidiary_name_strategic_city_name_promis_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_job_input
    ADD CONSTRAINT common_job_input_subsidiary_name_strategic_city_name_promis_key UNIQUE (subsidiary_name, strategic_city_name, promise_date, gsfs_receipt_no);


--
-- Name: common_region_master common_region_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_region_master
    ADD CONSTRAINT common_region_master_pkey PRIMARY KEY (subsidiary_name, strategic_city_name, postal_code);


--
-- Name: common_request_technician_input common_request_technician_input_context_employee_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_request_technician_input
    ADD CONSTRAINT common_request_technician_input_context_employee_key UNIQUE (subsidiary_name, strategic_city_name, promise_date, employee_code);


--
-- Name: common_request_technician_input common_request_technician_input_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_request_technician_input
    ADD CONSTRAINT common_request_technician_input_pkey PRIMARY KEY (record_id);


--
-- Name: common_routing_config_master common_routing_config_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_routing_config_master
    ADD CONSTRAINT common_routing_config_master_pkey PRIMARY KEY (subsidiary_name, strategic_city_name);


--
-- Name: common_routing_request common_routing_request_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_routing_request
    ADD CONSTRAINT common_routing_request_pkey PRIMARY KEY (request_id);


--
-- Name: common_routing_result common_routing_result_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_routing_result
    ADD CONSTRAINT common_routing_result_pkey PRIMARY KEY (request_id);


--
-- Name: common_technician_capability_master common_technician_capability_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_technician_capability_master
    ADD CONSTRAINT common_technician_capability_master_pkey PRIMARY KEY (subsidiary_name, strategic_city_name, employee_code, product_group_code, product_code);


--
-- Name: common_technician_master common_technician_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.common_technician_master
    ADD CONSTRAINT common_technician_master_pkey PRIMARY KEY (subsidiary_name, strategic_city_name, employee_code);


--
-- Name: common_geocode_attempt_log_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX common_geocode_attempt_log_updated_at_idx ON public.common_geocode_attempt_log USING btree (updated_at);


--
-- Name: common_geocode_cache_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX common_geocode_cache_updated_at_idx ON public.common_geocode_cache USING btree (updated_at);


--
-- PostgreSQL database dump complete
--

\unrestrict 3B0sLsRh13Ww25jCIifkI3DLuIMMvSt7gCSZjsEnhkHUFnhSJ1pqvTXbTwgvAWB

