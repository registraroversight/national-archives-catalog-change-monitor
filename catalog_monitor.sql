--
-- PostgreSQL database dump
--

-- Dumped from database version 17.2 (Debian 17.2-1.pgdg120+1)
-- Dumped by pg_dump version 17.2 (Debian 17.2-1.pgdg120+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
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
-- Name: master; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.master (
    naid character varying NOT NULL,
    title character varying NOT NULL,
    level_of_description character varying NOT NULL,
    parent_series_naid character varying,
    parent_series_title character varying,
    parent_file_unit_naid character varying,
    parent_file_unit_title character varying,
    creator character varying,
    inclusive_start_date character varying,
    inclusive_end_date character varying,
    coverage_start_date character varying,
    coverage_end_date character varying,
    series_extents character varying,
    access_restriction_status character varying,
    specific_access_restrictions character varying,
    security_classification character varying,
    accession_numbers character varying,
    disposition_authority_numbers character varying,
    ldr_count character varying,
    scope_and_content_note character varying,
    function_and_use_note character varying,
    general_notes character varying,
    crccrca_number character varying,
    scrape_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.master OWNER TO user;

--
-- Name: master_history; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.master_history (
    h_naid character varying NOT NULL,
    h_title character varying NOT NULL,
    h_level_of_description character varying NOT NULL,
    h_parent_series_naid character varying,
    h_parent_series_title character varying,
    h_parent_file_unit_naid character varying,
    h_parent_file_unit_title character varying,
    h_creator character varying,
    h_inclusive_start_date character varying,
    h_inclusive_end_date character varying,
    h_coverage_start_date character varying,
    h_coverage_end_date character varying,
    h_series_extents character varying,
    h_access_restriction_status character varying,
    h_specific_access_restrictions character varying,
    h_security_classification character varying,
    h_accession_numbers character varying,
    h_disposition_authority_numbers character varying,
    h_ldr_count character varying,
    h_scope_and_content_note character varying,
    h_function_and_use_note character varying,
    h_general_notes character varying,
    h_crccrca_number character varying,
    h_scrape_timestamp timestamp without time zone NOT NULL,
    h_history_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.master_history OWNER TO user;

--
-- Name: master_temp; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.master_temp (
    temp_naid character varying NOT NULL,
    temp_title character varying NOT NULL,
    temp_level_of_description character varying NOT NULL,
    temp_parent_series_naid character varying,
    temp_parent_series_title character varying,
    temp_parent_file_unit_naid character varying,
    temp_parent_file_unit_title character varying,
    temp_creator character varying,
    temp_inclusive_start_date character varying,
    temp_inclusive_end_date character varying,
    temp_coverage_start_date character varying,
    temp_coverage_end_date character varying,
    temp_series_extents character varying,
    temp_access_restriction_status character varying,
    temp_specific_access_restrictions character varying,
    temp_security_classification character varying,
    temp_accession_numbers character varying,
    temp_disposition_authority_numbers character varying,
    temp_ldr_count character varying,
    temp_scope_and_content_note character varying,
    temp_function_and_use_note character varying,
    temp_general_notes character varying,
    temp_crccrca_number character varying,
    temp_scrape_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.master_temp OWNER TO user;

--
-- Name: object_url; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.object_url (
    naid character varying NOT NULL,
    digital_object_url character varying NOT NULL,
    digital_object_id character varying NOT NULL,
    scrape_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.object_url OWNER TO user;

--
-- Name: object_url_history; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.object_url_history (
    h_naid character varying NOT NULL,
    h_digital_object_url character varying NOT NULL,
    h_digital_object_id character varying NOT NULL,
    h_scrape_timestamp timestamp without time zone NOT NULL,
    h_history_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.object_url_history OWNER TO user;

--
-- Name: object_url_temp; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.object_url_temp (
    temp_naid character varying NOT NULL,
    temp_digital_object_url character varying NOT NULL,
    temp_digital_object_id character varying NOT NULL,
    temp_scrape_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.object_url_temp OWNER TO user;

--
-- Name: master master_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.master
    ADD CONSTRAINT master_pkey PRIMARY KEY (naid);


--
-- PostgreSQL database dump complete
--

