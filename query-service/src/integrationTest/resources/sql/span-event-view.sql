-- Table: public.span-event-view
-- DROP TABLE IF EXISTS public."span-event-view"

CREATE TABLE IF NOT EXISTS public."span-event-view"
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    span_id bytea NOT NULL,
        span_kind text COLLATE pg_catalog."default",
        parent_span_id bytea,
        trace_id bytea NOT NULL,
        service_id text COLLATE pg_catalog."default",
        api_id text COLLATE pg_catalog."default",
        api_name text COLLATE pg_catalog."default",
        entry_api_id text COLLATE pg_catalog."default",
        protocol_name text COLLATE pg_catalog."default",
        backend_id text COLLATE pg_catalog."default",
        tags jsonb not null default '{}'::jsonb,
        status_code text COLLATE pg_catalog."default",
        start_time_millis bigint,
        end_time_millis bigint,
        duration_millis bigint,
        api_trace_id bytea,
        service_name text COLLATE pg_catalog."default",
        request_headers jsonb not null default '{}'::jsonb,
        request_params jsonb not null default '{}'::jsonb,
        response_headers jsonb not null default '{}'::jsonb,
        request_cookies jsonb not null default '{}'::jsonb,
        response_cookies jsonb not null default '{}'::jsonb,
        request_body text COLLATE pg_catalog."default",
        response_body text COLLATE pg_catalog."default",
        request_method text COLLATE pg_catalog."default",
        api_boundary_type text COLLATE pg_catalog."default",
        event_name text COLLATE pg_catalog."default",
        user_agent text COLLATE pg_catalog."default",
        status_message text COLLATE pg_catalog."default",
        status text COLLATE pg_catalog."default",
        request_body_type text COLLATE pg_catalog."default",
        response_body_type text COLLATE pg_catalog."default",
        api_trace_count integer,
        display_entity_name text COLLATE pg_catalog."default",
        display_span_name text COLLATE pg_catalog."default",
        api_has_pii boolean default false,
        user_identifier text COLLATE pg_catalog."default",
        namespace_name text COLLATE pg_catalog."default",
        cluster_name text COLLATE pg_catalog."default",
        request_url text COLLATE pg_catalog."default",
        http_url_path text COLLATE pg_catalog."default",
        api_is_external boolean default false,
        error_count integer,
        host_header text COLLATE pg_catalog."default",
        api_discovery_state text COLLATE pg_catalog."default",
        exception_count integer,
        user_role text COLLATE pg_catalog."default",
        user_scope text COLLATE pg_catalog."default",
        user_country text COLLATE pg_catalog."default",
        user_country_iso_code text COLLATE pg_catalog."default",
        user_state text COLLATE pg_catalog."default",
        user_city text COLLATE pg_catalog."default",
        user_latitude double precision,
        user_longitude double precision,
        ip_address text COLLATE pg_catalog."default",
        session_id text COLLATE pg_catalog."default",
        bytes_sent bigint,
        bytes_received bigint,
        domain_id text COLLATE pg_catalog."default",
        environment text COLLATE pg_catalog."default",
        api_exit_calls integer,
        api_callee_name_count jsonb not null default '{}'::jsonb,
        api_trace_error_span_count integer,
        processing_mode text COLLATE pg_catalog."default",
        is_bare boolean default false,
        is_sampled boolean default false,
        spans integer default 1,
        sensitive_data_types TEXT [],
        seq_id bigserial
)
WITH (
    OIDS = FALSE
);