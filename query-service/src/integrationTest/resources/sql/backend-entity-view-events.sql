-- Table: public.backend-entity-view-events
-- DROP TABLE IF EXISTS public."backend-entity-view-events"

CREATE TABLE IF NOT EXISTS public."backend-entity-view-events"
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    backend_id text COLLATE pg_catalog."default",
    backend_host text COLLATE pg_catalog."default",
    backend_port text COLLATE pg_catalog."default",
    backend_protocol text COLLATE pg_catalog."default",
    backend_path text COLLATE pg_catalog."default",
    attribute_map jsonb not null default '{}'::jsonb,
    start_time_millis bigint,
    end_time_millis bigint,
    duration_millis bigint,
    bytes_received bigint,
    bytes_sent bigint,
    span_kind text COLLATE pg_catalog."default",
    error_count integer,
    exception_count integer,
    num_calls integer,
    backend_name text COLLATE pg_catalog."default",
    backend_trace_id text COLLATE pg_catalog."default",
    display_name text COLLATE pg_catalog."default",
    request_headers jsonb not null default '{}'::jsonb,
    request_params jsonb not null default '{}'::jsonb,
    response_headers jsonb not null default '{}'::jsonb,
    request_body_type text COLLATE pg_catalog."default",
    response_body_type text COLLATE pg_catalog."default",
    status_code text COLLATE pg_catalog."default",
    status_message text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    tags jsonb not null default '{}'::jsonb,
    caller_service_id text COLLATE pg_catalog."default",
    caller_api_id text COLLATE pg_catalog."default",
    environment text COLLATE pg_catalog."default",
    backend_operation text COLLATE pg_catalog."default",
    backend_destination text COLLATE pg_catalog."default",
    is_bare boolean default false,
    is_sampled boolean default false,
    seq_id bigserial
)
WITH (
    OIDS = FALSE
);