-- Table: public.service-call-view-events
-- DROP TABLE IF EXISTS public."service-call-view-events"

CREATE TABLE IF NOT EXISTS public."service-call-view-events"
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    trace_id bytea NOT NULL,
    transaction_name text COLLATE pg_catalog."default",
    client_event_id bytea,
    server_event_id bytea,
    caller_service text COLLATE pg_catalog."default",
    caller_api text COLLATE pg_catalog."default",
    callee_namespace text COLLATE pg_catalog."default",
    callee_service text COLLATE pg_catalog."default",
    callee_api text COLLATE pg_catalog."default",
    request_url text COLLATE pg_catalog."default",
    request_method text COLLATE pg_catalog."default",
    protocol_name text COLLATE pg_catalog."default",
    response_status_code integer,
    start_time_millis bigint,
    duration_millis bigint,
    total_duration_millis bigint,
    server_bytes_received bigint,
    server_bytes_sent bigint,
    client_bytes_sent bigint,
    client_bytes_received bigint,
    error_count integer,
    exception_count integer,
    caller_service_id_str text COLLATE pg_catalog."default",
    caller_api_id_str text COLLATE pg_catalog."default",
    callee_service_id_str text COLLATE pg_catalog."default",
    callee_api_id_str text COLLATE pg_catalog."default",
    num_calls integer default 1,
    caller_namespace text COLLATE pg_catalog."default",
    callee_backend_id text COLLATE pg_catalog."default",
    callee_backend_name text COLLATE pg_catalog."default",
    caller_cluster_name text COLLATE pg_catalog."default",
    callee_cluster_name text COLLATE pg_catalog."default",
    caller_environment text COLLATE pg_catalog."default",
    callee_environment text COLLATE pg_catalog."default",
    seq_id bigserial
)
WITH (
    OIDS = FALSE
);