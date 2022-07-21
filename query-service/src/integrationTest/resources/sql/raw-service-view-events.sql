-- Table: public.backend-entity-view-events
-- DROP TABLE IF EXISTS public."raw-service-view-events"
CREATE TABLE IF NOT EXISTS public."raw-service-view-events"
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    service_name text COLLATE pg_catalog."default",
    api_name text COLLATE pg_catalog."default",
    api_is_external boolean NOT NULL DEFAULT FALSE,
    start_time_millis bigint,
    duration_millis bigint,
    bytes_received bigint,
    bytes_sent bigint,
    error_count integer DEFAULT 0,
    exception_count integer DEFAULT 0,
    protocol_name text COLLATE pg_catalog."default",
    status_code text COLLATE pg_catalog."default",
    service_id text COLLATE pg_catalog."default",
    api_id text COLLATE pg_catalog."default",
    num_calls integer DEFAULT 0,
    namespace_name text COLLATE pg_catalog."default",
    cluster_name text COLLATE pg_catalog."default",
    host_header text COLLATE pg_catalog."default",
    api_discovery_state text COLLATE pg_catalog."default",
    domain_id text COLLATE pg_catalog."default",
    environment text COLLATE pg_catalog."default",
    seq_id bigserial
)
WITH (
    OIDS = FALSE
);
