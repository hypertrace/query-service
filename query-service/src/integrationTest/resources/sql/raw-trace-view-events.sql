-- Table: public.raw-trace-view-events
-- DROP TABLE IF EXISTS public."raw-trace-view-events"

CREATE TABLE IF NOT EXISTS public."raw-trace-view-events"
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    trace_id bytea NOT NULL,
    transaction_name text COLLATE pg_catalog."default",
    services TEXT [],
    start_time_millis bigint,
    end_time_millis bigint,
    duration_millis bigint,
    num_services integer,
    num_spans integer,
    tags jsonb not null default '{}'::jsonb,
    environments TEXT [],
    seq_id bigserial
)
WITH (
    OIDS = FALSE
);
