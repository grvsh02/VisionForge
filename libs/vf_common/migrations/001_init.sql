-- VisionForge schema. PostgreSQL is the source of truth for job/page state; SQS moves the
-- work between stages; the outbox makes "change state + publish a message" atomic.

CREATE TABLE clients (
    client_id   text PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
    id            uuid PRIMARY KEY,
    client_id     text NOT NULL REFERENCES clients,
    status        text NOT NULL,              -- UPLOADED | SPLIT | COMPLETE | FAILED
    content_type  text NOT NULL,
    object_key    text NOT NULL,              -- the uploaded document in S3
    size_bytes    bigint NOT NULL,
    total_pages   integer NOT NULL,           -- counted by the ingest API at upload
    next_seq      bigint NOT NULL DEFAULT 0,  -- per-job stream event sequence
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz
);

-- One row per page: the state machine from the architecture doc (section 4).
CREATE TABLE pages (
    job_id          uuid NOT NULL REFERENCES jobs ON DELETE CASCADE,
    idx             integer NOT NULL,
    client_id       text NOT NULL,
    status          text NOT NULL,
    stage           text NOT NULL DEFAULT 'fast',   -- stage the page is in (or will retry in)
    object_key      text NOT NULL,                   -- single-page PDF in S3
    fast_attempts   integer NOT NULL DEFAULT 0,
    slow_attempts   integer NOT NULL DEFAULT 0,
    has_fast_result boolean NOT NULL DEFAULT false,  -- results/<job>/pages/<idx>/fast.json exists
    source          text,                            -- vlm | layout_fallback | none
    low_confidence  boolean,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    status_at       timestamptz NOT NULL DEFAULT now(),  -- when the current status was entered
    completed_at    timestamptz,
    PRIMARY KEY (job_id, idx)
);
CREATE INDEX pages_open ON pages (status, status_at) WHERE status NOT IN ('COMPLETED', 'FAILED');

-- Transactional outbox: rows are inserted in the same transaction as the state change they
-- announce, then published to SQS by the outbox publisher (at-least-once).
CREATE TABLE outbox_events (
    id          bigserial PRIMARY KEY,
    queue       text NOT NULL,            -- split | fast | slow
    payload     jsonb NOT NULL,
    delay_s     integer NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz
);
CREATE INDEX outbox_unsent ON outbox_events (id) WHERE sent_at IS NULL;

-- Every model call, for auditing and debugging (idempotency is enforced by the model's
-- Idempotency-Key cache and the deterministic S3 paths).
CREATE TABLE model_attempts (
    id           bigserial PRIMARY KEY,
    job_id       uuid NOT NULL,
    page_idx     integer NOT NULL,
    stage        text NOT NULL,
    idem_key     text NOT NULL,
    outcome      text NOT NULL,
    http_status  integer,
    latency_ms   integer,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Durable, per-job ordered stream log read by the stream gateway.
CREATE TABLE events (
    job_id      uuid NOT NULL REFERENCES jobs ON DELETE CASCADE,
    seq         bigint NOT NULL,
    kind        text NOT NULL,
    page_idx    integer,
    envelope    jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, seq)
);

CREATE FUNCTION notify_job_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('job_events', NEW.job_id::text || ':' || NEW.seq::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_notify AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_job_event();

-- Wakes the outbox publisher as soon as a transaction with outbox rows commits.
CREATE FUNCTION notify_outbox() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_notify AFTER INSERT ON outbox_events
    FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox();
