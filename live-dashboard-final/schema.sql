-- Runs automatically the first time the database initialises.

-- ---------------------------------------------------------------------------
-- messages: one row per record pulled from a source.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messages (
    -- The database's own key. Kept separate from the source's identity.
    id              BIGSERIAL PRIMARY KEY,

    -- The idempotency key, e.g. 'sheet:TKT-48201'. UNIQUE means Postgres
    -- itself rejects duplicates, so the guarantee does not depend on
    -- application code being correct.
    source_key      TEXT NOT NULL UNIQUE,

    source          TEXT NOT NULL,          -- 'google_sheet', 'slack', ...
    author          TEXT,
    body            TEXT NOT NULL,

    -- EVENT TIME: when it happened at the source. NULLABLE on purpose.
    -- Many real sources (including this sheet) carry no timestamp at all.
    -- NULL honestly means "the source didn't say". The alternative, filling
    -- it with the time we first saw the row, would put a processing time in
    -- an event-time column, and a backfill of 500 old rows would all claim
    -- to have happened at the same instant.
    occurred_at     TIMESTAMPTZ,

    -- PROCESSING TIME: when WE first saw it. Always known, always ours.
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The entire original record, exactly as the source gave it.
    -- JSONB is Postgres's binary JSON type: it stores structured data in one
    -- column and still lets you query inside it (raw->>'priority').
    -- Keeping the raw record means we can derive new columns later without
    -- re-fetching, and nothing the source sent is ever lost.
    raw             JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- --- enrichment fields, filled in by the worker ---
    category        TEXT,
    urgency         TEXT,

    -- Everything else the model produced (summary, rationale, review flag) plus
    -- provenance: which model, which prompt version, tokens, cost, latency.
    -- JSONB means a new field never needs a migration, and storing the prompt
    -- version beside each result lets you compare prompts later.
    enrichment      JSONB,

    -- --- queue bookkeeping: the messages table doubles as the work queue ---
    enrich_status   TEXT NOT NULL DEFAULT 'pending'
                    CHECK (enrich_status IN ('pending','processing','done','failed')),
    enrich_attempts INT  NOT NULL DEFAULT 0,
    enrich_error    TEXT,
    -- When a worker claimed the row. If a worker dies mid-job, this timestamp
    -- is how another worker knows the claim has expired.
    enrich_started_at      TIMESTAMPTZ,
    -- Earliest time a failed row may be retried (exponential backoff).
    enrich_next_attempt_at TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Indexes: index the columns you FILTER or SORT on.
-- ---------------------------------------------------------------------------

-- The worker's query is "find pending rows". A partial index covers only
-- those rows, so it stays small as the table grows.
CREATE INDEX IF NOT EXISTS idx_messages_pending
    ON messages (enrich_status, id)
    WHERE enrich_status = 'pending';

-- The dashboard shows most recently ingested first.
CREATE INDEX IF NOT EXISTS idx_messages_ingested_at
    ON messages (ingested_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- The NOTIFY trigger: this is what makes real-time push possible.
--
-- A TRIGGER runs automatically when a table changes. This one calls
-- pg_notify(), Postgres's built-in publish/subscribe. Any connection that
-- has run LISTEN on the channel receives the message immediately.
--
-- IMPORTANT: the payload carries ONLY the row id and the event type.
-- pg_notify payloads are capped at 8,000 bytes, and the trigger runs inside
-- the same transaction as the INSERT. If a payload is too big, the NOTIFY
-- raises an error and the whole INSERT is rolled back. A single long ticket
-- would then make the poller's batch fail on every cycle, forever (a
-- "poison pill"). So we send a tiny signal and let the listener fetch the
-- full row by id. This is the standard pattern.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_message_change() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'message_change',
        json_build_object('id', NEW.id, 'event', TG_OP)::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fire when a new row arrives...
CREATE TRIGGER messages_notify_insert
    AFTER INSERT ON messages
    FOR EACH ROW EXECUTE FUNCTION notify_message_change();

-- ...and when a row finishes enrichment (and only then, not on every update).
CREATE TRIGGER messages_notify_enriched
    AFTER UPDATE ON messages
    FOR EACH ROW
    WHEN (OLD.enrich_status IS DISTINCT FROM NEW.enrich_status
          AND NEW.enrich_status = 'done')
    EXECUTE FUNCTION notify_message_change();
