-- Migration 001: bring a database created from the original Phase 1 schema up
-- to date with the current schema.sql.
--
-- WHY THIS EXISTS
-- schema.sql only runs when Postgres initialises a brand-new, empty database.
-- A database that already holds data cannot be rebuilt that way without losing
-- the data, so it is changed with a MIGRATION: a small script that moves it
-- from one known state to the next. Real teams keep these in version control
-- and number them (001, 002, ...) using tools like Flyway or Alembic.
--
-- WHY IT IS SAFE
--  * It runs inside one transaction: either every change applies or none does.
--  * Every statement is idempotent, so running it twice does no harm.
--
-- RUN IT (from the project root; -T is needed so psql can read the file from stdin):
--   docker compose exec -T db psql -U dashboard -d dashboard -v ON_ERROR_STOP=1 < migrations/001_ticket_support.sql

BEGIN;

-- 1. Event time becomes optional.
--    Many real sources, including this ticket sheet, carry no timestamp.
--    NULL honestly means "the source didn't say". Filling it with the time we
--    first saw the row would put a processing time into an event-time column.
ALTER TABLE messages ALTER COLUMN occurred_at DROP NOT NULL;

-- 2. Keep the whole original record as JSONB.
--    Columns we don't model (Reported By, Channel, Status) stay queryable
--    through raw->>'reported by', and nothing the source sent is ever lost.
--    Existing rows get an empty object, because there is nothing to recover.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS raw JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 3. The dashboard now orders by arrival, so index that instead.
DROP INDEX IF EXISTS idx_messages_occurred_at;
CREATE INDEX IF NOT EXISTS idx_messages_ingested_at
    ON messages (ingested_at DESC, id DESC);

-- 4. Shrink the NOTIFY payload to just the row id and event type.
--    pg_notify payloads are capped at 8,000 bytes and the trigger runs inside the
--    INSERT's transaction, so an oversized payload would roll the INSERT back.
--    CREATE OR REPLACE swaps the function body; the two existing triggers keep
--    pointing at it, so they need no changes.
CREATE OR REPLACE FUNCTION notify_message_change() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'message_change',
        json_build_object('id', NEW.id, 'event', TG_OP)::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
