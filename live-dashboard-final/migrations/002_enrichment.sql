-- Migration 002: columns the enrichment worker (Phase 4) needs.
--
-- Run with:
--   docker compose exec -T db psql -U dashboard -d dashboard -v ON_ERROR_STOP=1 < migrations/002_enrichment.sql
--
-- Transactional and idempotent, like migration 001. Run 001 first.

BEGIN;

-- The model's full output plus provenance (model, prompt version, tokens, cost).
ALTER TABLE messages ADD COLUMN IF NOT EXISTS enrichment JSONB;

-- Queue bookkeeping.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS enrich_started_at      TIMESTAMPTZ;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS enrich_next_attempt_at TIMESTAMPTZ;

COMMIT;
