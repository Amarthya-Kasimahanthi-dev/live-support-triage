"""
Ingestion poller: reads a published Google Sheet (CSV) of support tickets on a
loop and writes new rows into Postgres.

Run with:
    python -m app.poller

Design points worth being able to explain:
  1. Stable keys from the source's own ID (Ticket ID). Never Python's hash(),
     which is randomised per process.
  2. Idempotent: ON CONFLICT (source_key) DO NOTHING, enforced by the database.
  3. No timestamp in the source: occurred_at stays NULL ("source didn't say").
     ingested_at records when WE first saw the row.
  4. Validation with reasons: bad rows are counted by cause, never fatal.
  5. Duplicate IDs inside the sheet are detected, the first wins, and we warn.
  6. The raw row is kept as JSONB, so nothing the source sent is lost.
  7. Body length is capped: protects the database and bounds LLM cost later.
"""

import asyncio
import csv
import io
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass, field

import asyncpg
import httpx
from dotenv import load_dotenv

from app.db import create_pool
from app.logging_config import setup_logging

load_dotenv()

SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL", "")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))

SOURCE = "google_sheet"

# ---- Column mapping: which sheet column feeds which field. -----------------
# This is the ONLY place that knows about this particular sheet's layout.
# Pointing the pipeline at a different sheet means editing these four lines.
COL_ID = "ticket id"
COL_AUTHOR = "customer account"
COL_SUBJECT = "subject"
COL_DESC = "description"

# ~500 tokens. Enough to classify any ticket; stops a pasted 50-page email
# thread from bloating the table or the LLM bill.
MAX_BODY_CHARS = 2000

setup_logging("poller")
log = logging.getLogger("poller")


# ---------------------------------------------------------------------------
# Parsing: pure functions, no network or database, so they are easy to test.
# ---------------------------------------------------------------------------
@dataclass
class ParseResult:
    records: list = field(default_factory=list)      # tuples ready for the INSERT
    rejected: Counter = field(default_factory=Counter)  # reason -> count
    duplicate_ids: list = field(default_factory=list)
    truncated: int = 0


def parse_rows(rows: list[dict]) -> ParseResult:
    result = ParseResult()
    seen: set[str] = set()

    for row in rows:
        # 1. A completely empty row: a spacer someone left in the sheet.
        if not any(row.values()):
            result.rejected["blank_row"] += 1
            continue

        # 2. No ID means no stable identity: we cannot track or deduplicate it.
        #    strip() and upper() so ' tkt-48240 ' and 'TKT-48240' are one ID.
        ticket_id = row.get(COL_ID, "").strip().upper()
        if not ticket_id:
            result.rejected["no_id"] += 1
            continue

        # 3. Nothing to classify.
        subject = row.get(COL_SUBJECT, "")
        desc = row.get(COL_DESC, "")
        body = "\n\n".join(p for p in (subject, desc) if p)
        if not body:
            result.rejected["empty_body"] += 1
            continue

        # 4. The same ID twice in one sheet: keep the first, report the rest.
        key = f"sheet:{ticket_id}"
        if key in seen:
            result.duplicate_ids.append(ticket_id)
            continue
        seen.add(key)

        # 5. Cap the length.
        if len(body) > MAX_BODY_CHARS:
            body = body[:MAX_BODY_CHARS] + " ...[truncated]"
            result.truncated += 1

        author = row.get(COL_AUTHOR, "") or None
        raw = json.dumps(row, ensure_ascii=False)
        # occurred_at is None: this source has no event timestamp.
        result.records.append((key, SOURCE, author, body, None, raw))

    return result


def parse_csv_text(text: str) -> list[dict]:
    """CSV text -> list of dicts with normalised headers and stripped values."""
    # Excel-exported CSVs can start with a byte-order mark (\ufeff). Left in, it
    # glues itself to the first header, so "ticket id" never matches and EVERY
    # row is rejected as no_id.
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    rows = []
    for r in reader:
        # k is None when a row has more cells than the header has columns.
        rows.append({k.strip().lower(): (v or "").strip() for k, v in r.items() if k})
    return rows


# ---------------------------------------------------------------------------
# Write: one statement, one round trip, idempotent
# ---------------------------------------------------------------------------
INSERT_SQL = """
INSERT INTO messages (source_key, source, author, body, occurred_at, raw)
SELECT k, s, a, b, t, r::jsonb
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::text[])
     AS u(k, s, a, b, t, r)
ON CONFLICT (source_key) DO NOTHING
RETURNING id
"""
# unnest zips six parallel arrays into rows, so N tickets travel in ONE
# statement. RETURNING id yields only rows actually inserted, which gives an
# exact count of what was new. r::jsonb converts the JSON text to JSONB.

_warned_duplicates: set[str] = set()   # warn once per process, not every 15s


async def ingest_rows(pool: asyncpg.Pool, rows: list[dict]) -> dict:
    parsed = parse_rows(rows)

    new_dupes = [d for d in parsed.duplicate_ids if d not in _warned_duplicates]
    if new_dupes:
        log.warning("duplicate Ticket ID in sheet, keeping first occurrence: %s", new_dupes)
        _warned_duplicates.update(new_dupes)

    inserted = []
    if parsed.records:
        # Transpose [(k,s,a,b,t,r), ...] into six columns for unnest.
        cols = [list(c) for c in zip(*parsed.records)]
        inserted = await pool.fetch(INSERT_SQL, *cols)

    stats = {
        "fetched": len(rows),
        "accepted": len(parsed.records),
        "rejected": dict(parsed.rejected),
        "duplicate_ids": len(parsed.duplicate_ids),
        "truncated": parsed.truncated,
        "inserted": len(inserted),
    }
    log.info("fetched=%(fetched)d accepted=%(accepted)d rejected=%(rejected)s "
             "duplicate_ids=%(duplicate_ids)d truncated=%(truncated)d inserted=%(inserted)d", stats)
    return stats


# ---------------------------------------------------------------------------
# Fetch and loop
# ---------------------------------------------------------------------------
async def fetch_rows(client: httpx.AsyncClient) -> list[dict]:
    resp = await client.get(SHEET_CSV_URL)
    resp.raise_for_status()
    return parse_csv_text(resp.text)


async def main() -> None:
    if not SHEET_CSV_URL:
        raise SystemExit("SHEET_CSV_URL is not set in .env")
    pool = await create_pool()
    # follow_redirects=True matters: published Sheet URLs redirect to another
    # Google domain, and httpx does not follow redirects by default.
    client = httpx.AsyncClient(timeout=10.0, follow_redirects=True)
    log.info("polling every %ds", POLL_SECONDS)
    try:
        while True:
            try:
                await ingest_rows(pool, await fetch_rows(client))
            except Exception as e:
                # A network blip or a DB restart must never kill the poller.
                log.error("cycle failed: %s: %s", type(e).__name__, e)
            await asyncio.sleep(POLL_SECONDS)
    finally:
        await client.aclose()
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("stopped")
