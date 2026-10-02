"""
The API service: REST endpoints, the live WebSocket feed, and the dashboard page.

Run locally:   python -m uvicorn app.api:app --reload
In Docker:     started for you by docker compose

  GET  /            the dashboard page
  GET  /messages    the latest tickets (JSON)
  GET  /stats       counts, urgency and category breakdowns, AI cost (JSON)
  POST /messages    add a ticket by hand (used by the dashboard's test form)
  GET  /health      "is this service working?" (used by Docker)
  WS   /ws          the live feed: every change is pushed here as it happens
  GET  /docs        interactive API documentation, generated from this code
"""

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from app.db import DATABASE_URL, create_pool
from app.logging_config import setup_logging
from app.models import LabelCount, MessageIn, MessageOut, Stats
from app.realtime import Hub, listen_forever

setup_logging("api")
log = logging.getLogger("api")

STATIC_DIR = Path(__file__).parent / "static"

# One SELECT used by every endpoint and by the live feed, so a ticket looks the
# same whichever way it reaches the browser. This is a fixed string: user input
# is NEVER added to it, only passed as $1, $2 parameters (see below).
# `raw->>'subject'` reaches into the stored JSON record: fields we never made
# columns for are still queryable.
MESSAGE_SELECT = """
SELECT id, source_key, source, author, body,
       raw->>'subject'      AS subject,
       raw->>'reported by'  AS reported_by,
       raw->>'channel'      AS channel,
       occurred_at, ingested_at,
       category, urgency,
       enrichment->>'summary'   AS summary,
       enrichment->>'rationale' AS rationale,
       (enrichment->>'needs_human_review')::boolean AS needs_human_review,
       (enrichment->>'cost_usd')::float             AS cost_usd,
       enrich_status, enrich_attempts, enrich_error
FROM messages
"""


async def fetch_message(pool: asyncpg.Pool, message_id: int) -> dict | None:
    row = await pool.fetchrow(MESSAGE_SELECT + " WHERE id = $1", message_id)
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Lifespan: runs once at startup (before `yield`) and once at shutdown (after).
# Startup creates the connection pool and the Hub of connected browsers, and
# launches the background task that listens for database notifications.
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state.hub = Hub()
    listener = asyncio.create_task(
        listen_forever(DATABASE_URL, app.state.hub, lambda message_id: fetch_message(app.state.pool, message_id))
    )
    log.info("api started")
    yield
    listener.cancel()
    with suppress(asyncio.CancelledError):
        await listener
    await app.state.pool.close()


app = FastAPI(
    title="Live Support Triage API",
    description="Support tickets pulled from a sheet, triaged by Claude, pushed live to a dashboard.",
    lifespan=lifespan,
)


def get_pool(request: Request) -> asyncpg.Pool:
    """Dependency injection: endpoints ask for the pool instead of using a global,
    which lets tests swap in a different one."""
    return request.app.state.pool


# ---------------------------------------------------------------------------
# The dashboard page
# ---------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(STATIC_DIR / "index.html")


# ---------------------------------------------------------------------------
# Health: checks the DATABASE, not just that Python is running. An API that
# cannot reach its database is not healthy, and Docker uses this to know.
# ---------------------------------------------------------------------------
@app.get("/health")
async def health(pool: asyncpg.Pool = Depends(get_pool)):
    try:
        await pool.fetchval("SELECT 1")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"database unreachable: {e}")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /messages?limit=50&category=bug&urgency=high
#
# $1, $2, $3 are PARAMETERS: values travel to Postgres separately from the SQL
# text, so user input can never be read as SQL. Building the query with an
# f-string instead would allow SQL injection (?category=x' OR '1'='1).
# ($2::text IS NULL OR category = $2) makes a filter optional while keeping one
# fixed query.
# ---------------------------------------------------------------------------
@app.get("/messages", response_model=list[MessageOut])
async def list_messages(
    limit: int = Query(default=50, ge=1, le=500),
    category: str | None = Query(default=None, max_length=50),
    urgency: str | None = Query(default=None, max_length=20),
    pool: asyncpg.Pool = Depends(get_pool),
):
    rows = await pool.fetch(
        MESSAGE_SELECT + """
        WHERE ($2::text IS NULL OR category = $2)
          AND ($3::text IS NULL OR urgency = $3)
        ORDER BY ingested_at DESC, id DESC
        LIMIT $1
        """,
        limit, category, urgency,
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /stats
# count(*) FILTER (WHERE ...) computes several counts in ONE pass over the table.
# ---------------------------------------------------------------------------
@app.get("/stats", response_model=Stats)
async def stats(pool: asyncpg.Pool = Depends(get_pool)):
    totals = await pool.fetchrow(
        """
        SELECT
            count(*)                                              AS total,
            count(*) FILTER (WHERE enrich_status = 'pending')      AS pending,
            count(*) FILTER (WHERE enrich_status = 'processing')   AS processing,
            count(*) FILTER (WHERE enrich_status = 'done')         AS done,
            count(*) FILTER (WHERE enrich_status = 'failed')       AS failed,
            count(*) FILTER (WHERE enrichment->>'needs_human_review' = 'true') AS needs_review,
            coalesce(sum((enrichment->>'cost_usd')::numeric), 0)::float        AS total_cost_usd
        FROM messages
        """
    )
    categories = await pool.fetch(
        "SELECT category AS label, count(*) AS count FROM messages "
        "WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC, label"
    )
    urgencies = await pool.fetch(
        "SELECT urgency AS label, count(*) AS count FROM messages "
        "WHERE urgency IS NOT NULL GROUP BY urgency"
    )
    return Stats(
        **dict(totals),
        by_category=[LabelCount(**dict(r)) for r in categories],
        by_urgency=[LabelCount(**dict(r)) for r in urgencies],
    )


# ---------------------------------------------------------------------------
# POST /messages: 201 = created, 409 = that source_key already exists,
# 422 = the body failed validation (FastAPI does this before our code runs).
# ---------------------------------------------------------------------------
@app.post("/messages", response_model=MessageOut, status_code=201)
async def create_message(msg: MessageIn, pool: asyncpg.Pool = Depends(get_pool)):
    try:
        new_id = await pool.fetchval(
            """
            INSERT INTO messages (source_key, source, author, body, occurred_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            msg.source_key, msg.source, msg.author, msg.body, msg.occurred_at,
        )
    except asyncpg.UniqueViolationError:
        # The UNIQUE constraint in schema.sql surfaces here. We translate the
        # database error into a meaningful HTTP status instead of a generic 500.
        raise HTTPException(status_code=409, detail=f"source_key '{msg.source_key}' already exists")
    return await fetch_message(pool, new_id)


# ---------------------------------------------------------------------------
# WS /ws: the live feed. The browser connects once and stays connected; the
# Hub (see realtime.py) writes every change to it.
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def live_feed(ws: WebSocket):
    hub: Hub = ws.app.state.hub
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()   # we expect nothing; this just notices when the browser leaves
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)
