"""
Real-time push: Postgres NOTIFY  ->  this process  ->  every open browser.

THE IDEA
  Normal HTTP is request/response: the browser has to keep asking "anything new?".
  Here the direction is reversed:

     1. A row is inserted or finishes classification.
     2. A trigger inside Postgres runs pg_notify('message_change', '{"id": 7, ...}').
     3. listen_forever() below is subscribed (LISTEN) and receives it instantly.
     4. It fetches the full row by id and hands it to the Hub.
     5. The Hub writes it down every open WebSocket. The page updates.

  No polling anywhere between the database and the screen.

TWO THINGS TO KNOW
  * LISTEN needs ONE long-lived connection. It cannot come from the connection
    pool, because pool connections are lent out and returned.
  * Postgres does NOT store notifications for a listener that is not connected.
    If our connection drops, events in the gap are lost. So after every
    (re)connect we broadcast {"type": "resync"} and each browser reloads its
    data from the REST API. Push for speed, pull to repair gaps.
"""

import asyncio
import json
import logging
from contextlib import suppress

import asyncpg
from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder

log = logging.getLogger("realtime")

CHANNEL = "message_change"
KEEPALIVE_SECONDS = 15


class Hub:
    """Keeps track of connected browsers and sends a message to all of them."""

    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)
        log.info("browser connected (%d open)", len(self.clients))

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)
        log.info("browser disconnected (%d open)", len(self.clients))

    async def broadcast(self, payload: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(payload)
        failed: list[WebSocket] = []

        async def send(ws: WebSocket) -> None:
            try:
                # A slow or dead browser must not hold up everyone else.
                await asyncio.wait_for(ws.send_text(text), timeout=2.0)
            except Exception:
                failed.append(ws)

        await asyncio.gather(*(send(ws) for ws in list(self.clients)))
        for ws in failed:
            self.clients.discard(ws)


async def _forward(payload: str, hub: Hub, fetch_message) -> None:
    """Turn one notification ({"id": 7, "event": "UPDATE"}) into a broadcast of the full row."""
    try:
        note = json.loads(payload)
        message_id, event = int(note["id"]), note.get("event", "UPDATE")
    except (ValueError, KeyError, TypeError):
        log.warning("ignoring malformed notification: %r", payload)
        return
    row = await fetch_message(message_id)
    if row is None:
        return
    await hub.broadcast({"type": "message", "event": event, "data": jsonable_encoder(row)})


async def listen_forever(dsn: str, hub: Hub, fetch_message) -> None:
    """Hold one LISTEN connection for the life of the process, reconnecting if it dies."""
    backoff = 1.0
    while True:
        conn = None
        try:
            # application_name makes this connection easy to spot in pg_stat_activity.
            conn = await asyncpg.connect(dsn, server_settings={"application_name": "dashboard-listener"})
            queue: asyncio.Queue = asyncio.Queue()
            # asyncpg calls these callbacks synchronously, so they only drop a note
            # on a queue; the real work happens below, in normal async code.
            await conn.add_listener(CHANNEL, lambda c, pid, ch, payload: queue.put_nowait(payload))
            conn.add_termination_listener(lambda c: queue.put_nowait(None))
            log.info("listening on channel %r", CHANNEL)
            backoff = 1.0
            await hub.broadcast({"type": "resync"})     # we may have missed events while disconnected

            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
                except asyncio.TimeoutError:
                    await conn.execute("SELECT 1")      # keepalive; raises if the connection died
                    continue
                if payload is None:
                    raise ConnectionError("database connection closed")
                await _forward(payload, hub, fetch_message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("listener lost (%s: %s); reconnecting in %.0fs", type(exc).__name__, exc, backoff)
        finally:
            if conn is not None:
                with suppress(Exception):
                    await conn.close()
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)
