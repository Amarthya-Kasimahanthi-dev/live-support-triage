# Live Support Triage

Support tickets are copied from a Google Sheet into a database. Claude classifies
each one (category, urgency, a one-line summary, and whether a human should look).
A web dashboard shows the results, and updates the instant anything changes.

```
 Google Sheet ──► poller ──► PostgreSQL ──► worker ──► Claude (Haiku)
 (published CSV)  every 15s     │   ▲          │
                                │   └──────────┘  writes category / urgency / summary back
                                │
                  NOTIFY on every insert and every finished classification
                                │
                                ▼
                               api ──► WebSocket ──► your browser (live dashboard)
                                └────► REST: /messages  /stats  /docs
```

Four containers, one database:

| Service | What it does | File |
|---|---|---|
| `db` | PostgreSQL. Holds the tickets, and is also the work queue | `schema.sql` |
| `poller` | Reads the sheet every 15s, inserts tickets it has not seen | `app/poller.py` |
| `worker` | Claims pending tickets, asks Claude, saves the result | `app/worker.py` |
| `api` | Serves the dashboard, the REST API and the live WebSocket feed | `app/api.py`, `app/realtime.py` |

## Run it (Docker)

You need Docker Desktop running, and a `.env` file next to this README.

```bash
cd ~/Desktop/live-dashboard-final
cp ../live-dashboard/.env .env        # reuse your existing one, or: cp .env.example .env  and edit it
docker compose up --build
```

The first build takes a few minutes. When the logs settle, open **http://localhost:8000**.

Within about a minute you should see all 40 tickets arrive, then get classified one by one.
(The worker paces itself to 40 requests a minute so a new Anthropic account's rate limit is never hit.)

Stop it with `Ctrl+C`, then `docker compose down`. Your data is kept. To wipe the database and start clean: `docker compose down -v`.

Before running, stop any older copy of this project, or the ports clash:
`cd ~/Desktop/live-dashboard && docker compose down` (and `Ctrl+C` any terminals still running uvicorn, the poller or the worker).

## Run it without Docker (fallback)

Docker is only used to run the database here; the three Python programs run in your own terminal.

```bash
docker compose up -d db                               # just the database
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Then three terminal tabs, each with the venv active and each in this folder:

```bash
python -m uvicorn app.api:app --reload     # tab 1
python -m app.poller                        # tab 2
python -m app.worker                        # tab 3
```

## Demo script (5 minutes)

1. **Open the dashboard.** Point at the green "live" pill. Explain the flow in 30 seconds using the diagram above.
2. **Send an outage ticket.** Expand "Send a test ticket", click the *Outage* preset, press Send.
   Watch the row appear as *queued*, change to *classifying…*, then settle as `outage / critical`. The cards on top move too. Nothing was refreshed.
3. **Send the Security preset.** It gets flagged `⚑ review`, which is the human-in-the-loop rule.
4. **Click a row.** Show the AI's rationale, the attempts, and what each ticket cost (about a tenth of a cent).
5. **Show the logs.** `docker compose logs -f worker`. Every line has the ticket, the labels, tokens and latency.
6. **Break it on purpose.** `docker compose stop worker`, send a ticket: it waits as *queued*. `docker compose start worker`: it catches up.
   The queue is the database, so nothing is lost when a worker is down.
7. **Show the REST side.** Open http://localhost:8000/docs and call `GET /stats`.

## If something goes wrong

| Symptom | Cause and fix |
|---|---|
| `env file .env not found` | Create `.env` (see above). It must sit next to `docker-compose.yml`. |
| `port is already allocated` (8000 or 5432) | An older copy is still running. `docker compose down` in the old folder, and close any local uvicorn. |
| Dashboard says *reconnecting…* | The api container is not up. `docker compose ps`, then `docker compose logs api`. |
| Tickets stay *queued* and the worker logs `CRITICAL` | Read the message. A bad API key, a wrong model name or an empty credit balance pauses the worker for 60s instead of failing your tickets. Fix `.env`, then `docker compose restart worker`. |
| Nothing arrives from the sheet | `docker compose logs poller`. A wrong `SHEET_CSV_URL`, or the sheet is not published as CSV. Google can also take a few minutes to republish edits. |
| Changed code but nothing changed | Rebuild: `docker compose up --build`. |

## What is where

```
docker-compose.yml     how the four containers fit together
Dockerfile             how the Python image is built (shared by api, poller, worker)
schema.sql             the database tables, indexes and the NOTIFY trigger
migrations/            how an EXISTING database is upgraded (a fresh one does not need them)
support_tickets.xlsx   the sample sheet: import it into Google Sheets and publish it as CSV
app/
  poller.py            sheet -> database (stable IDs, validation, duplicate detection)
  worker.py            database queue -> Claude -> database (retries, leases, breakers, redaction)
  api.py               REST endpoints, WebSocket route, serves the dashboard
  realtime.py          LISTEN to Postgres, push to browsers, reconnect if the link drops
  models.py            the exact shapes of API requests and responses
  db.py                the database connection pool
  logging_config.py    text logs for people, JSON logs for machines (LOG_FORMAT=json)
  static/index.html    the dashboard (plain HTML, CSS and JavaScript, no build step)
```

## Design decisions worth being able to explain

- **Postgres is the queue.** Workers claim rows with `FOR UPDATE SKIP LOCKED`, so several workers share the work and never take the same ticket. At much higher volume you would move to SQS or Kafka.
- **Idempotent ingestion.** The sheet is re-read every 15 seconds. A UNIQUE key plus `ON CONFLICT DO NOTHING` means re-reading never creates duplicates, and the database enforces that, not the code.
- **No invented timestamps.** The sheet has none, so `occurred_at` is NULL ("the source did not say") and `ingested_at` records when we first saw it.
- **The raw record is kept.** The original row is stored as JSONB, so columns we did not model (channel, reporter) are still queryable.
- **Three kinds of failure.** Glitches are retried with backoff. A bad ticket is failed at once. A broken setup (bad key, no credit) pauses the worker without charging the tickets an attempt. Six failures in a row also pause it.
- **A claim is a lease.** A crashed worker's rows are re-claimed after 5 minutes; the attempt counter doubles as a fencing token so a stale worker cannot overwrite a newer result.
- **Structured output, validated twice.** Claude must answer through a tool schema with fixed enums, and the code checks again. A prompt injection inside a ticket can bias a label but cannot do anything else.
- **Personal data is redacted before it leaves.** Emails and phone numbers become `[EMAIL]` and `[PHONE]` before the API call.
- **Push for speed, pull to repair.** A database trigger notifies the API, which pushes to browsers. Postgres does not store notifications for absent listeners, so after any reconnect the page reloads from the REST API.
- **Ticket text is untrusted.** The dashboard only ever uses `textContent`, never `innerHTML`, so a hostile ticket cannot run code in someone's browser.
- **Model choice is a cost decision.** Haiku for a high-volume, low-stakes label: roughly $0.001 per ticket.

## Known limitations

- **No authentication.** Anyone who can reach port 8000 can read tickets and `POST /messages`. The WebSocket does not check the origin either.
- **Edits to existing tickets are ignored.** The sheet is treated as an append-only feed. A status change from Open to Resolved is not picked up. The fix is an upsert keyed on a row hash, or change-data-capture.
- **Classification quality has not been measured.** There is no labelled set yet, so accuracy, and how often the same ticket gets a different label, are unknown. This SDK version also cannot set temperature.
- **The rate limiter is per process.** Two workers double the request rate. Production would use a shared limiter.
- **PII redaction is regex-based.** It catches emails and phone numbers, not names. Production would use a PII detection service.
- **Development settings.** The database password is in the compose file, there are no backups, and the dashboard shows the latest 100 tickets.
- **No automated tests in this folder.** The build was verified with tests that are not included. Adding them is a good next step.
