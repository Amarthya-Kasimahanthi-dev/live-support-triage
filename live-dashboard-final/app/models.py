"""
Pydantic models: the exact shapes of data entering and leaving the API.

MessageIn  = what a CLIENT may send. No id, no enrichment fields: clients must
             not be able to set those.
MessageOut = what the API RETURNS, including server-generated and AI fields.
Keeping them separate stops a client POSTing {"category": "low"} to skip triage.
"""

from datetime import datetime
from pydantic import BaseModel, Field


class MessageIn(BaseModel):
    source_key: str = Field(min_length=1, max_length=200,
                            description="Unique ID from the source system; duplicates are rejected")
    source: str = Field(default="manual", max_length=50)
    author: str | None = Field(default=None, max_length=200)
    body: str = Field(min_length=1, max_length=5000)
    occurred_at: datetime | None = Field(
        default=None,
        description="When it happened at the source. Leave empty if unknown: it is stored as NULL, never guessed")


class MessageOut(BaseModel):
    id: int
    source_key: str
    source: str
    author: str | None            # the customer account, for tickets
    subject: str | None           # pulled out of the raw record
    reported_by: str | None
    channel: str | None
    body: str
    occurred_at: datetime | None  # None when the source carries no event time
    ingested_at: datetime
    category: str | None          # --- filled in by the worker ---
    urgency: str | None
    summary: str | None
    rationale: str | None
    needs_human_review: bool | None
    cost_usd: float | None
    enrich_status: str
    enrich_attempts: int
    enrich_error: str | None


class LabelCount(BaseModel):
    label: str
    count: int


class Stats(BaseModel):
    total: int
    pending: int
    processing: int
    done: int
    failed: int
    needs_review: int
    total_cost_usd: float
    by_category: list[LabelCount]
    by_urgency: list[LabelCount]
