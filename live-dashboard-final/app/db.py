"""
Database connection pool.

One pool per process, created once at startup and closed at shutdown.
The API, the poller (Phase 3) and the worker (Phase 4) all import this,
so pool configuration lives in exactly one place.
"""

import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


async def create_pool() -> asyncpg.Pool:
    """
    min_size: connections opened immediately at startup, so the first
              requests don't pay the connection cost.
    max_size: the ceiling. If 50 requests arrive at once, 10 get a
              connection and the rest wait briefly in line. That queue is
              deliberate: it protects Postgres from being overwhelmed.
    command_timeout: any single query taking longer than this raises an
              error instead of hanging forever. Same principle as the
              30-second API timeout in the clinical agent.
    """
    return await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=10,
    )
