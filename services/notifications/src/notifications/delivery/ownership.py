"""Shared session advisory ownership for senders and maintenance."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from notifications.db.session import Database


@asynccontextmanager
async def delivery_ownership(
    database: Database, did: UUID
) -> AsyncGenerator[AsyncConnection | None, None]:
    # Dedicated connection: a session advisory lock must never leak into the pool.
    key = int.from_bytes(did.bytes[:8], "big", signed=True)
    async with database.engine.connect() as conn:
        acquired = await conn.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": key})
        await conn.commit()
        try:
            yield conn if acquired else None
        finally:
            if acquired:
                try:
                    await conn.rollback()
                    await conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                    await conn.commit()
                except BaseException:
                    await conn.invalidate()
                    raise
