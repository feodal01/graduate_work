"""Fence partition ownership and commit projection, marker and cursor together."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import AuthEvent, FilmEvent, LikesEvent, SourcePosition
from notifications.db.models import KafkaOffset, ProcessedEvent
from notifications.db.session import Database

Event = AuthEvent | LikesEvent | FilmEvent
Handler = Callable[[AsyncSession, Event], Awaitable[None]]


class OwnershipLost(RuntimeError):
    """A newer assignment owns this partition; the old owner must stop."""


class PositionError(RuntimeError):
    """A caller attempted to apply records outside partition order."""


class RetentionError(RuntimeError):
    """The durable cursor is no longer in the available Kafka log."""


@dataclass(frozen=True)
class Ownership:
    generation: int


class AtomicIngest:
    def __init__(self, database: Database, handler: Handler) -> None:
        self.database = database
        self.handler = handler
        self.owner_id: UUID = uuid4()
        self.ownership: dict[tuple[str, int], Ownership] = {}
        self._locks: dict[tuple[str, int], asyncio.Lock] = {}

    async def _locked(self, session: AsyncSession, topic: str, partition: int) -> KafkaOffset:
        return (
            await session.scalars(
                select(KafkaOffset)
                .where(
                    KafkaOffset.topic == topic,
                    KafkaOffset.partition == partition,
                )
                .with_for_update()
            )
        ).one()

    async def claim(self, topic: str, partition: int, *, earliest: int, latest: int) -> int:
        """Called only for Kafka-assigned partitions; SQL locks wait for old work.

        Kafka group assignment is the lease authority. The schema lease field is
        infinite: elapsed wall time never permits stealing an active transaction.
        Every assignment increments the generation, including same-process rejoins.
        """
        if not 0 <= earliest <= latest:
            raise ValueError("Invalid Kafka log bounds")
        key = topic, partition
        async with self.database.transaction() as session:
            await session.execute(
                insert(KafkaOffset)
                .values(
                    topic=topic,
                    partition=partition,
                    next_offset=earliest,
                )
                .on_conflict_do_nothing()
            )
            row = await self._locked(session, topic, partition)
            if not earliest <= row.next_offset <= latest:
                raise RetentionError(
                    f"{topic}[{partition}] saved offset {row.next_offset} outside "
                    f"Kafka range [{earliest}, {latest}]; manual recovery required"
                )
            row.owner_id = self.owner_id
            row.generation += 1
            row.lease_until = datetime.max.replace(tzinfo=UTC)
            generation, next_offset = row.generation, row.next_offset
        self.ownership[key] = Ownership(generation)
        self._locks.setdefault(key, asyncio.Lock())
        return next_offset

    async def release(self, topic: str, partition: int) -> None:
        key = topic, partition
        ownership = self.ownership.pop(key, None)
        if ownership is None:
            return
        async with self.database.transaction() as session:
            row = await self._locked(session, topic, partition)
            if row.owner_id == self.owner_id and row.generation == ownership.generation:
                row.owner_id = None
                row.lease_until = None

    async def handle_record(
        self,
        event: Event,
        position: SourcePosition,
        *,
        expected_offset: int | None = None,
    ) -> None:
        """Apply one record. Polling may supply the last durable cursor explicitly.

        Kafka control records and aborted transactions create legitimate offset
        gaps. Only the sequential broker reader supplies expected_offset to cross
        them; direct calls default to requiring a contiguous record offset.
        """
        key = position.topic, position.partition
        ownership = self.ownership.get(key)
        if ownership is None:
            raise OwnershipLost(f"No assignment for {key}")
        async with self._locks[key], self.database.transaction() as session:
            row = await self._locked(session, *key)
            if (
                self.ownership.get(key) != ownership
                or row.owner_id != self.owner_id
                or row.generation != ownership.generation
            ):
                raise OwnershipLost(f"Assignment fenced for {key}")
            expected = position.next_offset - 1 if expected_offset is None else expected_offset
            if expected != row.next_offset or position.next_offset <= row.next_offset:
                raise PositionError(
                    f"{key} expected record offset {row.next_offset}, "
                    f"got {position.next_offset - 1}"
                )
            marker = await session.scalar(
                insert(ProcessedEvent)
                .values(
                    event_id=event.event_id,
                )
                .on_conflict_do_nothing()
                .returning(ProcessedEvent.event_id)
            )
            if marker is not None:
                await self.handler(session, event)
            row.next_offset = position.next_offset
