"""Kafka is the log; PostgreSQL is the sole source of consumer positions."""

import asyncio
import logging
from collections.abc import Iterable, Mapping
from typing import Protocol, cast

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener  # type: ignore[import-untyped]
from aiokafka.errors import NoOffsetForPartitionError  # type: ignore[import-untyped]
from aiokafka.structs import ConsumerRecord, TopicPartition  # type: ignore[import-untyped]
from pydantic import ValidationError

from notifications.config import BatchSettings
from notifications.contracts import AuthEvent, FilmEvent, LikesEvent, SourcePosition
from notifications.db.session import Database
from notifications.ingest.transactions import AtomicIngest, Handler

logger = logging.getLogger(__name__)
EventModel = type[AuthEvent] | type[LikesEvent] | type[FilmEvent]


class KafkaClient(Protocol):
    """Typed boundary for the raw-byte consumer; aiokafka leaves these methods untyped."""

    def subscribe(self, topics: list[str], *, listener: ConsumerRebalanceListener) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def beginning_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]: ...

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]: ...

    def seek(self, partition: TopicPartition, offset: int) -> None: ...

    def pause(self, *partitions: TopicPartition) -> None: ...

    def resume(self, *partitions: TopicPartition) -> None: ...

    async def getmany(
        self, *, timeout_ms: int
    ) -> dict[TopicPartition, list[ConsumerRecord[bytes, bytes]]]: ...


class InvalidPayload(RuntimeError):
    """A poison record requires repair; its durable cursor stays unchanged."""


class KafkaIngestConsumer(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(
        self,
        database: Database,
        handler: Handler,
        *,
        bootstrap_servers: str,
        group_id: str,
        topic_models: Mapping[str, EventModel],
        batch_settings: BatchSettings | None = None,
    ) -> None:
        if not topic_models or not group_id:
            raise ValueError("Explicit group_id and topic_models are required")
        self.ingest = AtomicIngest(database, handler)
        self.topic_models = dict(topic_models)
        batch_settings = batch_settings if batch_settings is not None else BatchSettings()
        self.client = cast(
            KafkaClient,
            AIOKafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="none",
                isolation_level="read_committed",
                max_poll_records=batch_settings.consumer_batch_size,
            ),
        )
        self.client.subscribe(list(topic_models), listener=self)
        self.assignment_generation = 0
        self._positions: dict[TopicPartition, int] = {}
        self._active: dict[TopicPartition, asyncio.Task[None]] = {}
        self._failure: Exception | None = None
        self._ready = asyncio.Event()

    # aiokafka awaits async listeners, although its base return annotation says None.
    async def on_partitions_revoked(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, revoked: Iterable[TopicPartition]
    ) -> None:
        self.assignment_generation += 1
        self._ready.clear()
        partitions = list(revoked)
        # Remove ownership in memory before yielding, so no buffered record starts.
        for tp in partitions:
            self._positions.pop(tp, None)
        for tp in partitions:
            task = self._active.get(tp)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            try:
                await self.ingest.release(tp.topic, tp.partition)
            except Exception as exc:
                self._fail(exc)

    # aiokafka awaits async listeners, although its base return annotation says None.
    async def on_partitions_assigned(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, assigned: Iterable[TopicPartition]
    ) -> None:
        self.assignment_generation += 1
        partitions = list(assigned)
        if not partitions or self._failure is not None:
            self._ready.set()
            return
        try:
            starts = await self.client.beginning_offsets(partitions)
            ends = await self.client.end_offsets(partitions)
            for tp in partitions:
                position = await self.ingest.claim(
                    tp.topic,
                    tp.partition,
                    earliest=starts[tp],
                    latest=ends[tp],
                )
                self.client.seek(tp, position)
                # A new assignment retries from PostgreSQL, never past a poison record.
                self.client.resume(tp)
                self._positions[tp] = position
        except Exception as exc:
            # aiokafka logs and suppresses listener errors; explicitly propagate to run().
            self._fail(exc)
        finally:
            self._ready.set()

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
            logger.error("Kafka ingestion stopped: %s", type(error).__name__)

    async def run(self) -> None:
        """Pause poison partitions; retention and handler failures remain fatal.

        Deliberately one active transaction per consumer, preserving partition
        order without holding any application lock across Kafka polling/rebalance.
        The caller owns the Database lifecycle and supplies domain dispatch.
        """
        try:
            await self.client.start()
            while True:
                await self._ready.wait()
                if self._failure is not None:
                    raise self._failure
                generation = self.assignment_generation
                try:
                    batches = await self.client.getmany(timeout_ms=250)
                except NoOffsetForPartitionError:
                    if not self._ready.is_set():
                        continue
                    raise
                if generation != self.assignment_generation:
                    # getmany may straddle a rebalance. Refetch under current ownership.
                    for tp, position in self._positions.items():
                        self.client.seek(tp, position)
                    continue
                for tp, records in batches.items():
                    for record in records:
                        if generation != self.assignment_generation or tp not in self._positions:
                            break
                        try:
                            if record.value is None:
                                raise ValueError("Tombstone is not an event")
                            event = self.topic_models[tp.topic].model_validate_json(record.value)
                        except (ValidationError, ValueError):
                            # getmany has advanced its fetch cursor past this batch.
                            # Keep the durable cursor and discard the rest of this partition.
                            self.client.seek(tp, self._positions[tp])
                            self.client.pause(tp)
                            error = InvalidPayload(
                                f"Invalid payload at {tp.topic}[{tp.partition}] offset "
                                f"{record.offset}; partition paused; manual repair required"
                            )
                            logger.error("%s", error)
                            break
                        task = asyncio.create_task(
                            self.ingest.handle_record(
                                event,
                                SourcePosition(
                                    topic=tp.topic,
                                    partition=tp.partition,
                                    next_offset=record.offset + 1,
                                ),
                                expected_offset=self._positions[tp],
                            )
                        )
                        self._active[tp] = task
                        try:
                            await task
                        except asyncio.CancelledError:
                            current = asyncio.current_task()
                            if (
                                current is not None and current.cancelling()
                            ) or generation == self.assignment_generation:
                                raise
                            # Rebalance cancelled the transaction; assignment seeks from PG.
                            break
                        finally:
                            self._active.pop(tp, None)
                        if generation == self.assignment_generation and tp in self._positions:
                            self._positions[tp] = record.offset + 1
        except Exception as exc:
            self._fail(exc)
            raise
        finally:
            await self.on_partitions_revoked(list(self._positions))
            await self.client.stop()
