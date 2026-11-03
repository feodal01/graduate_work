import asyncio
import logging
from dataclasses import dataclass, field
from time import monotonic
from typing import Optional

from pydantic import ValidationError

from core.config import settings
from services.contracts import ConsumedRecord, EventConsumer, Warehouse
from services.memory import read_memory
from services.transform import EventRow, parse_envelope

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    batch_size: int = 0
    last_success_at: Optional[float] = None
    consecutive_errors: int = 0

    def last_success_age_seconds(self) -> Optional[float]:
        if self.last_success_at is None:
            return None
        return max(0.0, monotonic() - self.last_success_at)


@dataclass
class _PendingBatch:
    records: list[ConsumedRecord] = field(default_factory=list[ConsumedRecord])
    events: list[EventRow] = field(default_factory=list[EventRow])
    poisons: list[ConsumedRecord] = field(default_factory=list[ConsumedRecord])
    loaded: bool = False
    pending_since: Optional[float] = None


stats = PipelineStats()
_pending = _PendingBatch()


def reset_pending() -> None:
    _pending.records.clear()
    _pending.events.clear()
    _pending.poisons.clear()
    _pending.loaded = False
    _pending.pending_since = None
    stats.batch_size = 0


def pending_age_seconds() -> Optional[float]:
    if _pending.pending_since is None:
        return None
    return max(0.0, monotonic() - _pending.pending_since)


async def run_once(consumer: EventConsumer, warehouse: Warehouse) -> None:
    if not _pending.loaded:
        records = await consumer.poll_batch()
        stats.batch_size = len(records)
        if not records:
            return
        _pending.pending_since = monotonic()
        _pending.records.extend(records)
        for record in records:
            if len(record.value) > settings.kafka_max_request_size:
                logger.error(
                    'Record exceeds Kafka max request size: bytes=%s partition=%s offset=%s',
                    len(record.value),
                    record.partition,
                    record.offset,
                )
                _pending.poisons.append(record)
                continue
            try:
                _pending.events.append(parse_envelope(record.value))
            except (UnicodeDecodeError, ValidationError):
                _pending.poisons.append(record)
        _pending.loaded = True
        stats.batch_size = len(_pending.events) + len(_pending.poisons)

    if _pending.events:
        await warehouse.insert_batch(_pending.events)

    for record in _pending.poisons:
        await consumer.send_dlq(record.value, record.key)

    await consumer.commit(_pending.records)
    stats.last_success_at = monotonic()
    stats.consecutive_errors = 0
    flushed_rows = len(_pending.events)
    reset_pending()
    memory = read_memory()
    logger.info(
        'Flush done: rows=%s rss_bytes=%s tracemalloc_current=%s tracemalloc_peak=%s',
        flushed_rows,
        memory.rss_bytes,
        memory.tracemalloc_current,
        memory.tracemalloc_peak,
    )


def pipeline_is_running(task: Optional[object]) -> bool:
    done = getattr(task, 'done', None)
    if not callable(done):
        return False
    return not done()


def log_if_pipeline_stopped(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        logger.error('ETL pipeline stopped unexpectedly')
        return
    logger.error('ETL pipeline stopped unexpectedly', exc_info=exc)


async def run_forever(consumer: EventConsumer, warehouse: Warehouse) -> None:
    while True:
        try:
            await run_once(consumer, warehouse)
        except Exception:  # pylint: disable=broad-exception-caught
            stats.consecutive_errors += 1
            logger.exception('ETL tick failed, continuing')
            await asyncio.sleep(settings.etl_flush_interval_seconds)
