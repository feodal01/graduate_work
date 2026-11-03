import asyncio
import logging

import pytest

from core.config import settings
from services.contracts import ConsumedRecord
from services.loader import EVENTS_TABLE, ClickHouseLoader
from services.pipeline import log_if_pipeline_stopped, run_forever, run_once
from services.transform import parse_envelope
from tests.conftest import FILM_ID, FakeClickHouseClient, MemoryConsumer, MemoryWarehouse
from tests.test_transform import envelope_bytes


async def test_validation_error_goes_to_dlq_and_commits() -> None:
    poison = ConsumedRecord(value=b'{', key=b'user-1')
    consumer = MemoryConsumer([poison])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    assert warehouse.insert_calls == 0
    assert consumer.dlq == [(b'{', b'user-1')]
    assert consumer.committed == 1


async def test_oversized_record_goes_to_dlq_without_insert() -> None:
    huge = ConsumedRecord(
        value=b'x' * (settings.kafka_max_request_size + 1),
        key=b'user-1',
        partition=0,
        offset=7,
    )
    consumer = MemoryConsumer([huge])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    assert warehouse.insert_calls == 0
    assert consumer.dlq == [(huge.value, b'user-1')]
    assert consumer.committed == 1


async def test_success_inserts_once_then_commits() -> None:
    raw = envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(FILM_ID),
            'position_seconds': 120,
            'duration_seconds': 7200,
        },
    )
    consumer = MemoryConsumer([ConsumedRecord(value=raw, key=b'user-1')])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    assert warehouse.insert_calls == 1
    assert len(warehouse.inserted[0]) == 1
    assert warehouse.inserted[0][0].film_id == FILM_ID
    assert consumer.committed == 1
    assert warehouse.events + consumer.events == ['insert', 'commit']


async def test_warehouse_error_does_not_commit() -> None:
    raw = envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(FILM_ID),
            'position_seconds': 120,
            'duration_seconds': 7200,
        },
    )
    consumer = MemoryConsumer([ConsumedRecord(value=raw, key=b'user-1')])
    warehouse = MemoryWarehouse()
    warehouse.fail = True

    with pytest.raises(RuntimeError, match='clickhouse unavailable'):
        await run_once(consumer, warehouse)

    assert consumer.committed == 0
    assert consumer.dlq == []


async def test_loader_inserts_only_events_table() -> None:
    raw = envelope_bytes(
        event_type='click',
        payload={
            'page': '/film/demo',
            'element_type': 'movie',
            'element_id': str(FILM_ID),
        },
    )
    row = parse_envelope(raw)
    client = FakeClickHouseClient()
    loader = ClickHouseLoader(client)

    await loader.insert_batch([row])

    assert client.inserts == [EVENTS_TABLE]
    assert 'film_watches' not in ''.join(client.inserts)
    assert 'page_events' not in ''.join(client.inserts)


async def test_empty_insert_batch_does_not_touch_clickhouse() -> None:
    client = FakeClickHouseClient()
    loader = ClickHouseLoader(client)

    await loader.insert_batch([])

    assert client.inserts == []
    assert client.commands == []


async def test_warehouse_error_retries_same_batch_without_new_poll() -> None:
    raw = envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(FILM_ID),
            'position_seconds': 120,
            'duration_seconds': 7200,
        },
    )
    consumer = MemoryConsumer([ConsumedRecord(value=raw, key=b'user-1')])
    warehouse = MemoryWarehouse()
    warehouse.fail = True

    with pytest.raises(RuntimeError, match='clickhouse unavailable'):
        await run_once(consumer, warehouse)

    warehouse.fail = False
    await run_once(consumer, warehouse)

    assert warehouse.insert_calls == 2
    assert len(warehouse.inserted) == 1
    assert consumer.committed == 1
    assert consumer.records == []


async def test_run_forever_does_not_reapply_schema() -> None:
    consumer = MemoryConsumer()
    warehouse = MemoryWarehouse()
    started = asyncio.Event()

    async def poll_and_block() -> list[ConsumedRecord]:
        started.set()
        await asyncio.Event().wait()
        return []

    consumer.poll_batch = poll_and_block  # type: ignore[method-assign]
    task = asyncio.create_task(run_forever(consumer, warehouse))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert warehouse.schema_calls == 0


async def test_log_if_pipeline_stopped_on_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _boom() -> None:
        raise RuntimeError('pipeline boom')

    with caplog.at_level(logging.ERROR, logger='services.pipeline'):
        task = asyncio.create_task(_boom())
        task.add_done_callback(log_if_pipeline_stopped)
        with pytest.raises(RuntimeError, match='pipeline boom'):
            await task

    assert 'ETL pipeline stopped unexpectedly' in caplog.text
    assert 'pipeline boom' in caplog.text


async def test_log_if_pipeline_stopped_ignores_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _hang() -> None:
        await asyncio.Event().wait()

    with caplog.at_level(logging.ERROR, logger='services.pipeline'):
        task = asyncio.create_task(_hang())
        task.add_done_callback(log_if_pipeline_stopped)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert 'ETL pipeline stopped unexpectedly' not in caplog.text
