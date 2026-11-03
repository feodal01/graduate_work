import asyncio

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from core.config import Settings, settings
from services import pipeline
from services.contracts import ConsumedRecord
from tests.conftest import FILM_ID, MemoryConsumer, MemoryWarehouse
from tests.test_transform import envelope_bytes


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    instance = Clock()
    monkeypatch.setattr(pipeline, 'monotonic', instance, raising=False)
    monkeypatch.setattr(settings, 'etl_stall_timeout_seconds', 360.0)
    return instance


def valid_record() -> ConsumedRecord:
    return ConsumedRecord(
        value=envelope_bytes(event_type='watch_completed', payload={'film_id': str(FILM_ID)}),
        key=b'user-1',
    )


@pytest.mark.parametrize('stage', ['insert', 'dlq', 'commit'])
async def test_health_rejects_stalled_batch_with_healthy_dependencies(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    consumer.records = [valid_record()]
    if stage == 'insert':
        warehouse.fail = True
    elif stage == 'dlq':
        consumer.records = [ConsumedRecord(value=b'{', key=b'user-1')]
        consumer.dlq_fail = True
    else:

        async def fail_commit(_records: list[ConsumedRecord]) -> None:
            raise RuntimeError('commit failed')

        monkeypatch.setattr(consumer, 'commit', fail_commit)

    with pytest.raises(RuntimeError):
        await pipeline.run_once(consumer, warehouse)

    clock.now += 359
    assert client.get('/api/v1/health/').status_code == 200
    clock.now += 1
    response = client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json()['pipeline'] == 'unavailable'
    assert response.json()['kafka'] == 'ok'
    assert response.json()['clickhouse'] == 'ok'
    assert response.json()['pending_age_seconds'] == 360
    assert response.json()['last_success_age_seconds'] is None


@pytest.mark.parametrize('started_at', [0.0, 1000.0])
async def test_retries_do_not_reset_pending_age(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
    started_at: float,
) -> None:
    clock.now = started_at
    consumer.records = [valid_record()]
    warehouse.fail = True
    with pytest.raises(RuntimeError):
        await pipeline.run_once(consumer, warehouse)
    clock.now += 300
    with pytest.raises(RuntimeError):
        await pipeline.run_once(consumer, warehouse)
    clock.now += 60

    response = client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json()['pending_age_seconds'] == 360


async def test_idle_time_does_not_age_a_new_batch(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
) -> None:
    consumer.records = [valid_record()]
    await pipeline.run_once(consumer, warehouse)
    clock.now += 10000
    await pipeline.run_once(consumer, warehouse)
    response = client.get('/api/v1/health/')
    assert response.status_code == 200
    assert response.json()['pending_age_seconds'] is None
    assert response.json()['last_success_age_seconds'] == 10000

    consumer.records = [valid_record()]
    warehouse.fail = True
    with pytest.raises(RuntimeError):
        await pipeline.run_once(consumer, warehouse)

    response = client.get('/api/v1/health/')
    assert response.status_code == 200
    assert response.json()['pending_age_seconds'] == 0


async def test_health_detects_hanging_insert_without_completed_errors(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer.records = [valid_record()]
    entered = asyncio.Event()

    async def hang_insert(_events: object) -> None:
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(warehouse, 'insert_batch', hang_insert)
    task = asyncio.create_task(pipeline.run_once(consumer, warehouse))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        clock.now += 360
        response = client.get('/api/v1/health/')
        assert response.status_code == 503
        assert response.json()['consecutive_errors'] == 0
        assert response.json()['last_success_age_seconds'] is None
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_errors_are_counted_and_success_restores_health(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer.records = [valid_record()]
    monkeypatch.setattr(settings, 'etl_flush_interval_seconds', 0)
    original_insert = warehouse.insert_batch
    second_failure = asyncio.Event()
    calls = 0

    async def fail_insert(_events: object) -> None:
        nonlocal calls
        calls += 1
        if calls > 2:
            await asyncio.Event().wait()
        if calls == 2:
            second_failure.set()
        raise RuntimeError('insert failed')

    monkeypatch.setattr(warehouse, 'insert_batch', fail_insert)
    task = asyncio.create_task(pipeline.run_forever(consumer, warehouse))
    try:
        await asyncio.wait_for(second_failure.wait(), timeout=1)
        clock.now += 360
        response = client.get('/api/v1/health/')
        assert response.status_code == 503
        assert response.json()['consecutive_errors'] == 2
        assert pipeline.stats.last_success_at is None
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    monkeypatch.setattr(warehouse, 'insert_batch', original_insert)
    await pipeline.run_once(consumer, warehouse)
    response = client.get('/api/v1/health/')
    assert response.status_code == 200
    assert response.json()['consecutive_errors'] == 0
    assert response.json()['pending_age_seconds'] is None
    assert response.json()['last_success_age_seconds'] == 0
    assert consumer.committed == 1


async def test_health_uses_configured_stall_timeout(
    client: TestClient,
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ETL_STALL_TIMEOUT_SECONDS', '10')
    configured = Settings(_env_file=None)
    monkeypatch.setattr(settings, 'etl_stall_timeout_seconds', configured.etl_stall_timeout_seconds)
    consumer.records = [valid_record()]
    warehouse.fail = True
    with pytest.raises(RuntimeError):
        await pipeline.run_once(consumer, warehouse)
    clock.now += 9
    assert client.get('/api/v1/health/').status_code == 200
    clock.now += 1
    assert client.get('/api/v1/health/').status_code == 503


@pytest.mark.parametrize('value', ['0', '-1', 'inf', 'nan'])
def test_stall_timeout_must_be_positive_and_finite(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv('ETL_STALL_TIMEOUT_SECONDS', value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
