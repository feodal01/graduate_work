import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from redis.asyncio import Redis

from main import lifespan


@pytest.fixture
def producer(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    instance = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    monkeypatch.setattr('main.AIOKafkaProducer', Mock(return_value=instance))
    return instance


@pytest.fixture
def redis(monkeypatch: pytest.MonkeyPatch) -> Mock:
    instance = Mock(spec=Redis)
    instance.aclose = AsyncMock()
    monkeypatch.setattr('main.Redis', Mock(return_value=instance))
    return instance


@pytest.mark.parametrize(
    'error', [KafkaConnectionError('startup failed'), asyncio.CancelledError()]
)
async def test_lifespan_closes_producer_when_start_fails(
    producer: SimpleNamespace,
    error: BaseException,
) -> None:
    producer.start.side_effect = error

    with pytest.raises(type(error)) as caught:
        async with lifespan(FastAPI()):
            pytest.fail('Application must not start')

    producer.stop.assert_awaited_once()
    assert caught.value is error


async def test_lifespan_preserves_startup_error_when_producer_stop_fails(
    producer: SimpleNamespace,
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = KafkaConnectionError('startup failed')
    producer.start.side_effect = error
    producer.stop.side_effect = RuntimeError('cleanup failed')

    with pytest.raises(KafkaConnectionError) as caught:
        async with lifespan(FastAPI()):
            pytest.fail('Application must not start')

    producer.stop.assert_awaited_once()
    assert caught.value is error
    assert any(record.exc_info is not None for record in caplog.records)


async def test_lifespan_closes_resources_after_successful_start(
    producer: SimpleNamespace,
    redis: Mock,
) -> None:
    app = FastAPI()

    async with lifespan(app):
        assert app.state.event_broker is not None
        assert app.state.event_rate_limiter is not None
        producer.stop.assert_not_awaited()
        redis.aclose.assert_not_awaited()

    redis.aclose.assert_awaited_once()
    producer.stop.assert_awaited_once()


async def test_lifespan_closes_producer_when_redis_initialization_fails(
    producer: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError('redis initialization failed')
    monkeypatch.setattr('main.Redis', Mock(side_effect=error))

    with pytest.raises(RuntimeError) as caught:
        async with lifespan(FastAPI()):
            pytest.fail('Application must not start')

    producer.stop.assert_awaited_once()
    assert caught.value is error


async def test_lifespan_closes_producer_when_redis_close_fails(
    producer: SimpleNamespace,
    redis: Mock,
) -> None:
    error = RuntimeError('redis cleanup failed')
    redis.aclose.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        async with lifespan(FastAPI()):
            pass

    producer.stop.assert_awaited_once()
    assert caught.value is error


async def test_lifespan_propagates_producer_stop_error_after_successful_start(
    producer: SimpleNamespace,
    redis: Mock,
) -> None:
    error = RuntimeError('producer cleanup failed')
    producer.stop.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        async with lifespan(FastAPI()):
            pass

    redis.aclose.assert_awaited_once()
    assert caught.value is error
