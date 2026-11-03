import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Optional
from uuid import UUID, uuid4

os.environ.setdefault('AUTH_JWT_SECRET', 'test-secret-that-is-long-enough-for-hs256')
os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
os.environ.setdefault('KAFKA_TOPIC', 'ugc.events')
os.environ.setdefault('PROJECT_NAME', 'ugc-events')

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from services.rate_limit import RedisEventRateLimiter

TEST_JWT_SECRET = 'test-secret-that-is-long-enough-for-hs256'
TEST_USER_ID = UUID('11111111-1111-4111-8111-111111111111')
FILM_ID = UUID('3d825f60-9fff-4dfe-b294-1a45fa1e115d')
RECEIVED_AT = datetime(2026, 9, 15, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def fixed_event_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return RECEIVED_AT.astimezone(tz)

    monkeypatch.setattr('api.v1.events.datetime', FixedDatetime)


def make_token(
    *,
    secret: str = TEST_JWT_SECRET,
    token_type: str = 'access',
    user_id: str = str(TEST_USER_ID),
    expires_at: Optional[datetime] = None,
) -> str:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    payload = {
        'sub': user_id,
        'jti': str(uuid4()),
        'type': token_type,
        'session_epoch': str(uuid4()),
        'iat': issued_at,
        'exp': expires_at or issued_at + timedelta(minutes=15),
    }
    return jwt.encode(payload, secret, algorithm='HS256')


class MemoryEventBroker:
    def __init__(self) -> None:
        self.records: list[Any] = []
        self.reachable = True

    async def publish_batch(self, records: list[Any]) -> None:
        from services.event_broker import EventBrokerUnavailableError

        if not self.reachable:
            raise EventBrokerUnavailableError
        self.records.extend(records)

    async def ping(self) -> bool:
        return self.reachable


@pytest.fixture
def broker() -> MemoryEventBroker:
    return MemoryEventBroker()


@pytest.fixture
def client(broker: MemoryEventBroker) -> Iterator[TestClient]:
    from dependencies import get_event_broker
    from main import app

    namespace = f'ugc-events-test:{uuid4()}'

    @asynccontextmanager
    async def test_lifespan(test_app: FastAPI) -> AsyncIterator[None]:
        redis = Redis(
            host=os.environ.get('REDIS_HOST', '127.0.0.1'),
            port=int(os.environ.get('REDIS_PORT', '6379')),
            retry=Retry(NoBackoff(), 0),
        )
        test_app.state.event_rate_limiter = RedisEventRateLimiter(
            redis,
            rate=1,
            window_seconds=60,
            capacity=10,
            namespace=namespace,
        )
        try:
            yield
        finally:
            keys = [key async for key in redis.scan_iter(match=f'{namespace}:*')]
            if keys:
                await redis.unlink(*keys)
            await redis.aclose()

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = test_lifespan
    app.dependency_overrides[get_event_broker] = lambda: broker
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.router.lifespan_context = original_lifespan
        app.dependency_overrides.clear()


def click_event(
    *,
    event_id: Optional[UUID] = None,
    page: Optional[str] = '/film/3d825f60-9fff-4dfe-b294-1a45fa1e115d',
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'element_type': 'movie',
        'element_id': str(FILM_ID),
    }
    if page is not None:
        payload['page'] = page
    return {
        'event_id': str(event_id or uuid4()),
        'occurred_at': RECEIVED_AT.isoformat(),
        'event_type': 'click',
        'payload': payload,
    }


def auth_header(token: Optional[str] = None) -> dict[str, str]:
    return {'Authorization': f'Bearer {token or make_token()}'}
