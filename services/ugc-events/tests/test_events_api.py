import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from services.event_schema import MAX_PAGE_LENGTH, MAX_REQUEST_ID_LENGTH
from services.rate_limit import RedisEventRateLimiter
from tests.conftest import (
    FILM_ID,
    TEST_USER_ID,
    MemoryEventBroker,
    auth_header,
    click_event,
    make_token,
)


def test_ingest_rejects_missing_token_before_body_validation(
    client: TestClient,
) -> None:
    response = client.post(
        '/api/v1/ugc/events',
        json={'events': []},
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Not authenticated'}
    assert response.headers['www-authenticate'] == 'Bearer'


def test_ingest_rejects_refresh_token(client: TestClient) -> None:
    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(make_token(token_type='refresh')),
        json={'events': [click_event()]},
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid or expired token'}


def test_ingest_rejects_empty_batch(client: TestClient) -> None:
    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={'events': []},
    )

    assert response.status_code == 422


def test_ingest_accepts_valid_batch_and_stores_user_id_from_token(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    event_id = uuid4()

    response = client.post(
        '/api/v1/ugc/events',
        headers={**auth_header(), 'X-Request-Id': 'req-1'},
        json={'events': [click_event(event_id=event_id)]},
    )

    assert response.status_code == 202
    assert response.json() == {'accepted': 1}
    assert len(broker.records) == 1
    record = broker.records[0]
    assert record.key == str(TEST_USER_ID)
    payload = json.loads(record.value.decode('utf-8'))
    assert payload['event_id'] == str(event_id)
    assert payload['user_id'] == str(TEST_USER_ID)
    assert payload['event_type'] == 'click'
    assert payload['request_id'] == 'req-1'
    assert payload['payload']['page'] == '/film/3d825f60-9fff-4dfe-b294-1a45fa1e115d'
    assert payload['payload']['element_id'] == str(FILM_ID)


def test_ingest_returns_503_when_broker_unavailable(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    broker.reachable = False

    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={'events': [click_event()]},
    )

    assert response.status_code == 503
    assert response.json() == {'detail': 'Event broker is unavailable'}


def test_ingest_rejects_oversized_page_with_422(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={
            'events': [
                click_event(page='p' * (MAX_PAGE_LENGTH + 1)),
            ]
        },
    )

    assert response.status_code == 422
    assert broker.records == []


def test_ingest_rejects_long_request_id_with_422(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    response = client.post(
        '/api/v1/ugc/events',
        headers={
            **auth_header(),
            'X-Request-Id': 'r' * (MAX_REQUEST_ID_LENGTH + 1),
        },
        json={'events': [click_event(), click_event()]},
    )

    assert response.status_code == 422
    assert response.json() == {'detail': 'X-Request-Id is too long'}
    assert broker.records == []


def test_new_access_tokens_cannot_bypass_user_rate_limit(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    user_id = str(uuid4())
    responses = [
        client.post(
            '/api/v1/ugc/events',
            headers=auth_header(make_token(user_id=user_id)),
            json={'events': [click_event()]},
        )
        for _ in range(11)
    ]

    assert [response.status_code for response in responses] == [202] * 10 + [429]
    assert 1 <= int(responses[-1].headers['retry-after']) <= 60
    assert len(broker.records) == 10

    other_user = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(make_token(user_id=str(uuid4()))),
        json={'events': [click_event()]},
    )
    assert other_user.status_code == 202


@contextmanager
def unavailable_rate_limiter(
    client: TestClient,
    *,
    script_error: Exception | None = None,
) -> Iterator[None]:
    from dependencies import get_event_rate_limiter
    from main import app

    redis = Redis(
        host='127.0.0.1',
        port=0,
        socket_connect_timeout=0.1,
        socket_timeout=0.1,
        retry=Retry(NoBackoff(), 0),
    )
    limiter = RedisEventRateLimiter(redis, rate=1, window_seconds=60, capacity=10)
    if script_error is not None:
        limiter._script = AsyncMock(side_effect=script_error)
    app.dependency_overrides[get_event_rate_limiter] = lambda: limiter
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_event_rate_limiter)
        assert client.portal is not None
        client.portal.call(redis.aclose)


@pytest.mark.parametrize('script_error', [None, RedisTimeoutError('timed out')])
def test_redis_outage_accepts_ingest_and_logs_degradation(
    client: TestClient,
    broker: MemoryEventBroker,
    caplog: pytest.LogCaptureFixture,
    script_error: Exception | None,
) -> None:
    with (
        unavailable_rate_limiter(client, script_error=script_error),
        caplog.at_level(logging.WARNING, logger='api.v1.events'),
    ):
        response = client.post(
            '/api/v1/ugc/events',
            headers={**auth_header(), 'X-Request-Id': 'redis-outage'},
            json={'events': [click_event()]},
        )

    assert response.status_code == 202
    assert response.json() == {'accepted': 1}
    assert len(broker.records) == 1
    record = next(record for record in caplog.records if record.name == 'api.v1.events')
    assert record.request_id == 'redis-outage'
    assert getattr(record, 'rate_limit.store_available') is False


def test_health_detects_redis_outage(client: TestClient) -> None:
    with unavailable_rate_limiter(client):
        response = client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {'status': 'degraded', 'kafka': 'ok', 'redis': 'unavailable'}


def test_redis_script_error_is_not_hidden_by_fail_open(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    with (
        unavailable_rate_limiter(client, script_error=ResponseError('broken script')),
        pytest.raises(ResponseError),
    ):
        client.post(
            '/api/v1/ugc/events',
            headers=auth_header(),
            json={'events': [click_event()]},
        )

    assert broker.records == []


def test_redis_outage_does_not_hide_kafka_failure(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    broker.reachable = False
    with unavailable_rate_limiter(client):
        response = client.post(
            '/api/v1/ugc/events',
            headers=auth_header(),
            json={'events': [click_event()]},
        )
        health = client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json() == {'detail': 'Event broker is unavailable'}
    assert broker.records == []
    assert health.status_code == 503
    assert health.json() == {
        'status': 'unavailable',
        'kafka': 'unavailable',
        'redis': 'unavailable',
    }


def test_invalid_signature_does_not_consume_victims_bucket(client: TestClient) -> None:
    user_id = str(uuid4())
    for _ in range(11):
        response = client.post(
            '/api/v1/ugc/events',
            headers=auth_header(make_token(user_id=user_id, secret='wrong-secret' * 3)),
            json={'events': [click_event()]},
        )
        assert response.status_code == 401

    for _ in range(10):
        response = client.post(
            '/api/v1/ugc/events',
            headers=auth_header(make_token(user_id=user_id)),
            json={'events': [click_event()]},
        )
        assert response.status_code == 202
