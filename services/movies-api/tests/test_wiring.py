from collections.abc import Callable, Iterator
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient

from clients.auth import AuthClient
from db.cache.redis import RedisCache
from db.health import HealthCheck
from db.storage.elastic import ElasticStorage
from db.storage.elastic_query import ElasticQueryTranslator
from main import app
from models.film import Film
from tests.conftest import (
    FILM_ID,
    FILM_INDEX,
    FILM_NESTED_PATHS,
    ElasticsearchStub,
    FailingRedisStub,
    RedisStub,
    film_source,
)

STATE_KEYS = ('film_storage', 'cache', 'auth_client')

WireState = Callable[..., None]


def make_storage(elastic: Any) -> ElasticStorage[Film]:
    return ElasticStorage(
        index=FILM_INDEX,
        es=elastic,
        model=Film,
        translator=ElasticQueryTranslator(nested_paths=FILM_NESTED_PATHS),
    )


@pytest.fixture(autouse=True)
def wire_state() -> Iterator[WireState]:
    def _wire(**objects: Any) -> None:
        for name, value in objects.items():
            setattr(app.state, name, value)

    yield _wire

    for key in STATE_KEYS:
        app.state._state.pop(key, None)


def test_lifespan_builds_storage_and_cache() -> None:
    with TestClient(app):
        storage = app.state.film_storage
        cache = app.state.cache
        auth_client = app.state.auth_client

        assert isinstance(storage, ElasticStorage)
        assert isinstance(storage, HealthCheck)
        assert isinstance(storage.es, AsyncElasticsearch)
        assert storage.model is Film
        assert storage.translator.nested_paths == frozenset({'genres'})
        assert isinstance(cache, RedisCache)
        assert isinstance(auth_client, AuthClient)


def test_film_details_runs_through_state_wiring(
    client: TestClient,
    wire_state: WireState,
) -> None:
    elastic = ElasticsearchStub(source=film_source())
    redis = RedisStub()
    wire_state(film_storage=make_storage(elastic), cache=RedisCache(redis))

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 200
    assert response.json()['uuid'] == FILM_ID
    assert elastic.get_calls == 1
    assert f'film:{FILM_ID}' in redis.values


def test_health_ok_when_storage_is_reachable(
    client: TestClient,
    wire_state: WireState,
) -> None:
    elastic = ElasticsearchStub(reachable=True)
    redis = RedisStub()
    wire_state(
        film_storage=make_storage(elastic),
        cache=RedisCache(redis),
    )

    response = client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {
        'status': 'ok',
        'elasticsearch': 'ok',
        'redis': 'ok',
    }
    assert elastic.ping_calls == 1
    assert redis.ping_calls == 1


def test_health_degraded_when_redis_is_unreachable(
    client: TestClient,
    wire_state: WireState,
) -> None:
    elastic = ElasticsearchStub(reachable=True)
    wire_state(
        film_storage=make_storage(elastic),
        cache=RedisCache(FailingRedisStub()),  # type: ignore[arg-type]
    )

    response = client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {
        'status': 'degraded',
        'elasticsearch': 'ok',
        'redis': 'degraded',
    }
    assert elastic.ping_calls == 1


def test_health_fails_when_storage_is_unreachable(
    client: TestClient,
    wire_state: WireState,
) -> None:
    elastic = ElasticsearchStub(reachable=False)
    redis = RedisStub()
    wire_state(
        film_storage=make_storage(elastic),
        cache=RedisCache(redis),
    )

    response = client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json() == {
        'status': 'unavailable',
        'elasticsearch': 'unavailable',
        'redis': 'ok',
    }
    assert elastic.ping_calls == 1
