from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from elastic_transport import ConnectionError as ElasticConnectionError
from redis.exceptions import ConnectionError as RedisConnectionError

from core.config import settings
from db.cache.redis import RedisCache
from db.storage.elastic import ElasticStorage
from db.storage.elastic_errors import StorageUnavailableError
from db.storage.elastic_query import ElasticQueryTranslator
from models.film import Film
from tests.conftest import FILM_INDEX, FILM_NESTED_PATHS, film_source


def _storage(es: Any) -> ElasticStorage[Film]:
    return ElasticStorage(
        index=FILM_INDEX,
        es=es,
        model=Film,
        translator=ElasticQueryTranslator(nested_paths=FILM_NESTED_PATHS),
    )


@pytest.mark.asyncio
async def test_elastic_get_retries_connection_error_then_succeeds() -> None:
    es = AsyncMock()
    es.get = AsyncMock(
        side_effect=[
            ElasticConnectionError('temporarily unavailable'),
            ElasticConnectionError('temporarily unavailable'),
            {'_source': film_source()},
        ]
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        film = await _storage(es).get_by_id(film_source()['id'])

    assert film is not None
    assert film.title == film_source()['title']
    assert es.get.await_count == 3


@pytest.mark.asyncio
async def test_elastic_get_gives_up_after_max_tries() -> None:
    es = AsyncMock()
    es.get = AsyncMock(
        side_effect=ElasticConnectionError('still down'),
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(StorageUnavailableError):
            await _storage(es).get_by_id(film_source()['id'])

    assert es.get.await_count == settings.backoff_max_tries


@pytest.mark.asyncio
async def test_elastic_does_not_retry_not_found() -> None:
    from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
    from elasticsearch import NotFoundError

    meta = ApiResponseMeta(
        status=404,
        http_version='1.1',
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig('http', 'localhost', 9200),
    )
    es = AsyncMock()
    es.get = AsyncMock(side_effect=NotFoundError('not found', meta, {'found': False}))

    with patch('asyncio.sleep', new_callable=AsyncMock) as sleep:
        film = await _storage(es).get_by_id(film_source()['id'])

    assert film is None
    assert es.get.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_redis_get_retries_connection_error_then_succeeds() -> None:
    redis = AsyncMock()
    redis.get = AsyncMock(
        side_effect=[
            RedisConnectionError('temporarily unavailable'),
            b'cached',
        ]
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        value = await RedisCache(redis).get('film:1')

    assert value == b'cached'
    assert redis.get.await_count == 2


@pytest.mark.asyncio
async def test_redis_get_soft_fails_after_retries_exhausted() -> None:
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RedisConnectionError('still down'))

    with patch('asyncio.sleep', new_callable=AsyncMock):
        value = await RedisCache(redis).get('film:1')

    assert value is None
    assert redis.get.await_count == settings.backoff_max_tries
