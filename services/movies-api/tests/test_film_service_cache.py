import asyncio
import logging

import pytest

from db.cache.redis import RedisCache
from models.film import Film
from services.film import FILM_CACHE_EXPIRE_IN_SECONDS, FilmList
from tests.conftest import (
    FILM_ID,
    ElasticsearchStub,
    FailingRedisStub,
    MakeFilmService,
    RedisStub,
    film_hit,
    film_source,
)


def test_get_by_id_reads_namespaced_cache_key(
    make_film_service: MakeFilmService,
) -> None:
    cached_film = Film.model_validate(film_source())
    redis = RedisStub({f'film:{FILM_ID}': cached_film.model_dump_json()})
    elastic = ElasticsearchStub(source=film_source())
    service = make_film_service(redis, elastic)

    result = asyncio.run(service.get_by_id(FILM_ID))

    assert result == cached_film
    assert elastic.get_calls == 0


def test_get_by_id_writes_namespaced_cache_key(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub(source=film_source())
    service = make_film_service(redis, elastic)

    result = asyncio.run(service.get_by_id(FILM_ID))

    assert result is not None
    assert len(redis.set_calls) == 1
    key, value, expire = redis.set_calls[0]
    assert key == f'film:{FILM_ID}'
    assert Film.model_validate_json(value) == result
    assert expire == FILM_CACHE_EXPIRE_IN_SECONDS


def test_get_by_id_returns_none_for_missing_film(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub()
    service = make_film_service(redis, elastic)

    result = asyncio.run(service.get_by_id('00000000-0000-0000-0000-000000000000'))

    assert result is None
    assert elastic.get_calls == 1
    assert redis.set_calls == []


def test_cache_read_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cache = RedisCache(FailingRedisStub())  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR, logger='db.cache.redis'):
        result = asyncio.run(cache.get('film:some-id'))

    assert result is None
    assert 'Error getting from cache' in caplog.messages
    assert caplog.records[-1].exc_info is not None


def test_cache_write_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cache = RedisCache(FailingRedisStub())  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR, logger='db.cache.redis'):
        asyncio.run(cache.set('film:some-id', '{}', 300))

    assert 'Error putting to cache' in caplog.messages
    assert caplog.records[-1].exc_info is not None


def test_service_survives_unavailable_cache(
    make_film_service: MakeFilmService,
) -> None:
    elastic = ElasticsearchStub(source=film_source())
    service = make_film_service(FailingRedisStub(), elastic)

    result = asyncio.run(service.get_by_id(FILM_ID))

    assert result is not None
    assert result.id == FILM_ID
    assert elastic.get_calls == 1


def test_invalid_cache_is_treated_as_miss(
    make_film_service: MakeFilmService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = RedisStub({f'film:{FILM_ID}': b'{"not": "a film"}'})
    elastic = ElasticsearchStub(source=film_source())
    service = make_film_service(redis, elastic)

    with caplog.at_level(logging.WARNING, logger='services.film'):
        result = asyncio.run(service.get_by_id(FILM_ID))

    assert result is not None
    assert result.id == FILM_ID
    assert elastic.get_calls == 1
    assert any('Invalid cache payload' in message for message in caplog.messages)
    assert Film.model_validate_json(redis.set_calls[0][1]) == result


def test_invalid_list_cache_is_treated_as_miss(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub(
        {
            'film-list:-imdb_rating:all:1:10': b'[{"broken": true}]',
        }
    )
    elastic = ElasticsearchStub(hits=[film_hit()])
    service = make_film_service(redis, elastic)

    result = asyncio.run(
        service.list_films(
            sort='-imdb_rating',
            genre=None,
            page_number=1,
            page_size=10,
        )
    )

    assert len(result) == 1
    assert len(elastic.search_bodies) == 1
    assert FilmList.model_validate_json(redis.set_calls[0][1]).root == result
