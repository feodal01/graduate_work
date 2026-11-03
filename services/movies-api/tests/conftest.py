import os

# Юнит-тесты не ходят во внешние сервисы; дефолты нужны только для валидации Settings
# на чистом checkout (без .env). Выставляем до импорта приложения/конфига.
os.environ.setdefault('REDIS_HOST', '127.0.0.1')
os.environ.setdefault('ELASTICSEARCH_HOST', '127.0.0.1')
os.environ.setdefault('ELASTICSEARCH_INDEX', 'movies')
os.environ.setdefault('AUTH_JWT_SECRET', 'test-secret-that-is-long-enough-for-hs256')
os.environ.setdefault('AUTH_SERVICE_URL', 'http://127.0.0.1:8000/api/v1/auth')
os.environ.setdefault('AUTH_SERVICE_API_KEY', 'test-service-key-that-is-long-enough')

from collections.abc import Callable, Iterator
from datetime import date
from typing import Any

import pytest
from elasticsearch import NotFoundError
from fastapi.testclient import TestClient
from redis.exceptions import RedisError

from db.cache.redis import RedisCache
from db.storage.elastic import ElasticStorage
from db.storage.elastic_query import ElasticQueryTranslator
from dependencies import get_auth_client
from main import app
from models.film import Film
from services.film import FilmService

FILM_INDEX = 'movies'
FILM_NESTED_PATHS = frozenset({'genres'})
FILM_ID = '3d825f60-9fff-4dfe-b294-1a45fa1e115d'
GENRE_ID = '3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff'

MakeFilmService = Callable[[Any, Any], FilmService]


def film_source(film_id: str = FILM_ID) -> dict[str, Any]:
    return {
        'id': film_id,
        'imdb_rating': 8.6,
        'genres': [{'id': GENRE_ID, 'name': 'Action'}],
        'title': 'Star Wars: Episode IV - A New Hope',
        'description': 'A long time ago in a galaxy far, far away.',
        'directors_names': [],
        'actors_names': [],
        'writers_names': [],
        'directors': [],
        'actors': [],
        'writers': [],
    }


def film_hit(film_id: str = FILM_ID) -> dict[str, Any]:
    return {'_source': film_source(film_id)}


class RedisStub:
    def __init__(self, values: dict[str, bytes | str] | None = None) -> None:
        self.values = values or {}
        self.set_calls: list[tuple[str, bytes | str, int]] = []
        self.ping_calls = 0
        self.reachable = True

    async def get(self, key: str) -> bytes | str | None:
        return self.values.get(key)

    async def set(
        self,
        key: str,
        value: bytes | str,
        expire: int,
    ) -> None:
        self.values[key] = value
        self.set_calls.append((key, value, expire))

    async def ping(self) -> bool:
        self.ping_calls += 1
        return self.reachable


class FailingRedisStub:
    async def get(self, key: str) -> None:
        raise RedisError('Redis is unavailable')

    async def set(
        self,
        key: str,
        value: bytes | str,
        expire: int,
    ) -> None:
        raise RedisError('Redis is unavailable')

    async def ping(self) -> bool:
        raise RedisError('Redis is unavailable')


class ElasticsearchStub:
    def __init__(
        self,
        hits: list[dict[str, Any]] | None = None,
        source: dict[str, Any] | None = None,
        reachable: bool = True,
    ) -> None:
        self.hits = hits or []
        self.source = source
        self.reachable = reachable
        self.search_bodies: list[dict[str, Any]] = []
        self.get_calls = 0
        self.ping_calls = 0

    async def ping(self) -> bool:
        self.ping_calls += 1
        return self.reachable

    async def search(self, *, index: str, **kwargs: Any) -> dict[str, Any]:
        self.search_bodies.append(kwargs)
        return {'hits': {'hits': self.hits}}

    async def get(self, *, index: str, id: str) -> dict[str, Any]:
        self.get_calls += 1
        if self.source is None:
            raise NotFoundError('not found', None, None)  # type: ignore[arg-type]
        return {'_source': self.source}


class FilmServiceStub:
    def __init__(
        self,
        *,
        films: list[Any] | None = None,
        film: Any | None = None,
    ) -> None:
        self.films = films or []
        self.film = film
        self.list_calls: list[tuple[str, str | None, int, int]] = []
        self.search_calls: list[tuple[str, int, int]] = []
        self.get_calls: list[str] = []

    async def list_films(
        self,
        sort: str,
        genre: str | None,
        page_number: int,
        page_size: int,
    ) -> list[Any]:
        self.list_calls.append((sort, genre, page_number, page_size))
        return self.films

    async def search_by_title(
        self,
        query: str,
        page_number: int,
        page_size: int,
    ) -> list[Any]:
        self.search_calls.append((query, page_number, page_size))
        return self.films

    async def get_by_id(self, film_id: str) -> Any | None:
        self.get_calls.append(film_id)
        return self.film


class AllowAllAuthClient:
    async def can_read_film(
        self,
        *,
        creation_date: date | None,
        access_token: str | None,
        request_id: str,
    ) -> bool:
        del creation_date, access_token, request_id
        return True


@pytest.fixture(autouse=True)
def clear_dependency_overrides() -> Iterator[None]:
    app.dependency_overrides[get_auth_client] = AllowAllAuthClient
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(
        app,
        headers={'X-Request-Id': 'movies-test-request'},
    )


@pytest.fixture
def make_film_service() -> MakeFilmService:
    def _make(redis: Any, elastic: Any) -> FilmService:
        return FilmService(
            storage=ElasticStorage(
                index=FILM_INDEX,
                es=elastic,
                model=Film,
                translator=ElasticQueryTranslator(
                    nested_paths=FILM_NESTED_PATHS,
                ),
            ),
            cache=RedisCache(redis),
        )

    return _make
