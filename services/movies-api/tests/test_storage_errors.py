from unittest.mock import AsyncMock

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elasticsearch import ApiError, NotFoundError
from fastapi.testclient import TestClient

from db.storage.elastic_errors import StorageUnavailableError
from db.storage.query import SearchQuery
from dependencies import get_film_service
from main import app
from tests.conftest import FILM_ID, MakeFilmService, RedisStub
from tests.test_backoff import _storage


def _meta(status: int = 404) -> ApiResponseMeta:
    return ApiResponseMeta(
        status=status,
        http_version='1.1',
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig('http', 'localhost', 9200),
    )


def _index_not_found() -> NotFoundError:
    return NotFoundError(
        'index_not_found_exception',
        _meta(404),
        {
            'error': {
                'type': 'index_not_found_exception',
                'reason': 'no such index [movies]',
            },
            'status': 404,
        },
    )


@pytest.mark.asyncio
async def test_search_maps_api_error_to_storage_unavailable() -> None:
    es = AsyncMock()
    es.search = AsyncMock(side_effect=_index_not_found())

    with pytest.raises(StorageUnavailableError):
        await _storage(es).search(SearchQuery(page_number=1, page_size=10))


@pytest.mark.asyncio
async def test_get_maps_index_missing_to_storage_unavailable() -> None:
    es = AsyncMock()
    es.get = AsyncMock(side_effect=_index_not_found())

    with pytest.raises(StorageUnavailableError):
        await _storage(es).get_by_id(FILM_ID)


@pytest.mark.asyncio
async def test_get_missing_document_still_returns_none() -> None:
    es = AsyncMock()
    es.get = AsyncMock(side_effect=NotFoundError('not found', _meta(404), {'found': False}))

    assert await _storage(es).get_by_id(FILM_ID) is None


@pytest.mark.asyncio
async def test_search_maps_generic_api_error() -> None:
    es = AsyncMock()
    es.search = AsyncMock(side_effect=ApiError('cluster_block', _meta(503), {'error': 'blocked'}))

    with pytest.raises(StorageUnavailableError):
        await _storage(es).search(SearchQuery(page_number=1, page_size=10))


@pytest.mark.asyncio
async def test_search_maps_transport_error() -> None:
    from elastic_transport import SerializationError

    es = AsyncMock()
    es.search = AsyncMock(side_effect=SerializationError('bad payload'))

    with pytest.raises(StorageUnavailableError):
        await _storage(es).search(SearchQuery(page_number=1, page_size=10))


def test_api_returns_503_on_storage_unavailable(
    make_film_service: MakeFilmService,
    client: TestClient,
) -> None:
    elastic = AsyncMock()
    elastic.search = AsyncMock(side_effect=_index_not_found())
    app.dependency_overrides[get_film_service] = lambda: make_film_service(RedisStub(), elastic)

    response = client.get('/api/v1/films')

    assert response.status_code == 503
    assert response.json() == {'detail': 'Storage is unavailable'}
