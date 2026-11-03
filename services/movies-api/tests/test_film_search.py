import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from api.v1.films import get_film_service
from main import app
from tests.conftest import (
    ElasticsearchStub,
    FilmServiceStub,
    MakeFilmService,
    RedisStub,
    film_hit,
)


def test_search_requests_selected_page_from_elasticsearch(
    make_film_service: MakeFilmService,
) -> None:
    elastic = ElasticsearchStub()
    service = make_film_service(RedisStub(), elastic)

    asyncio.run(service.search_by_title('star', page_number=2, page_size=50))

    assert elastic.search_bodies[0]['from_'] == 50
    assert elastic.search_bodies[0]['size'] == 50


def test_search_uses_fuzzy_title_match(
    make_film_service: MakeFilmService,
) -> None:
    elastic = ElasticsearchStub()
    service = make_film_service(RedisStub(), elastic)

    asyncio.run(service.search_by_title('star', page_number=1, page_size=10))

    assert elastic.search_bodies[0]['query'] == {
        'bool': {
            'must': [
                {
                    'match': {
                        'title': {
                            'query': 'star',
                            'fuzziness': 'AUTO',
                        }
                    }
                }
            ]
        }
    }


def test_repeated_search_uses_cached_results(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub([film_hit()])
    service = make_film_service(redis, elastic)

    first_result = asyncio.run(service.search_by_title('star', page_number=1, page_size=10))
    second_result = asyncio.run(service.search_by_title('star', page_number=1, page_size=10))

    assert second_result == first_result
    assert len(elastic.search_bodies) == 1


def test_search_cache_separates_pages(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    first_page_elastic = ElasticsearchStub([film_hit('223e4317-e89b-22d3-f3b6-426614174000')])
    second_page_elastic = ElasticsearchStub([film_hit('524e4331-e14b-24d3-a456-426614174001')])

    first_page = asyncio.run(
        make_film_service(redis, first_page_elastic).search_by_title(
            'star', page_number=1, page_size=10
        )
    )
    second_page = asyncio.run(
        make_film_service(redis, second_page_elastic).search_by_title(
            'star', page_number=2, page_size=10
        )
    )

    assert first_page[0].id != second_page[0].id
    assert len(second_page_elastic.search_bodies) == 1


def test_empty_search_results_are_cached(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub()
    service = make_film_service(redis, elastic)

    first_result = asyncio.run(service.search_by_title('missing', page_number=1, page_size=10))
    second_result = asyncio.run(service.search_by_title('missing', page_number=1, page_size=10))

    assert first_result == []
    assert second_result == []
    assert len(elastic.search_bodies) == 1


def test_search_returns_short_film_contract(client: TestClient) -> None:
    service = FilmServiceStub(
        films=[
            SimpleNamespace(
                id='223e4317-e89b-22d3-f3b6-426614174000',
                title='Star Wars: Episode IV - A New Hope',
                imdb_rating=8.6,
            )
        ]
    )
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get(
        '/api/v1/films/search/',
        params={'query': 'star', 'page_number': 2, 'page_size': 50},
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            'uuid': '223e4317-e89b-22d3-f3b6-426614174000',
            'title': 'Star Wars: Episode IV - A New Hope',
            'imdb_rating': 8.6,
        }
    ]


def test_search_forwards_pagination_to_service(client: TestClient) -> None:
    service = FilmServiceStub()
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get(
        '/api/v1/films/search/',
        params={'query': 'star', 'page_number': 3, 'page_size': 25},
    )

    assert response.status_code == 200
    assert service.search_calls == [('star', 3, 25)]


def test_search_returns_empty_list_when_nothing_found(client: TestClient) -> None:
    service = FilmServiceStub()
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get(
        '/api/v1/films/search/',
        params={'query': 'missing', 'page_number': 1, 'page_size': 50},
    )

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.parametrize(
    'pagination',
    [
        {'page_number': 0, 'page_size': 50},
        {'page_number': 1, 'page_size': 0},
    ],
)
def test_search_rejects_non_positive_pagination(
    client: TestClient,
    pagination: dict[str, int],
) -> None:
    service = FilmServiceStub()
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get(
        '/api/v1/films/search/',
        params={'query': 'star', **pagination},
    )

    assert response.status_code == 422
    assert service.search_calls == []
