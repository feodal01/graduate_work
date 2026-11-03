from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from api.v1.films import get_film_service
from main import app
from tests.conftest import GENRE_ID, FilmServiceStub

FILM_ID = '524e4331-e14b-24d3-a156-426614174003'


def test_film_list_exact_path_uses_documented_defaults(client: TestClient) -> None:
    service = FilmServiceStub()
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get('/api/v1/films', follow_redirects=False)

    assert response.status_code == 200
    assert response.json() == []
    assert service.list_calls == [('-imdb_rating', None, 1, 50)]


def test_film_list_matches_short_contract_and_forwards_filter(
    client: TestClient,
) -> None:
    service = FilmServiceStub(
        films=[
            SimpleNamespace(
                id=FILM_ID,
                title='Ringo Rocket Star and His Song for Yuri Gagarin',
                imdb_rating=9.4,
            )
        ]
    )
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get(
        '/api/v1/films',
        params={
            'genre': GENRE_ID,
            'sort': 'imdb_rating',
            'page_number': 2,
            'page_size': 25,
        },
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            'uuid': FILM_ID,
            'title': 'Ringo Rocket Star and His Song for Yuri Gagarin',
            'imdb_rating': 9.4,
        }
    ]
    assert service.list_calls == [('imdb_rating', GENRE_ID, 2, 25)]


@pytest.mark.parametrize(
    'params',
    [
        {'sort': 'title'},
        {'genre': 'not-a-uuid'},
        {'page_number': 0},
        {'page_size': 0},
    ],
)
def test_film_list_rejects_invalid_parameters(
    client: TestClient,
    params: dict[str, str | int],
) -> None:
    service = FilmServiceStub()
    app.dependency_overrides[get_film_service] = lambda: service

    response = client.get('/api/v1/films', params=params)

    assert response.status_code == 422
    assert service.list_calls == []
