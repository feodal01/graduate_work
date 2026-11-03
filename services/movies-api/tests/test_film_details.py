from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from api.v1.films import get_film_service
from main import app
from models.film import Film as FilmDocument
from tests.conftest import FILM_ID, GENRE_ID, FilmServiceStub

ACTOR_ID = '5b4bf1bc-3397-4e83-9b17-8b10c6544ed1'


def _film() -> SimpleNamespace:
    return SimpleNamespace(
        id=FILM_ID,
        creation_date=None,
        title='Star Wars: Episode IV - A New Hope',
        imdb_rating=8.6,
        description='A long time ago in a galaxy far, far away.',
        genres=[SimpleNamespace(id=GENRE_ID, name='Action')],
        actors=[SimpleNamespace(id=ACTOR_ID, name='Harrison Ford')],
        writers=[],
        directors=[],
    )


def test_elasticsearch_source_preserves_nested_genre_ids() -> None:
    film = FilmDocument.model_validate(
        {
            'id': FILM_ID,
            'title': 'Star Wars: Episode IV - A New Hope',
            'imdb_rating': 8.6,
            'description': 'A long time ago in a galaxy far, far away.',
            'genres': [{'id': GENRE_ID, 'name': 'Action'}],
            'directors_names': [],
            'actors_names': ['Harrison Ford'],
            'writers_names': [],
            'directors': [],
            'actors': [{'id': ACTOR_ID, 'name': 'Harrison Ford'}],
            'writers': [],
        }
    )

    assert film.genres[0].id == GENRE_ID
    assert film.genres[0].name == 'Action'


def test_film_details_matches_api_contract(client: TestClient) -> None:
    app.dependency_overrides[get_film_service] = lambda: FilmServiceStub(film=_film())

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 200
    assert response.json() == {
        'uuid': FILM_ID,
        'title': 'Star Wars: Episode IV - A New Hope',
        'imdb_rating': 8.6,
        'description': 'A long time ago in a galaxy far, far away.',
        'genre': [{'uuid': GENRE_ID, 'name': 'Action'}],
        'actors': [{'uuid': ACTOR_ID, 'full_name': 'Harrison Ford'}],
        'writers': [],
        'directors': [],
    }


def test_film_details_returns_404_for_unknown_id(client: TestClient) -> None:
    app.dependency_overrides[get_film_service] = FilmServiceStub

    response = client.get('/api/v1/films/00000000-0000-0000-0000-000000000000/')

    assert response.status_code == 404
    assert response.json() == {'detail': 'Film not found'}


def test_film_details_rejects_non_uuid_id(client: TestClient) -> None:
    app.dependency_overrides[get_film_service] = lambda: FilmServiceStub(film=_film())

    response = client.get('/api/v1/films/not-a-uuid/')

    assert response.status_code == 422


def test_elasticsearch_port_has_one_environment_variable_name() -> None:
    env_example = (Path(__file__).resolve().parent.parent / '.env.example').read_text()
    variable_names = {
        line.partition('=')[0]
        for line in env_example.splitlines()
        if line and not line.startswith('#')
    }

    assert 'ELASTICSEARCH_PORT' in variable_names
    assert 'ELASTIC_PORT' not in variable_names
