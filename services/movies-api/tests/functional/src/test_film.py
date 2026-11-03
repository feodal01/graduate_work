"""Функциональные тесты /api/v1/films: список, поиск, детали, кэш."""

import uuid
from datetime import UTC, date, datetime, timedelta

import jwt
import pytest

from tests.functional.utils.helpers import film_document, genre, person

ACTION = genre('Action')
COMEDY = genre('Comedy')
JWT_SECRET = 'functional-jwt-secret-that-is-long-enough'


def subscriber_token() -> str:
    now = datetime.now(UTC).replace(microsecond=0)
    return jwt.encode(
        {
            'sub': str(uuid.uuid4()),
            'jti': str(uuid.uuid4()),
            'type': 'access',
            'iat': now,
            'exp': now + timedelta(minutes=15),
        },
        JWT_SECRET,
        algorithm='HS256',
    )


async def test_film_list_is_sorted_by_rating_desc_by_default(
    http_client,
    es_write_data,
) -> None:
    await es_write_data(
        [
            film_document(title='Middle', imdb_rating=7.0),
            film_document(title='Best', imdb_rating=9.5),
            film_document(title='Worst', imdb_rating=3.0),
        ]
    )

    async with http_client.get('/api/v1/films') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert [item['title'] for item in body] == ['Best', 'Middle', 'Worst']
    assert [item['imdb_rating'] for item in body] == [9.5, 7.0, 3.0]


async def test_film_list_sorted_by_rating_asc(http_client, es_write_data) -> None:
    await es_write_data(
        [
            film_document(title='Middle', imdb_rating=7.0),
            film_document(title='Best', imdb_rating=9.5),
            film_document(title='Worst', imdb_rating=3.0),
        ]
    )

    async with http_client.get('/api/v1/films', params={'sort': 'imdb_rating'}) as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert [item['title'] for item in body] == ['Worst', 'Middle', 'Best']


async def test_film_list_returns_only_short_fields(http_client, es_write_data) -> None:
    await es_write_data([film_document(title='Short Form')])

    async with http_client.get('/api/v1/films') as response:
        body = await response.json()

    assert [set(item) for item in body] == [{'uuid', 'title', 'imdb_rating'}]


async def test_film_list_filtered_by_genre(http_client, es_write_data) -> None:
    await es_write_data(
        [
            film_document(title='Action Film', genres=[ACTION]),
            film_document(title='Comedy Film', genres=[COMEDY]),
            film_document(title='Both Genres', genres=[ACTION, COMEDY]),
        ]
    )

    async with http_client.get('/api/v1/films', params={'genre': COMEDY['id']}) as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert sorted(item['title'] for item in body) == ['Both Genres', 'Comedy Film']


async def test_film_list_is_empty_when_genre_has_no_films(
    http_client,
    es_write_data,
) -> None:
    await es_write_data([film_document(title='Action Film', genres=[ACTION])])

    async with http_client.get('/api/v1/films', params={'genre': str(uuid.uuid4())}) as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body == []


async def test_film_list_pagination_splits_films_without_overlap(
    http_client,
    es_write_data,
) -> None:
    await es_write_data(
        [film_document(title=f'Film {index}', imdb_rating=float(index)) for index in range(1, 6)]
    )
    params = {'sort': '-imdb_rating', 'page_size': 2}

    async with http_client.get('/api/v1/films', params={**params, 'page_number': 1}) as response:
        first_page = await response.json()
    async with http_client.get('/api/v1/films', params={**params, 'page_number': 2}) as response:
        second_page = await response.json()
    async with http_client.get('/api/v1/films', params={**params, 'page_number': 3}) as response:
        third_page = await response.json()

    assert [item['title'] for item in first_page] == ['Film 5', 'Film 4']
    assert [item['title'] for item in second_page] == ['Film 3', 'Film 2']
    assert [item['title'] for item in third_page] == ['Film 1']


@pytest.mark.parametrize(
    'params',
    [
        pytest.param({'page_number': 0}, id='page-number-below-one'),
        pytest.param({'page_size': 0}, id='page-size-below-one'),
        pytest.param({'page_size': 101}, id='page-size-above-limit'),
        pytest.param({'page_number': 200, 'page_size': 100}, id='pagination-depth-exceeded'),
        pytest.param({'sort': 'title'}, id='unsupported-sort-field'),
        pytest.param({'genre': 'not-a-uuid'}, id='genre-is-not-uuid'),
    ],
)
async def test_film_list_rejects_invalid_params(
    http_client,
    es_write_data,
    params: dict,
) -> None:
    await es_write_data([film_document(title='Any Film')])

    async with http_client.get('/api/v1/films', params=params) as response:
        assert response.status == 422


@pytest.mark.parametrize(
    'query, expected_titles',
    [
        pytest.param('Star', ['Star Wars', 'The Star'], id='token-in-two-titles'),
        pytest.param('Mashed Potato', ['Mashed Potato'], id='single-match'),
        pytest.param('Zzzzqqq', [], id='no-match'),
    ],
)
async def test_film_search(
    http_client,
    es_write_data,
    query: str,
    expected_titles: list[str],
) -> None:
    await es_write_data(
        [
            film_document(title='The Star'),
            film_document(title='Star Wars'),
            film_document(title='Mashed Potato'),
        ]
    )

    async with http_client.get('/api/v1/films/search/', params={'query': query}) as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert sorted(item['title'] for item in body) == expected_titles


async def test_film_search_paginates_results(http_client, es_write_data) -> None:
    await es_write_data(
        [film_document(title=f'Star {index}', imdb_rating=float(index)) for index in range(1, 4)]
    )

    async with http_client.get(
        '/api/v1/films/search/',
        params={'query': 'Star', 'page_size': 2, 'page_number': 1},
    ) as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert len(body) == 2


async def test_film_search_without_query_is_rejected(http_client, es_index) -> None:
    async with http_client.get('/api/v1/films/search/') as response:
        assert response.status == 422


async def test_film_details_returns_full_film(http_client, es_write_data) -> None:
    ann = person('Ann Actor')
    will = person('Will Writer')
    dan = person('Dan Director')
    film = film_document(
        title='Detailed Film',
        imdb_rating=8.1,
        description='A very detailed film',
        genres=[ACTION],
        actors=[ann],
        writers=[will],
        directors=[dan],
    )
    await es_write_data([film])

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body == {
        'uuid': film['id'],
        'title': 'Detailed Film',
        'imdb_rating': 8.1,
        'description': 'A very detailed film',
        'genre': [{'uuid': ACTION['id'], 'name': ACTION['name']}],
        'actors': [{'uuid': ann['id'], 'full_name': ann['name']}],
        'writers': [{'uuid': will['id'], 'full_name': will['name']}],
        'directors': [{'uuid': dan['id'], 'full_name': dan['name']}],
    }


async def test_film_details_for_unknown_id_is_404(http_client, es_write_data) -> None:
    await es_write_data([film_document(title='Existing Film')])

    async with http_client.get(f'/api/v1/films/{uuid.uuid4()}/') as response:
        status = response.status
        body = await response.json()

    assert status == 404
    assert body == {'detail': 'Film not found'}


async def test_film_without_date_requires_subscriber_token(
    http_client,
    es_write_data,
) -> None:
    film = film_document(title='Unknown Date', creation_date=None)
    await es_write_data([film])

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        anonymous_status = response.status
    async with http_client.get(
        f'/api/v1/films/{film["id"]}/',
        headers={'Authorization': f'Bearer {subscriber_token()}'},
    ) as response:
        subscriber_status = response.status

    assert anonymous_status == 403
    assert subscriber_status == 200


@pytest.mark.parametrize(
    ('creation_date', 'expected_status'),
    (
        (date(1900, 1, 1), 200),
        (date(2999, 1, 1), 403),
    ),
)
async def test_film_details_degrade_on_auth_5xx(
    http_client,
    es_write_data,
    creation_date: date,
    expected_status: int,
) -> None:
    film = film_document(
        title='Auth Failure',
        creation_date=creation_date,
    )
    await es_write_data([film])

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        status = response.status

    assert status == expected_status


async def test_film_details_for_invalid_id_is_422(http_client, es_index) -> None:
    async with http_client.get('/api/v1/films/not-a-uuid/') as response:
        assert response.status == 422


async def test_film_details_are_served_from_cache(
    http_client,
    es_write_data,
    es_client,
    es_index,
) -> None:
    """Второй запрос обслуживается кэшем: данные из Elasticsearch уже удалены."""
    film = film_document(title='Cached Film')
    await es_write_data([film])

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        first_status = response.status
        first_body = await response.json()

    await es_client.delete(index=es_index, id=film['id'], refresh='wait_for')

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        second_status = response.status
        second_body = await response.json()

    assert first_status == 200
    assert second_status == 200
    assert second_body == first_body


async def test_film_list_is_served_from_cache(
    http_client,
    es_write_data,
    es_client,
    es_index,
) -> None:
    film = film_document(title='Cached List Item')
    await es_write_data([film])

    async with http_client.get('/api/v1/films') as response:
        first_body = await response.json()

    await es_client.delete(index=es_index, id=film['id'], refresh='wait_for')

    async with http_client.get('/api/v1/films') as response:
        second_status = response.status
        second_body = await response.json()

    assert first_body == [{'uuid': film['id'], 'title': 'Cached List Item', 'imdb_rating': 7.0}]
    assert second_status == 200
    assert second_body == first_body


async def test_cache_is_cold_after_data_is_gone(
    http_client,
    es_write_data,
    es_client,
    es_index,
    redis_client,
) -> None:
    """Контрольная проверка предыдущего теста: без кэша ответ становится пустым."""
    film = film_document(title='Not Cached')
    await es_write_data([film])

    async with http_client.get('/api/v1/films') as response:
        assert len(await response.json()) == 1

    await es_client.delete(index=es_index, id=film['id'], refresh='wait_for')
    await redis_client.flushall()

    async with http_client.get('/api/v1/films') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body == []
