import asyncio

from services.film import FILM_CACHE_EXPIRE_IN_SECONDS
from tests.conftest import (
    GENRE_ID,
    ElasticsearchStub,
    MakeFilmService,
    RedisStub,
    film_hit,
)


def test_list_films_builds_popularity_query_without_genre(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub()
    service = make_film_service(redis, elastic)

    asyncio.run(
        service.list_films(
            sort='-imdb_rating',
            genre=None,
            page_number=2,
            page_size=50,
        )
    )

    assert elastic.search_bodies == [
        {
            'from_': 50,
            'size': 50,
            'sort': [{'imdb_rating': {'order': 'desc', 'missing': '_last'}}],
            'query': {'match_all': {}},
        }
    ]


def test_list_films_filters_by_nested_genre(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub()
    service = make_film_service(redis, elastic)

    asyncio.run(
        service.list_films(
            sort='imdb_rating',
            genre=GENRE_ID,
            page_number=1,
            page_size=25,
        )
    )

    assert elastic.search_bodies[0]['sort'] == [
        {'imdb_rating': {'order': 'asc', 'missing': '_last'}}
    ]
    assert elastic.search_bodies[0]['query'] == {
        'bool': {
            'filter': [
                {
                    'nested': {
                        'path': 'genres',
                        'query': {'term': {'genres.id': GENRE_ID}},
                    }
                }
            ]
        }
    }


def test_repeated_list_films_uses_cache(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub([film_hit()])
    service = make_film_service(redis, elastic)

    first_result = asyncio.run(service.list_films('-imdb_rating', GENRE_ID, 1, 50))
    cached_result = asyncio.run(service.list_films('-imdb_rating', GENRE_ID, 1, 50))

    assert cached_result == first_result
    assert len(elastic.search_bodies) == 1
    assert redis.set_calls[0][0] == f'film-list:-imdb_rating:{GENRE_ID}:1:50'
    assert redis.set_calls[0][2] == FILM_CACHE_EXPIRE_IN_SECONDS


def test_list_cache_separates_genres(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    first_elastic = ElasticsearchStub([film_hit()])
    second_elastic = ElasticsearchStub([film_hit('524e4331-e14b-24d3-a156-426614174004')])

    first_result = asyncio.run(
        make_film_service(redis, first_elastic).list_films('-imdb_rating', GENRE_ID, 1, 50)
    )
    second_result = asyncio.run(
        make_film_service(redis, second_elastic).list_films(
            '-imdb_rating', '5373d043-3f41-4ea8-9947-4b746c601bbd', 1, 50
        )
    )

    assert first_result[0].id != second_result[0].id
    assert len(second_elastic.search_bodies) == 1


def test_empty_film_list_is_cached(
    make_film_service: MakeFilmService,
) -> None:
    redis = RedisStub()
    elastic = ElasticsearchStub()
    service = make_film_service(redis, elastic)

    first_result = asyncio.run(service.list_films('-imdb_rating', None, 1, 50))
    cached_result = asyncio.run(service.list_films('-imdb_rating', None, 1, 50))

    assert first_result == []
    assert cached_result == []
    assert len(elastic.search_bodies) == 1
