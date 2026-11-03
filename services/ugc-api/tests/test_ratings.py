from uuid import UUID

from fastapi.testclient import TestClient

from tests.conftest import OTHER_USER_ID, auth_header


def test_rating_requires_token(client: TestClient, film_id: UUID) -> None:
    response = client.put(f'/api/v1/ugc/films/{film_id}/rating', json={'score': 10})

    assert response.status_code == 401


def test_set_update_and_delete_rating(client: TestClient, film_id: UUID) -> None:
    url = f'/api/v1/ugc/films/{film_id}/rating'

    created = client.put(url, json={'score': 10}, headers=auth_header())
    updated = client.put(url, json={'score': 7}, headers=auth_header())
    fetched = client.get(url, headers=auth_header())

    assert created.status_code == 200
    assert updated.json()['score'] == 7
    assert updated.json()['created_at'] == created.json()['created_at']
    assert fetched.json()['score'] == 7

    assert client.delete(url, headers=auth_header()).status_code == 204
    assert client.get(url, headers=auth_header()).status_code == 404
    assert client.delete(url, headers=auth_header()).status_code == 404


def test_rating_out_of_range_is_rejected(client: TestClient, film_id: UUID) -> None:
    response = client.put(
        f'/api/v1/ugc/films/{film_id}/rating',
        json={'score': 11},
        headers=auth_header(),
    )

    assert response.status_code == 422


def test_summary_counts_likes_dislikes_and_average(client: TestClient, film_id: UUID) -> None:
    url = f'/api/v1/ugc/films/{film_id}/rating'
    client.put(url, json={'score': 10}, headers=auth_header())
    client.put(url, json={'score': 0}, headers=auth_header(OTHER_USER_ID))

    summary = client.get(f'{url}/summary').json()

    assert summary == {
        'film_id': str(film_id),
        'likes': 1,
        'dislikes': 1,
        'ratings_count': 2,
        'average_score': 5.0,
    }


def test_summary_of_unrated_film(client: TestClient, film_id: UUID) -> None:
    summary = client.get(f'/api/v1/ugc/films/{film_id}/rating/summary').json()

    assert summary['ratings_count'] == 0
    assert summary['average_score'] is None


def test_liked_films_list(client: TestClient, film_id: UUID) -> None:
    user = UUID('33333333-3333-4333-8333-333333333333')
    client.put(f'/api/v1/ugc/films/{film_id}/rating', json={'score': 10}, headers=auth_header(user))

    liked = client.get('/api/v1/ugc/users/me/ratings?score=10', headers=auth_header(user)).json()
    disliked = client.get('/api/v1/ugc/users/me/ratings?score=0', headers=auth_header(user)).json()

    assert [item['film_id'] for item in liked['items']] == [str(film_id)]
    assert disliked['items'] == []
