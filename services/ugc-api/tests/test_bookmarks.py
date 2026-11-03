from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from tests.conftest import auth_header


def test_bookmark_is_idempotent(client: TestClient, film_id: UUID) -> None:
    user = uuid4()
    url = f'/api/v1/ugc/films/{film_id}/bookmark'

    first = client.put(url, headers=auth_header(user))
    second = client.put(url, headers=auth_header(user))
    listed = client.get('/api/v1/ugc/users/me/bookmarks', headers=auth_header(user)).json()

    assert first.status_code == 200
    assert second.json()['created_at'] == first.json()['created_at']
    assert [item['film_id'] for item in listed['items']] == [str(film_id)]


def test_bookmarks_are_listed_newest_first_with_pagination(client: TestClient) -> None:
    user = uuid4()
    films = [uuid4() for _ in range(3)]
    for film in films:
        client.put(f'/api/v1/ugc/films/{film}/bookmark', headers=auth_header(user))

    page = client.get(
        '/api/v1/ugc/users/me/bookmarks?page_number=1&page_size=2',
        headers=auth_header(user),
    ).json()
    rest = client.get(
        '/api/v1/ugc/users/me/bookmarks?page_number=2&page_size=2',
        headers=auth_header(user),
    ).json()

    assert [item['film_id'] for item in page['items']] == [str(films[2]), str(films[1])]
    assert [item['film_id'] for item in rest['items']] == [str(films[0])]


def test_delete_bookmark(client: TestClient, film_id: UUID) -> None:
    user = uuid4()
    url = f'/api/v1/ugc/films/{film_id}/bookmark'
    client.put(url, headers=auth_header(user))

    assert client.delete(url, headers=auth_header(user)).status_code == 204
    assert client.delete(url, headers=auth_header(user)).status_code == 404


def test_page_size_is_limited(client: TestClient) -> None:
    response = client.get(
        '/api/v1/ugc/users/me/bookmarks?page_size=1000',
        headers=auth_header(),
    )

    assert response.status_code == 422
