from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from tests.conftest import auth_header


def create_review(client: TestClient, film_id: UUID, user: UUID, text: str = 'Хорошо') -> Any:
    response = client.post(
        f'/api/v1/ugc/films/{film_id}/reviews',
        json={'text': text},
        headers=auth_header(user),
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_review_takes_author_rating(client: TestClient, film_id: UUID) -> None:
    author = uuid4()
    client.put(
        f'/api/v1/ugc/films/{film_id}/rating',
        json={'score': 8},
        headers=auth_header(author),
    )

    review = create_review(client, film_id, author)
    client.put(
        f'/api/v1/ugc/films/{film_id}/rating',
        json={'score': 9},
        headers=auth_header(author),
    )

    assert review['score'] == 8
    assert client.get(f'/api/v1/ugc/reviews/{review["id"]}').json()['score'] == 9


def test_second_review_on_same_film_conflicts(client: TestClient, film_id: UUID) -> None:
    author = uuid4()
    create_review(client, film_id, author)

    response = client.post(
        f'/api/v1/ugc/films/{film_id}/reviews',
        json={'text': 'Ещё раз'},
        headers=auth_header(author),
    )

    assert response.status_code == 409


def test_only_author_can_edit_and_delete(client: TestClient, film_id: UUID) -> None:
    author, stranger = uuid4(), uuid4()
    review = create_review(client, film_id, author)
    url = f'/api/v1/ugc/reviews/{review["id"]}'

    assert (
        client.patch(url, json={'text': 'Чужое'}, headers=auth_header(stranger)).status_code == 403
    )
    assert client.delete(url, headers=auth_header(stranger)).status_code == 403

    edited = client.patch(url, json={'text': 'Исправил'}, headers=auth_header(author))
    assert edited.json()['text'] == 'Исправил'

    assert client.delete(url, headers=auth_header(author)).status_code == 204
    assert client.get(url).status_code == 404


def test_votes_update_counters(client: TestClient, film_id: UUID) -> None:
    review = create_review(client, film_id, uuid4())
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    voter, other = uuid4(), uuid4()

    liked = client.put(url, json={'value': 'like'}, headers=auth_header(voter)).json()
    repeated = client.put(url, json={'value': 'like'}, headers=auth_header(voter)).json()
    other_liked = client.put(url, json={'value': 'like'}, headers=auth_header(other)).json()
    switched = client.put(url, json={'value': 'dislike'}, headers=auth_header(voter)).json()
    revoked = client.delete(url, headers=auth_header(voter)).json()

    assert (liked['likes'], liked['dislikes']) == (1, 0)
    assert (repeated['likes'], repeated['dislikes']) == (1, 0)
    assert (other_liked['likes'], other_liked['dislikes']) == (2, 0)
    assert (switched['likes'], switched['dislikes']) == (1, 1)
    assert (revoked['likes'], revoked['dislikes']) == (1, 0)
    assert client.delete(url, headers=auth_header(voter)).status_code == 404


def test_reviews_sorted_by_likes(client: TestClient, film_id: UUID) -> None:
    quiet = create_review(client, film_id, uuid4(), 'Тихая')
    popular = create_review(client, film_id, uuid4(), 'Популярная')
    for _ in range(2):
        client.put(
            f'/api/v1/ugc/reviews/{popular["id"]}/vote',
            json={'value': 'like'},
            headers=auth_header(uuid4()),
        )

    by_likes = client.get(f'/api/v1/ugc/films/{film_id}/reviews?sort=-likes').json()
    by_date = client.get(f'/api/v1/ugc/films/{film_id}/reviews?sort=created_at').json()

    assert [item['id'] for item in by_likes['items']] == [popular['id'], quiet['id']]
    assert [item['id'] for item in by_date['items']] == [quiet['id'], popular['id']]


def test_unknown_review_is_404(client: TestClient) -> None:
    missing = '0123456789abcdef01234567'

    assert client.get(f'/api/v1/ugc/reviews/{missing}').status_code == 404
    assert (
        client.put(
            f'/api/v1/ugc/reviews/{missing}/vote',
            json={'value': 'like'},
            headers=auth_header(),
        ).status_code
        == 404
    )
    assert client.get('/api/v1/ugc/reviews/not-an-id').status_code == 422
