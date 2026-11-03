from fastapi.testclient import TestClient

from tests.conftest import make_token


def test_health_ok(client: TestClient) -> None:
    response = client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {'status': 'ok', 'mongo': 'ok'}


def test_refresh_token_is_rejected(client: TestClient) -> None:
    response = client.get(
        '/api/v1/ugc/users/me/bookmarks',
        headers={'Authorization': f'Bearer {make_token(token_type="refresh")}'},
    )

    assert response.status_code == 401


def test_docs_are_published(client: TestClient) -> None:
    assert client.get('/ugc-api/openapi.json').status_code == 200
