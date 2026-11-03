from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from clients.auth import (
    AuthContractError,
    AuthRateLimitedError,
    AuthTokenRejectedError,
    AuthUnavailableError,
)
from core.config import settings
from dependencies import get_auth_client, get_film_service
from main import app
from tests.conftest import FILM_ID, FilmServiceStub


class AuthClientStub:
    def __init__(
        self,
        result: bool | Exception,
    ) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    async def can_read_film(
        self,
        *,
        creation_date: date | None,
        access_token: str | None,
        request_id: str,
    ) -> bool:
        self.calls.append(
            {
                'creation_date': creation_date,
                'access_token': access_token,
                'request_id': request_id,
            }
        )
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def film(creation_date: date | None) -> SimpleNamespace:
    return SimpleNamespace(
        id=FILM_ID,
        title='Film',
        imdb_rating=8.6,
        description='Description',
        genres=[],
        actors=[],
        writers=[],
        directors=[],
        creation_date=creation_date,
    )


def access_token() -> str:
    now = datetime.now(UTC).replace(microsecond=0)
    return jwt.encode(
        {
            'sub': str(uuid4()),
            'jti': str(uuid4()),
            'type': 'access',
            'iat': now,
            'exp': now + timedelta(minutes=15),
        },
        settings.auth_jwt_secret.get_secret_value(),
        algorithm='HS256',
    )


def wire(
    auth_client: AuthClientStub,
    *,
    creation_date: date | None,
) -> None:
    app.dependency_overrides[get_film_service] = lambda: FilmServiceStub(film=film(creation_date))
    app.dependency_overrides[get_auth_client] = lambda: auth_client


@pytest.mark.parametrize('creation_date', (date(2020, 1, 1), None))
def test_film_details_always_asks_auth_for_found_film(
    client: TestClient,
    creation_date: date | None,
) -> None:
    auth_client = AuthClientStub(True)
    wire(auth_client, creation_date=creation_date)

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 200
    assert auth_client.calls == [
        {
            'creation_date': creation_date,
            'access_token': None,
            'request_id': 'movies-test-request',
        }
    ]


def test_film_details_forwards_verified_access_token(
    client: TestClient,
) -> None:
    auth_client = AuthClientStub(True)
    wire(auth_client, creation_date=None)
    token = access_token()

    response = client.get(
        f'/api/v1/films/{FILM_ID}/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 200
    assert auth_client.calls == [
        {
            'creation_date': None,
            'access_token': token,
            'request_id': 'movies-test-request',
        }
    ]


def test_film_details_generates_request_id_for_direct_call() -> None:
    auth_client = AuthClientStub(True)
    wire(auth_client, creation_date=None)

    with TestClient(app) as direct_client:
        response = direct_client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 200
    generated = auth_client.calls[0]['request_id']
    assert UUID(generated).version == 4


def test_film_details_returns_403_when_auth_denies(
    client: TestClient,
) -> None:
    auth_client = AuthClientStub(False)
    wire(auth_client, creation_date=date(2026, 1, 1))

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 403
    assert response.json() == {'detail': 'Film access denied'}


def test_film_details_returns_401_when_auth_rejects_token(
    client: TestClient,
) -> None:
    auth_client = AuthClientStub(AuthTokenRejectedError())
    wire(auth_client, creation_date=None)

    response = client.get(
        f'/api/v1/films/{FILM_ID}/',
        headers={'Authorization': f'Bearer {access_token()}'},
    )

    assert response.status_code == 401
    assert response.headers['www-authenticate'] == 'Bearer'


@pytest.mark.parametrize(
    ('creation_date', 'expected_status'),
    (
        (date(2000, 1, 1), 200),
        (date(2100, 1, 1), 403),
        (None, 403),
    ),
)
def test_film_details_degrades_when_auth_is_unavailable(
    client: TestClient,
    creation_date: date | None,
    expected_status: int,
) -> None:
    auth_client = AuthClientStub(AuthUnavailableError())
    wire(auth_client, creation_date=creation_date)

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == expected_status


@pytest.mark.parametrize(
    ('creation_date', 'expected_status'),
    (
        (date(2000, 1, 1), 200),
        (date(2100, 1, 1), 403),
        (None, 403),
    ),
)
def test_film_details_uses_age_degradation_when_auth_is_rate_limited(
    client: TestClient,
    creation_date: date | None,
    expected_status: int,
) -> None:
    auth_client = AuthClientStub(AuthRateLimitedError(1))
    wire(auth_client, creation_date=creation_date)

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == expected_status


def test_film_details_closes_access_on_auth_contract_error(
    client: TestClient,
) -> None:
    auth_client = AuthClientStub(AuthContractError())
    wire(auth_client, creation_date=date(2000, 1, 1))

    response = client.get(f'/api/v1/films/{FILM_ID}/')

    assert response.status_code == 403
    assert response.json() == {'detail': 'Film access denied'}


def test_invalid_local_token_stops_before_film_and_auth(
    client: TestClient,
) -> None:
    film_service = FilmServiceStub(film=film(None))
    auth_client = AuthClientStub(True)
    app.dependency_overrides[get_film_service] = lambda: film_service
    app.dependency_overrides[get_auth_client] = lambda: auth_client

    response = client.get(
        f'/api/v1/films/{FILM_ID}/',
        headers={'Authorization': 'Bearer invalid-token'},
    )

    assert response.status_code == 401
    assert film_service.get_calls == []
    assert auth_client.calls == []


def test_unknown_film_does_not_call_auth(client: TestClient) -> None:
    auth_client = AuthClientStub(True)
    app.dependency_overrides[get_film_service] = FilmServiceStub
    app.dependency_overrides[get_auth_client] = lambda: auth_client

    response = client.get('/api/v1/films/00000000-0000-0000-0000-000000000000/')

    assert response.status_code == 404
    assert auth_client.calls == []


def test_list_and_search_do_not_call_auth(client: TestClient) -> None:
    auth_client = AuthClientStub(True)
    app.dependency_overrides[get_film_service] = FilmServiceStub
    app.dependency_overrides[get_auth_client] = lambda: auth_client

    listed = client.get('/api/v1/films')
    searched = client.get('/api/v1/films/search/?query=film')

    assert listed.status_code == searched.status_code == 200
    assert auth_client.calls == []
