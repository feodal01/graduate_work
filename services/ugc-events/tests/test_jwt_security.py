from datetime import UTC, datetime, timedelta
from typing import Optional
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from security.jwt import VerifiedAccessToken, required_jwt_bearer

SECRET = 'test-secret-that-is-long-enough-for-hs256'
OTHER_SECRET = 'other-secret-that-is-long-enough-for-hs256'
USER_ID = UUID('11111111-1111-4111-8111-111111111111')


def make_token(
    *,
    secret: str = SECRET,
    token_type: str = 'access',
    issued_at: Optional[datetime] = None,
    expires_at: Optional[datetime] = None,
    user_id: str = str(USER_ID),
    token_id: Optional[str] = None,
    omit: Optional[str] = None,
) -> str:
    issued_at = issued_at or datetime.now(UTC).replace(microsecond=0)
    expires_at = expires_at or issued_at + timedelta(minutes=15)
    payload = {
        'sub': user_id,
        'jti': token_id or str(uuid4()),
        'type': token_type,
        'session_epoch': str(uuid4()),
        'iat': issued_at,
        'exp': expires_at,
    }
    if omit is not None:
        payload.pop(omit)
    return jwt.encode(payload, secret, algorithm='HS256')


@pytest.fixture
def security_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    from security import jwt as jwt_security

    monkeypatch.setattr(
        jwt_security.settings,
        'auth_jwt_secret',
        SecretStr(SECRET),
    )
    monkeypatch.setattr(jwt_security.settings, 'auth_jwt_algorithm', 'HS256')
    test_app = FastAPI()

    @test_app.get('/token')
    async def token_route(
        token: VerifiedAccessToken = Depends(required_jwt_bearer),
    ) -> dict[str, str]:
        return {'user_id': str(token.user_id)}

    return TestClient(test_app)


def test_required_bearer_rejects_missing_header(
    security_client: TestClient,
) -> None:
    response = security_client.get('/token')

    assert response.status_code == 401
    assert response.json() == {'detail': 'Not authenticated'}
    assert response.headers['www-authenticate'] == 'Bearer'


def test_required_bearer_returns_typed_verified_access_token(
    security_client: TestClient,
) -> None:
    response = security_client.get(
        '/token',
        headers={'Authorization': f'Bearer {make_token()}'},
    )

    assert response.status_code == 200
    assert response.json() == {'user_id': str(USER_ID)}


@pytest.mark.parametrize(
    'token',
    (
        make_token(token_type='refresh'),
        make_token(secret=OTHER_SECRET),
        make_token(expires_at=datetime.now(UTC) - timedelta(seconds=1)),
        make_token(user_id='not-a-uuid'),
        make_token(omit='sub'),
        make_token(omit='jti'),
        make_token(omit='type'),
        make_token(omit='iat'),
        make_token(omit='exp'),
    ),
)
def test_required_bearer_rejects_invalid_access_token(
    security_client: TestClient,
    token: str,
) -> None:
    response = security_client.get(
        '/token',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid or expired token'}
    assert response.headers['www-authenticate'] == 'Bearer'
