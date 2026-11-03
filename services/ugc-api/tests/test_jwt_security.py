import base64
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from tests.conftest import OTHER_USER_ID, TEST_JWT_SECRET, TEST_USER_ID, make_token

PROTECTED_URL = '/api/v1/ugc/users/me/bookmarks'


def b64url(data: dict[str, object]) -> str:
    raw = json.dumps(data, separators=(',', ':')).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()


def claims(**overrides: object) -> dict[str, object]:
    issued_at = datetime.now(timezone.utc).replace(microsecond=0)
    payload: dict[str, object] = {
        'sub': str(TEST_USER_ID),
        'jti': str(uuid4()),
        'type': 'access',
        'iat': int(issued_at.timestamp()),
        'exp': int((issued_at + timedelta(minutes=15)).timestamp()),
    }
    payload.update(overrides)
    return {key: value for key, value in payload.items() if value is not None}


def get_with_token(client: TestClient, token: str) -> int:
    return client.get(PROTECTED_URL, headers={'Authorization': f'Bearer {token}'}).status_code


def test_valid_token_is_accepted(client: TestClient) -> None:
    assert get_with_token(client, make_token()) == 200


def test_token_signed_with_other_secret_is_rejected(client: TestClient) -> None:
    token = make_token(secret='attacker-secret-that-is-long-enough-for-hs256')

    assert get_with_token(client, token) == 401


def test_payload_swapped_under_valid_signature_is_rejected(client: TestClient) -> None:
    header, _payload, signature = make_token().split('.')
    forged_payload = b64url(claims(sub=str(OTHER_USER_ID)))

    assert get_with_token(client, f'{header}.{forged_payload}.{signature}') == 401


def test_unsigned_alg_none_token_is_rejected(client: TestClient) -> None:
    token = f'{b64url({"alg": "none", "typ": "JWT"})}.{b64url(claims())}.'

    assert get_with_token(client, token) == 401


def test_other_algorithm_is_rejected(client: TestClient) -> None:
    token = jwt.encode(claims(), TEST_JWT_SECRET + TEST_JWT_SECRET, algorithm='HS512')

    assert get_with_token(client, token) == 401


def test_expired_token_is_rejected(client: TestClient) -> None:
    past = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp())
    token = jwt.encode(claims(iat=past - 900, exp=past), TEST_JWT_SECRET, algorithm='HS256')

    assert get_with_token(client, token) == 401


@pytest.mark.parametrize('missing', ['sub', 'jti', 'type', 'iat', 'exp'])
def test_token_without_required_claim_is_rejected(client: TestClient, missing: str) -> None:
    token = jwt.encode(claims(**{missing: None}), TEST_JWT_SECRET, algorithm='HS256')

    assert get_with_token(client, token) == 401


def test_non_uuid_subject_is_rejected(client: TestClient) -> None:
    token = jwt.encode(claims(sub='admin'), TEST_JWT_SECRET, algorithm='HS256')

    assert get_with_token(client, token) == 401


def test_non_bearer_scheme_is_rejected(client: TestClient) -> None:
    response = client.get(PROTECTED_URL, headers={'Authorization': f'Basic {make_token()}'})

    assert response.status_code == 401


def test_user_id_comes_only_from_verified_token(client: TestClient) -> None:
    film_id = uuid4()
    client.put(
        f'/api/v1/ugc/films/{film_id}/bookmark',
        headers={'Authorization': f'Bearer {make_token(user_id=OTHER_USER_ID)}'},
    )

    mine = client.get(PROTECTED_URL, headers={'Authorization': f'Bearer {make_token()}'}).json()

    assert str(film_id) not in {item['film_id'] for item in mine['items']}
