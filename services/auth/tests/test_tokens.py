from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import jwt
import pytest

from security.tokens import (
    InvalidTokenError,
    TokenExpiredError,
    TokenService,
)

SECRET = 'test-secret-that-is-long-enough-for-hs256'


@pytest.fixture
def token_service() -> TokenService:
    """Создаёт сервис JWT с фиксированными тестовыми настройками."""
    return TokenService(
        secret_key=SECRET,
        algorithm='HS256',
        access_ttl_seconds=900,
        refresh_ttl_seconds=604800,
    )


def test_issued_pair_contains_required_typed_claims(
    token_service: TokenService,
) -> None:
    """Пропуск claim или одинаковый jti для пары должен ломать тест."""
    user_id = uuid4()
    session_epoch = uuid4()

    pair = token_service.issue_pair(user_id, session_epoch)

    access = token_service.decode(pair.access.value, 'access')
    refresh = token_service.decode(pair.refresh.value, 'refresh')
    assert access.user_id == user_id
    assert access.token_type == 'access'
    assert access.session_epoch == session_epoch
    assert refresh.user_id == user_id
    assert refresh.token_type == 'refresh'
    assert refresh.session_epoch == session_epoch
    assert access.token_id != refresh.token_id
    assert access.expires_at - access.issued_at == timedelta(seconds=900)
    assert refresh.expires_at - refresh.issued_at == timedelta(seconds=604800)


def test_token_type_cannot_be_substituted(
    token_service: TokenService,
) -> None:
    """Refresh нельзя использовать в роли access и наоборот."""
    pair = token_service.issue_pair(uuid4(), uuid4())

    with pytest.raises(InvalidTokenError):
        token_service.decode(pair.refresh.value, 'access')


def test_token_with_another_signature_is_invalid(
    token_service: TokenService,
) -> None:
    """Изменение проверки подписи должно обнаруживаться."""
    foreign_service = TokenService(
        secret_key='another-test-secret-that-is-long-enough',
        algorithm='HS256',
        access_ttl_seconds=900,
        refresh_ttl_seconds=604800,
    )
    token = foreign_service.issue_pair(uuid4(), uuid4()).access.value

    with pytest.raises(InvalidTokenError):
        token_service.decode(token, 'access')


def test_malformed_token_is_invalid(token_service: TokenService) -> None:
    """Best-effort parsing повреждённого JWT запрещён."""
    with pytest.raises(InvalidTokenError):
        token_service.decode('not-a-jwt', 'access')


def test_expired_token_has_distinct_error(token_service: TokenService) -> None:
    """Истёкший JWT должен отличаться от неверной подписи."""
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            'sub': str(uuid4()),
            'jti': str(uuid4()),
            'type': 'access',
            'session_epoch': str(uuid4()),
            'iat': now - timedelta(minutes=2),
            'exp': now - timedelta(minutes=1),
        },
        SECRET,
        algorithm='HS256',
    )

    with pytest.raises(TokenExpiredError):
        token_service.decode(token, 'access')


@pytest.mark.parametrize(
    ('payload', 'remove_required_times'),
    [
        (
            {'sub': str(uuid4()), 'jti': str(uuid4()), 'type': 'access'},
            True,
        ),
        (
            {
                'sub': 'not-a-uuid',
                'jti': str(uuid4()),
                'type': 'access',
            },
            False,
        ),
        (
            {
                'sub': str(uuid4()),
                'jti': 'not-a-uuid',
                'type': 'access',
            },
            False,
        ),
    ],
)
def test_missing_or_invalid_claims_are_rejected(
    token_service: TokenService,
    payload: dict[str, str],
    remove_required_times: bool,
) -> None:
    """Неполный или неверно типизированный principal не создаётся."""
    now = datetime.now(UTC)
    complete_payload: dict[str, str | datetime] = {
        'sub': str(UUID(int=0)),
        'jti': str(UUID(int=0)),
        'type': 'access',
        'session_epoch': str(UUID(int=0)),
        'iat': now,
        'exp': now + timedelta(minutes=5),
    }
    complete_payload.update(payload)
    if remove_required_times:
        complete_payload.pop('iat')
        complete_payload.pop('exp')
    token = jwt.encode(complete_payload, SECRET, algorithm='HS256')

    with pytest.raises(InvalidTokenError):
        token_service.decode(token, 'access')


@pytest.mark.parametrize('session_epoch', [None, 'not-a-uuid'])
def test_missing_or_invalid_session_epoch_is_rejected(
    token_service: TokenService,
    session_epoch: str | None,
) -> None:
    """Токен без подписанного UUID epoch не создаёт principal."""
    now = datetime.now(UTC)
    payload: dict[str, str | datetime] = {
        'sub': str(uuid4()),
        'jti': str(uuid4()),
        'type': 'access',
        'iat': now,
        'exp': now + timedelta(minutes=5),
    }
    if session_epoch is not None:
        payload['session_epoch'] = session_epoch
    token = jwt.encode(payload, SECRET, algorithm='HS256')

    with pytest.raises(InvalidTokenError):
        token_service.decode(token, 'access')
