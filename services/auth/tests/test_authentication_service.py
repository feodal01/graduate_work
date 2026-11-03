import asyncio
import logging
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest

from db.cache.errors import CacheUnavailableError
from db.sql.errors import DatabaseUnavailableError
from repositories.errors import LoginAlreadyTakenError
from security.passwords import PasswordHasher
from security.tokens import TokenService
from services.authentication import AuthenticationService
from services.errors import (
    AccessTokenExpiredError,
    AccessTokenRevokedError,
    InvalidAccessTokenError,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)
from services.types import ClientInfo, CurrentUser
from tests.fakes import (
    InMemoryLoginHistoryRepository,
    InMemoryTokenSessionStore,
    InMemoryUserRepository,
)
from validation.credentials import CredentialValidationError, PasswordPolicy

SECRET = 'test-secret-that-is-long-enough-for-hs256'
UNKNOWN_CLIENT = ClientInfo(None, None)


@pytest.fixture
def users() -> InMemoryUserRepository:
    """Создаёт изолированный репозиторий пользователей."""
    return InMemoryUserRepository()


@pytest.fixture
def histories() -> InMemoryLoginHistoryRepository:
    """Создаёт изолированный репозиторий истории входов."""
    return InMemoryLoginHistoryRepository()


@pytest.fixture
def token_sessions() -> InMemoryTokenSessionStore:
    """Создаёт изолированное хранилище токенов и сессий."""
    return InMemoryTokenSessionStore()


@pytest.fixture
def password_hasher() -> PasswordHasher:
    """Создаёт хешер паролей для тестовых пользователей."""
    return PasswordHasher()


@pytest.fixture
def password_policy() -> PasswordPolicy:
    """Создаёт политику паролей с тестовыми ограничениями."""
    return PasswordPolicy(min_length=8, min_score=3)


@pytest.fixture
def token_service() -> TokenService:
    """Создаёт сервис JWT с фиксированными тестовыми настройками."""
    return TokenService(
        secret_key=SECRET,
        algorithm='HS256',
        access_ttl_seconds=900,
        refresh_ttl_seconds=604800,
    )


@pytest.fixture
def service(
    users: InMemoryUserRepository,
    histories: InMemoryLoginHistoryRepository,
    token_sessions: InMemoryTokenSessionStore,
    password_hasher: PasswordHasher,
    password_policy: PasswordPolicy,
    token_service: TokenService,
) -> AuthenticationService:
    """Собирает сервис аутентификации из тестовых зависимостей."""
    return AuthenticationService(
        users=users,
        login_history=histories,
        token_sessions=token_sessions,
        password_hasher=password_hasher,
        password_policy=password_policy,
        token_service=token_service,
    )


@pytest.mark.asyncio
async def test_signup_hashes_password_and_preserves_case(
    service: AuthenticationService,
    password_hasher: PasswordHasher,
) -> None:
    """Signup не должен хранить plaintext или lower-case логин."""
    user = await service.signup(
        login='  Alice  ',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    assert user.login == 'Alice'
    assert user.password_hash != 'correct horse'
    assert user.password_hash is not None
    assert password_hasher.verify('correct horse', user.password_hash)


@pytest.mark.asyncio
async def test_signup_rejects_duplicate_login(service: AuthenticationService) -> None:
    """Duplicate signup должен давать доменный conflict."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    with pytest.raises(LoginAlreadyTakenError):
        await service.signup(
            login='Alice',
            password='another horse',
            first_name=None,
            last_name=None,
            tz='Europe/Moscow',
        )


@pytest.mark.asyncio
async def test_signup_rejects_password_matching_login(
    service: AuthenticationService,
) -> None:
    """Разница только в регистре не должна позволять использовать login как пароль."""
    with pytest.raises(
        CredentialValidationError,
        match='password must not match login',
    ):
        await service.signup(
            login='SamePassword',
            password='samepassword',
            first_name=None,
            last_name=None,
            tz='Europe/Moscow',
        )


@pytest.mark.asyncio
async def test_login_records_history_and_stores_refresh(
    service: AuthenticationService,
    histories: InMemoryLoginHistoryRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Успешный login обязан выполнить обе persistence side effects."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    client = ClientInfo(
        ip_address='203.0.113.10',
        user_agent='test-agent',
    )

    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=client,
    )

    assert pair.access.claims.user_id == user.id
    assert pair.refresh.claims.user_id == user.id
    assert pair.access.claims.session_epoch == pair.refresh.claims.session_epoch
    assert pair.access.claims.session_epoch == token_sessions.epochs[user.id]
    assert token_sessions.refresh[pair.refresh.claims.token_id] == user.id
    assert token_sessions.refresh_ttls[pair.refresh.claims.token_id] > 0
    assert len(histories.items) == 1
    assert histories.items[0].user_id == user.id
    assert histories.items[0].ip_address == '203.0.113.10'
    assert histories.items[0].user_agent == 'test-agent'


@pytest.mark.asyncio
async def test_login_rejects_pair_when_epoch_changes_before_refresh_store(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """JWT нельзя вернуть, если Redis уже начал смену session state."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    token_sessions.reject_store = True

    with pytest.raises(CacheUnavailableError):
        await service.login(
            login='Alice',
            password='correct horse',
            client=UNKNOWN_CLIENT,
        )


@pytest.mark.asyncio
async def test_unknown_login_and_wrong_password_share_error(
    service: AuthenticationService,
) -> None:
    """API не должен раскрывать, существует ли login."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    client = ClientInfo(ip_address=None, user_agent=None)

    with pytest.raises(InvalidCredentialsError) as unknown:
        await service.login(
            login='Unknown',
            password='correct horse',
            client=client,
        )
    with pytest.raises(InvalidCredentialsError) as wrong:
        await service.login(
            login='Alice',
            password='wrong horse',
            client=client,
        )

    assert str(unknown.value) == str(wrong.value) == 'Invalid credentials'


@pytest.mark.asyncio
async def test_social_only_user_can_add_password_then_use_local_login(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    histories: InMemoryLoginHistoryRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """A NULL password must reject login until profile sets a real password."""
    user = await users.create(
        login='yandex_123',
        password_hash=None,
        first_name='Alice',
        last_name=None,
        tz='Europe/Moscow',
    )
    client = ClientInfo(ip_address='203.0.113.10', user_agent='test-agent')

    with pytest.raises(InvalidCredentialsError) as missing_password:
        await service.login(
            login='yandex_123',
            password='correct horse',
            client=client,
        )

    current = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=await token_sessions.get_or_create_epoch(user.id),
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    await service.update_profile(
        current,
        login=None,
        password='new correct horse',
    )
    pair = await service.login(
        login='yandex_123',
        password='new correct horse',
        client=client,
    )

    assert str(missing_password.value) == 'Invalid credentials'
    assert pair.access.claims.user_id == user.id
    assert len(histories.items) == 1


@pytest.mark.asyncio
async def test_refresh_rotates_pair_and_old_token_becomes_invalid(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Пропуск удаления старого refresh должен ломать тест."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    original = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )

    rotated = await service.rotate_refresh(
        service.verify_refresh_token(original.refresh.value)
    )

    assert rotated.refresh.claims.user_id == user.id
    assert rotated.refresh.claims.token_id != original.refresh.claims.token_id
    assert original.refresh.claims.token_id not in token_sessions.refresh
    assert token_sessions.refresh[rotated.refresh.claims.token_id] == user.id
    with pytest.raises(InvalidRefreshTokenError):
        await service.rotate_refresh(
            service.verify_refresh_token(original.refresh.value)
        )


@pytest.mark.asyncio
async def test_concurrent_refresh_has_exactly_one_success(
    service: AuthenticationService,
) -> None:
    """Неатомарное consume допустит две новые пары."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    original = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )

    claims = service.verify_refresh_token(original.refresh.value)
    results = await asyncio.gather(
        service.rotate_refresh(claims),
        service.rotate_refresh(claims),
        return_exceptions=True,
    )

    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, InvalidRefreshTokenError) for result in results) == 1


@pytest.mark.asyncio
async def test_refresh_rejects_wrong_type_unknown_and_mismatched_session(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """JWT подписи недостаточно без type и привязки Redis к user."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )

    with pytest.raises(InvalidRefreshTokenError):
        service.verify_refresh_token(pair.access.value)
    with pytest.raises(InvalidRefreshTokenError):
        service.verify_refresh_token('not-a-jwt')

    token_sessions.refresh[pair.refresh.claims.token_id] = uuid4()
    with pytest.raises(InvalidRefreshTokenError):
        await service.rotate_refresh(service.verify_refresh_token(pair.refresh.value))


@pytest.mark.asyncio
async def test_refresh_rejects_token_from_previous_epoch(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Украденный refresh не переживает смену epoch пользователя."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    token_sessions.epochs[user.id] = uuid4()

    with pytest.raises(InvalidRefreshTokenError):
        await service.rotate_refresh(service.verify_refresh_token(pair.refresh.value))


@pytest.mark.asyncio
async def test_authenticate_access_returns_typed_current_user(
    service: AuthenticationService,
) -> None:
    """Future RBAC boundary должен получать user, jti и exp."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )

    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    assert current.user_id == user.id
    assert current.token_id == pair.access.claims.token_id
    assert current.expires_at == pair.access.claims.expires_at


@pytest.mark.asyncio
async def test_authenticate_access_rejects_token_from_previous_epoch(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Старый access не создаёт CurrentUser после смены epoch."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    token_sessions.epochs[user.id] = uuid4()

    with pytest.raises(AccessTokenRevokedError):
        await service.authenticate_access(
            pair.access.value,
            client=UNKNOWN_CLIENT,
        )


@pytest.mark.asyncio
async def test_authenticate_access_preserves_error_categories(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Expired, invalid и revoked access не должны сливаться в одну ветку."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    token_sessions.revoked_access[pair.access.claims.token_id] = 30

    with pytest.raises(AccessTokenRevokedError):
        await service.authenticate_access(
            pair.access.value,
            client=UNKNOWN_CLIENT,
        )
    with pytest.raises(InvalidAccessTokenError):
        await service.authenticate_access(
            'not-a-jwt',
            client=UNKNOWN_CLIENT,
        )

    now = datetime.now(UTC)
    expired = jwt.encode(
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
    with pytest.raises(AccessTokenExpiredError):
        await service.authenticate_access(
            expired,
            client=UNKNOWN_CLIENT,
        )


@pytest.mark.asyncio
async def test_revoked_access_attempt_is_logged_without_raw_token(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Попытка с отозванным токеном логируется без самого JWT."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    token_sessions.revoked_access[pair.access.claims.token_id] = 30
    client = ClientInfo('203.0.113.10', None)

    with caplog.at_level(logging.WARNING, logger='services.authentication'):
        with pytest.raises(AccessTokenRevokedError):
            await service.authenticate_access(
                pair.access.value,
                client=client,
            )

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    assert str(pair.access.claims.user_id) in message
    assert str(pair.access.claims.token_id) in message
    assert '203.0.113.10' in message
    assert pair.access.value not in message


@pytest.mark.asyncio
async def test_profile_update_changes_login_and_password(
    service: AuthenticationService,
    password_hasher: PasswordHasher,
) -> None:
    """PATCH обязан применить оба переданных поля и перехешировать пароль."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    updated = await service.update_profile(
        current,
        login='AliceUpdated',
        password='new correct horse',
    )

    assert updated.id == user.id
    assert updated.login == 'AliceUpdated'
    assert updated.password_hash != 'new correct horse'
    assert updated.password_hash is not None
    assert password_hasher.verify('new correct horse', updated.password_hash)
    with pytest.raises(InvalidCredentialsError):
        await service.login(
            login='Alice',
            password='correct horse',
            client=ClientInfo(None, None),
        )
    await service.login(
        login='AliceUpdated',
        password='new correct horse',
        client=ClientInfo(None, None),
    )


@pytest.mark.asyncio
async def test_password_update_revokes_all_existing_pairs(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Смена пароля отзывает access и refresh на всех устройствах."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    first_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    second_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        first_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    old_epoch = token_sessions.epochs[user.id]

    await service.update_profile(
        current,
        login=None,
        password='new correct horse',
    )

    assert token_sessions.epochs[user.id] != old_epoch
    assert user.id not in token_sessions.markers
    for pair in (first_pair, second_pair):
        with pytest.raises(AccessTokenRevokedError):
            await service.authenticate_access(
                pair.access.value,
                client=UNKNOWN_CLIENT,
            )
        with pytest.raises(InvalidRefreshTokenError):
            await service.rotate_refresh(
                service.verify_refresh_token(pair.refresh.value)
            )


@pytest.mark.asyncio
async def test_stale_authenticated_password_update_is_rejected_before_database(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """A PATCH that authenticated before another password change loses CAS."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    first_request = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    stale_request = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    await service.update_profile(
        first_request,
        login=None,
        password='first new correct horse',
    )
    completed_updates = users.update_calls
    current_epoch = token_sessions.epochs[first_request.user_id]

    with pytest.raises(AccessTokenRevokedError):
        await service.update_profile(
            stale_request,
            login=None,
            password='second new correct horse',
        )

    assert users.update_calls == completed_updates
    assert token_sessions.epochs[first_request.user_id] == current_epoch
    assert first_request.user_id not in token_sessions.markers


@pytest.mark.asyncio
async def test_login_only_update_preserves_existing_pairs(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Смена логина не является security-событием для других сессий."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    first_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    second_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        first_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    old_epoch = token_sessions.epochs[user.id]

    await service.update_profile(
        current,
        login='AliceUpdated',
        password=None,
    )

    assert token_sessions.epochs[user.id] == old_epoch
    for pair in (first_pair, second_pair):
        authenticated = await service.authenticate_access(
            pair.access.value,
            client=UNKNOWN_CLIENT,
        )
        rotated = await service.rotate_refresh(
            service.verify_refresh_token(pair.refresh.value)
        )
        assert authenticated.user_id == user.id
        assert rotated.refresh.claims.session_epoch == old_epoch


@pytest.mark.asyncio
async def test_password_update_stops_before_database_when_redis_fails(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Без смены epoch password hash нельзя сохранять в PostgreSQL."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    old_hash = user.password_hash
    old_epoch = token_sessions.epochs[user.id]
    token_sessions.fail_password_change = True

    with pytest.raises(CacheUnavailableError):
        await service.update_profile(
            current,
            login=None,
            password='new correct horse',
        )

    assert users.update_calls == 0
    assert user.password_hash == old_hash
    assert token_sessions.epochs[user.id] == old_epoch


@pytest.mark.asyncio
async def test_password_update_reports_finish_failure_after_database_success(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
    password_hasher: PasswordHasher,
) -> None:
    """A committed password with unfinished Redis cleanup returns 503."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    token_sessions.fail_password_change_finish = True

    with pytest.raises(CacheUnavailableError):
        await service.update_profile(
            current,
            login=None,
            password='new correct horse',
        )

    assert users.update_calls == 1
    assert user.password_hash is not None
    assert password_hasher.verify('new correct horse', user.password_hash)
    assert user.id in token_sessions.markers


@pytest.mark.asyncio
async def test_database_error_wins_over_password_change_finish_failure(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """A cleanup outage cannot hide the failed PostgreSQL update."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    old_epoch = token_sessions.epochs[user.id]
    users.fail_update = True
    token_sessions.fail_password_change_finish = True

    with pytest.raises(DatabaseUnavailableError):
        await service.update_profile(
            current,
            login=None,
            password='new correct horse',
        )

    assert token_sessions.epochs[user.id] != old_epoch
    assert user.id in token_sessions.markers


@pytest.mark.asyncio
async def test_database_failure_keeps_sessions_revoked_and_old_password(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Rollback PostgreSQL не должен откатывать уже опубликованный epoch."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    first_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    second_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    current = await service.authenticate_access(
        first_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    old_epoch = token_sessions.epochs[user.id]
    users.fail_update = True

    with pytest.raises(DatabaseUnavailableError):
        await service.update_profile(
            current,
            login=None,
            password='new correct horse',
        )

    assert token_sessions.epochs[user.id] != old_epoch
    assert user.id not in token_sessions.markers
    for pair in (first_pair, second_pair):
        with pytest.raises(AccessTokenRevokedError):
            await service.authenticate_access(
                pair.access.value,
                client=UNKNOWN_CLIENT,
            )
        with pytest.raises(InvalidRefreshTokenError):
            await service.rotate_refresh(
                service.verify_refresh_token(pair.refresh.value)
            )

    users.fail_update = False
    recovered = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    assert recovered.access.claims.session_epoch == token_sessions.epochs[user.id]
    with pytest.raises(InvalidCredentialsError):
        await service.login(
            login='Alice',
            password='new correct horse',
            client=UNKNOWN_CLIENT,
        )


@pytest.mark.asyncio
async def test_get_profile_returns_current_user(
    service: AuthenticationService,
) -> None:
    """Чтение профиля должно загрузить пользователя по ID из access-токена."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name='Alice',
        last_name='Example',
        tz='Europe/Moscow',
    )
    current_user = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    profile = await service.get_profile(current_user)

    assert profile is user


@pytest.mark.asyncio
async def test_get_profile_rejects_deleted_user(
    service: AuthenticationService,
) -> None:
    """Токен не должен подтверждать пользователя, которого больше нет в Auth."""
    current_user = CurrentUser(
        user_id=uuid4(),
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    with pytest.raises(InvalidAccessTokenError):
        await service.get_profile(current_user)


@pytest.mark.asyncio
async def test_profile_login_conflict_preserves_original_data(
    service: AuthenticationService,
) -> None:
    """Failed UPDATE не должен частично менять профиль."""
    alice = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    await service.signup(
        login='Bob',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    with pytest.raises(LoginAlreadyTakenError):
        await service.update_profile(current, login='Bob', password=None)

    assert alice.login == 'Alice'


@pytest.mark.asyncio
async def test_empty_profile_update_is_rejected(service: AuthenticationService) -> None:
    """Пустой PATCH не должен превращаться в успешный no-op."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    with pytest.raises(ValueError, match='At least one profile field'):
        await service.update_profile(current, login=None, password=None)


@pytest.mark.asyncio
async def test_login_history_is_scoped_to_current_user(
    service: AuthenticationService,
) -> None:
    """GET history не должен вернуть входы другого пользователя."""
    alice = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    await service.signup(
        login='Bob',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    old_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo('203.0.113.1', 'alice-old'),
    )
    await service.login(
        login='Bob',
        password='correct horse',
        client=ClientInfo('203.0.113.2', 'bob'),
    )
    current_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo('203.0.113.3', 'alice-new'),
    )
    current = await service.authenticate_access(
        current_pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    result = await service.get_login_history(current)

    assert current.user_id == alice.id
    assert [item.user_agent for item in result] == ['alice-new', 'alice-old']
    assert old_pair.access.claims.user_id == current.user_id


@pytest.mark.asyncio
async def test_logout_revokes_access_and_refresh(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """204 logout side effects должны сделать оба токена непригодными."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    current = await service.authenticate_access(
        pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    await service.logout(current, pair.refresh.value)

    assert pair.refresh.claims.token_id not in token_sessions.refresh
    assert 1 <= token_sessions.revoked_access[current.token_id] <= 900
    with pytest.raises(AccessTokenRevokedError):
        await service.authenticate_access(
            pair.access.value,
            client=UNKNOWN_CLIENT,
        )
    with pytest.raises(InvalidRefreshTokenError):
        await service.rotate_refresh(service.verify_refresh_token(pair.refresh.value))


@pytest.mark.asyncio
async def test_logout_preserves_second_session_epoch(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Обычный logout не должен превращаться в logout-all."""
    user = await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    first_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    second_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=UNKNOWN_CLIENT,
    )
    epoch_before = first_pair.access.claims.session_epoch
    first_current = await service.authenticate_access(
        first_pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    await service.logout(first_current, first_pair.refresh.value)

    with pytest.raises(AccessTokenRevokedError):
        await service.authenticate_access(
            first_pair.access.value,
            client=UNKNOWN_CLIENT,
        )
    with pytest.raises(InvalidRefreshTokenError):
        await service.rotate_refresh(
            service.verify_refresh_token(first_pair.refresh.value)
        )

    second_current = await service.authenticate_access(
        second_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    second_rotated = await service.rotate_refresh(
        service.verify_refresh_token(second_pair.refresh.value)
    )

    assert second_current.user_id == user.id
    assert second_rotated.refresh.claims.session_epoch == epoch_before
    assert token_sessions.epochs[user.id] == epoch_before


@pytest.mark.asyncio
async def test_logout_with_invalid_or_foreign_refresh_only_revokes_caller_access(
    service: AuthenticationService,
    token_sessions: InMemoryTokenSessionStore,
) -> None:
    """Logout не должен удалить refresh другой user-сессии."""
    await service.signup(
        login='Alice',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    await service.signup(
        login='Bob',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    alice_pair = await service.login(
        login='Alice',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    bob_pair = await service.login(
        login='Bob',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    alice = await service.authenticate_access(
        alice_pair.access.value,
        client=UNKNOWN_CLIENT,
    )

    await service.logout(alice, bob_pair.refresh.value)

    assert alice.token_id in token_sessions.revoked_access
    assert bob_pair.refresh.claims.token_id in token_sessions.refresh

    second_pair = await service.login(
        login='Bob',
        password='correct horse',
        client=ClientInfo(None, None),
    )
    bob = await service.authenticate_access(
        second_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
    await service.logout(bob, 'not-a-jwt')
    assert bob.token_id in token_sessions.revoked_access


@pytest.mark.parametrize('failure', ['cache_begin', 'database', 'cache_finish'])
async def test_delete_profile_failure_semantics(
    service: AuthenticationService,
    users: InMemoryUserRepository,
    token_sessions: InMemoryTokenSessionStore,
    failure: str,
) -> None:
    """Cache failures fail closed; SQL failure preserves the user after revocation."""
    user = await service.signup(
        login='DeleteMe',
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    pair = await service.login(
        login=user.login, password='correct horse', client=UNKNOWN_CLIENT
    )
    current = await service.authenticate_access(
        pair.access.value, client=UNKNOWN_CLIENT
    )
    token_sessions.fail_password_change = failure == 'cache_begin'
    token_sessions.fail_password_change_finish = failure == 'cache_finish'
    users.fail_update = failure == 'database'
    expected = (
        DatabaseUnavailableError if failure == 'database' else CacheUnavailableError
    )
    with pytest.raises(expected):
        await service.delete_profile(current)
    assert (user.id in users.items) == (failure != 'cache_finish')
    if failure != 'cache_begin':
        with pytest.raises(AccessTokenRevokedError):
            await service.authenticate_access(pair.access.value, client=UNKNOWN_CLIENT)
