import base64
from hashlib import sha256

import pytest

from db.cache.oauth_state import OAuthStateData
from security.passwords import PasswordHasher
from security.tokens import TokenService
from services.authentication import AuthenticationService
from services.errors import (
    InvalidOAuthStateError,
    OAuthStateGenerationError,
)
from services.social_authentication import SocialAuthenticationService
from services.types import ClientInfo, SocialIdentity
from tests.fakes import (
    FakeSocialProviderClient,
    InMemoryLoginHistoryRepository,
    InMemoryOAuthStateStore,
    InMemorySocialAccountRepository,
    InMemoryTokenSessionStore,
    InMemoryUserRepository,
)
from validation.credentials import PasswordPolicy

SECRET = 'test-secret-that-is-long-enough-for-hs256'


def _services(
    events: list[str],
) -> tuple[
    SocialAuthenticationService,
    InMemoryOAuthStateStore,
    FakeSocialProviderClient,
    InMemorySocialAccountRepository,
    InMemoryTokenSessionStore,
]:
    users = InMemoryUserRepository()
    histories = InMemoryLoginHistoryRepository(events=events)
    token_sessions = InMemoryTokenSessionStore(events=events)
    authentication = AuthenticationService(
        users=users,
        login_history=histories,
        token_sessions=token_sessions,
        password_hasher=PasswordHasher(),
        password_policy=PasswordPolicy(min_length=8, min_score=3),
        token_service=TokenService(
            secret_key=SECRET,
            algorithm='HS256',
            access_ttl_seconds=900,
            refresh_ttl_seconds=604800,
        ),
    )
    state_store = InMemoryOAuthStateStore(events=events)
    provider = FakeSocialProviderClient(
        identity=SocialIdentity(
            provider='yandex',
            provider_user_id='123',
            provider_login='alice',
            email='alice@yandex.ru',
            first_name='Alice',
            last_name='Viewer',
        ),
        events=events,
    )
    accounts = InMemorySocialAccountRepository(users, events=events)
    service = SocialAuthenticationService(
        provider_name='yandex',
        state_store=state_store,
        provider_client=provider,
        accounts=accounts,
        authentication=authentication,
        state_ttl_seconds=600,
    )
    return service, state_store, provider, accounts, token_sessions


@pytest.mark.asyncio
async def test_start_stores_pkce_verifier_and_returns_matching_url() -> None:
    """The redirect challenge must be derived from the stored verifier."""
    events: list[str] = []
    service, state_store, provider, _, _ = _services(events)

    url = await service.start(tz='Europe/Moscow')

    assert len(state_store.saved) == 1
    saved = state_store.saved[0]
    assert saved.provider == 'yandex'
    assert saved.state
    assert saved.code_verifier
    assert saved.state != saved.code_verifier
    assert saved.ttl_seconds == 600
    expected_challenge = (
        base64.urlsafe_b64encode(sha256(saved.code_verifier.encode()).digest())
        .rstrip(b'=')
        .decode()
    )
    assert provider.authorization_calls == [(saved.state, expected_challenge)]
    assert url == f'https://provider.example/login?state={saved.state}'


@pytest.mark.asyncio
async def test_callback_consumes_state_before_provider_and_issues_auth_pair() -> None:
    """Callback order must prevent provider calls for replayed state."""
    events: list[str] = []
    service, state_store, _, accounts, token_sessions = _services(events)
    state_store.items[('yandex', 'test-state')] = OAuthStateData(
        code_verifier='test-verifier', tz='Europe/Moscow'
    )

    pair = await service.authenticate(
        code='test-code',
        state='test-state',
        client=ClientInfo('203.0.113.10', 'test-agent'),
    )

    assert pair.access.claims.user_id == accounts.last_user.id
    assert (
        pair.access.claims.session_epoch
        == (token_sessions.epochs[accounts.last_user.id])
    )
    assert pair.refresh.claims.session_epoch == pair.access.claims.session_epoch
    assert events == [
        'consume_state',
        'exchange_code',
        'get_identity',
        'get_or_create_user',
        'get_or_create_epoch',
        'record_login_history',
        'store_refresh',
    ]


@pytest.mark.asyncio
async def test_callback_replay_stops_before_provider() -> None:
    """A consumed state must not exchange the authorization code twice."""
    events: list[str] = []
    service, state_store, provider, _, _ = _services(events)
    state_store.items[('yandex', 'test-state')] = OAuthStateData(
        code_verifier='test-verifier', tz='Europe/Moscow'
    )
    client = ClientInfo(None, None)
    await service.authenticate(
        code='test-code',
        state='test-state',
        client=client,
    )

    with pytest.raises(InvalidOAuthStateError):
        await service.authenticate(
            code='test-code',
            state='test-state',
            client=client,
        )

    assert len(provider.exchange_calls) == 1


@pytest.mark.asyncio
async def test_cancel_consumes_state_without_provider_calls() -> None:
    """Cancelling consent must invalidate state without provider calls."""
    events: list[str] = []
    service, state_store, provider, _, _ = _services(events)
    state_store.items[('yandex', 'test-state')] = OAuthStateData(
        code_verifier='test-verifier', tz='Europe/Moscow'
    )

    await service.cancel(state='test-state')

    assert ('yandex', 'test-state') not in state_store.items
    assert provider.exchange_calls == []
    with pytest.raises(InvalidOAuthStateError):
        await service.cancel(state='test-state')


@pytest.mark.asyncio
async def test_start_stops_after_three_state_collisions() -> None:
    """Repeated SET NX collisions must not create an untracked redirect."""
    events: list[str] = []
    service, state_store, provider, _, _ = _services(events)
    state_store.reject_saves = 3

    with pytest.raises(OAuthStateGenerationError):
        await service.start(tz='Europe/Moscow')

    assert len(state_store.saved) == 3
    assert provider.authorization_calls == []


@pytest.mark.asyncio
async def test_oauth_timezone_is_carried_to_new_user() -> None:
    """The explicit login timezone reaches registration via consumed state."""
    service, store, _, accounts, _ = _services([])
    await service.start(tz='Europe/Moscow')
    await service.authenticate(
        code='code', state=store.saved[0].state, client=ClientInfo(None, None)
    )
    assert accounts.last_user.tz == 'Europe/Moscow'
