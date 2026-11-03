from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from dependencies import get_social_authentication_service
from main import create_app
from security.tokens import IssuedToken, IssuedTokenPair, TokenClaims
from services.types import ClientInfo


def _token_pair() -> IssuedTokenPair:
    issued_at = datetime.now(UTC)
    user_id = uuid4()
    session_epoch = uuid4()
    return IssuedTokenPair(
        access=IssuedToken(
            value='access-value',
            claims=TokenClaims(
                user_id=user_id,
                token_id=uuid4(),
                token_type='access',
                session_epoch=session_epoch,
                issued_at=issued_at,
                expires_at=issued_at + timedelta(minutes=15),
            ),
        ),
        refresh=IssuedToken(
            value='refresh-value',
            claims=TokenClaims(
                user_id=user_id,
                token_id=uuid4(),
                token_type='refresh',
                session_epoch=session_epoch,
                issued_at=issued_at,
                expires_at=issued_at + timedelta(days=7),
            ),
        ),
    )


class StubSocialAuthenticationService:
    def __init__(
        self,
        authorization_url: str = ('https://oauth.yandex.ru/authorize?state=test-state'),
    ) -> None:
        self.pair = _token_pair()
        self.authorization_url = authorization_url
        self.authenticate_calls: list[tuple[str, str, ClientInfo]] = []
        self.cancel_calls: list[str] = []

    async def start(self, *, tz: str) -> str:
        """Return the provider redirect used by the route."""
        return self.authorization_url

    async def authenticate(
        self,
        *,
        code: str,
        state: str,
        client: ClientInfo,
    ) -> IssuedTokenPair:
        """Record callback inputs and return a stable token pair."""
        self.authenticate_calls.append((code, state, client))
        return self.pair

    async def cancel(self, *, state: str) -> None:
        """Record provider rejection."""
        self.cancel_calls.append(state)


@pytest.fixture
def social_service() -> StubSocialAuthenticationService:
    """Create a deterministic social workflow for HTTP boundary tests."""
    return StubSocialAuthenticationService()


@pytest.fixture
async def client(
    social_service: StubSocialAuthenticationService,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Create the Auth API with only the social workflow replaced."""
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_social_authentication_service] = lambda: social_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'social-auth-test'},
    ) as test_client:
        yield test_client


@pytest.mark.asyncio
async def test_login_start_redirects_to_provider_without_following_it(
    client: httpx.AsyncClient,
) -> None:
    """The Auth route must return the provider URL as an HTTP redirect."""
    response = await client.get(
        '/api/v1/auth/social/yandex/login?tz=Europe/Moscow',
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert response.headers['Location'] == (
        'https://oauth.yandex.ru/authorize?state=test-state'
    )


@pytest.mark.asyncio
async def test_registered_provider_uses_the_shared_login_route() -> None:
    """A registered provider must not require a provider-specific endpoint."""
    service = StubSocialAuthenticationService(
        'https://accounts.google.com/o/oauth2/auth?state=test-state'
    )
    resolved_provider_names: list[str] = []

    async def resolve_service(
        provider_name: str,
    ) -> StubSocialAuthenticationService:
        resolved_provider_names.append(provider_name)
        return service

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_social_authentication_service] = resolve_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'social-auth-test'},
    ) as test_client:
        response = await test_client.get(
            '/api/v1/auth/social/google/login?tz=Europe/Moscow',
            follow_redirects=False,
        )

    assert response.status_code == 307
    assert response.headers['Location'] == (
        'https://accounts.google.com/o/oauth2/auth?state=test-state'
    )
    assert resolved_provider_names == ['google']


@pytest.mark.asyncio
async def test_unknown_provider_is_rejected_before_service_construction() -> None:
    """An unregistered provider must fail with the public HTTP 404 contract."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'social-auth-test'},
    ) as test_client:
        response = await test_client.get(
            '/api/v1/auth/social/unknown/login?tz=Europe/Moscow',
            follow_redirects=False,
        )

    assert response.status_code == 404
    assert response.json() == {'detail': "Social provider 'unknown' is not supported"}


@pytest.mark.asyncio
async def test_callback_returns_the_existing_auth_token_contract(
    client: httpx.AsyncClient,
    social_service: StubSocialAuthenticationService,
) -> None:
    """Social login must produce the same public token pair as local login."""
    response = await client.get(
        '/api/v1/auth/social/yandex/callback',
        params={'code': 'auth-code', 'state': 'oauth-state'},
        headers={'user-agent': 'social-test-agent'},
    )

    assert response.status_code == 200
    assert response.json() == {
        'access_token': 'access-value',
        'refresh_token': 'refresh-value',
        'token_type': 'bearer',
    }
    code, state, client_info = social_service.authenticate_calls[0]
    assert code == 'auth-code'
    assert state == 'oauth-state'
    assert client_info.user_agent == 'social-test-agent'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'params',
    [
        {},
        {'code': 'auth-code'},
        {'error': 'access_denied'},
        {'state': 'oauth-state'},
        {
            'code': 'auth-code',
            'error': 'access_denied',
            'state': 'oauth-state',
        },
    ],
)
async def test_callback_rejects_invalid_parameter_combinations(
    client: httpx.AsyncClient,
    params: dict[str, str],
) -> None:
    """Invalid callback shapes must return HTTP 400 instead of HTTP 422."""
    response = await client.get(
        '/api/v1/auth/social/yandex/callback',
        params=params,
    )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_callback_does_not_expose_provider_error_description(
    client: httpx.AsyncClient,
    social_service: StubSocialAuthenticationService,
) -> None:
    """A provider diagnostic must not cross the Auth HTTP boundary."""
    response = await client.get(
        '/api/v1/auth/social/yandex/callback',
        params={
            'error': 'access_denied',
            'error_description': 'private provider diagnostic',
            'state': 'oauth-state',
        },
    )

    assert response.status_code == 400
    assert response.json() == {'detail': 'Yandex authentication was cancelled'}
    assert 'private provider diagnostic' not in response.text
    assert social_service.cancel_calls == ['oauth-state']


@pytest.mark.parametrize('query', ['', '?tz=', '?tz=Unknown/Zone'])
async def test_social_login_requires_valid_timezone(
    client: httpx.AsyncClient, query: str
) -> None:
    """OAuth starts only with an explicit timezone suitable for registration."""
    response = await client.get('/api/v1/auth/social/yandex/login' + query)
    assert response.status_code == 422
