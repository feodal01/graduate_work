import base64
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import clients.yandex_oauth as yandex_oauth
from clients.yandex_oauth import YandexOAuthClient
from services.errors import (
    InvalidOAuthCodeError,
    SocialProviderContractError,
    SocialProviderUnavailableError,
)
from services.types import SocialIdentity


def _client(
    transport: httpx.AsyncBaseTransport,
    *,
    max_tries: int = 3,
) -> tuple[httpx.AsyncClient, YandexOAuthClient]:
    http_client = httpx.AsyncClient(transport=transport)
    provider = YandexOAuthClient(
        http_client=http_client,
        client_id='test-client-id',
        client_secret='test-client-secret',
        redirect_uri='http://testserver/api/v1/auth/social/yandex/callback',
        userinfo_max_tries=max_tries,
        userinfo_backoff_max_seconds=2.0,
    )
    return http_client, provider


@pytest.mark.asyncio
async def test_authorization_url_contains_state_pkce_and_minimal_scopes() -> None:
    """Removing a security parameter or adding a scope must break the URL."""
    http_client, provider = _client(httpx.MockTransport(lambda _: httpx.Response(500)))
    try:
        url = provider.build_authorization_url(
            state='test-state',
            code_challenge='test-challenge',
        )
    finally:
        await http_client.aclose()

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    assert parsed.scheme == 'https'
    assert parsed.netloc == 'oauth.yandex.ru'
    assert parsed.path == '/authorize'
    assert query == {
        'response_type': ['code'],
        'client_id': ['test-client-id'],
        'redirect_uri': ['http://testserver/api/v1/auth/social/yandex/callback'],
        'scope': ['login:info login:email'],
        'state': ['test-state'],
        'code_challenge': ['test-challenge'],
        'code_challenge_method': ['S256'],
    }


@pytest.mark.asyncio
async def test_code_exchange_uses_basic_auth_and_pkce() -> None:
    """The backend must authenticate itself and send the original verifier."""

    async def handler(request: httpx.Request) -> httpx.Response:
        expected = base64.b64encode(b'test-client-id:test-client-secret').decode()
        assert request.headers['Authorization'] == f'Basic {expected}'
        body = parse_qs((await request.aread()).decode())
        assert body == {
            'grant_type': ['authorization_code'],
            'code': ['test-code'],
            'code_verifier': ['test-verifier'],
        }
        return httpx.Response(
            200,
            json={
                'token_type': 'bearer',
                'access_token': 'yandex-access',
                'expires_in': 3600,
            },
        )

    http_client, provider = _client(httpx.MockTransport(handler))
    try:
        access_token = await provider.exchange_code(
            code='test-code',
            code_verifier='test-verifier',
        )
    finally:
        await http_client.aclose()

    assert access_token == 'yandex-access'


@pytest.mark.asyncio
async def test_code_exchange_does_not_retry_server_failure() -> None:
    """Reusing a one-time authorization code must never be attempted."""
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    http_client, provider = _client(httpx.MockTransport(handler))
    try:
        with pytest.raises(SocialProviderUnavailableError):
            await provider.exchange_code(
                code='one-time-code',
                code_verifier='test-verifier',
            )
    finally:
        await http_client.aclose()

    assert calls == 1


@pytest.mark.asyncio
async def test_invalid_grant_is_a_public_invalid_code_error() -> None:
    """Provider diagnostics must not replace the stable Auth API error."""
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            400,
            json={
                'error': 'invalid_grant',
                'error_description': 'private provider diagnostic',
            },
        )
    )
    http_client, provider = _client(transport)
    try:
        with pytest.raises(InvalidOAuthCodeError) as captured:
            await provider.exchange_code(
                code='expired-code',
                code_verifier='test-verifier',
            )
    finally:
        await http_client.aclose()

    assert str(captured.value) == 'OAuth code is invalid or expired'
    assert 'private provider diagnostic' not in str(captured.value)


@pytest.mark.asyncio
async def test_malformed_token_json_is_rejected_as_provider_contract() -> None:
    """A successful HTTP status must not bypass token response validation."""
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            content=b'{',
            headers={'Content-Type': 'application/json'},
        )
    )
    http_client, provider = _client(transport)
    try:
        with pytest.raises(SocialProviderContractError):
            await provider.exchange_code(
                code='test-code',
                code_verifier='test-verifier',
            )
    finally:
        await http_client.aclose()


@pytest.mark.asyncio
async def test_userinfo_returns_confirmed_social_identity() -> None:
    """The Yandex profile must be bound to the configured OAuth client."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers['Authorization'] == 'OAuth yandex-access'
        return httpx.Response(
            200,
            json={
                'id': '123',
                'login': 'alice',
                'client_id': 'test-client-id',
                'default_email': 'alice@yandex.ru',
                'first_name': 'Alice',
                'last_name': 'Viewer',
            },
        )

    http_client, provider = _client(httpx.MockTransport(handler))
    try:
        identity = await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()

    assert identity == SocialIdentity(
        provider='yandex',
        provider_user_id='123',
        provider_login='alice',
        email='alice@yandex.ru',
        first_name='Alice',
        last_name='Viewer',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'payload',
    [
        {'login': 'alice', 'client_id': 'test-client-id'},
        {'id': '123', 'login': '', 'client_id': 'test-client-id'},
        {'id': '123', 'login': 'alice', 'client_id': 'another-client'},
        {'id': 123, 'login': 'alice', 'client_id': 'test-client-id'},
    ],
)
async def test_invalid_userinfo_contract_is_rejected(
    payload: dict[str, object],
) -> None:
    """Missing or mismatched identity fields must not create a local user."""
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))
    http_client, provider = _client(transport)
    try:
        with pytest.raises(SocialProviderContractError):
            await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()


@pytest.mark.asyncio
async def test_long_provider_names_are_truncated_for_the_local_model() -> None:
    """Long optional names must not block an otherwise valid login."""
    payload = {
        'id': '123',
        'login': 'alice',
        'client_id': 'test-client-id',
        'first_name': 'A' * 80,
        'last_name': 'B' * 80,
    }
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))
    http_client, provider = _client(transport)
    try:
        identity = await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()

    assert identity.first_name == 'A' * 50
    assert identity.last_name == 'B' * 50


@pytest.mark.asyncio
async def test_userinfo_retries_only_bounded_transient_responses() -> None:
    """A temporary read failure must retry without exceeding the limit."""
    responses = iter(
        [
            httpx.Response(503),
            httpx.Response(429, headers={'Retry-After': '1'}),
            httpx.Response(
                200,
                json={
                    'id': '123',
                    'login': 'alice',
                    'client_id': 'test-client-id',
                },
            ),
        ]
    )
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return next(responses)

    http_client, provider = _client(httpx.MockTransport(handler))
    try:
        with patch(
            'clients.yandex_oauth.asyncio.sleep',
            new_callable=AsyncMock,
        ) as sleep:
            identity = await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()

    assert identity.provider_user_id == '123'
    assert calls == 3
    assert sleep.await_args_list[0].args == (1.0,)
    assert sleep.await_args_list[1].args == (1.0,)


@pytest.mark.asyncio
async def test_invalid_retry_after_uses_bounded_exponential_backoff() -> None:
    """An invalid provider delay must not disable or extend local backoff."""
    responses = iter(
        [
            httpx.Response(429, headers={'Retry-After': 'not-a-number'}),
            httpx.Response(
                200,
                json={
                    'id': '123',
                    'login': 'alice',
                    'client_id': 'test-client-id',
                },
            ),
        ]
    )
    http_client, provider = _client(
        httpx.MockTransport(lambda _request: next(responses))
    )
    try:
        with patch(
            'clients.yandex_oauth.asyncio.sleep',
            new_callable=AsyncMock,
        ) as sleep:
            await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()

    sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_userinfo_does_not_retry_client_error() -> None:
    """A permanent HTTP error must not consume the retry budget."""
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401)

    http_client, provider = _client(httpx.MockTransport(handler))
    try:
        with pytest.raises(SocialProviderUnavailableError):
            await provider.get_identity('yandex-access')
    finally:
        await http_client.aclose()

    assert calls == 1


@pytest.mark.asyncio
async def test_oauth_spans_do_not_contain_credentials_or_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracing may expose operation metadata but never OAuth credentials."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        yandex_oauth,
        'tracer',
        provider.get_tracer('test-yandex-oauth'),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == '/token':
            return httpx.Response(
                200,
                json={
                    'token_type': 'bearer',
                    'access_token': 'provider-access-token',
                    'expires_in': 3600,
                },
            )
        return httpx.Response(
            200,
            json={
                'id': '123',
                'login': 'alice',
                'client_id': 'test-client-id',
            },
        )

    http_client, oauth_client = _client(httpx.MockTransport(handler))
    try:
        token = await oauth_client.exchange_code(
            code='one-time-code',
            code_verifier='pkce-verifier',
        )
        await oauth_client.get_identity(token)
    finally:
        await http_client.aclose()
        provider.shutdown()

    exported = repr(
        [
            (span.name, dict(span.attributes or {}))
            for span in exporter.get_finished_spans()
        ]
    )
    assert 'test-client-secret' not in exported
    assert 'one-time-code' not in exported
    assert 'pkce-verifier' not in exported
    assert 'provider-access-token' not in exported
