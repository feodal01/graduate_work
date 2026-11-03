from collections.abc import Mapping
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from clients.auth import (
    AuthClient,
    AuthContractError,
    AuthRateLimitedError,
    AuthTokenRejectedError,
    AuthUnavailableError,
)


class ResponseStub:
    def __init__(
        self,
        status: int,
        payload: Any,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def json(self) -> Any:
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class RequestContextStub:
    def __init__(
        self,
        response: ResponseStub | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._response = response
        self._error = error

    async def __aenter__(self) -> ResponseStub:
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response

    async def __aexit__(self, *args: object) -> None:
        del args


class SessionStub:
    def __init__(
        self,
        *,
        response: ResponseStub | None = None,
        error: BaseException | None = None,
        outcomes: list[ResponseStub | BaseException] | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.outcomes = outcomes or []
        self.calls: list[dict[str, Any]] = []

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json: dict[str, object],
    ) -> RequestContextStub:
        self.calls.append({'url': url, 'headers': dict(headers), 'json': json})
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                return RequestContextStub(error=outcome)
            return RequestContextStub(response=outcome)
        return RequestContextStub(self.response, self.error)


def make_client(session: SessionStub) -> AuthClient:
    return AuthClient(
        session=session,  # type: ignore[arg-type]
        base_url='http://auth:8000/api/v1/auth',
        service_api_key='service-secret',
    )


@pytest.mark.asyncio
async def test_auth_client_sends_exact_authenticated_film_request() -> None:
    session = SessionStub(response=ResponseStub(200, {'allowed': True}))
    client = make_client(session)

    allowed = await client.can_read_film(
        creation_date=date(2023, 8, 15),
        access_token='access-token',
        request_id='movies-request-id',
    )

    assert allowed is True
    assert session.calls == [
        {
            'url': 'http://auth:8000/api/v1/auth/permissions/check',
            'headers': {
                'X-Service-Key': 'service-secret',
                'Authorization': 'Bearer access-token',
                'X-Request-Id': 'movies-request-id',
            },
            'json': {
                'resource': 'film',
                'action': 'read',
                'attributes': {'creation_date': '2023-08-15'},
            },
        }
    ]


@pytest.mark.asyncio
async def test_auth_client_sends_null_date_for_anonymous_user() -> None:
    session = SessionStub(response=ResponseStub(200, {'allowed': False}))
    client = make_client(session)

    allowed = await client.can_read_film(
        creation_date=None,
        access_token=None,
        request_id='anonymous-request-id',
    )

    assert allowed is False
    assert len(session.calls) == 1
    assert session.calls[0]['headers'] == {
        'X-Service-Key': 'service-secret',
        'X-Request-Id': 'anonymous-request-id',
    }
    assert session.calls[0]['json'] == {
        'resource': 'film',
        'action': 'read',
        'attributes': {'creation_date': None},
    }


@pytest.mark.asyncio
async def test_auth_client_distinguishes_rejected_user_token() -> None:
    session = SessionStub(response=ResponseStub(401, {'detail': 'Invalid token'}))

    with pytest.raises(AuthTokenRejectedError):
        await make_client(session).can_read_film(
            creation_date=None,
            access_token='revoked-token',
            request_id='movies-request-id',
        )


@pytest.mark.asyncio
async def test_auth_client_treats_invalid_service_key_as_contract_error() -> None:
    session = SessionStub(response=ResponseStub(401, {'detail': 'Invalid service credentials'}))

    with pytest.raises(AuthContractError):
        await make_client(session).can_read_film(
            creation_date=None,
            access_token=None,
            request_id='movies-request-id',
        )


@pytest.mark.asyncio
async def test_auth_client_preserves_rate_limit_retry_after_without_retry() -> None:
    session = SessionStub(
        response=ResponseStub(
            429,
            {'detail': 'Too many requests'},
            headers={'Retry-After': '1'},
        ),
    )

    with pytest.raises(AuthRateLimitedError) as raised:
        await make_client(session).can_read_film(
            creation_date=date(2020, 1, 1),
            access_token=None,
            request_id='rate-limited-request',
        )

    assert raised.value.retry_after_seconds == 1
    assert len(session.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'headers',
    ({}, {'Retry-After': '0'}, {'Retry-After': '-1'}, {'Retry-After': 'later'}),
)
async def test_auth_client_ignores_invalid_rate_limit_retry_after(
    headers: Mapping[str, str],
) -> None:
    session = SessionStub(
        response=ResponseStub(
            429,
            {'detail': 'Too many requests'},
            headers=headers,
        ),
    )

    with pytest.raises(AuthRateLimitedError) as raised:
        await make_client(session).can_read_film(
            creation_date=date(2020, 1, 1),
            access_token=None,
            request_id='rate-limited-request',
        )

    assert raised.value.retry_after_seconds is None
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_auth_client_retries_connection_error_then_succeeds() -> None:
    session = SessionStub(
        outcomes=[
            aiohttp.ClientConnectionError('temporarily offline'),
            ResponseStub(200, {'allowed': True}),
        ]
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        allowed = await make_client(session).can_read_film(
            creation_date=date(2020, 1, 1),
            access_token=None,
            request_id='retry-request-id',
        )

    assert allowed is True
    assert len(session.calls) == 2
    assert [call['headers']['X-Request-Id'] for call in session.calls] == [
        'retry-request-id',
        'retry-request-id',
    ]


@pytest.mark.asyncio
async def test_auth_client_retries_503_then_succeeds() -> None:
    session = SessionStub(
        outcomes=[
            ResponseStub(503, {'detail': 'temporarily unavailable'}),
            ResponseStub(200, {'allowed': False}),
        ]
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        allowed = await make_client(session).can_read_film(
            creation_date=None,
            access_token=None,
            request_id='movies-request-id',
        )

    assert allowed is False
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    'error',
    (TimeoutError(), aiohttp.ClientConnectionError('offline')),
)
@pytest.mark.asyncio
async def test_auth_client_gives_up_after_two_transport_failures(
    error: BaseException,
) -> None:
    session = SessionStub(error=error)

    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(AuthUnavailableError):
            await make_client(session).can_read_film(
                creation_date=date(2020, 1, 1),
                access_token=None,
                request_id='movies-request-id',
            )

    assert len(session.calls) == 2


@pytest.mark.parametrize('status', (500, 502, 503, 504))
@pytest.mark.asyncio
async def test_auth_client_maps_server_error_to_unavailable(status: int) -> None:
    session = SessionStub(response=ResponseStub(status, {'detail': 'failed'}))

    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(AuthUnavailableError):
            await make_client(session).can_read_film(
                creation_date=None,
                access_token=None,
                request_id='movies-request-id',
            )

    expected_calls = 1 if status == 500 else 2
    assert len(session.calls) == expected_calls


@pytest.mark.parametrize(
    ('status', 'payload'),
    (
        (200, {'allowed': 'yes'}),
        (200, {'allowed': 1}),
        (200, {'allowed': True, 'extra': 'field'}),
        (200, {}),
        (200, []),
        (200, ValueError('not-json')),
        (
            200,
            aiohttp.ContentTypeError(
                request_info=None,  # type: ignore[arg-type]
                history=(),
            ),
        ),
        (400, {'detail': 'bad request'}),
        (401, {'detail': 'unexpected'}),
        (404, {'detail': 'not found'}),
        (422, {'detail': []}),
    ),
)
@pytest.mark.asyncio
async def test_auth_client_rejects_unexpected_contract(
    status: int,
    payload: Any,
) -> None:
    session = SessionStub(response=ResponseStub(status, payload))

    with pytest.raises(AuthContractError):
        await make_client(session).can_read_film(
            creation_date=None,
            access_token=None,
            request_id='movies-request-id',
        )

    assert len(session.calls) == 1
