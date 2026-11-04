from uuid import uuid4

import httpx
import pytest

from clients.auth import (
    AuthInvalidCredentialsError,
    AuthRateLimitExceededError,
    AuthServiceUnavailableError,
    HttpAuthGateway,
)


@pytest.mark.asyncio
async def test_http_auth_gateway_login_profile_and_error_mapping() -> None:
    """HttpAuthGateway передаёт X-Request-Id и переводит 401/429/5xx в исключения."""
    user_id = uuid4()
    seen_request_ids: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_request_ids.append(request.headers.get('X-Request-Id', ''))
        if request.url.path == '/api/v1/auth/login':
            body = request.read().decode('utf-8')
            if '"valid"' in body:
                return httpx.Response(
                    200,
                    json={
                        'access_token': 'acc-1',
                        'refresh_token': 'ref-1',
                    },
                )
            if '"ratelimit"' in body:
                return httpx.Response(429, json={'detail': 'Too many requests'})
            if '"down"' in body:
                return httpx.Response(503, json={'detail': 'Service unavailable'})
            return httpx.Response(401, json={'detail': 'Invalid credentials'})

        if request.url.path == '/api/v1/auth/profile':
            auth_header = request.headers.get('Authorization', '')
            if auth_header == 'Bearer acc-1':
                return httpx.Response(
                    200,
                    json={
                        'id': str(user_id),
                        'login': 'valid',
                        'tz': 'Asia/Yekaterinburg',
                    },
                )
            if auth_header == 'Bearer down':
                return httpx.Response(502)
            return httpx.Response(401, json={'detail': 'Expired'})

        if request.url.path == '/api/v1/auth/logout':
            return httpx.Response(204)

        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        gateway = HttpAuthGateway(http_client=http_client, base_url='http://auth:8000')

        acc, ref = await gateway.login('valid', 'secret', request_id='req-1')
        assert (acc, ref) == ('acc-1', 'ref-1')

        uid, login, tz = await gateway.get_current_user('acc-1', request_id='req-2')
        assert uid == user_id
        assert login == 'valid'
        assert tz == 'Asia/Yekaterinburg'

        await gateway.logout('acc-1', 'ref-1', request_id='req-3')
        assert seen_request_ids == ['req-1', 'req-2', 'req-3']

        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.login('wrong', 'secret', request_id='req-4')

        with pytest.raises(AuthRateLimitExceededError):
            await gateway.login('ratelimit', 'secret', request_id='req-5')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.login('down', 'secret', request_id='req-6')

        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.get_current_user('expired-token', request_id='req-7')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.get_current_user('down', request_id='req-8')
