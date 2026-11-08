import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest

from clients.auth import (
    AuthInvalidCredentialsError,
    AuthRateLimitExceededError,
    AuthServiceUnavailableError,
    HttpAuthGateway,
    LogoutOutcome,
)


@pytest.mark.asyncio
async def test_http_auth_gateway_login_profile_and_error_mapping() -> None:
    """HttpAuthGateway передаёт X-Request-Id и переводит коды ошибок в исключения."""
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
            if '"badjson"' in body:
                return httpx.Response(200, text='not-json')
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
            if auth_header == 'Bearer badjson':
                return httpx.Response(200, json={'id': 'not-a-uuid'})
            if auth_header == 'Bearer forbidden':
                return httpx.Response(403, json={'detail': 'Forbidden'})
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

        # Пустой токен при выходе не делает сетевого вызова
        await gateway.logout('', '', request_id='req-noop')
        assert len(seen_request_ids) == 3

        # Пустой логин/пароль или токен выбрасывают AuthInvalidCredentialsError
        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.login('   ', 'secret', request_id='req-blank')
        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.get_current_user('', request_id='req-empty-tok')

        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.login('wrong', 'secret', request_id='req-4')

        with pytest.raises(AuthRateLimitExceededError):
            await gateway.login('ratelimit', 'secret', request_id='req-5')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.login('down', 'secret', request_id='req-6')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.login('badjson', 'secret', request_id='req-badjson')

        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.get_current_user('expired-token', request_id='req-7')

        with pytest.raises(AuthInvalidCredentialsError):
            await gateway.get_current_user('forbidden', request_id='req-403')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.get_current_user('down', request_id='req-8')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.get_current_user('badjson', request_id='req-bad-profile')


@pytest.mark.asyncio
async def test_profile_rate_limit_is_not_reported_as_auth_outage() -> None:
    """Регрессия: 429 от /profile — лимит пользователя, а не недоступность Auth."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={'detail': 'Too many requests'})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        gateway = HttpAuthGateway(http_client=http_client, base_url='http://auth:8000')

        with pytest.raises(AuthRateLimitExceededError):
            await gateway.get_current_user('tok', request_id='req-429')


@pytest.mark.asyncio
async def test_http_auth_gateway_network_failures_and_best_effort_logout() -> None:
    """Обрыв сети при login/profile даёт 503, а при logout подавляется."""

    def failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('Auth unreachable', request=request)

    transport = httpx.MockTransport(failing_handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        gateway = HttpAuthGateway(http_client=http_client, base_url='http://auth:8000')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.login('user', 'pass', request_id='req-net-1')

        with pytest.raises(AuthServiceUnavailableError):
            await gateway.get_current_user('tok', request_id='req-net-2')

        # Best-effort logout не бросает исключений при обрыве связи
        await gateway.logout('tok', 'ref', request_id='req-net-3')


class _FakeAuthSessions:
    """Минимальный Auth для логаута: logout/refresh с учётом состояния токенов."""

    def __init__(self) -> None:
        self.live_access: set[str] = set()
        self.live_refresh: set[str] = set()
        self.calls: list[tuple[str, str, str, dict[str, str]]] = []
        self.forced_status: dict[str, int] = {}
        self.rotations = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b'{}')
        bearer = request.headers.get('Authorization', '').removeprefix('Bearer ')
        path = request.url.path.rsplit('/', 1)[-1]
        self.calls.append((path, bearer, request.headers.get('X-Request-Id', ''), body))
        if path in self.forced_status:
            return httpx.Response(self.forced_status.pop(path), text='forced')
        if path == 'logout':
            if bearer not in self.live_access:
                return httpx.Response(401, json={'detail': 'Token expired'})
            self.live_access.discard(bearer)
            self.live_refresh.discard(body.get('refresh_token', ''))
            return httpx.Response(204)
        if path == 'refresh':
            old = body['refresh_token']
            if old not in self.live_refresh:
                return httpx.Response(401, json={'detail': 'Invalid refresh token'})
            self.live_refresh.discard(old)
            self.rotations += 1
            pair = (f'acc-rot-{self.rotations}', f'ref-rot-{self.rotations}')
            self.live_access.add(pair[0])
            self.live_refresh.add(pair[1])
            return httpx.Response(
                200, json={'access_token': pair[0], 'refresh_token': pair[1]}
            )
        return httpx.Response(404)


@pytest.fixture
async def auth_sessions() -> AsyncIterator[tuple[HttpAuthGateway, _FakeAuthSessions]]:
    """HttpAuthGateway поверх stateful-фейка Auth."""
    fake = _FakeAuthSessions()
    transport = httpx.MockTransport(fake.handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        yield (
            HttpAuthGateway(http_client=http_client, base_url='http://auth:8000'),
            fake,
        )


@pytest.mark.asyncio
async def test_logout_with_live_access_revokes_session_in_one_call(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
) -> None:
    """BIL-NFR-SEC-05: живой access — один вызов logout с refresh в теле."""
    gateway, fake = auth_sessions
    fake.live_access.add('acc')
    fake.live_refresh.add('ref')

    outcome = await gateway.logout('acc', 'ref', request_id='req-ok')

    assert outcome is LogoutOutcome.REVOKED
    assert fake.calls == [('logout', 'acc', 'req-ok', {'refresh_token': 'ref'})]
    assert not fake.live_access and not fake.live_refresh


@pytest.mark.asyncio
async def test_logout_with_expired_access_revokes_refresh_via_rotation(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
) -> None:
    """Главный баг ревью: протухший access не оставляет живой refresh в Auth."""
    gateway, fake = auth_sessions
    fake.live_refresh.add('ref')  # access истёк, refresh ещё жив

    outcome = await gateway.logout('acc-expired', 'ref', request_id='req-exp')

    assert outcome is LogoutOutcome.REVOKED
    assert [call[0] for call in fake.calls] == ['logout', 'refresh', 'logout']
    assert fake.calls[2][1] == 'acc-rot-1'
    assert fake.calls[2][3] == {'refresh_token': 'ref-rot-1'}
    # NFR-S-01: X-Request-Id проброшен во все вызовы, включая refresh
    assert {call[2] for call in fake.calls} == {'req-exp'}
    assert not fake.live_access and not fake.live_refresh


@pytest.mark.asyncio
async def test_logout_with_dead_access_and_refresh_is_already_invalid(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
) -> None:
    """Повторный выход: оба токена уже мертвы, второй logout не делается."""
    gateway, fake = auth_sessions

    outcome = await gateway.logout('acc', 'ref', request_id='req-dead')

    assert outcome is LogoutOutcome.ALREADY_INVALID
    assert [call[0] for call in fake.calls] == ['logout', 'refresh']


@pytest.mark.asyncio
async def test_logout_with_single_cookie(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
) -> None:
    """Только access — logout без refresh; только refresh — через ротацию."""
    gateway, fake = auth_sessions
    fake.live_access.add('acc-only')
    fake.live_refresh.add('ref-only')

    assert (
        await gateway.logout('acc-only', '', request_id='req-a')
        is LogoutOutcome.REVOKED
    )
    assert fake.calls[-1] == ('logout', 'acc-only', 'req-a', {})

    assert (
        await gateway.logout('', 'ref-only', request_id='req-r')
        is LogoutOutcome.REVOKED
    )
    assert [call[0] for call in fake.calls[1:]] == ['refresh', 'logout']
    assert not fake.live_access and not fake.live_refresh

    fake.calls.clear()
    assert (
        await gateway.logout('acc-dead', '', request_id='req-d')
        is LogoutOutcome.ALREADY_INVALID
    )
    assert [call[0] for call in fake.calls] == ['logout']

    fake.calls.clear()
    assert (
        await gateway.logout('', '', request_id='req-none')
        is LogoutOutcome.NOTHING_TO_REVOKE
    )
    assert fake.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('access', 'forced'),
    [
        ('acc', {'logout': 503}),
        ('acc', {'logout': 500}),
        ('acc', {'logout': 429}),
        ('acc-expired', {'refresh': 503}),
        ('acc-expired', {'refresh': 429}),
    ],
)
async def test_logout_temporary_auth_errors_fail_softly(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
    caplog: pytest.LogCaptureFixture,
    access: str,
    forced: dict[str, int],
) -> None:
    """429/5xx от Auth: без исключения, итог failed и WARNING в логе."""
    gateway, fake = auth_sessions
    fake.live_access.add('acc')
    fake.live_refresh.add('ref')
    fake.forced_status.update(forced)

    with caplog.at_level(logging.WARNING, logger='clients.auth'):
        outcome = await gateway.logout(access, 'ref', request_id='req-5xx')

    assert outcome is LogoutOutcome.FAILED
    assert 'revocation on logout failed' in caplog.text
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)


@pytest.mark.asyncio
async def test_logout_rotated_session_logout_failure_still_counts_as_revoked(
    auth_sessions: tuple[HttpAuthGateway, _FakeAuthSessions],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """После ротации исходные токены уже мертвы, даже если второй logout упал."""
    _, fake = auth_sessions
    fake.live_refresh.add('ref')

    def flaky(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith('/logout') and fake.rotations:
            return httpx.Response(503)
        return fake.handler(request)

    transport = httpx.MockTransport(flaky)
    async with httpx.AsyncClient(transport=transport) as http_client:
        flaky_gateway = HttpAuthGateway(
            http_client=http_client, base_url='http://auth:8000'
        )
        with caplog.at_level(logging.WARNING, logger='clients.auth'):
            outcome = await flaky_gateway.logout(
                'acc-expired', 'ref', request_id='req-rot'
            )

    assert outcome is LogoutOutcome.REVOKED
    assert 'ref' not in fake.live_refresh
    assert 'rotated session returned HTTP 503' in caplog.text


@pytest.mark.asyncio
async def test_logout_network_error_and_bad_refresh_json_fail_softly() -> None:
    """Обрыв сети и битый JSON на refresh не выходят наружу исключением."""

    def broken(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith('/refresh'):
            return httpx.Response(200, text='not-json')
        if request.headers.get('Authorization') == 'Bearer net':
            raise httpx.ConnectError('Auth unreachable', request=request)
        return httpx.Response(401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as client:
        gateway = HttpAuthGateway(http_client=client, base_url='http://auth:8000')
        assert (
            await gateway.logout('net', 'ref', request_id='req-n')
            is LogoutOutcome.FAILED
        )
        assert (
            await gateway.logout('acc', 'ref', request_id='req-j')
            is LogoutOutcome.FAILED
        )


@pytest.mark.asyncio
async def test_logout_is_bounded_by_overall_timeout() -> None:
    """Зависший Auth не держит выход дольше logout_timeout_seconds."""

    async def hanging(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(hanging)) as client:
        gateway = HttpAuthGateway(
            http_client=client,
            base_url='http://auth:8000',
            logout_timeout_seconds=0.05,
        )
        started = time.monotonic()
        outcome = await gateway.logout('acc', 'ref', request_id='req-hang')
        elapsed = time.monotonic() - started

    assert outcome is LogoutOutcome.FAILED
    assert elapsed < 1.0
