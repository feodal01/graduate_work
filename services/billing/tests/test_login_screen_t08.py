from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import Depends, FastAPI

from clients.auth import (
    AuthInvalidCredentialsError,
    AuthRateLimitExceededError,
    AuthServiceUnavailableError,
    LogoutOutcome,
    SubscriptionAccessSyncResult,
)
from dependencies import (
    AuthenticatedUser,
    get_auth_gateway,
    require_api_user,
    require_ui_user,
)
from main import create_app


class FakeAuthGateway:
    """Тестовый двойник клиента Auth для проверки сценариев входа Э1."""

    def __init__(self) -> None:
        self.login_status: int = 200
        self.access_token: str = 'valid-access-token'
        self.refresh_token: str = 'valid-refresh-token'
        self.profile_status: int = 200
        self.user_id: UUID = uuid4()
        self.user_login: str = 'cinema_viewer'
        self.user_tz: str = 'Europe/Moscow'
        self.logout_called_with: tuple[str, str] | None = None
        self.last_request_id: str | None = None

    async def login(
        self,
        login: str,
        password: str,
        *,
        request_id: str,
    ) -> tuple[str, str]:
        """Эмулирует вход через Auth API."""
        self.last_request_id = request_id
        if self.login_status == 429:
            raise AuthRateLimitExceededError()
        if self.login_status == 503:
            raise AuthServiceUnavailableError()
        if (
            self.login_status == 200
            and login == self.user_login
            and password == 'secret'
        ):
            return self.access_token, self.refresh_token
        raise AuthInvalidCredentialsError()

    async def get_current_user(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[UUID, str, str]:
        """Эмулирует проверку токена и профиля пользователя в Auth."""
        self.last_request_id = request_id
        if self.profile_status == 503:
            raise AuthServiceUnavailableError()
        if self.profile_status == 429:
            raise AuthRateLimitExceededError()
        if self.profile_status == 401 or access_token != self.access_token:
            raise AuthInvalidCredentialsError()
        return self.user_id, self.user_login, self.user_tz

    async def logout(
        self,
        access_token: str,
        refresh_token: str,
        *,
        request_id: str,
    ) -> LogoutOutcome:
        """Эмулирует выход пользователя в Auth."""
        self.last_request_id = request_id
        self.logout_called_with = (access_token, refresh_token)
        return LogoutOutcome.REVOKED

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        """Эмулирует запись состояния доступа."""
        return SubscriptionAccessSyncResult(applied=True, current_version=version)


@pytest.fixture
async def login_client() -> AsyncGenerator[
    tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    None,
]:
    """Создаёт тестовый клиент с подменённым AuthGateway."""
    fake_auth = FakeAuthGateway()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_auth_gateway] = lambda: fake_auth
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'billing-login-test'},
        follow_redirects=False,
    ) as client:
        yield client, app, fake_auth


@pytest.mark.asyncio
async def test_get_login_renders_html_form_e1(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """Э1 (BIL-FR-12, BIL-API-01): GET /billing/login отображает форму входа."""
    client, _, _ = login_client

    response = await client.get('/billing/login')

    assert response.status_code == 200
    assert 'text/html' in response.headers['content-type']
    html = response.text
    assert 'name="login"' in html
    assert 'name="password"' in html
    assert 'name="next"' in html
    assert 'Войти' in html


@pytest.mark.asyncio
async def test_post_login_valid_credentials_sets_httponly_cookies_and_redirects(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01: успешный вход ставит HttpOnly-cookie и редиректит."""
    client, _, fake_auth = login_client

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'secret'},
    )

    assert response.status_code == 303
    assert response.headers['location'] == '/billing/'
    assert fake_auth.last_request_id == 'billing-login-test'

    set_cookie_headers = response.headers.get_list('set-cookie')
    assert any(
        'billing_access_token=valid-access-token' in header
        and 'HttpOnly' in header
        and 'SameSite=lax' in header
        and 'Path=/' in header
        for header in set_cookie_headers
    )
    assert any(
        'billing_refresh_token=valid-refresh-token' in header
        and 'HttpOnly' in header
        and 'SameSite=lax' in header
        and 'Path=/' in header
        for header in set_cookie_headers
    )

    # При X-Forwarded-Proto: https выставляется атрибут Secure
    https_resp = await client.post(
        '/billing/login',
        data={
            'login': 'cinema_viewer',
            'password': 'secret',
            'next': '/billing/payments/123',
        },
        headers={'X-Forwarded-Proto': 'https'},
    )
    assert https_resp.status_code == 303
    assert https_resp.headers['location'] == '/billing/payments/123'
    https_cookies = https_resp.headers.get_list('set-cookie')
    assert all('Secure' in header for header in https_cookies)


@pytest.mark.asyncio
async def test_login_open_redirect_protection_and_xss_escaping(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """Защита от Open Redirect в next и экранирование XSS (BIL-NFR-SEC-03)."""
    client, _, fake_auth = login_client

    for unsafe_next in (
        'https://evil.example/phish',
        '//evil.example/phish',
        '/\\evil.example',
        '/admin/billing/',
        'javascript:alert(1)',
    ):
        resp = await client.post(
            '/billing/login',
            data={
                'login': 'cinema_viewer',
                'password': 'secret',
                'next': unsafe_next,
            },
        )
        assert resp.status_code == 303
        assert resp.headers['location'] == '/billing/'

    # Проверка экранирования XSS при 401 и 429
    xss_payload = '"><script>alert(1)</script>'
    resp_401 = await client.post(
        '/billing/login',
        data={'login': xss_payload, 'password': 'wrong'},
    )
    assert resp_401.status_code == 401
    assert '<script>alert(1)</script>' not in resp_401.text
    assert '&lt;script&gt;alert(1)&lt;/script&gt;' in resp_401.text

    fake_auth.login_status = 429
    resp_429 = await client.post(
        '/billing/login',
        data={'login': xss_payload, 'password': 'secret'},
    )
    assert resp_429.status_code == 429
    assert '<script>alert(1)</script>' not in resp_429.text


@pytest.mark.asyncio
async def test_post_login_invalid_credentials_returns_401_on_login_page(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01: при неверном пароле возвращается 401 с сообщением на экране Э1."""
    client, _, _ = login_client

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'wrong-password'},
    )

    assert response.status_code == 401
    assert 'text/html' in response.headers['content-type']
    assert 'Неверный логин или пароль' in response.text
    assert 'billing_access_token' not in response.headers.get('set-cookie', '')


@pytest.mark.asyncio
async def test_post_login_rate_limited_returns_429_message(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01: при превышении лимита попыток в Auth возвращается 429."""
    client, _, fake_auth = login_client
    fake_auth.login_status = 429

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'secret'},
    )

    assert response.status_code == 429
    assert 'Слишком много попыток входа' in response.text


@pytest.mark.asyncio
async def test_post_login_when_auth_is_unavailable_renders_503_error_screen_e4(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01, BIL-NFR-SEC-03: при сбое Auth возвращается экран Э4 (503)."""
    client, _, fake_auth = login_client
    fake_auth.login_status = 503

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'secret'},
    )

    assert response.status_code == 503
    assert 'Сервис авторизации временно недоступен' in response.text
    assert 'href="/billing/"' in response.text


@pytest.mark.asyncio
async def test_post_logout_clears_cookies_and_redirects_to_login(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01: POST /billing/logout чистит cookie (и с ними, и без них)."""
    client, _, fake_auth = login_client

    # Выход без cookie не вызывает Auth, но возвращает 303 на /billing/login
    no_cookie_resp = await client.post('/billing/logout')
    assert no_cookie_resp.status_code == 303
    assert no_cookie_resp.headers['location'] == '/billing/login'
    assert fake_auth.logout_called_with is None

    client.cookies.set('billing_access_token', 'valid-access-token')
    client.cookies.set('billing_refresh_token', 'valid-refresh-token')

    response = await client.post('/billing/logout')

    assert response.status_code == 303
    assert response.headers['location'] == '/billing/login'
    assert fake_auth.logout_called_with == ('valid-access-token', 'valid-refresh-token')
    set_cookie_headers = response.headers.get_list('set-cookie')
    assert any(
        'billing_access_token=""' in header or 'Max-Age=0' in header
        for header in set_cookie_headers
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('cookies', 'expected_call'),
    [
        ({'billing_access_token': 'only-access'}, ('only-access', '')),
        ({'billing_refresh_token': 'only-refresh'}, ('', 'only-refresh')),
    ],
)
async def test_post_logout_calls_auth_with_any_single_cookie(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    cookies: dict[str, str],
    expected_call: tuple[str, str],
) -> None:
    """BIL-NFR-SEC-05: Auth вызывается, даже если пришёл только один токен."""
    client, _, fake_auth = login_client
    for name, value in cookies.items():
        client.cookies.set(name, value)

    response = await client.post('/billing/logout')

    assert response.status_code == 303
    assert fake_auth.logout_called_with == expected_call


@pytest.mark.asyncio
async def test_post_logout_clears_cookies_even_if_gateway_crashes(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BIL-API-01: дефект в gateway не мешает выйти — всегда 303 и очистка cookie."""
    client, _, fake_auth = login_client

    async def crash(*_: object, **__: object) -> None:
        raise RuntimeError('unexpected gateway bug')

    monkeypatch.setattr(fake_auth, 'logout', crash)
    client.cookies.set('billing_access_token', 'valid-access-token')
    client.cookies.set('billing_refresh_token', 'valid-refresh-token')

    response = await client.post('/billing/logout')

    assert response.status_code == 303
    assert response.headers['location'] == '/billing/login'
    cleared = ' '.join(response.headers.get_list('set-cookie'))
    assert 'billing_access_token=""' in cleared
    assert 'billing_refresh_token=""' in cleared


@pytest.mark.asyncio
async def test_ui_and_api_user_authentication_redirects_or_returns_401_and_503(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-NFR-SEC-03: без токена экран редиректит на вход с next, при сбое — 503."""
    client, app, fake_auth = login_client

    @app.get('/billing/protected-probe')
    async def ui_probe(
        user: AuthenticatedUser = Depends(require_ui_user),
    ) -> dict[str, str]:
        return {'login': user.login, 'tz': user.tz}

    @app.get('/api/v1/billing/protected-probe')
    async def api_probe(
        user: AuthenticatedUser = Depends(require_api_user),
    ) -> dict[str, str]:
        return {'login': user.login, 'tz': user.tz}

    # 1. Экран без cookie -> 303 редирект на /billing/login?next=...
    unauth_ui = await client.get('/billing/protected-probe')
    assert unauth_ui.status_code == 303
    assert (
        unauth_ui.headers['location'] == '/billing/login?next=/billing/protected-probe'
    )

    # 2. JSON API без токена -> 401
    unauth_api = await client.get('/api/v1/billing/protected-probe')
    assert unauth_api.status_code == 401
    assert unauth_api.json()['error_code'] == 'unauthenticated'

    # 3. С валидным cookie экран пропускает пользователя
    client.cookies.set('billing_access_token', 'valid-access-token')
    ok_ui = await client.get('/billing/protected-probe')
    assert ok_ui.status_code == 200
    assert ok_ui.json() == {'login': 'cinema_viewer', 'tz': 'Europe/Moscow'}

    # 4. С валидным заголовком Authorization: Bearer JSON API тоже работает
    client.cookies.clear()
    ok_api = await client.get(
        '/api/v1/billing/protected-probe',
        headers={'Authorization': 'Bearer valid-access-token'},
    )
    assert ok_api.status_code == 200
    assert ok_api.json() == {'login': 'cinema_viewer', 'tz': 'Europe/Moscow'}

    # 5. При недоступности Auth экран возвращает 503 Э4, а JSON API — 503 JSON
    fake_auth.profile_status = 503
    client.cookies.set('billing_access_token', 'valid-access-token')
    down_ui = await client.get('/billing/protected-probe')
    assert down_ui.status_code == 503
    assert 'Сервис авторизации временно недоступен' in down_ui.text

    down_api = await client.get(
        '/api/v1/billing/protected-probe',
        headers={'Authorization': 'Bearer valid-access-token'},
    )
    assert down_api.status_code == 503
    assert down_api.json()['error_code'] == 'auth_unavailable'


async def test_user_rate_limit_in_auth_shows_429_not_auth_outage(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """Регрессия: лимит запросов в Auth — это 429, а не «Auth недоступен».

    Раньше 11-я быстрая загрузка «Моей подписки» за минуту показывала экран
    «Сервис авторизации временно недоступен» (503), хотя Auth работал.
    """
    client, app, fake_auth = login_client

    @app.get('/billing/rate-probe')
    async def ui_probe(
        user: AuthenticatedUser = Depends(require_ui_user),
    ) -> dict[str, str]:
        return {'login': user.login}

    @app.get('/api/v1/billing/rate-probe')
    async def api_probe(
        user: AuthenticatedUser = Depends(require_api_user),
    ) -> dict[str, str]:
        return {'login': user.login}

    fake_auth.profile_status = 429
    client.cookies.set('billing_access_token', 'valid-access-token')

    ui = await client.get('/billing/rate-probe')
    assert ui.status_code == 429
    assert 'Слишком много запросов' in ui.text
    assert 'временно недоступен' not in ui.text

    api = await client.get(
        '/api/v1/billing/rate-probe',
        headers={'Authorization': 'Bearer valid-access-token'},
    )
    assert api.status_code == 429
    assert api.json()['error_code'] == 'auth_rate_limited'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'headers',
    [
        {'Origin': 'https://evil.example'},
        {'Origin': 'null'},
        {'Referer': 'https://evil.example/attack.html'},
        # Другая страница на том же хосте (Jaeger, Kibana, dev-сервер) — чужой сайт.
        {'Origin': 'http://testserver:16686'},
        {'Origin': 'https://localhost'},
    ],
    ids=[
        'foreign-origin',
        'null-origin',
        'foreign-referer',
        'same-host-other-port',
        'other-scheme',
    ],
)
async def test_cross_site_login_form_is_rejected(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    headers: dict[str, str],
) -> None:
    """Login CSRF: форма входа с чужого сайта не логинит жертву в чужой аккаунт.

    Регрессия: страница атакующего отправляла форму с его логином, жертва
    оказывалась в его аккаунте и оплачивала подписку на него.
    """
    client, _, fake_auth = login_client

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'secret'},
        headers=headers,
    )

    assert response.status_code == 403
    assert fake_auth.last_request_id is None
    assert 'billing_access_token' not in response.headers.get('set-cookie', '')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'headers',
    [
        {},
        {'Origin': 'http://testserver'},
        {'Referer': 'http://testserver/billing/login'},
        # Через шлюз: Host без порта, полный адрес в X-Forwarded-Host.
        {
            'Origin': 'http://localhost:18081',
            'Host': 'localhost',
            'X-Forwarded-Host': 'localhost:18081',
            'X-Forwarded-Proto': 'http',
        },
        {'Origin': 'http://localhost'},  # BILLING_PUBLIC_BASE_URL
    ],
    ids=[
        'no-origin',
        'same-origin',
        'same-origin-referer',
        'gateway-with-port',
        'public-base-url',
    ],
)
async def test_same_site_login_form_is_accepted(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    headers: dict[str, str],
) -> None:
    """Свой сайт (и не браузерный клиент без Origin) входит как раньше."""
    client, _, _ = login_client

    response = await client.post(
        '/billing/login',
        data={'login': 'cinema_viewer', 'password': 'secret'},
        headers=headers,
    )

    assert response.status_code == 303


@pytest.mark.asyncio
async def test_cross_site_logout_and_subscribe_are_rejected(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """Остальные POST-формы экранов Биллинга тоже закрыты от CSRF."""
    client, _, fake_auth = login_client
    evil = {'Origin': 'https://evil.example'}

    logout = await client.post('/billing/logout', headers=evil)
    subscribe = await client.post('/billing/subscribe', headers=evil)

    assert logout.status_code == 403
    assert subscribe.status_code == 403
    assert fake_auth.logout_called_with is None
