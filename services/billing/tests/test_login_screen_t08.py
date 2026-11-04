from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import Depends, FastAPI

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
        from clients.auth import (
            AuthInvalidCredentialsError,
            AuthRateLimitExceededError,
            AuthServiceUnavailableError,
        )

        self.last_request_id = request_id
        if (
            self.login_status == 200
            and login == self.user_login
            and password == 'secret'
        ):
            return self.access_token, self.refresh_token
        if self.login_status == 401 or password != 'secret':
            raise AuthInvalidCredentialsError()
        if self.login_status == 429:
            raise AuthRateLimitExceededError()
        raise AuthServiceUnavailableError()

    async def get_current_user(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[UUID, str, str]:
        """Эмулирует проверку токена и профиля пользователя в Auth."""
        from clients.auth import (
            AuthInvalidCredentialsError,
            AuthServiceUnavailableError,
        )

        self.last_request_id = request_id
        if self.profile_status == 503:
            raise AuthServiceUnavailableError()
        if self.profile_status == 401 or access_token != self.access_token:
            raise AuthInvalidCredentialsError()
        return self.user_id, self.user_login, self.user_tz

    async def logout(
        self,
        access_token: str,
        refresh_token: str,
        *,
        request_id: str,
    ) -> None:
        """Эмулирует выход пользователя в Auth."""
        self.last_request_id = request_id
        self.logout_called_with = (access_token, refresh_token)


@pytest.fixture
async def login_client() -> AsyncGenerator[
    tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
    None,
]:
    """Создаёт тестовый клиент с подменённым AuthGateway."""
    from dependencies import get_auth_gateway

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
    assert 'Войти' in html


@pytest.mark.asyncio
async def test_post_login_valid_credentials_sets_httponly_cookies_and_redirects(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-API-01: успешный вход ставит HttpOnly-cookie и редиректит на /billing/."""
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
        'billing_access_token=valid-access-token' in header and 'HttpOnly' in header
        for header in set_cookie_headers
    )
    assert any(
        'billing_refresh_token=valid-refresh-token' in header and 'HttpOnly' in header
        for header in set_cookie_headers
    )


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
    """BIL-API-01: POST /billing/logout чистит cookie и редиректит на /billing/login."""
    client, _, fake_auth = login_client
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
async def test_ui_and_api_user_authentication_redirects_or_returns_401_and_503(
    login_client: tuple[httpx.AsyncClient, FastAPI, FakeAuthGateway],
) -> None:
    """BIL-NFR-SEC-03: без токена экран редиректит на вход, а при сбое Auth даёт 503."""
    from dependencies import AuthenticatedUser, require_api_user, require_ui_user

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
        return {'user_id': str(user.user_id), 'login': user.login, 'tz': user.tz}

    # 1. Без cookie экран редиректит на /billing/login, а API возвращает 401
    ui_unauth = await client.get('/billing/protected-probe')
    assert ui_unauth.status_code == 303
    assert ui_unauth.headers['location'] == '/billing/login'

    api_unauth = await client.get('/api/v1/billing/protected-probe')
    assert api_unauth.status_code == 401
    assert api_unauth.json()['error_code'] == 'unauthenticated'

    # 2. С валидной cookie возвращается подтверждённый пользователь и его tz
    client.cookies.set('billing_access_token', 'valid-access-token')
    ui_ok = await client.get('/billing/protected-probe')
    assert ui_ok.status_code == 200
    assert ui_ok.json() == {'login': 'cinema_viewer', 'tz': 'Europe/Moscow'}

    # 3. При недоступности Auth возвращается 503 (экран Э4 для HTML и 503 JSON для API)
    fake_auth.profile_status = 503
    ui_degraded = await client.get('/billing/protected-probe')
    assert ui_degraded.status_code == 503
    assert 'Сервис авторизации временно недоступен' in ui_degraded.text

    api_degraded = await client.get('/api/v1/billing/protected-probe')
    assert api_degraded.status_code == 503
    assert api_degraded.json()['error_code'] == 'auth_unavailable'
