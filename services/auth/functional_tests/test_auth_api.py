from uuid import uuid4

import httpx
import pytest


def _assert_status(response: httpx.Response, expected: int) -> None:
    assert response.status_code == expected, response.text


@pytest.mark.asyncio
async def test_yandex_login_is_explicitly_unavailable_without_configuration(
    http_client: httpx.AsyncClient,
) -> None:
    """Disabled social login must return a stable service error."""
    response = await http_client.get(
        '/api/v1/auth/social/yandex/login',
        params={'tz': 'Europe/Moscow'},
        follow_redirects=False,
    )

    _assert_status(response, 503)
    assert response.json() == {'detail': 'Yandex OAuth is not configured'}


@pytest.mark.asyncio
async def test_complete_authentication_lifecycle(
    http_client: httpx.AsyncClient,
) -> None:
    """Проверяет регистрацию, вход, профиль, refresh и отзыв токенов."""
    suffix = uuid4().hex[:12]
    login = f'FunctionalUser{suffix}'
    updated_login = f'UpdatedUser{suffix}'
    password = 'Cobalt-Tundra-Functional-User-2026!'
    updated_password = 'Quartz-Citadel-Functional-User-2026!'

    signup = await http_client.post(
        '/api/v1/auth/signup',
        json={
            'login': login,
            'password': password,
            'first_name': 'Functional',
            'last_name': 'Test',
            'tz': 'Europe/Moscow',
        },
    )
    _assert_status(signup, 201)
    user = signup.json()
    assert user['login'] == login
    assert 'password' not in user
    assert 'password_hash' not in user

    duplicate = await http_client.post(
        '/api/v1/auth/signup',
        json={'login': login, 'password': password, 'tz': 'Europe/Moscow'},
    )
    _assert_status(duplicate, 409)

    login_response = await http_client.post(
        '/api/v1/auth/login',
        json={'login': login, 'password': password},
        headers={
            'X-Forwarded-For': '203.0.113.10',
            'User-Agent': 'auth-functional-tests',
        },
    )
    _assert_status(login_response, 200)
    first_tokens = login_response.json()

    bearer = {'Authorization': f'Bearer {first_tokens["access_token"]}'}
    history = await http_client.get(
        '/api/v1/auth/login-history',
        headers=bearer,
    )
    _assert_status(history, 200)
    assert history.json()[0]['user_id'] == user['id']
    assert history.json()[0]['user_agent'] == 'auth-functional-tests'

    profile = await http_client.patch(
        '/api/v1/auth/profile',
        json={
            'new_login': updated_login,
            'new_password': updated_password,
        },
        headers=bearer,
    )
    _assert_status(profile, 200)
    assert profile.json()['login'] == updated_login

    old_refresh = await http_client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': first_tokens['refresh_token']},
    )
    _assert_status(old_refresh, 401)
    assert old_refresh.json() == {'detail': 'Invalid refresh token'}

    old_access = await http_client.get(
        '/api/v1/auth/login-history',
        headers=bearer,
    )
    _assert_status(old_access, 401)
    assert old_access.json() == {'detail': 'Token revoked'}

    old_password = await http_client.post(
        '/api/v1/auth/login',
        json={'login': updated_login, 'password': password},
    )
    _assert_status(old_password, 401)
    assert old_password.json() == {'detail': 'Invalid credentials'}

    updated_login_response = await http_client.post(
        '/api/v1/auth/login',
        json={'login': updated_login, 'password': updated_password},
    )
    _assert_status(updated_login_response, 200)
    current_tokens = updated_login_response.json()

    refresh = await http_client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': current_tokens['refresh_token']},
    )
    _assert_status(refresh, 200)
    rotated_tokens = refresh.json()
    assert rotated_tokens['refresh_token'] != current_tokens['refresh_token']

    replay = await http_client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': current_tokens['refresh_token']},
    )
    _assert_status(replay, 401)

    rotated_bearer = {
        'Authorization': f'Bearer {rotated_tokens["access_token"]}',
    }
    logout = await http_client.post(
        '/api/v1/auth/logout',
        json={'refresh_token': rotated_tokens['refresh_token']},
        headers=rotated_bearer,
    )
    _assert_status(logout, 204)
    assert logout.content == b''

    revoked_access = await http_client.get(
        '/api/v1/auth/login-history',
        headers=rotated_bearer,
    )
    _assert_status(revoked_access, 401)

    revoked_refresh = await http_client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': rotated_tokens['refresh_token']},
    )
    _assert_status(revoked_refresh, 401)
