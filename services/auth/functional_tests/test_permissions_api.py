from uuid import uuid4

import httpx
import pytest


def _assert_status(response: httpx.Response, expected: int) -> None:
    assert response.status_code == expected, response.text


def _film_request(creation_date: str | None = None) -> dict[str, object]:
    attributes = {}
    if creation_date is not None:
        attributes['creation_date'] = creation_date
    return {
        'resource': 'film',
        'action': 'read',
        'attributes': attributes,
    }


async def _login(
    client: httpx.AsyncClient,
    *,
    login: str,
    password: str,
) -> dict[str, str]:
    response = await client.post(
        '/api/v1/auth/login',
        json={'login': login, 'password': password},
    )
    _assert_status(response, 200)
    return response.json()


@pytest.mark.asyncio
async def test_film_policy_for_anonymous_user(
    http_client: httpx.AsyncClient,
    functional_settings,
) -> None:
    """Проверяет служебный ключ, неизвестную дату и правило трёх лет."""
    old_film = _film_request('2000-01-01')

    missing_key = await http_client.post(
        '/api/v1/auth/permissions/check',
        json=old_film,
    )
    _assert_status(missing_key, 401)

    invalid_key = await http_client.post(
        '/api/v1/auth/permissions/check',
        json=old_film,
        headers={'X-Service-Key': 'invalid-service-key'},
    )
    _assert_status(invalid_key, 401)

    service_headers = {
        'X-Service-Key': functional_settings.service_api_key,
    }
    public = await http_client.post(
        '/api/v1/auth/permissions/check',
        json=old_film,
        headers=service_headers,
    )
    _assert_status(public, 200)
    assert public.json() == {'allowed': True}

    recent = await http_client.post(
        '/api/v1/auth/permissions/check',
        json=_film_request('9999-01-01'),
        headers=service_headers,
    )
    _assert_status(recent, 200)
    assert recent.json() == {'allowed': False}

    missing_date = await http_client.post(
        '/api/v1/auth/permissions/check',
        json=_film_request(),
        headers=service_headers,
    )
    _assert_status(missing_date, 200)
    assert missing_date.json() == {'allowed': False}


@pytest.mark.asyncio
async def test_role_change_immediately_changes_film_access(
    http_client: httpx.AsyncClient,
    functional_settings,
) -> None:
    """Проверяет CRUD роли и немедленную инвалидацию снимка пользователя."""
    suffix = uuid4().hex[:12]
    target_login = f'RoleTarget{suffix}'
    target_password = 'Saffron-Citadel-Role-Target-2026!'
    role_name = f'subscriber-{suffix}'

    signup = await http_client.post(
        '/api/v1/auth/signup',
        json={
            'login': target_login,
            'password': target_password,
            'tz': 'Europe/Moscow',
        },
    )
    _assert_status(signup, 201)
    target_id = signup.json()['id']

    target_tokens = await _login(
        http_client,
        login=target_login,
        password=target_password,
    )
    root_tokens = await _login(
        http_client,
        login=functional_settings.superuser_login,
        password=functional_settings.superuser_password,
    )
    root_headers = {
        'Authorization': f'Bearer {root_tokens["access_token"]}',
    }

    created = await http_client.post(
        '/api/v1/auth/roles',
        json={
            'name': role_name,
            'permissions': ['films.early_access:read'],
        },
        headers=root_headers,
    )
    _assert_status(created, 201)
    role_id = created.json()['id']

    try:
        updated = await http_client.patch(
            f'/api/v1/auth/roles/{role_id}',
            json={'name': f'{role_name}-updated'},
            headers=root_headers,
        )
        _assert_status(updated, 200)

        roles = await http_client.get(
            '/api/v1/auth/roles',
            headers=root_headers,
        )
        _assert_status(roles, 200)
        assert role_id in {role['id'] for role in roles.json()}

        assigned = await http_client.post(
            f'/api/v1/auth/users/{target_id}/roles',
            json={'role_id': role_id},
            headers=root_headers,
        )
        _assert_status(assigned, 204)

        permission_headers = {
            'X-Service-Key': functional_settings.service_api_key,
            'Authorization': (f'Bearer {target_tokens["access_token"]}'),
        }
        allowed = await http_client.post(
            '/api/v1/auth/permissions/check',
            json=_film_request('9999-01-01'),
            headers=permission_headers,
        )
        _assert_status(allowed, 200)
        assert allowed.json() == {'allowed': True}

        allowed_without_date = await http_client.post(
            '/api/v1/auth/permissions/check',
            json=_film_request(),
            headers=permission_headers,
        )
        _assert_status(allowed_without_date, 200)
        assert allowed_without_date.json() == {'allowed': True}

        revoked = await http_client.delete(
            f'/api/v1/auth/users/{target_id}/roles/{role_id}',
            headers=root_headers,
        )
        _assert_status(revoked, 204)

        denied = await http_client.post(
            '/api/v1/auth/permissions/check',
            json=_film_request('9999-01-01'),
            headers=permission_headers,
        )
        _assert_status(denied, 200)
        assert denied.json() == {'allowed': False}
    finally:
        deleted = await http_client.delete(
            f'/api/v1/auth/roles/{role_id}',
            headers=root_headers,
        )
        _assert_status(deleted, 204)
