from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from unittest import TestCase
from unittest.mock import patch
from uuid import UUID

import httpx
from django.test import override_settings

from users.auth_client import (
    AuthClient,
    AuthContractError,
    AuthForbiddenError,
    AuthInvalidCredentialsError,
    AuthOperationError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})

    def json(self) -> Any:
        return self._payload


class FakeHTTPClient:
    def __init__(self, *responses: FakeResponse | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append({'method': method, 'url': url, **kwargs})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class AuthClientTests(TestCase):
    base_url = 'http://auth:8000/api/v1/auth'
    request_id = 'admin-request-id'

    def test_login_sends_credentials_and_returns_strict_token_pair(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                200,
                {
                    'access_token': 'access-token',
                    'refresh_token': 'refresh-token',
                    'token_type': 'bearer',
                },
            )
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        pair = client.login(
            login='Admin',
            password='secret-password',
            request_id=self.request_id,
        )

        self.assertEqual(pair.access_token, 'access-token')
        self.assertEqual(pair.refresh_token, 'refresh-token')
        self.assertEqual(
            http.requests,
            [
                {
                    'method': 'POST',
                    'url': f'{self.base_url}/login',
                    'json': {
                        'login': 'Admin',
                        'password': 'secret-password',
                    },
                    'headers': {'X-Request-Id': self.request_id},
                }
            ],
        )

    def test_profile_returns_typed_auth_user(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                200,
                {
                    'id': '11111111-1111-4111-8111-111111111111',
                    'login': 'Admin',
                    'first_name': 'Ada',
                    'last_name': 'Lovelace',
                    'created_at': '2026-08-25T12:00:00Z',
                },
            )
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        profile = client.get_profile(
            'access-token',
            request_id=self.request_id,
        )

        self.assertEqual(
            profile.id,
            UUID('11111111-1111-4111-8111-111111111111'),
        )
        self.assertEqual(profile.login, 'Admin')
        self.assertEqual(profile.first_name, 'Ada')
        self.assertEqual(profile.last_name, 'Lovelace')
        self.assertEqual(
            profile.created_at,
            datetime.fromisoformat('2026-08-25T12:00:00+00:00'),
        )
        self.assertEqual(
            http.requests[0],
            {
                'method': 'GET',
                'url': f'{self.base_url}/profile',
                'headers': {
                    'Authorization': 'Bearer access-token',
                    'X-Request-Id': self.request_id,
                },
            },
        )

    def test_list_roles_returns_typed_roles(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                200,
                [
                    {
                        'id': '22222222-2222-4222-8222-222222222222',
                        'name': 'admin',
                        'permissions': [
                            'roles:manage',
                            'user_roles:manage',
                        ],
                        'created_at': '2026-08-25T12:00:00Z',
                    }
                ],
            )
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        roles = client.list_roles(
            'access-token',
            request_id=self.request_id,
        )

        self.assertEqual(len(roles), 1)
        self.assertEqual(roles[0].name, 'admin')
        self.assertEqual(
            roles[0].permissions,
            ('roles:manage', 'user_roles:manage'),
        )

    def test_role_mutations_use_auth_contract(self) -> None:
        role_id = UUID('22222222-2222-4222-8222-222222222222')
        user_id = UUID('33333333-3333-4333-8333-333333333333')
        role_payload = {
            'id': str(role_id),
            'name': 'reviewer',
            'permissions': ['films.early_access:read'],
            'created_at': '2026-08-25T12:00:00Z',
        }
        http = FakeHTTPClient(
            FakeResponse(201, role_payload),
            FakeResponse(200, role_payload),
            FakeResponse(204),
            FakeResponse(204),
            FakeResponse(204),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        created = client.create_role(
            access_token='access-token',
            name='reviewer',
            permissions={'films.early_access:read'},
            request_id=self.request_id,
        )
        updated = client.update_role(
            access_token='access-token',
            role_id=role_id,
            name='reviewer',
            permissions={'films.early_access:read'},
            request_id=self.request_id,
        )
        client.delete_role(
            access_token='access-token',
            role_id=role_id,
            request_id=self.request_id,
        )
        client.assign_role(
            access_token='access-token',
            user_id=user_id,
            role_id=role_id,
            request_id=self.request_id,
        )
        client.revoke_role(
            access_token='access-token',
            user_id=user_id,
            role_id=role_id,
            request_id=self.request_id,
        )

        self.assertEqual(created.id, role_id)
        self.assertEqual(updated.id, role_id)
        self.assertEqual(
            [(item['method'], item['url']) for item in http.requests],
            [
                ('POST', f'{self.base_url}/roles'),
                ('PATCH', f'{self.base_url}/roles/{role_id}'),
                ('DELETE', f'{self.base_url}/roles/{role_id}'),
                ('POST', f'{self.base_url}/users/{user_id}/roles'),
                (
                    'DELETE',
                    f'{self.base_url}/users/{user_id}/roles/{role_id}',
                ),
            ],
        )

    def test_refresh_and_logout_use_token_pair(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                200,
                {
                    'access_token': 'new-access',
                    'refresh_token': 'new-refresh',
                    'token_type': 'bearer',
                },
            ),
            FakeResponse(204),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        pair = client.refresh('old-refresh', request_id=self.request_id)
        client.logout(
            access_token=pair.access_token,
            refresh_token=pair.refresh_token,
            request_id=self.request_id,
        )

        self.assertEqual(pair.access_token, 'new-access')
        self.assertEqual(
            http.requests[1],
            {
                'method': 'POST',
                'url': f'{self.base_url}/logout',
                'headers': {
                    'Authorization': 'Bearer new-access',
                    'X-Request-Id': self.request_id,
                },
                'json': {'refresh_token': 'new-refresh'},
            },
        )

    def test_http_statuses_keep_distinct_meanings(self) -> None:
        cases = (
            (401, AuthUnauthorizedError),
            (403, AuthForbiddenError),
            (500, AuthUnavailableError),
        )
        for status_code, expected_error in cases:
            with self.subTest(status_code=status_code):
                http = FakeHTTPClient(
                    FakeResponse(status_code, {'detail': 'error'}),
                    FakeResponse(200, []),
                )
                client = AuthClient(
                    base_url=self.base_url,
                    http_client=http,
                )

                with self.assertRaises(expected_error):
                    client.list_roles(
                        'access-token',
                        request_id=self.request_id,
                    )

                self.assertEqual(len(http.requests), 1)

    def test_rate_limit_preserves_retry_after_without_retry(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                429,
                {'detail': 'Too many requests'},
                headers={'Retry-After': '12'},
            ),
            FakeResponse(200, []),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with self.assertRaises(AuthRateLimitedError) as raised:
            client.list_roles(
                'access-token',
                request_id=self.request_id,
            )

        self.assertEqual(raised.exception.retry_after_seconds, 12)
        self.assertEqual(len(http.requests), 1)

    def test_rate_limit_ignores_invalid_retry_after(self) -> None:
        headers = (
            {},
            {'Retry-After': '0'},
            {'Retry-After': '-1'},
            {'Retry-After': 'later'},
        )
        for response_headers in headers:
            with self.subTest(response_headers=response_headers):
                http = FakeHTTPClient(
                    FakeResponse(
                        429,
                        {'detail': 'Too many requests'},
                        headers=response_headers,
                    ),
                    FakeResponse(200, []),
                )
                client = AuthClient(base_url=self.base_url, http_client=http)

                with self.assertRaises(AuthRateLimitedError) as raised:
                    client.list_roles(
                        'access-token',
                        request_id=self.request_id,
                    )

                self.assertIsNone(raised.exception.retry_after_seconds)
                self.assertEqual(len(http.requests), 1)

    def test_login_401_is_invalid_credentials(self) -> None:
        client = AuthClient(
            base_url=self.base_url,
            http_client=FakeHTTPClient(
                FakeResponse(401, {'detail': 'Invalid credentials'})
            ),
        )

        with self.assertRaises(AuthInvalidCredentialsError):
            client.login(
                login='Admin',
                password='wrong-password',
                request_id=self.request_id,
            )

    def test_expected_role_errors_are_available_to_forms(self) -> None:
        for status_code in (404, 409, 422):
            with self.subTest(status_code=status_code):
                client = AuthClient(
                    base_url=self.base_url,
                    http_client=FakeHTTPClient(
                        FakeResponse(status_code, {'detail': 'role error'})
                    ),
                )

                with self.assertRaises(AuthOperationError) as raised:
                    client.create_role(
                        access_token='access-token',
                        name='reviewer',
                        permissions=set(),
                        request_id=self.request_id,
                    )

                self.assertEqual(raised.exception.status_code, status_code)
                self.assertEqual(raised.exception.detail, 'role error')

    def test_success_response_with_wrong_shape_is_contract_error(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(
                200,
                {
                    'access_token': 'access-token',
                    'refresh_token': 'refresh-token',
                    'token_type': 'wrong-type',
                },
            ),
            FakeResponse(
                200,
                {
                    'access_token': 'second-access-token',
                    'refresh_token': 'second-refresh-token',
                    'token_type': 'bearer',
                },
            ),
        )
        client = AuthClient(
            base_url=self.base_url,
            http_client=http,
        )

        with self.assertRaises(AuthContractError):
            client.login(
                login='Admin',
                password='secret-password',
                request_id=self.request_id,
            )

        self.assertEqual(len(http.requests), 1)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_get_retries_connect_error_then_succeeds(self) -> None:
        http = FakeHTTPClient(
            httpx.ConnectError('temporarily offline'),
            FakeResponse(
                200,
                {
                    'id': '11111111-1111-4111-8111-111111111111',
                    'login': 'Admin',
                    'first_name': 'Ada',
                    'last_name': 'Lovelace',
                    'created_at': '2026-08-25T12:00:00Z',
                },
            ),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'):
            profile = client.get_profile(
                'access-token',
                request_id=self.request_id,
            )

        self.assertEqual(profile.login, 'Admin')
        self.assertEqual(len(http.requests), 2)
        self.assertEqual(
            [
                request['headers']['X-Request-Id']
                for request in http.requests
            ],
            [self.request_id, self.request_id],
        )

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_get_retries_503_then_succeeds(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(503, {'detail': 'temporarily unavailable'}),
            FakeResponse(200, []),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'):
            roles = client.list_roles(
                'access-token',
                request_id=self.request_id,
            )

        self.assertEqual(roles, ())
        self.assertEqual(len(http.requests), 2)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_get_retries_ambiguous_response_failures(self) -> None:
        errors = (
            httpx.ReadTimeout('read timeout'),
            httpx.ReadError('read failed'),
            httpx.RemoteProtocolError('invalid remote response'),
            httpx.ProxyError('proxy unavailable'),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__):
                http = FakeHTTPClient(error, FakeResponse(200, []))
                client = AuthClient(base_url=self.base_url, http_client=http)

                with patch('time.sleep'):
                    roles = client.list_roles(
                        'access-token',
                        request_id=self.request_id,
                    )

                self.assertEqual(roles, ())
                self.assertEqual(len(http.requests), 2)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_exhausted_retry_becomes_auth_unavailable(self) -> None:
        http = FakeHTTPClient(
            httpx.ConnectError('offline'),
            httpx.ConnectError('still offline'),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'), self.assertRaises(AuthUnavailableError):
            client.list_roles(
                'access-token',
                request_id=self.request_id,
            )

        self.assertEqual(len(http.requests), 2)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_post_retries_connect_timeout_then_succeeds(self) -> None:
        http = FakeHTTPClient(
            httpx.ConnectTimeout('temporarily offline'),
            FakeResponse(
                200,
                {
                    'access_token': 'access-token',
                    'refresh_token': 'refresh-token',
                    'token_type': 'bearer',
                },
            ),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'):
            pair = client.login(
                login='Admin',
                password='secret-password',
                request_id=self.request_id,
            )

        self.assertEqual(pair.access_token, 'access-token')
        self.assertEqual(len(http.requests), 2)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_refresh_does_not_retry_ambiguous_read_timeout(self) -> None:
        http = FakeHTTPClient(
            httpx.ReadTimeout('response was not received'),
            FakeResponse(
                200,
                {
                    'access_token': 'new-access-token',
                    'refresh_token': 'new-refresh-token',
                    'token_type': 'bearer',
                },
            ),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'), self.assertRaises(AuthUnavailableError):
            client.refresh(
                'refresh-token',
                request_id=self.request_id,
            )

        self.assertEqual(len(http.requests), 1)

    @override_settings(
        AUTH_CLIENT_BACKOFF_MAX_TRIES=2,
        AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS=0.5,
    )
    def test_post_does_not_retry_503(self) -> None:
        http = FakeHTTPClient(
            FakeResponse(503, {'detail': 'temporarily unavailable'}),
            FakeResponse(204),
        )
        client = AuthClient(base_url=self.base_url, http_client=http)

        with patch('time.sleep'), self.assertRaises(AuthUnavailableError):
            client.logout(
                access_token='access-token',
                refresh_token='refresh-token',
                request_id=self.request_id,
            )

        self.assertEqual(len(http.requests), 1)

    def test_unexpected_exception_is_not_hidden(self) -> None:
        error = RuntimeError('client bug')
        client = AuthClient(
            base_url=self.base_url,
            http_client=FakeHTTPClient(error),
        )

        with self.assertRaises(RuntimeError) as raised:
            client.list_roles(
                'access-token',
                request_id=self.request_id,
            )

        self.assertIs(raised.exception, error)

    def test_local_protocol_error_is_not_hidden(self) -> None:
        error = httpx.LocalProtocolError('invalid request construction')
        client = AuthClient(
            base_url=self.base_url,
            http_client=FakeHTTPClient(error),
        )

        with self.assertRaises(httpx.LocalProtocolError) as raised:
            client.list_roles(
                'access-token',
                request_id=self.request_id,
            )

        self.assertIs(raised.exception, error)
