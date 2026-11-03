from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.sessions.middleware import SessionMiddleware
from django.http import HttpResponse
from django.test import RequestFactory, TestCase

from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.auth_client import (
    AuthForbiddenError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
)
from users.auth_middleware import AuthAuthorizationMiddleware
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


def protected_request(user):
    request = RequestFactory().get(
        '/admin/movies/genre/',
        headers={'X-Request-Id': 'admin-middleware-request'},
    )
    SessionMiddleware(lambda current_request: None).process_request(request)
    request.session[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
    request.session[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
    request.user = user
    return request


class AuthAuthorizationMiddlewareTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='technical',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )

    def test_success_exposes_roles_and_calls_protected_view(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        called = []
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: called.append(current_request)
            or HttpResponse('view-called'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(called, [request])
        self.assertEqual(request.auth_roles, auth_client.roles)
        self.assertEqual(
            auth_client.calls,
            [
                (
                    'list_roles',
                    {
                        'access_token': 'access-token',
                        'request_id': 'admin-middleware-request',
                    },
                )
            ],
        )

    def test_unauthorized_access_refreshes_once_and_retries_roles(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        attempts = 0
        original_list_roles = auth_client.list_roles

        def list_roles(access_token, *, request_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise AuthUnauthorizedError
            return original_list_roles(
                access_token,
                request_id=request_id,
            )

        auth_client.list_roles = list_roles
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: HttpResponse('view-called'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)
        self.assertEqual(
            request.session[AUTH_ACCESS_TOKEN_SESSION_KEY],
            'new-access-token',
        )
        self.assertEqual(
            request.session[AUTH_REFRESH_TOKEN_SESSION_KEY],
            'new-refresh-token',
        )
        self.assertEqual(
            [
                value['request_id']
                for _, value in auth_client.calls
            ],
            ['admin-middleware-request'] * 2,
        )

    def test_rejected_refresh_ends_local_session(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        auth_client.errors['list_roles'] = AuthUnauthorizedError()
        auth_client.errors['refresh'] = AuthUnauthorizedError()
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: HttpResponse('must-not-run'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 302)
        self.assertIn('/admin/login/', response['Location'])
        self.assertNotIn(AUTH_ACCESS_TOKEN_SESSION_KEY, request.session)

    def test_forbidden_roles_ends_local_session(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        auth_client.errors['list_roles'] = AuthForbiddenError()
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: HttpResponse('must-not-run'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 302)
        self.assertIn('/admin/login/', response['Location'])
        self.assertNotIn(AUTH_ACCESS_TOKEN_SESSION_KEY, request.session)

    def test_auth_unavailable_returns_503_without_calling_view(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        auth_client.errors['list_roles'] = AuthUnavailableError()
        called = []
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: called.append(current_request)
            or HttpResponse('must-not-run'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(called, [])
        self.assertEqual(
            request.session[AUTH_ACCESS_TOKEN_SESSION_KEY],
            'access-token',
        )

    def test_auth_rate_limit_returns_429_without_calling_view(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        auth_client.errors['list_roles'] = AuthRateLimitedError(7)
        called = []
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: called.append(current_request)
            or HttpResponse('must-not-run'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response['Retry-After'], '7')
        self.assertEqual(called, [])
        self.assertIn(AUTH_ACCESS_TOKEN_SESSION_KEY, request.session)

    def test_refresh_rate_limit_returns_429_without_calling_view(self) -> None:
        request = protected_request(self.user)
        auth_client = FakeAdminAuthClient()
        auth_client.errors['list_roles'] = AuthUnauthorizedError()
        auth_client.errors['refresh'] = AuthRateLimitedError(6)
        called = []
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: called.append(current_request)
            or HttpResponse('must-not-run'),
            auth_client=auth_client,
        )

        response = middleware(request)

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response['Retry-After'], '6')
        self.assertEqual(called, [])

    def test_login_logout_and_non_admin_routes_skip_auth(self) -> None:
        auth_client = FakeAdminAuthClient()
        middleware = AuthAuthorizationMiddleware(
            lambda current_request: HttpResponse('view-called'),
            auth_client=auth_client,
        )
        for path in ('/admin/login/', '/admin/logout/', '/health/'):
            with self.subTest(path=path):
                request = RequestFactory().get(path)
                SessionMiddleware(
                    lambda current_request: None
                ).process_request(request)
                request.user = self.user

                response = middleware(request)

                self.assertEqual(response.status_code, 200)
        self.assertEqual(auth_client.calls, [])
