from __future__ import annotations

from datetime import datetime, timezone
from unittest import TestCase as UnitTestCase
from uuid import UUID, uuid4

from django.contrib.auth import get_user_model
from django.contrib.sessions.middleware import SessionMiddleware
from django.test import RequestFactory, TestCase

from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_LOGIN_ERROR_ATTRIBUTE,
    AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
    AuthLoginError,
    AuthServiceBackend,
)
from users.auth_client import (
    AuthForbiddenError,
    AuthInvalidCredentialsError,
    AuthRateLimitedError,
    AuthRole,
    AuthUnavailableError,
    AuthUser,
    TokenPair,
)
from users.models import AuthIdentity

User = get_user_model()


class FakeAuthClient:
    def __init__(
        self,
        *,
        profile: AuthUser | None = None,
        error: Exception | None = None,
    ) -> None:
        self.profile = profile or make_profile()
        self.error = error
        self.calls: list[tuple[str, object]] = []

    def login(
        self,
        *,
        login: str,
        password: str,
        request_id: str,
    ) -> TokenPair:
        self.calls.append(('login', request_id))
        if self.error is not None:
            raise self.error
        return TokenPair('access-token', 'refresh-token')

    def get_profile(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> AuthUser:
        del access_token
        self.calls.append(('profile', request_id))
        if self.error is not None:
            raise self.error
        return self.profile

    def list_roles(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[AuthRole, ...]:
        del access_token
        self.calls.append(('roles', request_id))
        if self.error is not None:
            raise self.error
        return (
            AuthRole(
                id=uuid4(),
                name='admin',
                permissions=('roles:manage', 'user_roles:manage'),
                created_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
            ),
        )


def make_profile(
    *,
    user_id: UUID | None = None,
    login: str = 'Admin',
    first_name: str | None = 'Ada',
    last_name: str | None = 'Lovelace',
) -> AuthUser:
    return AuthUser(
        id=user_id or UUID('11111111-1111-4111-8111-111111111111'),
        login=login,
        first_name=first_name,
        last_name=last_name,
        created_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
    )


def request_with_session():
    request = RequestFactory().post(
        '/admin/login/',
        headers={'X-Request-Id': 'admin-backend-request'},
    )
    SessionMiddleware(lambda current_request: None).process_request(request)
    return request


class AuthServiceBackendTests(TestCase):
    def test_first_successful_login_creates_one_technical_user_and_identity(
        self,
    ) -> None:
        request = request_with_session()
        auth_client = FakeAuthClient()
        backend = AuthServiceBackend(auth_client=auth_client)

        user = backend.authenticate(
            request,
            username='Admin',
            password='secret-password',
        )

        self.assertIsNotNone(user)
        assert user is not None
        identity = AuthIdentity.objects.get()
        self.assertEqual(User.objects.count(), 1)
        self.assertEqual(identity.user_id, user.id)
        self.assertEqual(identity.auth_user_id, auth_client.profile.id)
        self.assertEqual(identity.login, 'Admin')
        self.assertEqual(user.username, f'auth_{auth_client.profile.id.hex}')
        self.assertEqual(user.first_name, 'Ada')
        self.assertEqual(user.last_name, 'Lovelace')
        self.assertTrue(user.is_active)
        self.assertTrue(user.is_staff)
        self.assertTrue(user.is_superuser)
        self.assertFalse(user.has_usable_password())
        self.assertEqual(
            request.session[AUTH_ACCESS_TOKEN_SESSION_KEY],
            'access-token',
        )
        self.assertEqual(
            request.session[AUTH_REFRESH_TOKEN_SESSION_KEY],
            'refresh-token',
        )
        self.assertEqual(
            [name for name, _ in auth_client.calls],
            ['login', 'profile', 'roles'],
        )
        self.assertEqual(
            [request_id for _, request_id in auth_client.calls],
            ['admin-backend-request'] * 3,
        )

    def test_repeat_login_updates_profile_without_changing_local_ids(self) -> None:
        auth_user_id = UUID('11111111-1111-4111-8111-111111111111')
        first_backend = AuthServiceBackend(
            auth_client=FakeAuthClient(profile=make_profile(user_id=auth_user_id))
        )
        first_user = first_backend.authenticate(
            request_with_session(),
            username='Admin',
            password='secret-password',
        )
        assert first_user is not None
        first_identity = AuthIdentity.objects.get()
        local_user_id = first_user.id
        local_identity_id = first_identity.id

        updated_profile = make_profile(
            user_id=auth_user_id,
            login='AdminRenamed',
            first_name='Grace',
            last_name='Hopper',
        )
        second_user = AuthServiceBackend(
            auth_client=FakeAuthClient(profile=updated_profile)
        ).authenticate(
            request_with_session(),
            username='AdminRenamed',
            password='secret-password',
        )

        assert second_user is not None
        first_identity.refresh_from_db()
        self.assertEqual(User.objects.count(), 1)
        self.assertEqual(AuthIdentity.objects.count(), 1)
        self.assertEqual(second_user.id, local_user_id)
        self.assertEqual(first_identity.id, local_identity_id)
        self.assertEqual(first_identity.login, 'AdminRenamed')
        self.assertEqual(second_user.first_name, 'Grace')
        self.assertEqual(second_user.last_name, 'Hopper')
        self.assertEqual(second_user.username, f'auth_{auth_user_id.hex}')

    def test_stale_login_copy_does_not_block_another_auth_user(self) -> None:
        first_id = UUID('11111111-1111-4111-8111-111111111111')
        second_id = UUID('22222222-2222-4222-8222-222222222222')
        backend = AuthServiceBackend(
            auth_client=FakeAuthClient(
                profile=make_profile(user_id=first_id, login='ReusedLogin')
            )
        )
        first_user = backend.authenticate(
            request_with_session(),
            username='ReusedLogin',
            password='secret-password',
        )
        assert first_user is not None

        second_user = AuthServiceBackend(
            auth_client=FakeAuthClient(
                profile=make_profile(user_id=second_id, login='ReusedLogin')
            )
        ).authenticate(
            request_with_session(),
            username='ReusedLogin',
            password='secret-password',
        )

        self.assertIsNotNone(second_user)
        self.assertEqual(User.objects.count(), 2)
        self.assertEqual(AuthIdentity.objects.count(), 2)
        self.assertEqual(
            AuthIdentity.objects.filter(login='ReusedLogin').count(),
            2,
        )

    def test_auth_failures_do_not_create_local_rows(self) -> None:
        cases = (
            (
                AuthInvalidCredentialsError(),
                AuthLoginError.INVALID_CREDENTIALS,
            ),
            (AuthForbiddenError(), AuthLoginError.FORBIDDEN),
            (AuthUnavailableError(), AuthLoginError.UNAVAILABLE),
        )
        for error, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                request = request_with_session()
                backend = AuthServiceBackend(
                    auth_client=FakeAuthClient(error=error)
                )

                user = backend.authenticate(
                    request,
                    username='Admin',
                    password='secret-password',
                )

                self.assertIsNone(user)
                self.assertEqual(
                    getattr(request, AUTH_LOGIN_ERROR_ATTRIBUTE),
                    expected_reason,
                )
                self.assertEqual(User.objects.count(), 0)
                self.assertEqual(AuthIdentity.objects.count(), 0)
                self.assertNotIn(
                    AUTH_ACCESS_TOKEN_SESSION_KEY,
                    request.session,
                )

    def test_rate_limited_login_keeps_retry_after_without_local_rows(
        self,
    ) -> None:
        request = request_with_session()
        backend = AuthServiceBackend(
            auth_client=FakeAuthClient(error=AuthRateLimitedError(12))
        )

        user = backend.authenticate(
            request,
            username='Admin',
            password='secret-password',
        )

        self.assertIsNone(user)
        self.assertEqual(
            getattr(request, AUTH_LOGIN_ERROR_ATTRIBUTE),
            AuthLoginError.RATE_LIMITED,
        )
        self.assertEqual(
            getattr(request, AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE),
            12,
        )
        self.assertEqual(User.objects.count(), 0)
        self.assertEqual(AuthIdentity.objects.count(), 0)

    def test_get_user_restores_only_local_user_by_primary_key(self) -> None:
        local_user = User.objects.create_user(username='technical')
        backend = AuthServiceBackend(auth_client=FakeAuthClient())

        restored = backend.get_user(local_user.id)
        missing = backend.get_user(local_user.id + 1)

        self.assertEqual(restored, local_user)
        self.assertIsNone(missing)


class AuthenticationSettingsTests(UnitTestCase):
    def test_model_backend_is_not_enabled(self) -> None:
        from django.conf import settings

        self.assertEqual(
            settings.AUTHENTICATION_BACKENDS,
            ['users.auth_backend.AuthServiceBackend'],
        )
