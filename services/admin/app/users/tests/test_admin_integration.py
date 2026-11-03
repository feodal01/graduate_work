from unittest.mock import patch

from django.contrib.admin.models import ADDITION, LogEntry
from django.contrib.auth import BACKEND_SESSION_KEY, SESSION_KEY, get_user_model
from django.test import TestCase
from django.urls import reverse

from movies.models import Genre
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.auth_client import (
    AuthForbiddenError,
    AuthInvalidCredentialsError,
    AuthRateLimitedError,
    AuthUnavailableError,
)
from users.models import AuthIdentity
from users.tests.fakes import AUTH_USER_ID, FakeAdminAuthClient

User = get_user_model()


class AdminAuthIntegrationTests(TestCase):
    def test_login_form_accepts_auth_login_length(self) -> None:
        response = self.client.get(reverse('admin:login'))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'maxlength="255"')

    def test_successful_auth_login_creates_session_and_local_link(self) -> None:
        auth_client = FakeAdminAuthClient()
        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(
                reverse('admin:login'),
                {'username': 'Admin', 'password': 'secret-password'},
                headers={'X-Request-Id': 'admin-integration-request'},
            )

        self.assertEqual(response.status_code, 302)
        user = User.objects.get()
        identity = AuthIdentity.objects.get()
        session = self.client.session
        self.assertEqual(identity.user_id, user.id)
        self.assertEqual(identity.auth_user_id, AUTH_USER_ID)
        self.assertEqual(session[SESSION_KEY], str(user.id))
        self.assertEqual(
            session[BACKEND_SESSION_KEY],
            'users.auth_backend.AuthServiceBackend',
        )
        self.assertEqual(
            session[AUTH_ACCESS_TOKEN_SESSION_KEY],
            'access-token',
        )
        self.assertEqual(
            session[AUTH_REFRESH_TOKEN_SESSION_KEY],
            'refresh-token',
        )
        self.assertEqual(
            [value['request_id'] for _, value in auth_client.calls],
            ['admin-integration-request'] * 3,
        )

    def test_login_errors_do_not_create_local_rows(self) -> None:
        cases = (
            (AuthInvalidCredentialsError(), 200),
            (AuthForbiddenError(), 200),
            (AuthUnavailableError(), 503),
        )
        for error, expected_status in cases:
            with self.subTest(error=type(error).__name__):
                auth_client = FakeAdminAuthClient()
                auth_client.errors['login'] = error
                with patch(
                    'users.auth_backend.get_auth_client',
                    return_value=auth_client,
                ):
                    response = self.client.post(
                        reverse('admin:login'),
                        {
                            'username': 'Admin',
                            'password': 'secret-password',
                        },
                    )

                self.assertEqual(response.status_code, expected_status)
                self.assertEqual(User.objects.count(), 0)
                self.assertEqual(AuthIdentity.objects.count(), 0)

    def test_rate_limited_login_returns_429_without_local_rows(self) -> None:
        auth_client = FakeAdminAuthClient()
        auth_client.errors['login'] = AuthRateLimitedError(12)

        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(
                reverse('admin:login'),
                {'username': 'Admin', 'password': 'secret-password'},
                headers={'X-Request-Id': 'rate-limited-login'},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response['Retry-After'], '12')
        self.assertContains(
            response,
            'Too many authentication requests',
            status_code=429,
        )
        self.assertEqual(User.objects.count(), 0)
        self.assertEqual(AuthIdentity.objects.count(), 0)

    def test_existing_local_password_cannot_bypass_auth(self) -> None:
        User.objects.create_superuser(
            username='legacy-admin',
            password='local-password',
        )
        auth_client = FakeAdminAuthClient()
        auth_client.errors['login'] = AuthInvalidCredentialsError()

        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(
                reverse('admin:login'),
                {'username': 'legacy-admin', 'password': 'local-password'},
            )

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(SESSION_KEY, self.client.session)

    def test_catalog_change_writes_admin_log_for_technical_user(self) -> None:
        auth_client = FakeAdminAuthClient()
        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            login_response = self.client.post(
                reverse('admin:login'),
                {'username': 'Admin', 'password': 'secret-password'},
            )
        self.assertEqual(login_response.status_code, 302)
        user = User.objects.get()

        with patch(
            'users.auth_middleware.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(
                reverse('admin:movies_genre_add'),
                {'name': 'Temporary genre', 'description': 'For audit test'},
            )

        self.assertEqual(response.status_code, 302)
        genre = Genre.objects.get(name='Temporary genre')
        log_entry = LogEntry.objects.get(
            object_id=str(genre.id),
            action_flag=ADDITION,
        )
        self.assertEqual(log_entry.user_id, user.id)

    def test_logout_always_clears_local_session_when_auth_is_unavailable(
        self,
    ) -> None:
        auth_client = FakeAdminAuthClient()
        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            self.client.post(
                reverse('admin:login'),
                {'username': 'Admin', 'password': 'secret-password'},
            )
        auth_client.errors['logout'] = AuthUnavailableError()

        with patch(
            'example.admin_site.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(reverse('admin:logout'))

        self.assertEqual(response.status_code, 302)
        self.assertNotIn(SESSION_KEY, self.client.session)
        self.assertEqual(User.objects.count(), 1)
        self.assertEqual(AuthIdentity.objects.count(), 1)

    def test_logout_clears_local_session_when_auth_is_rate_limited(
        self,
    ) -> None:
        auth_client = FakeAdminAuthClient()
        with patch(
            'users.auth_backend.get_auth_client',
            return_value=auth_client,
        ):
            self.client.post(
                reverse('admin:login'),
                {'username': 'Admin', 'password': 'secret-password'},
            )
        auth_client.errors['logout'] = AuthRateLimitedError(8)

        with patch(
            'example.admin_site.get_auth_client',
            return_value=auth_client,
        ):
            response = self.client.post(reverse('admin:logout'))

        self.assertEqual(response.status_code, 302)
        self.assertNotIn(SESSION_KEY, self.client.session)
