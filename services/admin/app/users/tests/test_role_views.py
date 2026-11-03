from unittest.mock import patch
from uuid import UUID

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.auth_client import (
    AuthForbiddenError,
    AuthOperationError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
)
from users.tests.fakes import ROLE_ID, FakeAdminAuthClient

User = get_user_model()


class RoleViewsTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='technical',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.user.set_unusable_password()
        self.user.save()
        self.client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        session = self.client.session
        session[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        session[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        session.save()
        self.auth_client = FakeAdminAuthClient()

    def request_with_auth(self, method: str, url: str, data=None):
        with patch(
            'users.auth_middleware.get_auth_client',
            return_value=self.auth_client,
        ), patch(
            'users.auth_views.get_auth_client',
            return_value=self.auth_client,
        ):
            return getattr(self.client, method)(
                url,
                data=data or {},
                headers={'X-Request-Id': 'admin-role-view-request'},
            )

    def test_role_list_uses_roles_from_admin_gate(self) -> None:
        response = self.request_with_auth(
            'get',
            reverse('admin:auth_role_list'),
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'admin')
        self.assertEqual(
            [name for name, _ in self.auth_client.calls],
            ['list_roles'],
        )

    def test_create_role_posts_to_auth_and_redirects(self) -> None:
        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {
                'name': 'reviewer',
                'permissions': ['films.early_access:read'],
            },
        )

        self.assertEqual(response.status_code, 302)
        create_call = next(
            value
            for name, value in self.auth_client.calls
            if name == 'create_role'
        )
        self.assertEqual(create_call['name'], 'reviewer')
        self.assertEqual(
            create_call['permissions'],
            {'films.early_access:read'},
        )
        self.assertEqual(
            create_call['request_id'],
            'admin-role-view-request',
        )

    def test_assignment_and_revocation_post_to_auth(self) -> None:
        user_id = UUID('33333333-3333-4333-8333-333333333333')

        assigned = self.request_with_auth(
            'post',
            reverse('admin:auth_user_role_add'),
            {'user_id': str(user_id), 'role_id': str(ROLE_ID)},
        )
        revoked = self.request_with_auth(
            'post',
            reverse('admin:auth_user_role_delete'),
            {'user_id': str(user_id), 'role_id': str(ROLE_ID)},
        )

        self.assertEqual(assigned.status_code, 302)
        self.assertEqual(revoked.status_code, 302)
        self.assertIn(
            'assign_role',
            [name for name, _ in self.auth_client.calls],
        )
        self.assertIn(
            'revoke_role',
            [name for name, _ in self.auth_client.calls],
        )

    def test_target_forbidden_keeps_session_and_returns_403(self) -> None:
        self.auth_client.errors['create_role'] = AuthForbiddenError()

        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {'name': 'reviewer', 'permissions': []},
        )

        self.assertEqual(response.status_code, 403)
        self.assertIn(AUTH_ACCESS_TOKEN_SESSION_KEY, self.client.session)

    def test_target_operation_error_is_shown_in_form(self) -> None:
        self.auth_client.errors['create_role'] = AuthOperationError(
            409,
            'Role already exists',
        )

        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {'name': 'reviewer', 'permissions': []},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Role already exists')

    def test_target_unauthorized_ends_session(self) -> None:
        self.auth_client.errors['create_role'] = AuthUnauthorizedError()

        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {'name': 'reviewer', 'permissions': []},
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn('/admin/login/', response['Location'])
        self.assertNotIn(AUTH_ACCESS_TOKEN_SESSION_KEY, self.client.session)

    def test_target_unavailable_returns_503(self) -> None:
        self.auth_client.errors['create_role'] = AuthUnavailableError()

        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {'name': 'reviewer', 'permissions': []},
        )

        self.assertEqual(response.status_code, 503)

    def test_target_rate_limit_returns_429_without_success(self) -> None:
        self.auth_client.errors['create_role'] = AuthRateLimitedError(9)

        response = self.request_with_auth(
            'post',
            reverse('admin:auth_role_add'),
            {'name': 'reviewer', 'permissions': []},
        )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response['Retry-After'], '9')
        self.assertNotContains(
            response,
            'Role created.',
            status_code=429,
        )
