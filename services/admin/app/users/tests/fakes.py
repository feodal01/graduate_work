from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from users.auth_client import AuthRole, AuthUser, TokenPair


ROLE_ID = UUID('22222222-2222-4222-8222-222222222222')
AUTH_USER_ID = UUID('11111111-1111-4111-8111-111111111111')


class FakeAdminAuthClient:
    def __init__(self) -> None:
        self.profile = AuthUser(
            id=AUTH_USER_ID,
            login='Admin',
            first_name='Ada',
            last_name='Lovelace',
            created_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
        )
        self.roles = (
            AuthRole(
                id=ROLE_ID,
                name='admin',
                permissions=('roles:manage', 'user_roles:manage'),
                created_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
            ),
        )
        self.errors: dict[str, Exception] = {}
        self.calls: list[tuple[str, object]] = []

    def _raise(self, operation: str) -> None:
        error = self.errors.get(operation)
        if error is not None:
            raise error

    def login(
        self,
        *,
        login: str,
        password: str,
        request_id: str,
    ) -> TokenPair:
        self.calls.append(
            (
                'login',
                {
                    'login': login,
                    'password': password,
                    'request_id': request_id,
                },
            )
        )
        self._raise('login')
        return TokenPair('access-token', 'refresh-token')

    def get_profile(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> AuthUser:
        self.calls.append(
            (
                'profile',
                {'access_token': access_token, 'request_id': request_id},
            )
        )
        self._raise('profile')
        return self.profile

    def list_roles(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[AuthRole, ...]:
        self.calls.append(
            (
                'list_roles',
                {'access_token': access_token, 'request_id': request_id},
            )
        )
        self._raise('list_roles')
        return self.roles

    def refresh(self, refresh_token: str, *, request_id: str) -> TokenPair:
        self.calls.append(
            (
                'refresh',
                {'refresh_token': refresh_token, 'request_id': request_id},
            )
        )
        self._raise('refresh')
        return TokenPair('new-access-token', 'new-refresh-token')

    def logout(
        self,
        *,
        access_token: str,
        refresh_token: str,
        request_id: str,
    ) -> None:
        self.calls.append(
            (
                'logout',
                {
                    'access_token': access_token,
                    'refresh_token': refresh_token,
                    'request_id': request_id,
                },
            )
        )
        self._raise('logout')

    def create_role(self, **kwargs):
        self.calls.append(('create_role', kwargs))
        self._raise('create_role')
        return self.roles[0]

    def update_role(self, **kwargs):
        self.calls.append(('update_role', kwargs))
        self._raise('update_role')
        return self.roles[0]

    def delete_role(self, **kwargs) -> None:
        self.calls.append(('delete_role', kwargs))
        self._raise('delete_role')

    def assign_role(self, **kwargs) -> None:
        self.calls.append(('assign_role', kwargs))
        self._raise('assign_role')

    def revoke_role(self, **kwargs) -> None:
        self.calls.append(('revoke_role', kwargs))
        self._raise('revoke_role')
