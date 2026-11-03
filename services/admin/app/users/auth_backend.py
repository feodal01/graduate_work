from __future__ import annotations

from enum import Enum
from typing import Any

from django.contrib.auth import get_user_model
from django.contrib.auth.backends import BaseBackend
from django.db import transaction

from users.auth_client import (
    AuthClient,
    AuthContractError,
    AuthForbiddenError,
    AuthInvalidCredentialsError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
    AuthUser,
    get_auth_client,
)
from users.models import AuthIdentity
from users.request_id import get_request_id

AUTH_ACCESS_TOKEN_SESSION_KEY = 'auth_access_token'
AUTH_REFRESH_TOKEN_SESSION_KEY = 'auth_refresh_token'
AUTH_LOGIN_ERROR_ATTRIBUTE = 'auth_login_error'
AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE = 'auth_login_retry_after'


class AuthLoginError(str, Enum):
    INVALID_CREDENTIALS = 'invalid_credentials'
    FORBIDDEN = 'forbidden'
    RATE_LIMITED = 'rate_limited'
    UNAVAILABLE = 'unavailable'


class AuthServiceBackend(BaseBackend):
    """Аутентифицирует Django Admin через HTTP API сервиса Auth."""

    def __init__(self, auth_client: AuthClient | None = None) -> None:
        self._auth_client = auth_client

    @property
    def auth_client(self) -> AuthClient:
        return self._auth_client or get_auth_client()

    def authenticate(
        self,
        request: Any,
        username: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ):
        if request is None or username is None or password is None:
            return None
        request_id = get_request_id(request)
        try:
            pair = self.auth_client.login(
                login=username,
                password=password,
                request_id=request_id,
            )
            profile = self.auth_client.get_profile(
                pair.access_token,
                request_id=request_id,
            )
            self.auth_client.list_roles(
                pair.access_token,
                request_id=request_id,
            )
        except AuthInvalidCredentialsError:
            self._set_login_error(request, AuthLoginError.INVALID_CREDENTIALS)
            return None
        except AuthForbiddenError:
            self._set_login_error(request, AuthLoginError.FORBIDDEN)
            return None
        except AuthUnauthorizedError:
            self._set_login_error(request, AuthLoginError.INVALID_CREDENTIALS)
            return None
        except AuthRateLimitedError as exc:
            self._set_login_error(request, AuthLoginError.RATE_LIMITED)
            setattr(
                request,
                AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE,
                exc.retry_after_seconds,
            )
            return None
        except (AuthUnavailableError, AuthContractError):
            self._set_login_error(request, AuthLoginError.UNAVAILABLE)
            return None

        user = self._sync_local_user(profile)
        request.session[AUTH_ACCESS_TOKEN_SESSION_KEY] = pair.access_token
        request.session[AUTH_REFRESH_TOKEN_SESSION_KEY] = pair.refresh_token
        return user

    def get_user(self, user_id: object):
        user_model = get_user_model()
        try:
            return user_model.objects.get(pk=user_id)
        except user_model.DoesNotExist:
            return None

    @staticmethod
    def _set_login_error(request: Any, error: AuthLoginError) -> None:
        setattr(request, AUTH_LOGIN_ERROR_ATTRIBUTE, error)

    @staticmethod
    @transaction.atomic
    def _sync_local_user(profile: AuthUser):
        identity = (
            AuthIdentity.objects.select_for_update()
            .select_related('user')
            .filter(auth_user_id=profile.id)
            .first()
        )
        if identity is None:
            user = get_user_model()(
                username=f'auth_{profile.id.hex}',
            )
            identity = AuthIdentity(user=user, auth_user_id=profile.id)
        else:
            user = identity.user

        user.first_name = profile.first_name or ''
        user.last_name = profile.last_name or ''
        user.is_active = True
        user.is_staff = True
        user.is_superuser = True
        user.set_unusable_password()
        user.save()

        identity.login = profile.login
        identity.user = user
        identity.save()
        return user
