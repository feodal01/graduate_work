from __future__ import annotations

from typing import Callable

from django.contrib.auth import logout as django_logout
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.auth_client import (
    AuthClient,
    AuthContractError,
    AuthForbiddenError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
    get_auth_client,
)
from users.request_id import get_request_id
from users.rate_limit import rate_limited_response


class AuthAuthorizationMiddleware:
    """Подтверждает право Admin через Auth до вызова защищённого view."""

    def __init__(
        self,
        get_response: Callable[[HttpRequest], HttpResponse],
        auth_client: AuthClient | None = None,
    ) -> None:
        self._get_response = get_response
        self._auth_client = auth_client

    @property
    def auth_client(self) -> AuthClient:
        return self._auth_client or get_auth_client()

    def __call__(self, request: HttpRequest) -> HttpResponse:
        if self._is_excluded(request.path) or not request.user.is_authenticated:
            return self._get_response(request)

        access_token = request.session.get(AUTH_ACCESS_TOKEN_SESSION_KEY)
        refresh_token = request.session.get(AUTH_REFRESH_TOKEN_SESSION_KEY)
        if not isinstance(access_token, str) or not isinstance(
            refresh_token,
            str,
        ):
            return self._end_session(request)

        request_id = get_request_id(request)
        try:
            roles = self.auth_client.list_roles(
                access_token,
                request_id=request_id,
            )
        except AuthUnauthorizedError:
            try:
                pair = self.auth_client.refresh(
                    refresh_token,
                    request_id=request_id,
                )
                request.session[AUTH_ACCESS_TOKEN_SESSION_KEY] = pair.access_token
                request.session[AUTH_REFRESH_TOKEN_SESSION_KEY] = pair.refresh_token
                roles = self.auth_client.list_roles(
                    pair.access_token,
                    request_id=request_id,
                )
            except (AuthUnauthorizedError, AuthForbiddenError):
                return self._end_session(request)
            except AuthRateLimitedError as exc:
                return rate_limited_response(
                    request,
                    exc.retry_after_seconds,
                )
            except (AuthUnavailableError, AuthContractError):
                return self._unavailable(request)
        except AuthForbiddenError:
            return self._end_session(request)
        except AuthRateLimitedError as exc:
            return rate_limited_response(request, exc.retry_after_seconds)
        except (AuthUnavailableError, AuthContractError):
            return self._unavailable(request)

        request.auth_roles = roles
        return self._get_response(request)

    @staticmethod
    def _is_excluded(path: str) -> bool:
        if not path.startswith('/admin/'):
            return True
        return path in {'/admin/login/', '/admin/logout/'}

    @staticmethod
    def _end_session(request: HttpRequest) -> HttpResponse:
        django_logout(request)
        return redirect('admin:login')

    @staticmethod
    def _unavailable(request: HttpRequest) -> HttpResponse:
        return render(
            request,
            'admin/auth/unavailable.html',
            status=503,
        )
