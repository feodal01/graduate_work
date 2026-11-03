from __future__ import annotations

import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Protocol
from uuid import UUID

import backoff
import httpx
from django.conf import settings

logger = logging.getLogger(__name__)

_RETRYABLE_AUTH_STATUSES = frozenset({502, 503, 504})
_RETRYABLE_CONNECT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)
_RETRYABLE_GET_ERRORS = (
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)
_KNOWN_TECHNICAL_ERRORS = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


class HTTPClient(Protocol):
    def request(self, method: str, url: str, **kwargs: Any) -> Any: ...


class AuthInvalidCredentialsError(Exception):
    """Auth отклонил логин или пароль."""


class AuthUnauthorizedError(Exception):
    """Auth отклонил bearer- или refresh-токен."""


class AuthForbiddenError(Exception):
    """Auth запретил действие текущему пользователю."""


class AuthUnavailableError(Exception):
    """Auth недоступен по технической причине."""


class AuthRateLimitedError(Exception):
    """Auth отклонил запрос из-за превышения лимита."""

    def __init__(self, retry_after_seconds: int | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__('Too many requests')


class _RetryableAuthError(Exception):
    """Технический отказ, после которого запрос можно безопасно повторить."""


class AuthContractError(Exception):
    """Ответ Auth не соответствует согласованному контракту."""


class AuthOperationError(Exception):
    """Auth отклонил корректный запрос управления ролями."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class TokenPair:
    access_token: str
    refresh_token: str


@dataclass(frozen=True, slots=True)
class AuthUser:
    id: UUID
    login: str
    first_name: str | None
    last_name: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AuthRole:
    id: UUID
    name: str
    permissions: tuple[str, ...]
    created_at: datetime


class AuthClient:
    """Синхронный клиент публичного HTTP-контракта Auth."""

    def __init__(self, *, base_url: str, http_client: HTTPClient) -> None:
        self._base_url = base_url.rstrip('/')
        self._http_client = http_client

    def login(
        self,
        *,
        login: str,
        password: str,
        request_id: str,
    ) -> TokenPair:
        response = self._request(
            'POST',
            '/login',
            request_id=request_id,
            json={'login': login, 'password': password},
        )
        if response.status_code == 401:
            raise AuthInvalidCredentialsError
        self._raise_for_status(response, expected_status=200)
        return self._parse_token_pair(response)

    def get_profile(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> AuthUser:
        response = self._request(
            'GET',
            '/profile',
            request_id=request_id,
            headers=self._bearer(access_token),
        )
        self._raise_for_status(response, expected_status=200)
        payload = self._json_object(response)
        if set(payload) != {
            'id',
            'login',
            'first_name',
            'last_name',
            'created_at',
        }:
            raise AuthContractError
        login = payload['login']
        first_name = payload['first_name']
        last_name = payload['last_name']
        if not isinstance(login, str) or not login:
            raise AuthContractError
        if first_name is not None and not isinstance(first_name, str):
            raise AuthContractError
        if last_name is not None and not isinstance(last_name, str):
            raise AuthContractError
        return AuthUser(
            id=self._uuid(payload['id']),
            login=login,
            first_name=first_name,
            last_name=last_name,
            created_at=self._datetime(payload['created_at']),
        )

    def refresh(self, refresh_token: str, *, request_id: str) -> TokenPair:
        response = self._request(
            'POST',
            '/refresh',
            request_id=request_id,
            json={'refresh_token': refresh_token},
        )
        self._raise_for_status(response, expected_status=200)
        return self._parse_token_pair(response)

    def logout(
        self,
        *,
        access_token: str,
        refresh_token: str,
        request_id: str,
    ) -> None:
        response = self._request(
            'POST',
            '/logout',
            request_id=request_id,
            headers=self._bearer(access_token),
            json={'refresh_token': refresh_token},
        )
        self._raise_for_status(response, expected_status=204)

    def list_roles(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[AuthRole, ...]:
        response = self._request(
            'GET',
            '/roles',
            request_id=request_id,
            headers=self._bearer(access_token),
        )
        self._raise_for_status(response, expected_status=200)
        try:
            payload = response.json()
        except (TypeError, ValueError) as exc:
            raise AuthContractError from exc
        if not isinstance(payload, list):
            raise AuthContractError
        return tuple(self._parse_role(item) for item in payload)

    def create_role(
        self,
        *,
        access_token: str,
        name: str,
        permissions: Collection[str],
        request_id: str,
    ) -> AuthRole:
        response = self._request(
            'POST',
            '/roles',
            request_id=request_id,
            headers=self._bearer(access_token),
            json={'name': name, 'permissions': sorted(permissions)},
        )
        self._raise_for_status(response, expected_status=201)
        return self._parse_role(self._json_object(response))

    def update_role(
        self,
        *,
        access_token: str,
        role_id: UUID,
        name: str | None,
        permissions: Collection[str] | None,
        request_id: str,
    ) -> AuthRole:
        payload: dict[str, object] = {}
        if name is not None:
            payload['name'] = name
        if permissions is not None:
            payload['permissions'] = sorted(permissions)
        response = self._request(
            'PATCH',
            f'/roles/{role_id}',
            request_id=request_id,
            headers=self._bearer(access_token),
            json=payload,
        )
        self._raise_for_status(response, expected_status=200)
        return self._parse_role(self._json_object(response))

    def delete_role(
        self,
        *,
        access_token: str,
        role_id: UUID,
        request_id: str,
    ) -> None:
        response = self._request(
            'DELETE',
            f'/roles/{role_id}',
            request_id=request_id,
            headers=self._bearer(access_token),
        )
        self._raise_for_status(response, expected_status=204)

    def assign_role(
        self,
        *,
        access_token: str,
        user_id: UUID,
        role_id: UUID,
        request_id: str,
    ) -> None:
        response = self._request(
            'POST',
            f'/users/{user_id}/roles',
            request_id=request_id,
            headers=self._bearer(access_token),
            json={'role_id': str(role_id)},
        )
        self._raise_for_status(response, expected_status=204)

    def revoke_role(
        self,
        *,
        access_token: str,
        user_id: UUID,
        role_id: UUID,
        request_id: str,
    ) -> None:
        response = self._request(
            'DELETE',
            f'/users/{user_id}/roles/{role_id}',
            request_id=request_id,
            headers=self._bearer(access_token),
        )
        self._raise_for_status(response, expected_status=204)

    def _request(
        self,
        method: str,
        path: str,
        *,
        request_id: str,
        **kwargs: Any,
    ) -> Any:
        headers = dict(kwargs.pop('headers', {}))
        headers['X-Request-Id'] = request_id
        try:
            return self._request_with_backoff(
                method,
                path,
                headers=headers,
                **kwargs,
            )
        except _RetryableAuthError as exc:
            raise AuthUnavailableError from exc

    @backoff.on_exception(
        backoff.expo,
        _RetryableAuthError,
        max_tries=lambda: settings.AUTH_CLIENT_BACKOFF_MAX_TRIES,
        max_time=lambda: settings.AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS,
        jitter=backoff.full_jitter,
        logger=logger,
    )
    def _request_with_backoff(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        try:
            response = self._http_client.request(
                method,
                f'{self._base_url}{path}',
                **kwargs,
            )
        except _RETRYABLE_CONNECT_ERRORS as exc:
            raise _RetryableAuthError from exc
        except _RETRYABLE_GET_ERRORS as exc:
            if method == 'GET':
                raise _RetryableAuthError from exc
            raise AuthUnavailableError from exc
        except _KNOWN_TECHNICAL_ERRORS as exc:
            raise AuthUnavailableError from exc

        if method == 'GET' and response.status_code in _RETRYABLE_AUTH_STATUSES:
            raise _RetryableAuthError(
                f'Auth returned HTTP {response.status_code}'
            )
        return response

    @staticmethod
    def _bearer(access_token: str) -> dict[str, str]:
        return {'Authorization': f'Bearer {access_token}'}

    @classmethod
    def _raise_for_status(cls, response: Any, *, expected_status: int) -> None:
        if response.status_code == expected_status:
            return
        if response.status_code == 429:
            raise AuthRateLimitedError(cls._retry_after_seconds(response))
        if response.status_code == 401:
            raise AuthUnauthorizedError
        if response.status_code == 403:
            raise AuthForbiddenError
        if response.status_code >= 500:
            raise AuthUnavailableError
        if response.status_code in {404, 409, 422}:
            raise AuthOperationError(
                response.status_code,
                cls._error_detail(response),
            )
        raise AuthContractError

    @staticmethod
    def _retry_after_seconds(response: Any) -> int | None:
        raw_value = response.headers.get('Retry-After')
        if not isinstance(raw_value, str):
            return None
        try:
            value = int(raw_value)
        except ValueError:
            return None
        return value if value > 0 else None

    @classmethod
    def _parse_token_pair(cls, response: Any) -> TokenPair:
        payload = cls._json_object(response)
        if set(payload) != {'access_token', 'refresh_token', 'token_type'}:
            raise AuthContractError
        access_token = payload['access_token']
        refresh_token = payload['refresh_token']
        if (
            not isinstance(access_token, str)
            or not access_token
            or not isinstance(refresh_token, str)
            or not refresh_token
            or payload['token_type'] != 'bearer'
        ):
            raise AuthContractError
        return TokenPair(access_token, refresh_token)

    @classmethod
    def _parse_role(cls, payload: object) -> AuthRole:
        if not isinstance(payload, Mapping):
            raise AuthContractError
        if set(payload) != {'id', 'name', 'permissions', 'created_at'}:
            raise AuthContractError
        name = payload['name']
        permissions = payload['permissions']
        if not isinstance(name, str) or not name:
            raise AuthContractError
        if not isinstance(permissions, list) or not all(
            isinstance(item, str) for item in permissions
        ):
            raise AuthContractError
        return AuthRole(
            id=cls._uuid(payload['id']),
            name=name,
            permissions=tuple(permissions),
            created_at=cls._datetime(payload['created_at']),
        )

    @staticmethod
    def _json_object(response: Any) -> Mapping[str, object]:
        try:
            payload = response.json()
        except (TypeError, ValueError) as exc:
            raise AuthContractError from exc
        if not isinstance(payload, Mapping):
            raise AuthContractError
        return payload

    @classmethod
    def _error_detail(cls, response: Any) -> str:
        payload = cls._json_object(response)
        detail = payload.get('detail')
        if not isinstance(detail, str) or not detail:
            raise AuthContractError
        return detail

    @staticmethod
    def _uuid(value: object) -> UUID:
        if not isinstance(value, str):
            raise AuthContractError
        try:
            return UUID(value)
        except ValueError as exc:
            raise AuthContractError from exc

    @staticmethod
    def _datetime(value: object) -> datetime:
        if not isinstance(value, str):
            raise AuthContractError
        normalized = value[:-1] + '+00:00' if value.endswith('Z') else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise AuthContractError from exc
        if parsed.tzinfo is None:
            raise AuthContractError
        return parsed


@lru_cache(maxsize=1)
def get_auth_client() -> AuthClient:
    timeout = httpx.Timeout(
        settings.AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS,
        connect=settings.AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS,
    )
    return AuthClient(
        base_url=settings.AUTH_SERVICE_URL,
        http_client=httpx.Client(timeout=timeout),
    )
