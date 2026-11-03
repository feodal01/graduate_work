import logging
from datetime import date
from typing import Any

import aiohttp
import backoff
from pydantic import BaseModel, ConfigDict, ValidationError

from core.config import settings

logger = logging.getLogger(__name__)

_RETRYABLE_AUTH_STATUSES = frozenset({502, 503, 504})


class AuthUnavailableError(Exception):
    """Movies API не может связаться с Auth."""


class AuthRateLimitedError(Exception):
    """Auth ограничил частоту запросов Movies API."""

    def __init__(self, retry_after_seconds: int | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__('Too many requests')


class AuthTokenRejectedError(Exception):
    """Auth отклонил пользовательский access-токен."""


class AuthContractError(Exception):
    """Ответ Auth не соответствует внутреннему контракту."""


class _RetryableAuthError(Exception):
    """Транзиентная ошибка, для которой разрешён короткий повтор."""


class _PermissionCheckResponse(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)

    allowed: bool


class AuthClient:
    """Проверяет право чтения фильма через внутренний HTTP API Auth."""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        base_url: str,
        service_api_key: str,
    ) -> None:
        self._session = session
        self._permission_url = f'{base_url.rstrip("/")}/permissions/check'
        self._service_api_key = service_api_key

    async def can_read_film(
        self,
        *,
        creation_date: date | None,
        access_token: str | None,
        request_id: str,
    ) -> bool:
        headers = {
            'X-Service-Key': self._service_api_key,
            'X-Request-Id': request_id,
        }
        if access_token is not None:
            headers['Authorization'] = f'Bearer {access_token}'
        payload: dict[str, Any] = {
            'resource': 'film',
            'action': 'read',
            'attributes': {
                'creation_date': (creation_date.isoformat() if creation_date is not None else None)
            },
        }

        try:
            (
                response_status,
                response_payload,
                retry_after_seconds,
            ) = await self._request_permission(
                headers=headers,
                payload=payload,
            )
        except _RetryableAuthError as exc:
            raise AuthUnavailableError from exc

        if response_status == 429:
            raise AuthRateLimitedError(retry_after_seconds)
        if response_status == 401:
            if response_payload == {'detail': 'Invalid service credentials'}:
                raise AuthContractError
            if access_token is not None:
                raise AuthTokenRejectedError
            raise AuthContractError
        if response_status != 200:
            raise AuthContractError
        try:
            permission = _PermissionCheckResponse.model_validate(response_payload)
        except ValidationError as exc:
            raise AuthContractError from exc
        return permission.allowed

    @backoff.on_exception(
        backoff.expo,
        _RetryableAuthError,
        max_tries=lambda: settings.auth_backoff_max_tries,
        max_time=lambda: settings.auth_backoff_max_time,
        jitter=backoff.full_jitter,
        logger=logger,
    )
    async def _request_permission(
        self,
        *,
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> tuple[int, Any, int | None]:
        try:
            async with self._session.post(
                self._permission_url,
                headers=headers,
                json=payload,
            ) as response:
                if response.status in _RETRYABLE_AUTH_STATUSES:
                    raise _RetryableAuthError(f'Auth returned HTTP {response.status}')
                if response.status >= 500:
                    raise AuthUnavailableError
                try:
                    response_payload = await response.json()
                except (aiohttp.ContentTypeError, TypeError, ValueError) as exc:
                    raise AuthContractError from exc
        except (_RetryableAuthError, AuthUnavailableError, AuthContractError):
            raise
        except (
            TimeoutError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
        ) as exc:
            raise _RetryableAuthError from exc
        except aiohttp.ClientError as exc:
            raise AuthUnavailableError from exc

        retry_after_seconds = self._retry_after_seconds(response.headers.get('Retry-After'))
        return response.status, response_payload, retry_after_seconds

    @staticmethod
    def _retry_after_seconds(raw_value: object) -> int | None:
        if not isinstance(raw_value, str):
            return None
        try:
            value = int(raw_value)
        except ValueError:
            return None
        return value if value > 0 else None
