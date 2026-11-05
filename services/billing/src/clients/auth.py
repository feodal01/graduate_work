import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)


class AuthInvalidCredentialsError(Exception):
    """Учётные данные или токен отклонены сервисом Auth (401/422)."""


class AuthRateLimitExceededError(Exception):
    """Превышен лимит запросов к сервису Auth (429)."""


class AuthServiceUnavailableError(Exception):
    """Сервис Auth недоступен или вернул временную ошибку (5xx / 429 / сбой сети)."""


class AuthPermanentDeliveryError(Exception):
    """Постоянная ошибка (4xx, кроме 429) при доставке доступа в Auth (BIL-FR-14)."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f'Auth HTTP {status_code}: {message}')


@dataclass(frozen=True, slots=True)
class SubscriptionAccessSyncResult:
    applied: bool
    current_version: int


class AuthGateway(Protocol):
    async def login(
        self,
        login: str,
        password: str,
        *,
        request_id: str,
    ) -> tuple[str, str]: ...

    async def get_current_user(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[UUID, str, str]: ...

    async def logout(
        self,
        access_token: str,
        refresh_token: str,
        *,
        request_id: str,
    ) -> None: ...

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult: ...


class HttpAuthGateway:
    """HTTP-клиент к сервису Auth для входа, профиля и доставки доступа (T-08, T-16)."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        base_url: str,
        *,
        service_api_key: str = '',
    ) -> None:
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._service_api_key = service_api_key

    async def login(
        self,
        login: str,
        password: str,
        *,
        request_id: str,
    ) -> tuple[str, str]:
        if not login.strip() or not password:
            raise AuthInvalidCredentialsError()
        try:
            response = await self._http.post(
                f'{self._base_url}/api/v1/auth/login',
                json={'login': login, 'password': password},
                headers={'X-Request-Id': request_id},
            )
        except httpx.HTTPError as exc:
            logger.warning('Auth login request failed: %s', exc)
            raise AuthServiceUnavailableError() from exc

        if response.status_code in (401, 422):
            raise AuthInvalidCredentialsError()
        if response.status_code == 429:
            raise AuthRateLimitExceededError()
        if response.status_code != 200:
            logger.error(
                'Unexpected Auth login response status=%s',
                response.status_code,
            )
            raise AuthServiceUnavailableError()

        try:
            data = response.json()
            access_token = str(data['access_token'])
            refresh_token = str(data['refresh_token'])
        except (ValueError, KeyError, TypeError) as exc:
            raise AuthServiceUnavailableError() from exc
        return access_token, refresh_token

    async def get_current_user(
        self,
        access_token: str,
        *,
        request_id: str,
    ) -> tuple[UUID, str, str]:
        if not access_token:
            raise AuthInvalidCredentialsError()
        try:
            response = await self._http.get(
                f'{self._base_url}/api/v1/auth/profile',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'X-Request-Id': request_id,
                },
            )
        except httpx.HTTPError as exc:
            logger.warning('Auth profile check failed: %s', exc)
            raise AuthServiceUnavailableError() from exc

        if response.status_code == 401:
            raise AuthInvalidCredentialsError()
        if response.status_code != 200:
            logger.error(
                'Unexpected Auth profile response status=%s',
                response.status_code,
            )
            raise AuthServiceUnavailableError()

        try:
            data = response.json()
            user_id = UUID(str(data['id']))
            user_login = str(data['login'])
            user_tz = str(data.get('tz') or 'UTC')
        except (ValueError, KeyError, TypeError) as exc:
            raise AuthServiceUnavailableError() from exc
        return user_id, user_login, user_tz

    async def logout(
        self,
        access_token: str,
        refresh_token: str,
        *,
        request_id: str,
    ) -> None:
        if not access_token or not refresh_token:
            return
        try:
            await self._http.post(
                f'{self._base_url}/api/v1/auth/logout',
                json={'refresh_token': refresh_token},
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'X-Request-Id': request_id,
                },
            )
        except httpx.HTTPError as exc:
            logger.warning('Best-effort Auth logout failed: %s', exc)

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        """Записывает состояние доступа с версией в Auth (AUTH-API-01, BIL-FR-11)."""
        try:
            response = await self._http.put(
                f'{self._base_url}/api/v1/internal/users/{user_id}/subscription-access',
                json={'version': version, 'expires_at': expires_at},
                headers={
                    'X-Request-Id': request_id,
                    'X-Service-Api-Key': self._service_api_key,
                },
            )
        except httpx.HTTPError as exc:
            logger.warning('Auth subscription-access PUT failed: %s', exc)
            raise AuthServiceUnavailableError(str(exc)) from exc

        if response.status_code == 200:
            try:
                data = response.json()
                return SubscriptionAccessSyncResult(
                    applied=bool(data['applied']),
                    current_version=int(data['current_version']),
                )
            except (ValueError, KeyError, TypeError) as exc:
                raise AuthServiceUnavailableError('Invalid Auth response JSON') from exc

        if response.status_code == 429 or response.status_code >= 500:
            raise AuthServiceUnavailableError(
                f'Auth returned temporary HTTP {response.status_code}'
            )

        detail = response.text[:250]
        try:
            body = response.json()
            if isinstance(body, dict) and 'detail' in body:
                detail = str(body['detail'])
        except ValueError:
            pass
        raise AuthPermanentDeliveryError(response.status_code, detail)
