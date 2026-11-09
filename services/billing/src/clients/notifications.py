from typing import Any, Protocol
from uuid import UUID

import httpx


class NotificationsTransientError(Exception):
    """Временная ошибка Notifications (5xx, 429, 409 profile_not_ready, сеть).

    Сюда же относятся 401/403: это отклонённый сервисный ключ, то есть ошибка
    конфигурации (ключ сменили не одновременно на обеих сторонах). После её
    исправления письмо должно уйти само, без ручного «Повторить».
    """


def _raise_if_transient(status_code: int, detail: str) -> None:
    if status_code in (401, 403):
        raise NotificationsTransientError(
            f'Notifications rejected the service key ({status_code}): {detail}; '
            'check NOTIFICATIONS_CAMPAIGN_API_KEY on both sides'
        )
    if status_code == 429 or status_code >= 500:
        raise NotificationsTransientError(
            f'Notifications returned {status_code}: {detail}'
        )


class NotificationsPermanentError(Exception):
    """Постоянная ошибка запроса к Notifications (4xx, кроме 429 и 409 готовности)."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f'Notifications returned {status_code}: {detail}')


class NotificationsGateway(Protocol):
    async def send_campaign_batch(
        self,
        *,
        campaign_id: UUID,
        batch_id: UUID,
        user_id: UUID,
        template_id: str,
        subject: str,
        text: str,
        available_at: str | None,
        request_id: str,
    ) -> None: ...

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None: ...


class HttpNotificationsGateway:
    """HTTP-клиент Campaign API сервиса Notifications (T-20, NTF-API-01, NTF-API-02)."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        base_url: str,
        api_key: str,
    ) -> None:
        self._client = http_client
        self._base_url = base_url.rstrip('/')
        self._api_key = api_key

    def _headers(self, request_id: str) -> dict[str, str]:
        return {
            'X-API-Key': self._api_key,
            'X-Request-Id': request_id,
        }

    @staticmethod
    def _extract_error_code_and_detail(response: httpx.Response) -> tuple[str, str]:
        try:
            body: Any = response.json()
            if isinstance(body, dict):
                error_code = str(body.get('error_code') or '')
                detail = str(body.get('detail') or error_code or response.text[:200])
                return error_code, detail
        except ValueError:
            pass
        return '', response.text[:200] or f'HTTP {response.status_code}'

    async def send_campaign_batch(
        self,
        *,
        campaign_id: UUID,
        batch_id: UUID,
        user_id: UUID,
        template_id: str,
        subject: str,
        text: str,
        available_at: str | None,
        request_id: str,
    ) -> None:
        """Отправляет или планирует письмо через POST .../campaigns/{id}/batches."""
        payload: dict[str, Any] = {
            'batch_id': str(batch_id),
            'created_by_user_id': str(user_id),
            'items': [
                {
                    'user_id': str(user_id),
                    'template_id': template_id,
                    'subject': subject,
                    'text': text,
                    'type': 'email',
                    'available_at': available_at,
                }
            ],
        }
        try:
            response = await self._client.post(
                f'{self._base_url}/api/v1/notifications/campaigns/{campaign_id}/batches',
                json=payload,
                headers=self._headers(request_id),
            )
        except httpx.HTTPError as exc:
            raise NotificationsTransientError(
                f'Notifications network error: {exc}'
            ) from exc

        if response.status_code in (200, 202):
            return

        error_code, detail = self._extract_error_code_and_detail(response)
        if response.status_code == 409 and error_code == 'profile_not_ready':
            raise NotificationsTransientError(
                f'Notifications profile_not_ready (409): {detail}'
            )
        _raise_if_transient(response.status_code, detail)
        raise NotificationsPermanentError(response.status_code, detail)

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None:
        """Отменяет запланированную кампанию через POST .../campaigns/{id}/cancel."""
        try:
            response = await self._client.post(
                f'{self._base_url}/api/v1/notifications/campaigns/{campaign_id}/cancel',
                headers=self._headers(request_id),
            )
        except httpx.HTTPError as exc:
            raise NotificationsTransientError(
                f'Notifications network error: {exc}'
            ) from exc

        if response.status_code in (200, 202, 204):
            return

        _, detail = self._extract_error_code_and_detail(response)
        _raise_if_transient(response.status_code, detail)
        raise NotificationsPermanentError(response.status_code, detail)
