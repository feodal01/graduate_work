import logging
from collections.abc import Mapping

import httpx
import sentry_sdk

from clients.payment_provider import (
    PaymentProviderGateway,
    ProviderTransientError,
    YooKassaProviderAdapter,
)
from core.config import PaymentProvidersSettings

logger = logging.getLogger(__name__)

SUPPORTED_PROVIDER_CODES: tuple[str, ...] = (YooKassaProviderAdapter.provider_code,)


class UnknownPaymentProviderError(ProviderTransientError):
    """У операции `provider_code`, для которого не настроен адаптер.

    Это ошибка конфигурации, а не отказ провайдера: операция остаётся
    незавершённой и сверяется снова, когда адаптер вернут в конфигурацию.
    """


class PaymentProviderRegistry:
    """Адаптеры платёжных провайдеров по `provider_code` (BIL-NFR-S-02, 3.6).

    Новые платежи создаются через активного провайдера. Сверка и возвраты
    существующих операций идут через провайдера, записанного в самой операции.
    """

    def __init__(
        self,
        providers: Mapping[str, PaymentProviderGateway],
        *,
        active_code: str,
        http_clients: tuple[httpx.AsyncClient, ...] = (),
    ) -> None:
        for code, provider in providers.items():
            if provider.provider_code != code:
                raise ValueError(
                    f'Payment provider registered as {code!r} '
                    f'reports provider_code={provider.provider_code!r}'
                )
        if active_code not in providers:
            raise ValueError(
                f'Active payment provider {active_code!r} is not configured; '
                f'available: {sorted(providers)}'
            )
        self._providers = dict(providers)
        self._active_code = active_code
        self._http_clients = http_clients

    @classmethod
    def single(cls, provider: PaymentProviderGateway) -> 'PaymentProviderRegistry':
        """Реестр из одного провайдера, он же активный."""
        return cls(
            {provider.provider_code: provider},
            active_code=provider.provider_code,
        )

    @property
    def active_code(self) -> str:
        return self._active_code

    @property
    def active(self) -> PaymentProviderGateway:
        return self._providers[self._active_code]

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(self._providers)

    def get(self, provider_code: str) -> PaymentProviderGateway:
        provider = self._providers.get(provider_code)
        if provider is None:
            message = (
                f'No adapter configured for payment provider {provider_code!r}; '
                f'available: {sorted(self._providers)}'
            )
            logger.error(message)
            sentry_sdk.capture_message(message, level='error')
            raise UnknownPaymentProviderError(message)
        return provider

    async def aclose(self) -> None:
        for client in self._http_clients:
            await client.aclose()


def build_payment_provider_registry(
    settings: PaymentProvidersSettings,
) -> PaymentProviderRegistry:
    """Собирает адаптеры всех настроенных провайдеров (общая точка для API и worker).

    Новый провайдер добавляется сюда отдельным блоком: свой HTTP-клиент,
    свои реквизиты из `settings.<code>`, запись в `providers` и код в
    `SUPPORTED_PROVIDER_CODES`.
    """
    # Проверяем до создания HTTP-клиентов, чтобы не оставлять их открытыми.
    if settings.active not in SUPPORTED_PROVIDER_CODES:
        raise ValueError(
            f'Active payment provider {settings.active!r} is not supported; '
            f'available: {list(SUPPORTED_PROVIDER_CODES)}'
        )
    yookassa_http = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.yookassa.timeout_seconds)
    )
    yookassa = YooKassaProviderAdapter(
        http_client=yookassa_http,
        shop_id=settings.yookassa.shop_id,
        secret_key=settings.yookassa.secret_key.get_secret_value(),
        base_url=str(settings.yookassa.api_base_url),
    )
    return PaymentProviderRegistry(
        {yookassa.provider_code: yookassa},
        active_code=settings.active,
        http_clients=(yookassa_http,),
    )
