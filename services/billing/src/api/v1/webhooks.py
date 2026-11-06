import re
from typing import Annotated

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from api.errors import BillingDomainError
from clients.payment_provider import (
    PaymentProviderError,
    ProviderIntegrityMismatchError,
    ProviderProtocolAnomalyError,
)
from dependencies import get_payment_reconciler
from services.reconciler import PaymentReconciler

router = APIRouter(tags=['billing-webhooks'])

_SAFE_PROVIDER_ID_RE = re.compile(r'^[A-Za-z0-9._-]{1,128}$')


class YooKassaWebhookObject(BaseModel):
    """Из тела вебхука берётся ТОЛЬКО id операции (BIL-FR-07, BIL-NFR-SEC-04)."""

    model_config = ConfigDict(extra='ignore')

    id: Annotated[str, Field(strict=True, min_length=1, max_length=128)]

    @field_validator('id')
    @classmethod
    def validate_nonblank_id(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned or not _SAFE_PROVIDER_ID_RE.fullmatch(cleaned):
            raise ValueError('object.id must be a safe non-empty provider operation id')
        return cleaned


class YooKassaWebhookRequest(BaseModel):
    model_config = ConfigDict(extra='ignore')

    type: str | None = None
    event: str | None = None
    object: YooKassaWebhookObject


class YooKassaWebhookResponse(BaseModel):
    status: str = 'ok'


@router.post(
    '/webhooks/yookassa',
    response_model=YooKassaWebhookResponse,
    status_code=status.HTTP_200_OK,
)
async def handle_yookassa_webhook(
    payload: YooKassaWebhookRequest,
    reconciler: Annotated[PaymentReconciler, Depends(get_payment_reconciler)],
) -> YooKassaWebhookResponse:
    """Обработчик входящих уведомлений ЮKassa (BIL-FR-07, BIL-API-05, US-S-01)."""
    try:
        reconcile_fn = getattr(
            reconciler,
            'reconcile_by_provider_operation_id',
            reconciler.reconcile_by_provider_payment_id,
        )
        await reconcile_fn(
            payload.object.id,
            source='webhook',
            respect_throttle=False,
        )
    except (ProviderIntegrityMismatchError, ProviderProtocolAnomalyError):
        # Аномалия уже отправлена в мониторинг, повторы вебхука не нужны
        return YooKassaWebhookResponse(status='ok')
    except PaymentProviderError as exc:
        raise BillingDomainError(
            'Не удалось связаться с платёжной системой для сверки вебхука',
            error_code='payment_provider_unavailable',
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        ) from exc

    return YooKassaWebhookResponse(status='ok')
