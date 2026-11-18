"""Задачи `outbox_tasks`, которые порождает итог платежа (BIL-FR-11).

Ключи дедупликации и состав задач живут здесь, а не в репозитории платежей:
их используют и фиксация итога, и чтение экранов пользователя, и досылка
письма об отказе, и все должны получить один и тот же `dedup_key`.
"""

from datetime import datetime
from uuid import UUID

from domain.subscription import (
    build_payment_failed_email_payload,
    build_payment_succeeded_email_payload,
    build_subscription_expiration_email_payload,
)
from models import OutboxTask
from repositories.records import PaymentRecordProtocol


def payment_access_task_key(payment_id: UUID) -> str:
    """Ключ задачи «открыть доступ» после оплаты; у платежа она ровно одна.

    После возврата у того же платежа появляется вторая задача `sync_access`
    (закрыть доступ), поэтому экраны ищут задачу оплаты по этому ключу.
    """
    return f'payment_succeeded:{payment_id}:auth_access'


def payment_failed_email_task_key(payment_id: UUID) -> str:
    """Ключ письма об отказе: одно письмо на платёж, кто бы его ни поставил."""
    return f'payment_failed:{payment_id}:notify_failed'


def payment_failed_email_task(
    payment: PaymentRecordProtocol,
    *,
    failure_reason: str,
    tariff_title: str,
    public_base_url: str,
    now_utc: datetime,
) -> OutboxTask:
    """Задача письма «Оплата не прошла» (BIL-FR-06)."""
    return OutboxTask(
        dedup_key=payment_failed_email_task_key(payment.id),
        target_service='notifications',
        task_type='payment_failed_email',
        payment_id=payment.id,
        user_id=payment.user_id,
        access_version=None,
        payload=build_payment_failed_email_payload(
            payment_id=payment.id,
            user_id=payment.user_id,
            failure_reason=failure_reason,
            public_base_url=public_base_url,
            tariff_title=tariff_title,
        ),
        next_retry_at=now_utc,
    )


def payment_succeeded_tasks(
    payment: PaymentRecordProtocol,
    *,
    subscription_id: UUID,
    ends_at: datetime,
    access_version: int,
    tariff_title: str,
    public_base_url: str,
    now_utc: datetime,
) -> list[OutboxTask]:
    """Доступ до `ends_at`, письмо об оплате и письмо об окончании (BIL-FR-11)."""
    return [
        OutboxTask(
            dedup_key=payment_access_task_key(payment.id),
            target_service='auth',
            task_type='sync_access',
            payment_id=payment.id,
            user_id=payment.user_id,
            access_version=access_version,
            payload={
                'user_id': str(payment.user_id),
                'version': access_version,
                'expires_at': ends_at.isoformat(),
            },
            next_retry_at=now_utc,
        ),
        OutboxTask(
            dedup_key=f'payment_succeeded:{payment.id}:notify_paid',
            target_service='notifications',
            task_type='payment_succeeded_email',
            payment_id=payment.id,
            user_id=payment.user_id,
            access_version=None,
            payload=build_payment_succeeded_email_payload(
                payment_id=payment.id,
                subscription_id=subscription_id,
                user_id=payment.user_id,
                ends_at=ends_at,
                public_base_url=public_base_url,
                tariff_title=tariff_title,
            ),
            next_retry_at=now_utc,
        ),
        OutboxTask(
            dedup_key=f'subscription_expiration:{subscription_id}:notify_expiring',
            target_service='notifications',
            task_type='subscription_expiration_email',
            payment_id=payment.id,
            user_id=payment.user_id,
            access_version=None,
            payload=build_subscription_expiration_email_payload(
                subscription_id=subscription_id,
                user_id=payment.user_id,
                ends_at=ends_at,
                public_base_url=public_base_url,
                tariff_title=tariff_title,
            ),
            next_retry_at=now_utc,
        ),
    ]
