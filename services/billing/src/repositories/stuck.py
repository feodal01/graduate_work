"""Правило «зависшей операции» — одно для сигнала мониторинга и фильтра админки.

Ожидающий платёж с номером ЮKassa ждёт пользователя на странице оплаты:
ЮKassa держит его около часа (2.5.4), поэтому зависшим он считается только
после отдельного, более длинного порога. Платёж без номера и возврат в
обработке ни от кого не зависят и зависают по общему порогу (6.4, US-A-02).
"""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, or_

from models import Payment, Refund


def stuck_payment_clause(
    now: datetime,
    *,
    threshold_seconds: int,
    waiting_payment_threshold_seconds: int,
) -> Any:
    """SQL-условие «ожидающий платёж завис»."""
    return and_(
        Payment.status == 'pending',
        or_(
            and_(
                Payment.provider_payment_id.is_(None),
                Payment.created_at <= now - timedelta(seconds=threshold_seconds),
            ),
            and_(
                Payment.provider_payment_id.is_not(None),
                Payment.created_at
                <= now - timedelta(seconds=waiting_payment_threshold_seconds),
            ),
        ),
    )


def stuck_refund_clause(now: datetime, *, threshold_seconds: int) -> Any:
    """SQL-условие «возврат в обработке завис»."""
    return and_(
        Refund.status == 'processing',
        Refund.created_at <= now - timedelta(seconds=threshold_seconds),
    )


def payment_is_stuck(
    *,
    status: str,
    provider_payment_id: str | None,
    created_at: datetime,
    now: datetime,
    threshold_seconds: int,
    waiting_payment_threshold_seconds: int,
) -> bool:
    """То же правило для уже прочитанной строки (и для фейка в тестах)."""
    if status != 'pending':
        return False
    seconds = (
        waiting_payment_threshold_seconds if provider_payment_id else threshold_seconds
    )
    return created_at <= now - timedelta(seconds=seconds)


def refund_is_stuck(
    *,
    status: str,
    created_at: datetime,
    now: datetime,
    threshold_seconds: int,
) -> bool:
    return status == 'processing' and created_at <= now - timedelta(
        seconds=threshold_seconds
    )
