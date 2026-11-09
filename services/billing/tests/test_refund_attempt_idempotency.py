"""Повтор формы возврата идемпотентен по UUID попытки (BIL-NFR-R-02, инв. 4).

Регрессия: после отказа ЮKassa повтор той же формы (F5, двойной клик) молча
создавал новую попытку с новым ключом и новым POST в ЮKassa, а после
выполненного возврата повтор получал 409 вместо существующей записи.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from clients.payment_provider import ProviderPermanentError
from domain.errors import RuleConflictError
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)


def _paid_subscription(repo: InMemoryPurchaseRepository) -> FakePaymentRow:
    now_utc = datetime.now(UTC)
    payment_id = uuid4()
    payment = FakePaymentRow(
        id=payment_id,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='succeeded',
        idempotency_key=payment_id,
        provider_payment_id='yk-pay-paid',
        created_at=now_utc - timedelta(days=1),
    )
    repo.payments[payment.id] = payment
    sub_id = uuid4()
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=payment.user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment.id,
        starts_at=now_utc - timedelta(days=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(days=1),
    )
    return payment


def _reconciler() -> tuple[
    PaymentReconciler, InMemoryPurchaseRepository, FakeProviderGateway
]:
    from clients.payment_provider_registry import PaymentProviderRegistry

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    return reconciler, repo, provider


async def test_resubmitted_form_after_rejection_does_not_create_new_attempt() -> None:
    """После отказа повтор той же формы возвращает отказ, а не новый POST."""
    reconciler, repo, provider = _reconciler()
    payment = _paid_subscription(repo)
    provider.create_refund_error = ProviderPermanentError(400, 'refund rejected')
    attempt_id = uuid4()

    first = await reconciler.create_or_reconcile_refund(
        payment.id, admin_user_id=uuid4(), reason='Жалоба', refund_id=attempt_id
    )
    provider.create_refund_error = None
    again = await reconciler.create_or_reconcile_refund(
        payment.id, admin_user_id=uuid4(), reason='Жалоба', refund_id=attempt_id
    )

    assert first.status == again.status == 'rejected'
    assert again.refund_id == attempt_id
    assert len(provider.create_refund_calls) == 1
    assert len(repo.refunds) == 1

    # Новая форма (новый UUID) — явная новая попытка после отказа.
    retry = await reconciler.create_or_reconcile_refund(
        payment.id, admin_user_id=uuid4(), reason='Повтор', refund_id=uuid4()
    )
    assert retry.refund_id != attempt_id
    assert len(provider.create_refund_calls) == 2


async def test_resubmitted_form_after_success_returns_existing_refund() -> None:
    """После выполненного возврата повтор возвращает его, а не 409."""
    reconciler, repo, provider = _reconciler()
    payment = _paid_subscription(repo)
    created = await reconciler.create_or_reconcile_refund(
        payment.id, admin_user_id=uuid4(), reason='Жалоба', refund_id=uuid4()
    )
    await repo.finalize_refund_succeeded(created.refund_id, source='webhook')

    again = await reconciler.create_or_reconcile_refund(
        payment.id, admin_user_id=uuid4(), reason='Ещё раз', refund_id=uuid4()
    )

    assert again.refund_id == created.refund_id
    assert again.status == 'succeeded'
    assert len(provider.create_refund_calls) == 1


async def test_attempt_uuid_of_another_payment_is_rejected() -> None:
    """UUID попытки нельзя переиспользовать для другого платежа."""
    reconciler, repo, _ = _reconciler()
    first = _paid_subscription(repo)
    second = _paid_subscription(repo)
    attempt_id = uuid4()
    await reconciler.create_or_reconcile_refund(
        first.id, admin_user_id=uuid4(), reason='Жалоба', refund_id=attempt_id
    )

    with pytest.raises(RuleConflictError) as exc_info:
        await reconciler.create_or_reconcile_refund(
            second.id, admin_user_id=uuid4(), reason='Жалоба', refund_id=attempt_id
        )
    assert exc_info.value.error_code == 'refund_id_conflict'
