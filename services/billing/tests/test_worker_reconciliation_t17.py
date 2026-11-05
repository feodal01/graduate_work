from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from clients.payment_provider import (
    ProviderIntegrityMismatchError,
    ProviderPaymentSnapshot,
    ProviderPermanentError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderTransientError,
)
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    ConfigurableAuthGateway,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


def _create_pending_payment(
    repo: InMemoryPurchaseRepository,
    *,
    created_at: datetime,
    provider_payment_id: str | None = 'yk-bg-1',
    last_checked_at: datetime | None = None,
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=uuid4(),
        tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        provider_payment_id=provider_payment_id,
        confirmation_url='https://yoomoney.ru/checkout/bg-1',
        last_checked_at=last_checked_at,
        created_at=created_at,
        updated_at=created_at,
    )
    repo.payments[pid] = row
    return row


@pytest.mark.asyncio
async def test_background_reconcile_respects_min_age_seconds() -> None:
    """BIL-FR-05: свежий платёж младше порога пропускается, старше — сверяется."""
    now = datetime.now(UTC)
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth = ConfigurableAuthGateway(user_id=uuid4())
    dispatcher = OutboxDispatcher(repository=repo, auth_gateway=auth)
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    # 1. Платёж создан 1 секунду назад (< min_age_seconds=5.0)
    young_payment = _create_pending_payment(
        repo,
        created_at=now - timedelta(seconds=1),
        provider_payment_id='yk-young-1',
    )
    # 2. Платёж создан 10 секунд назад (>= min_age_seconds=5.0)
    mature_payment = _create_pending_payment(
        repo,
        created_at=now - timedelta(seconds=10),
        provider_payment_id='yk-mature-1',
    )
    provider.get_status = 'succeeded'

    processed = await reconciler.reconcile_due_pending_payments(
        min_age_seconds=5.0,
        limit=20,
    )

    assert processed == 1
    assert provider.get_calls == ['yk-mature-1']
    assert repo.payments[young_payment.id].status == 'pending'
    assert repo.payments[mature_payment.id].status == 'succeeded'
    assert mature_payment.id in repo.subscriptions


@pytest.mark.asyncio
async def test_background_reconcile_recreates_missing_provider_id_within_24h() -> None:
    """BIL-FR-05, инв. 6, 13: платёж без номера младше 24 ч досоздаётся и сверяется."""
    now = datetime.now(UTC)
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway(id_prefix='yk-recovered')
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    # Платёж создан 2 часа назад, процесс упал до сохранения provider_payment_id
    uncreated_payment = _create_pending_payment(
        repo,
        created_at=now - timedelta(hours=2),
        provider_payment_id=None,
    )
    provider.get_status = 'succeeded'

    processed = await reconciler.reconcile_due_pending_payments(
        min_age_seconds=5.0,
        limit=20,
    )

    assert processed == 1
    assert len(provider.create_calls) == 1
    assert provider.create_calls[0].idempotency_key == uncreated_payment.idempotency_key
    assert provider.get_calls == ['yk-recovered-1']
    assert repo.payments[uncreated_payment.id].provider_payment_id == 'yk-recovered-1'
    assert repo.payments[uncreated_payment.id].status == 'succeeded'


@pytest.mark.asyncio
async def test_background_reconcile_closes_payment_older_than_24h() -> None:
    """BIL-FR-05, BIL-FR-06, инв. 13: через 24 часа без номера платёж -> failed."""
    now = datetime.now(UTC)
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    expired_payment = _create_pending_payment(
        repo,
        created_at=now - timedelta(hours=24, minutes=5),
        provider_payment_id=None,
    )

    processed = await reconciler.reconcile_due_pending_payments(
        min_age_seconds=5.0,
        limit=20,
    )

    assert processed == 1
    assert len(provider.create_calls) == 0
    assert len(provider.get_calls) == 0
    assert repo.payments[expired_payment.id].status == 'failed'
    assert repo.payments[expired_payment.id].failure_reason == 'idempotency_key_expired'
    assert f'payment_failed:{expired_payment.id}:notify_failed' in repo.outbox_tasks


@pytest.mark.asyncio
async def test_background_reconcile_handles_4xx_and_anomalies() -> None:
    """Инв. 10, 15, 16, 17: 401 на POST -> failed; 401/аномалии на GET -> pending."""
    now = datetime.now(UTC)
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    # 1. Платёж без номера получает 401 на POST -> переводится в failed (инв. 15 п. 1)
    p_create_401 = _create_pending_payment(
        repo,
        created_at=now - timedelta(minutes=1),
        provider_payment_id=None,
    )
    provider.create_error = ProviderPermanentError(401, 'Unauthorized on POST')
    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    assert repo.payments[p_create_401.id].status == 'failed'
    provider.create_error = None

    # 2. Платёж с номером получает 401/аномалию на GET -> остаётся pending
    for err in (
        ProviderReconciliationError(401, 'Unauthorized on GET'),
        ProviderProtocolAnomalyError('waiting_for_capture anomaly'),
        ProviderIntegrityMismatchError('amount mismatch'),
        ProviderTransientError('503 from YooKassa'),
    ):
        p_problem = _create_pending_payment(
            repo,
            created_at=now - timedelta(minutes=2),
            provider_payment_id=f'yk-prob-{uuid4().hex[:6]}',
        )
        p_healthy = _create_pending_payment(
            repo,
            created_at=now - timedelta(minutes=1),
            provider_payment_id=f'yk-ok-{uuid4().hex[:6]}',
        )

        orig_get = provider.get_payment

        async def selective_get(
            provider_payment_id: str,
            *,
            expected_payment_id: UUID | None = None,
            expected_amount_kopeks: int | None = None,
            expected_currency: str | None = None,
            _err: Exception = err,
            _prob_id: str | None = p_problem.provider_payment_id,
        ) -> ProviderPaymentSnapshot:
            if provider_payment_id == _prob_id:
                raise _err
            provider.get_status = 'succeeded'
            return await orig_get(
                provider_payment_id,
                expected_payment_id=expected_payment_id,
                expected_amount_kopeks=expected_amount_kopeks,
                expected_currency=expected_currency,
            )

        provider.get_payment = selective_get  # type: ignore[method-assign]
        await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
        assert repo.payments[p_problem.id].status == 'pending'
        assert repo.payments[p_problem.id].last_checked_at is not None
        prob_details = repo.payments[p_problem.id].provider_details
        assert prob_details is not None and prob_details.get('_reconcile_errors') == 1
        assert repo.payments[p_healthy.id].status == 'succeeded'
        provider.get_payment = orig_get  # type: ignore[method-assign]

    # 3. Повторная ошибка сверки увеличивает счётчик ошибок и backoff
    p_backoff = _create_pending_payment(
        repo,
        created_at=now - timedelta(minutes=10),
        provider_payment_id='yk-backoff-test',
    )
    provider.get_error = ProviderTransientError('YooKassa 503')
    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    first_checked = repo.payments[p_backoff.id].last_checked_at
    assert first_checked is not None

    # Сдвигаем last_checked_at назад, чтобы имитировать наступление 2-й попытки
    repo.payments[p_backoff.id].last_checked_at = now - timedelta(seconds=10)
    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    second_checked = repo.payments[p_backoff.id].last_checked_at
    assert second_checked is not None
    # На 2-й ошибке backoff = 10s -> last_checked_at сдвинут вперёд в будущее
    assert second_checked > datetime.now(UTC)
    details = repo.payments[p_backoff.id].provider_details
    assert details is not None and details.get('_reconcile_errors') == 2
