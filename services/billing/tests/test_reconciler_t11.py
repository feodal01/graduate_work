from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    ProviderIntegrityMismatchError,
    ProviderPaymentSnapshot,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderTransientError,
)
from domain.subscription import (
    SubscriptionState,
    compute_subscription_state,
    deterministic_id,
    format_msk_datetime,
)
from services.reconciler import PaymentReconciler, ReconcileOutcome
from tests.fakes import FakePaymentRow, FakeProviderGateway, InMemoryPurchaseRepository


class StubProviderGateway(FakeProviderGateway):
    provider_code = 'yookassa'

    def __init__(self) -> None:
        super().__init__()
        self.get_calls: list[str] = []
        self.snapshot: ProviderPaymentSnapshot | None = None
        self.error: Exception | None = None

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        """Не должен вызываться при сверке платежа с известным provider_payment_id."""
        raise AssertionError('create_payment should not be called during GET reconcile')

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot:
        """Возвращает снимок состояния из ЮKassa или выбрасывает ошибку."""
        self.get_calls.append(provider_payment_id)
        if self.error is not None:
            raise self.error
        assert self.snapshot is not None
        return self.snapshot


def _seed_pending_payment(
    repo: InMemoryPurchaseRepository,
    *,
    provider_payment_id: str | None = 'yk-pay-101',
    confirmation_url: str | None = 'https://yoomoney.ru/checkout/101',
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
        confirmation_url=confirmation_url,
        last_checked_at=last_checked_at,
    )
    repo.payments[pid] = row
    return row


def test_compute_subscription_state_and_msk_formatting() -> None:
    """BIL-FR-08, инв. 12, 4.4: вычисление статуса подписки, МСК и детерминизм ID."""
    now = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
    future_end = now + timedelta(days=10)
    past_end = now - timedelta(seconds=1)

    assert (
        compute_subscription_state(
            ends_at=future_end,
            has_succeeded_refund=False,
            now_utc=now,
        )
        == SubscriptionState.ACTIVE
    )
    assert (
        compute_subscription_state(
            ends_at=now,
            has_succeeded_refund=False,
            now_utc=now,
        )
        == SubscriptionState.EXPIRED
    )
    assert (
        compute_subscription_state(
            ends_at=past_end,
            has_succeeded_refund=False,
            now_utc=now,
        )
        == SubscriptionState.EXPIRED
    )
    assert (
        compute_subscription_state(
            ends_at=future_end,
            has_succeeded_refund=True,
            now_utc=now,
        )
        == SubscriptionState.REFUNDED
    )
    assert (
        compute_subscription_state(
            ends_at=past_end,
            has_succeeded_refund=True,
            now_utc=now,
        )
        == SubscriptionState.REFUNDED
    )

    # 12:00 UTC -> 15:00 МСК
    assert format_msk_datetime(now) == '04.11.2026 15:00 МСК'

    # Детерминированный UUID (инв. 12)
    op_id = uuid4()
    assert deterministic_id('payment_succeeded_email', op_id) == deterministic_id(
        'payment_succeeded_email', op_id
    )
    assert deterministic_id('payment_succeeded_email', op_id) != deterministic_id(
        'payment_failed_email', op_id
    )


async def test_reconciler_succeeded_creates_subscription_version_and_outbox() -> None:
    """BIL-FR-04, BIL-FR-08, BIL-FR-13, инв. 6, 7, 11: фиксация succeeded."""
    repo = InMemoryPurchaseRepository()
    provider = StubProviderGateway()
    payment = _seed_pending_payment(repo)
    provider.snapshot = ProviderPaymentSnapshot(
        provider_code='yookassa',
        provider_payment_id='yk-pay-101',
        domain_status='succeeded',
        amount_kopeks=29_900,
        currency='RUB',
        confirmation_url=None,
        failure_reason=None,
        metadata_payment_id=str(payment.id),
        provider_details={'id': 'yk-pay-101', 'status': 'succeeded', 'paid': True},
    )

    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )
    outcome: ReconcileOutcome = await reconciler.reconcile_payment(
        payment.id,
        source='webhook',
        respect_throttle=False,
    )

    assert outcome.status == 'succeeded'
    assert outcome.transitioned is True
    assert repo.payments[payment.id].status == 'succeeded'
    assert payment.id in repo.subscriptions
    sub = repo.subscriptions[payment.id]
    assert sub.ends_at - sub.starts_at == timedelta(days=30)
    assert repo.access_versions[payment.user_id] == 1
    assert len(repo.outbox_tasks) == 3
    assert len(repo.events) == 1
    assert repo.events[0][:5] == (
        'payment',
        payment.id,
        'pending',
        'succeeded',
        'webhook',
    )

    # Повторный вызов для завершённого платежа не идёт в ЮKassa (инв. 7)
    outcome_again = await reconciler.reconcile_payment(
        payment.id,
        source='reconcile',
        respect_throttle=False,
    )
    assert outcome_again.status == 'succeeded'
    assert outcome_again.transitioned is False
    assert len(provider.get_calls) == 1
    assert repo.access_versions[payment.user_id] == 1
    assert len(repo.outbox_tasks) == 3
    assert len(repo.events) == 1


async def test_reconciler_failed_marks_payment_failed_and_enqueues_failure_email() -> (
    None
):
    """BIL-FR-06, BIL-FR-13: фиксация canceled -> failed с причиной и письмом."""
    repo = InMemoryPurchaseRepository()
    provider = StubProviderGateway()
    payment = _seed_pending_payment(repo)
    provider.snapshot = ProviderPaymentSnapshot(
        provider_code='yookassa',
        provider_payment_id='yk-pay-101',
        domain_status='failed',
        amount_kopeks=29_900,
        currency='RUB',
        confirmation_url=None,
        failure_reason='insufficient_funds',
        metadata_payment_id=str(payment.id),
        provider_details={
            'id': 'yk-pay-101',
            'status': 'canceled',
            'cancellation_details': {'party': 'payment_network'},
        },
    )

    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )
    outcome = await reconciler.reconcile_payment(
        payment.id,
        source='reconcile',
        respect_throttle=True,
    )

    assert outcome.status == 'failed'
    assert outcome.transitioned is True
    assert repo.payments[payment.id].status == 'failed'
    assert repo.payments[payment.id].failure_reason == 'insufficient_funds'
    assert payment.id not in repo.subscriptions
    assert payment.user_id not in repo.access_versions
    assert len(repo.outbox_tasks) == 1
    failed_task = next(iter(repo.outbox_tasks.values()))
    assert failed_task.task_type == 'payment_failed_email'
    assert repo.events[0][:5] == (
        'payment',
        payment.id,
        'pending',
        'failed',
        'reconcile',
    )
    event_details = repo.events[0][5]
    assert event_details is not None
    assert event_details['failure_reason'] == 'insufficient_funds'
    assert event_details['cancellation_details'] == {'party': 'payment_network'}


async def test_reconciler_pending_snapshot_and_lookup_branches() -> None:
    """Проверка ветки pending, reconcile_by_provider_payment_id и LookupError."""
    repo = InMemoryPurchaseRepository()
    provider = StubProviderGateway()
    payment = _seed_pending_payment(repo, confirmation_url=None)
    provider.snapshot = ProviderPaymentSnapshot(
        provider_code='yookassa',
        provider_payment_id='yk-pay-101',
        domain_status='pending',
        amount_kopeks=29_900,
        currency='RUB',
        confirmation_url='https://yoomoney.ru/checkout/new-101',
        failure_reason=None,
        metadata_payment_id=str(payment.id),
        provider_details={'id': 'yk-pay-101', 'status': 'pending'},
    )

    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    # 1. Сверка по provider_payment_id обновляет last_checked_at и confirmation_url
    outcome = await reconciler.reconcile_by_provider_payment_id(
        'yk-pay-101',
        source='webhook',
        respect_throttle=False,
    )
    assert outcome is not None
    assert outcome.status == 'pending'
    assert outcome.transitioned is False
    assert outcome.confirmation_url == 'https://yoomoney.ru/checkout/new-101'
    assert repo.payments[payment.id].last_checked_at is not None

    # 2. Неизвестный внешний provider_payment_id возвращает None
    unknown_by_provider = await reconciler.reconcile_by_provider_payment_id(
        'yk-nonexistent',
        source='webhook',
    )
    assert unknown_by_provider is None

    # 3. Неизвестный внутренний payment_id вызывает LookupError
    with pytest.raises(LookupError):
        await reconciler.reconcile_payment(
            uuid4(),
            source='admin',
            respect_throttle=False,
        )

    # 4. Платёж без provider_payment_id возвращается без вызова GET в ЮKassa
    no_provider_payment = _seed_pending_payment(repo, provider_payment_id=None)
    before_calls = len(provider.get_calls)
    no_provider_outcome = await reconciler.reconcile_payment(
        no_provider_payment.id,
        source='reconcile',
        respect_throttle=True,
    )
    assert no_provider_outcome.status == 'pending'
    assert no_provider_outcome.transitioned is False
    assert len(provider.get_calls) == before_calls


async def test_reconciler_throttle_for_continue_and_bypass_for_admin() -> None:
    """BIL-NFR-R-06, BIL-FR-15: лимит 5 с (без confirmation_url) и обход для admin."""
    repo = InMemoryPurchaseRepository()
    provider = StubProviderGateway()
    recent_check = datetime.now(UTC) - timedelta(seconds=1)
    payment = _seed_pending_payment(
        repo,
        confirmation_url=None,
        last_checked_at=recent_check,
    )
    provider.snapshot = ProviderPaymentSnapshot(
        provider_code='yookassa',
        provider_payment_id='yk-pay-101',
        domain_status='succeeded',
        amount_kopeks=29_900,
        currency='RUB',
        confirmation_url=None,
        failure_reason=None,
        metadata_payment_id=str(payment.id),
        provider_details={'id': 'yk-pay-101', 'status': 'succeeded'},
    )

    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    # При respect_throttle=True и свежем last_checked_at запрос в ЮKassa не отправляется
    throttled = await reconciler.reconcile_payment(
        payment.id,
        source='continue',
        respect_throttle=True,
    )
    assert throttled.status == 'pending'
    assert throttled.throttled is True
    assert throttled.transitioned is False
    assert len(provider.get_calls) == 0

    # Для webhook или admin (respect_throttle=False) сверка идёт сразу в обход 5 с
    forced = await reconciler.reconcile_payment(
        payment.id,
        source='admin',
        respect_throttle=False,
    )
    assert forced.status == 'succeeded'
    assert forced.throttled is False
    assert forced.transitioned is True
    assert len(provider.get_calls) == 1
    assert repo.events[0][4] == 'admin'


@pytest.mark.parametrize(
    'provider_error',
    [
        ProviderTransientError('YooKassa timeout'),
        ProviderReconciliationError(401, 'Unauthorized on GET'),
        ProviderProtocolAnomalyError('waiting_for_capture anomaly'),
        ProviderIntegrityMismatchError('amount mismatch'),
    ],
)
async def test_reconciler_unhappy_provider_errors_never_corrupt_pending_payment(
    provider_error: Exception,
) -> None:
    """Инв. 10, 15, 16, 17: при сбоях или аномалиях статус остаётся pending."""
    repo = InMemoryPurchaseRepository()
    provider = StubProviderGateway()
    payment = _seed_pending_payment(repo)
    provider.error = provider_error

    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )
    with pytest.raises(type(provider_error)):
        await reconciler.reconcile_payment(
            payment.id,
            source='reconcile',
            respect_throttle=False,
        )

    assert repo.payments[payment.id].status == 'pending'
    assert payment.id not in repo.subscriptions
    assert len(repo.outbox_tasks) == 0
    assert len(repo.events) == 0
