"""Возврат на границах неизвестного исхода и 24-часового ключа (инв. 14, 15).

Инв. 15: 400/401/403 на повторный POST /v3/refunds после отправки с
неизвестным исходом не доказывает, что возврата нет: попытка остаётся
«В обработке», POST по ней больше не шлётся, сигнал — один на попытку.
Инв. 14: у границы жизни ключа POST не повторяется; «не создан» — только после
нескольких пустых розысков по списку с паузой; поздний ответ POST по уже
завершённой попытке поднимает сигнал. Параметры — настройки `BILLING_REFUND_*`.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
import sentry_sdk
from pydantic import ValidationError

from clients.payment_provider import (
    CreateProviderRefundRequest,
    ProviderPermanentError,
    ProviderRefundSnapshot,
    ProviderTransientError,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from core.config import WorkerSettings, settings
from repositories.records import (
    LOOKUP_NOT_FOUND_KEY,
    POST_OUTCOME_UNKNOWN_KEY,
    POST_STOPPED_KEY,
)
from services.reconciler import PaymentReconciler, RefundKeyPolicy
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Сообщения, отправленные в GlitchTip через sentry_sdk.capture_message."""
    captured: list[str] = []

    def fake_capture_message(message: str, level: str = 'info', **_: object) -> str:
        if level == 'error':
            captured.append(message)
        return 'evt-id'

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture_message)
    return captured


def _setup() -> tuple[
    PaymentReconciler, InMemoryPurchaseRepository, FakeProviderGateway, UUID
]:
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
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
        provider_payment_id='yk-pay-boundary',
        created_at=now_utc - timedelta(days=3),
    )
    repo.payments[payment_id] = payment
    sub_id = uuid4()
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=payment.user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment_id,
        starts_at=now_utc - timedelta(days=3),
        ends_at=now_utc + timedelta(days=27),
        created_at=now_utc - timedelta(days=3),
    )
    repo.access_versions[payment.user_id] = 1
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    return reconciler, repo, provider, payment_id


async def _attempt_with_lost_post_answer(
    reconciler: PaymentReconciler,
    provider: FakeProviderGateway,
    payment_id: UUID,
) -> UUID:
    """Первый POST ушёл, но ответ потерялся (тайм-аут): исход неизвестен."""
    provider.create_refund_error = ProviderTransientError('timeout')
    outcome = await reconciler.create_or_reconcile_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    assert outcome.status == 'processing'
    return outcome.refund_id


def _count_lookups(provider: FakeProviderGateway) -> list[UUID]:
    calls: list[UUID] = []
    original = provider.find_refund_by_metadata

    async def counting(
        provider_payment_id: str, *, refund_id: UUID, **kwargs: Any
    ) -> ProviderRefundSnapshot | None:
        calls.append(refund_id)
        return await original(provider_payment_id, refund_id=refund_id, **kwargs)

    provider.find_refund_by_metadata = counting  # type: ignore[method-assign]
    return calls


# --- Инв. 15: 400/401/403 после POST с неизвестным исходом ----------------------


async def test_unknown_outcome_post_is_marked_durably_on_admin_path() -> None:
    """Сбой первого POST из формы админки оставляет отметку в provider_details."""
    reconciler, repo, provider, payment_id = _setup()

    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)

    details = repo.refunds[refund_id].provider_details or {}
    assert details.get(POST_OUTCOME_UNKNOWN_KEY) is True


async def test_unknown_outcome_post_is_marked_durably_on_background_path() -> None:
    """Сбой POST в фоновой сверке тоже ставит отметку, а не только счётчик."""
    reconciler, repo, provider, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    provider.create_refund_error = ProviderTransientError('YooKassa 503')

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    details = repo.refunds[refund.id].provider_details or {}
    assert details.get(POST_OUTCOME_UNKNOWN_KEY) is True
    assert repo.refunds[refund.id].status == 'processing'


async def test_permanent_error_after_unknown_post_keeps_refund_in_background(
    signals: list[str],
) -> None:
    """401 на повтор POST после тайм-аута — не отказ: решает розыск после 24 ч."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)
    refund = repo.refunds[refund_id]
    provider.metadata_refunds[refund_id] = 'yk-ref-real'
    provider.create_refund_error = ProviderPermanentError(401, 'invalid credentials')

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert refund.status == 'processing'
    assert refund.rejection_reason is None
    assert any(str(refund_id) in m and '401' in m for m in signals)
    assert not any(e[3] == 'rejected' for e in repo.events)

    # Через 24 часа розыск по metadata находит возврат, созданный первым POST.
    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    provider.create_refund_error = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert refund.provider_refund_id == 'yk-ref-real'
    assert refund.status == 'succeeded'


async def test_permanent_error_after_unknown_post_keeps_refund_on_admin_resubmit(
    signals: list[str],
) -> None:
    """Повтор той же формы получил 400 после тайм-аута — попытка в обработке."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)
    provider.create_refund_error = ProviderPermanentError(400, 'amount exceeds')

    outcome = await reconciler.create_or_reconcile_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат', refund_id=refund_id
    )

    assert outcome.status == 'processing'
    assert repo.refunds[refund_id].status == 'processing'
    assert any(str(refund_id) in m for m in signals)


async def test_first_send_permanent_error_still_rejects_in_background() -> None:
    """Без отправки с неизвестным исходом 400 на POST — по-прежнему отказ."""
    reconciler, repo, provider, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    provider.create_refund_error = ProviderPermanentError(400, 'bad request')

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert repo.refunds[refund.id].status == 'rejected'
    assert repo.refunds[refund.id].rejection_reason == 'bad request'


async def test_unknown_outcome_marker_survives_provider_details_rewrites() -> None:
    """Сохранение снимков ЮKassa не стирает отметку, пока возврат в обработке."""
    _, repo, _, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    await repo.mark_refund_post_outcome_unknown(refund.id)

    await repo.save_provider_refund_created(
        refund.id, provider_refund_id='yk-ref-1', provider_details={'id': 'yk-ref-1'}
    )
    await repo.record_refund_checked(
        refund.id, provider_details={'id': 'yk-ref-1', 'status': 'pending'}
    )

    details = repo.refunds[refund.id].provider_details or {}
    assert details['status'] == 'pending'
    assert details[POST_OUTCOME_UNKNOWN_KEY] is True


# --- Инв. 14: граница 24-часового ключа ----------------------------------------


@pytest.mark.parametrize('age', [timedelta(hours=23), timedelta(hours=23, minutes=59)])
async def test_no_refund_post_in_last_hour_before_key_expiry(age: timedelta) -> None:
    """С 23 ч до розыска после 24 ч попытка ждёт: ни POST, ни списка."""
    reconciler, repo, provider, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    refund.created_at = datetime.now(UTC) - age
    lookups = _count_lookups(provider)

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    outcome = await reconciler.create_or_reconcile_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат', refund_id=refund.id
    )

    assert provider.create_refund_calls == []
    assert lookups == []
    assert outcome.status == 'processing'
    assert repo.refunds[refund.id].status == 'processing'


async def test_refund_post_is_still_repeated_before_last_hour() -> None:
    """До 23 ч повтор POST по тому же ключу идёт, как раньше (инв. 14)."""
    reconciler, repo, provider, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    refund.created_at = datetime.now(UTC) - timedelta(hours=22, minutes=59)

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert [c.refund_id for c in provider.create_refund_calls] == [refund.id]


async def test_not_created_only_after_two_empty_lookups_with_interval() -> None:
    """Первый пустой розыск отмечается; отказ — после второго через интервал."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)
    refund = repo.refunds[refund_id]
    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    lookups = _count_lookups(provider)

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert lookups == [refund_id]
    assert refund.status == 'processing'
    marker = (refund.provider_details or {})[LOOKUP_NOT_FOUND_KEY]
    assert marker['count'] == 1

    # До истечения интервала повторного розыска нет.
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    assert lookups == [refund_id]
    assert refund.status == 'processing'

    # Через интервал второй пустой розыск переводит попытку в «Отклонён».
    marker['last_at'] = (
        datetime.now(UTC) - RefundKeyPolicy().lookup_interval - timedelta(seconds=1)
    ).isoformat()
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert lookups == [refund_id, refund_id]
    assert refund.status == 'rejected'
    assert refund.rejection_reason == 'not_created_in_provider'


async def test_refund_appearing_in_list_on_second_lookup_is_attached() -> None:
    """Возврат, появившийся в списке ко второму розыску, привязывается к попытке."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)
    refund = repo.refunds[refund_id]
    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    assert refund.status == 'processing'

    provider.metadata_refunds[refund_id] = 'yk-ref-late-in-list'
    marker = (refund.provider_details or {})[LOOKUP_NOT_FOUND_KEY]
    marker['last_at'] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert refund.provider_refund_id == 'yk-ref-late-in-list'
    assert refund.status == 'succeeded'
    assert repo.access_versions[repo.payments[payment_id].user_id] == 2


async def test_late_post_answer_for_finished_attempt_raises_signal(
    signals: list[str],
) -> None:
    """Ответ POST с номером по уже завершённой попытке не теряется молча."""
    reconciler, repo, provider, payment_id = _setup()
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    original_create = provider.create_refund

    async def create_while_attempt_is_closed(
        request: CreateProviderRefundRequest,
    ) -> ProviderRefundSnapshot:
        snapshot = await original_create(request)
        # Пока шёл POST, другой процесс завершил попытку.
        await repo.finalize_refund_rejected(
            request.refund_id,
            rejection_reason='not_created_in_provider',
            source='reconcile',
        )
        return snapshot

    provider.create_refund = create_while_attempt_is_closed  # type: ignore[method-assign]

    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert refund.status == 'rejected'
    assert refund.provider_refund_id is None
    assert any(
        str(refund.id) in m and "'rejected'" in m and 'yk-ref-1' in m for m in signals
    )


# --- После 400/401/403 на повтор POST больше не шлётся ----------------------------


async def _attempt_with_post_stopped(
    reconciler: PaymentReconciler,
    repo: InMemoryPurchaseRepository,
    provider: FakeProviderGateway,
    payment_id: UUID,
) -> UUID:
    """Тайм-аут на первом POST, затем 401 на повторе в фоновой сверке."""
    refund_id = await _attempt_with_lost_post_answer(reconciler, provider, payment_id)
    provider.create_refund_error = ProviderPermanentError(401, 'invalid credentials')
    repo.refunds[refund_id].last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    return refund_id


async def test_post_is_stopped_after_permanent_answer_following_unknown_outcome(
    signals: list[str],
) -> None:
    """Постоянный ответ не повторяется: ни фоновая сверка, ни форма не шлют POST."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_post_stopped(reconciler, repo, provider, payment_id)
    refund = repo.refunds[refund_id]
    calls_when_stopped = len(provider.create_refund_calls)
    assert calls_when_stopped == 2
    assert (refund.provider_details or {})[POST_STOPPED_KEY] is True

    for _ in range(3):
        refund.last_checked_at = None
        await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    resubmitted = await reconciler.create_or_reconcile_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат', refund_id=refund_id
    )

    assert len(provider.create_refund_calls) == calls_when_stopped
    assert resubmitted.status == 'processing'
    assert refund.status == 'processing'
    # Сигнал сверки — один, когда отметка поставлена впервые.
    assert len([m for m in signals if str(refund_id) in m]) == 1
    # Отметка переживает сохранение снимка ЮKassa.
    await repo.save_provider_refund_created(
        refund_id, provider_refund_id='yk-ref-x', provider_details={'id': 'yk-ref-x'}
    )
    assert (refund.provider_details or {})[POST_STOPPED_KEY] is True


async def test_stopped_attempt_is_found_by_lookup_after_key_expiry() -> None:
    """После 24 ч розыск находит возврат первого POST и сверка его проводит."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_post_stopped(reconciler, repo, provider, payment_id)
    refund = repo.refunds[refund_id]
    calls_when_stopped = len(provider.create_refund_calls)
    provider.metadata_refunds[refund_id] = 'yk-ref-first-post'

    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert len(provider.create_refund_calls) == calls_when_stopped
    assert refund.provider_refund_id == 'yk-ref-first-post'
    assert refund.status == 'succeeded'


async def test_stopped_attempt_not_found_twice_is_rejected() -> None:
    """После 24 ч два пустых розыска с паузой — «не создан», без новых POST."""
    reconciler, repo, provider, payment_id = _setup()
    refund_id = await _attempt_with_post_stopped(reconciler, repo, provider, payment_id)
    refund = repo.refunds[refund_id]
    calls_when_stopped = len(provider.create_refund_calls)

    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    assert refund.status == 'processing'
    marker = (refund.provider_details or {})[LOOKUP_NOT_FOUND_KEY]
    marker['last_at'] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert len(provider.create_refund_calls) == calls_when_stopped
    assert refund.status == 'rejected'
    assert refund.rejection_reason == 'not_created_in_provider'


# --- Настройки BILLING_REFUND_* -------------------------------------------------


def test_refund_policy_defaults_match_settings_defaults() -> None:
    """Умолчания политики в коде и в настройках совпадают."""
    assert RefundKeyPolicy.from_settings(WorkerSettings.model_construct()) == (
        RefundKeyPolicy()
    )
    assert RefundKeyPolicy() == RefundKeyPolicy(
        post_stop_before_key_expiry=timedelta(hours=1),
        lookup_not_found_attempts=2,
        lookup_interval=timedelta(minutes=5),
    )


async def test_refund_settings_reach_reconciler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`BILLING_REFUND_*` доходят до сверки, которую собирает billing-api."""
    from dependencies import get_payment_reconciler
    from services.outbox_dispatcher import OutboxDispatcher
    from tests.fakes import in_memory_repositories

    worker = settings.worker
    monkeypatch.setattr(worker, 'billing_refund_post_stop_before_key_expiry_seconds', 0)
    monkeypatch.setattr(worker, 'billing_refund_lookup_not_found_attempts', 3)
    monkeypatch.setattr(worker, 'billing_refund_lookup_interval_seconds', 900)
    repo = InMemoryPurchaseRepository()

    reconciler = await get_payment_reconciler(
        repos=in_memory_repositories(repo),
        payment_providers=PaymentProviderRegistry.single(FakeProviderGateway()),
        outbox_dispatcher=OutboxDispatcher(repository=repo, auth_gateway=None),  # type: ignore[arg-type]
    )

    assert reconciler._refund_policy == RefundKeyPolicy(
        post_stop_before_key_expiry=timedelta(0),
        lookup_not_found_attempts=3,
        lookup_interval=timedelta(minutes=15),
    )


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('billing_refund_post_stop_before_key_expiry_seconds', 24 * 3600),
        ('billing_refund_post_stop_before_key_expiry_seconds', -1),
        ('billing_refund_lookup_not_found_attempts', 0),
        ('billing_refund_lookup_interval_seconds', 0),
    ],
)
def test_refund_settings_are_validated(field: str, value: float) -> None:
    """Запас меньше 24 ч, хотя бы один розыск, пауза больше нуля."""
    with pytest.raises(ValidationError):
        WorkerSettings.model_validate({field: value})


async def test_policy_values_are_honoured_by_reconciler() -> None:
    """Запас 0 — POST до самых 24 ч; один розыск — отказ сразу."""
    _, repo, provider, payment_id = _setup()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        refund_policy=RefundKeyPolicy(
            post_stop_before_key_expiry=timedelta(0),
            lookup_not_found_attempts=1,
        ),
    )
    refund, _ = await repo.get_or_create_processing_refund(
        payment_id, admin_user_id=uuid4(), reason='Возврат'
    )
    refund.created_at = datetime.now(UTC) - timedelta(hours=23, minutes=30)
    provider.create_refund_error = ProviderTransientError('timeout')
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    assert len(provider.create_refund_calls) == 1

    refund.created_at = datetime.now(UTC) - timedelta(hours=25)
    refund.last_checked_at = None
    await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
    assert refund.status == 'rejected'
    assert refund.rejection_reason == 'not_created_in_provider'
