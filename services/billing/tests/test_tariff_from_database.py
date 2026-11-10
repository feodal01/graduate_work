"""Тариф берётся только из таблицы `tariffs`, а не из кода.

Регрессия: название «Премиум», срок «30 дней» и цена «299 ₽» были зашиты в
описание платежа ЮKassa, тексты писем и экран «Моя подписка». После смены
строки в `tariffs` новые покупки шли по новой цене, а тексты оставались старыми.
"""

import dataclasses
from datetime import timedelta
from uuid import uuid4

import pytest

from clients.payment_provider_registry import PaymentProviderRegistry
from domain.errors import DependencyUnavailableError
from domain.subscription import build_payment_description, format_days_ru
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from tests.fakes import FakeProviderGateway, InMemoryPurchaseRepository


def _service() -> tuple[
    PurchaseService, InMemoryPurchaseRepository, FakeProviderGateway
]:
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    registry = PaymentProviderRegistry.single(provider)
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )
    service = PurchaseService(
        payments=repo,
        user_views=repo,
        payment_providers=registry,
        public_base_url='http://localhost',
        reconciler=reconciler,
    )
    return service, repo, provider


@pytest.mark.parametrize(
    ('days', 'text'),
    [
        (1, '1 день'),
        (2, '2 дня'),
        (5, '5 дней'),
        (11, '11 дней'),
        (21, '21 день'),
        (30, '30 дней'),
        (92, '92 дня'),
        (112, '112 дней'),
        (365, '365 дней'),
    ],
)
def test_format_days_ru(days: int, text: str) -> None:
    """Срок тарифа склоняется по-русски для любого числа дней."""
    assert format_days_ru(days) == text


async def test_changed_tariff_reaches_payment_screen_and_emails() -> None:
    """Новые цена, срок и название тарифа видны везде без изменения кода."""
    service, repo, provider = _service()
    repo.tariff = dataclasses.replace(
        repo.tariff, title='Годовой', price_kopeks=99_900, duration_days=365
    )
    user_id = uuid4()

    view = await service.get_my_subscription_view(user_id=user_id, user_tz='UTC')
    assert view.action_label == 'Оформить за 999 ₽'

    outcome = await service.start_or_continue_purchase(user_id)
    payment = repo.payments[outcome.payment_id]
    assert payment.amount_kopeks == 99_900
    assert provider.create_calls[-1].description == build_payment_description(
        tariff_title='Годовой', duration_days=365
    )
    assert provider.create_calls[-1].description == 'Подписка «Годовой» на 365 дней'

    _, transitioned = await repo.finalize_payment_succeeded(
        payment.id, source='webhook'
    )
    assert transitioned
    sub = repo.subscriptions[payment.id]
    assert sub.ends_at - sub.starts_at == timedelta(days=365)
    emails = [
        t.payload
        for t in repo.outbox_tasks.values()
        if t.payment_id == payment.id and t.target_service == 'notifications'
    ]
    assert emails
    for payload in emails:
        assert '«Годовой»' in str(payload['subject'])
        assert 'Премиум' not in str(payload['subject']) + str(payload['text'])


async def test_no_active_tariff_means_no_purchase_and_no_invented_price() -> None:
    """Нет активного тарифа: кнопки покупки нет, создать платёж нельзя (503)."""
    service, repo, provider = _service()
    repo.tariff_active = False
    user_id = uuid4()

    view = await service.get_my_subscription_view(user_id=user_id, user_tz='UTC')
    assert view.can_subscribe is False
    assert view.action_label is None

    with pytest.raises(DependencyUnavailableError) as exc_info:
        await service.start_or_continue_purchase(user_id)
    assert exc_info.value.error_code == 'tariff_unavailable'
    assert provider.create_calls == []
