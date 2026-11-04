from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    ProviderPaymentSnapshot,
    ProviderPermanentError,
    ProviderTransientError,
)
from dependencies import AuthenticatedUser, get_auth_gateway, get_payment_provider
from main import create_app


@dataclass
class FakePaymentRow:
    id: UUID
    user_id: UUID
    tariff_id: UUID
    provider_code: str
    amount_kopeks: int
    currency: str
    status: str
    idempotency_key: UUID
    provider_payment_id: str | None = None
    confirmation_url: str | None = None
    failure_reason: str | None = None
    last_checked_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class FakePurchaseRepository:
    """In-memory репозиторий покупок для модульных TDD-тестов T-10."""

    def __init__(self) -> None:
        self.has_active_subscription: bool = False
        self.has_processing_refund: bool = False
        self.payments: dict[UUID, FakePaymentRow] = {}
        self.events: list[tuple[str, UUID, str | None, str, str]] = []

    async def check_purchase_allowed(self, user_id: UUID) -> tuple[bool, str | None]:
        """Проверяет отсутствие активной подписки и возврата в обработке."""
        if self.has_active_subscription:
            return False, 'subscription_already_active'
        if self.has_processing_refund:
            return False, 'refund_in_processing'
        return True, None

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[FakePaymentRow, bool]:
        """Возвращает существующий или создаёт новый pending-платёж."""
        from api.errors import BillingDomainError

        allowed, block_code = await self.check_purchase_allowed(user_id)
        if not allowed:
            if block_code == 'subscription_already_active':
                raise BillingDomainError(
                    'Подписка уже активна',
                    error_code='subscription_already_active',
                    status_code=409,
                )
            raise BillingDomainError(
                'Нельзя оформить новую подписку, пока возврат находится в обработке',
                error_code='refund_in_processing',
                status_code=409,
            )
        for payment in self.payments.values():
            if payment.user_id == user_id and payment.status == 'pending':
                return payment, False
        pid = uuid4()
        row = FakePaymentRow(
            id=pid,
            user_id=user_id,
            tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='pending',
            idempotency_key=pid,
        )
        self.payments[pid] = row
        self.events.append(('payment', pid, None, 'pending', 'continue'))
        return row, True

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, object],
    ) -> FakePaymentRow:
        """Сохраняет номер платежа провайдера и confirmation_url."""
        row = self.payments[payment_id]
        if row.status == 'pending':
            row.provider_payment_id = provider_payment_id
            if confirmation_url is not None:
                row.confirmation_url = confirmation_url
        return row

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, object] | None = None,
    ) -> FakePaymentRow:
        """Фиксирует время последней сверки GET (last_checked_at)."""
        row = self.payments[payment_id]
        if row.status == 'pending':
            if confirmation_url:
                row.confirmation_url = confirmation_url
            row.last_checked_at = datetime.now(UTC)
        return row

    async def mark_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: str,
        provider_details: dict[str, object] | None = None,
    ) -> FakePaymentRow:
        """Переводит платёж в статус failed."""
        row = self.payments[payment_id]
        if row.status == 'pending':
            old_status = row.status
            row.status = 'failed'
            row.failure_reason = failure_reason
            self.events.append(('payment', payment_id, old_status, 'failed', source))
        return row


class FakeProviderGateway:
    provider_code = 'yookassa'

    def __init__(self) -> None:
        self.create_calls: list[CreateProviderPaymentRequest] = []
        self.get_calls: list[str] = []
        self.create_error: Exception | None = None
        self.get_status: str = 'pending'
        self.next_provider_num: int = 1

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        """Эмулирует создание платежа в ЮKassa."""
        self.create_calls.append(request)
        if self.create_error is not None:
            raise self.create_error
        provider_id = f'yk-pay-{self.next_provider_num}'
        self.next_provider_num += 1
        return ProviderPaymentSnapshot(
            provider_code='yookassa',
            provider_payment_id=provider_id,
            domain_status='pending',
            amount_kopeks=request.amount_kopeks,
            currency=request.currency,
            confirmation_url=(
                f'https://yoomoney.ru/checkout/payments/v2/contract?orderId={provider_id}'
            ),
            failure_reason=None,
            metadata_payment_id=str(request.payment_id),
            provider_details={'id': provider_id, 'status': 'pending'},
        )

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot:
        """Эмулирует чтение статуса платежа в ЮKassa."""
        self.get_calls.append(provider_payment_id)
        return ProviderPaymentSnapshot(
            provider_code='yookassa',
            provider_payment_id=provider_payment_id,
            domain_status=self.get_status,  # type: ignore[arg-type]
            amount_kopeks=expected_amount_kopeks or 29_900,
            currency=expected_currency or 'RUB',
            confirmation_url=(
                f'https://yoomoney.ru/checkout/payments/v2/contract?orderId={provider_payment_id}'
            ),
            failure_reason=(
                'expired_on_confirmation' if self.get_status == 'failed' else None
            ),
            metadata_payment_id=(
                str(expected_payment_id) if expected_payment_id else None
            ),
            provider_details={'id': provider_payment_id, 'status': self.get_status},
        )


class StubAuthGateway:
    def __init__(self, user_id: UUID) -> None:
        self.user_id = user_id

    async def login(
        self, login: str, password: str, *, request_id: str
    ) -> tuple[str, str]:
        """Эмулирует вход."""
        return 'acc', 'ref'

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        """Возвращает пользователя для тестового токена."""
        return self.user_id, 'viewer', 'Europe/Moscow'

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> None:
        """Эмулирует выход."""
        return None


@pytest.fixture
async def purchase_test_env() -> AsyncGenerator[
    tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
    None,
]:
    """Создаёт тестовое окружение для сценариев покупки T-10."""
    from dependencies import get_purchase_service
    from services.purchase import PurchaseService

    user_id = uuid4()
    user = AuthenticatedUser(
        user_id=user_id,
        login='viewer',
        tz='Europe/Moscow',
        access_token='valid-token',
    )
    repo = FakePurchaseRepository()
    provider = FakeProviderGateway()
    service = PurchaseService(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_auth_gateway] = lambda: StubAuthGateway(user_id)
    app.dependency_overrides[get_payment_provider] = lambda: provider
    app.dependency_overrides[get_purchase_service] = lambda: service

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={
            'X-Request-Id': 'purchase-test-req',
            'Authorization': 'Bearer valid-token',
        },
        follow_redirects=False,
    ) as client:
        yield client, app, repo, provider, user


@pytest.mark.asyncio
async def test_create_purchase_creates_pending_payment_calls_yookassa_and_redirects(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-01, BIL-API-03: POST /billing/subscribe создаёт платёж и редиректит."""
    client, _, repo, provider, _ = purchase_test_env

    response = await client.post('/billing/subscribe')

    assert response.status_code == 303
    assert (
        response.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-pay-1'
    )
    assert len(provider.create_calls) == 1
    assert len(repo.payments) == 1
    saved = next(iter(repo.payments.values()))
    assert saved.status == 'pending'
    assert saved.provider_payment_id == 'yk-pay-1'
    assert saved.idempotency_key == saved.id


@pytest.mark.asyncio
async def test_repeat_purchase_checks_yookassa_and_reuses_live_pending_payment(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02: повтор при живом pending в ЮKassa возвращает тот же платёж."""
    client, _, repo, provider, _ = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    assert first.status_code == 200
    first_data = first.json()

    second = await client.post('/api/v1/billing/payments')
    assert second.status_code == 200
    second_data = second.json()

    assert first_data['payment_id'] == second_data['payment_id']
    assert first_data['confirmation_url'] == second_data['confirmation_url']
    assert len(provider.create_calls) == 1
    assert provider.get_calls == ['yk-pay-1']
    assert len(repo.payments) == 1


@pytest.mark.asyncio
async def test_repeat_purchase_when_pending_was_canceled_creates_new_payment(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02: если pending отменён в ЮKassa, он закрывается и создаётся новый."""
    client, _, repo, provider, _ = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    first_id = UUID(first.json()['payment_id'])

    # ЮKassa отменила первый платёж по тайм-ауту
    provider.get_status = 'failed'

    second = await client.post('/billing/subscribe')
    assert second.status_code == 303
    assert (
        second.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-pay-2'
    )
    assert repo.payments[first_id].status == 'failed'
    assert len(repo.payments) == 2
    assert len(provider.create_calls) == 2


@pytest.mark.asyncio
async def test_repeat_purchase_recovers_payment_without_provider_id_by_same_key(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02, инв. 13: повтор после сбоя до номера вызывает POST с тем же ключом."""
    client, _, repo, provider, _ = purchase_test_env
    provider.create_error = ProviderTransientError('Timeout on first attempt')

    failed_ui = await client.post('/billing/subscribe')
    assert failed_ui.status_code == 503
    assert 'Не удалось связаться с платёжной системой' in failed_ui.text
    assert len(repo.payments) == 1
    pending_row = next(iter(repo.payments.values()))
    assert pending_row.provider_payment_id is None

    # Вторая попытка проходит успешно с тем же ключом идемпотентности
    provider.create_error = None
    recovered = await client.post('/billing/subscribe')
    assert recovered.status_code == 303
    assert len(provider.create_calls) == 2
    assert (
        provider.create_calls[0].idempotency_key
        == provider.create_calls[1].idempotency_key
    )
    assert pending_row.provider_payment_id == 'yk-pay-1'


@pytest.mark.asyncio
@pytest.mark.parametrize('block_reason', ['active_subscription', 'processing_refund'])
async def test_purchase_rejected_with_409_when_active_subscription_or_processing_refund(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
    block_reason: str,
) -> None:
    """BIL-FR-03: покупка отклоняется с 409 при активной подписке или возврате."""
    client, _, repo, provider, _ = purchase_test_env
    if block_reason == 'active_subscription':
        repo.has_active_subscription = True
    else:
        repo.has_processing_refund = True

    api_resp = await client.post('/api/v1/billing/payments')
    assert api_resp.status_code == 409
    assert api_resp.json()['error_code'] in (
        'subscription_already_active',
        'refund_in_processing',
    )

    ui_resp = await client.post('/billing/subscribe')
    assert ui_resp.status_code == 409
    assert len(provider.create_calls) == 0


@pytest.mark.asyncio
async def test_purchase_marks_payment_failed_when_yookassa_returns_400_401_403(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-06, инв. 15: при 400/401/403 от ЮKassa платёж переводится в failed."""
    client, _, repo, provider, _ = purchase_test_env
    provider.create_error = ProviderPermanentError(401, 'Invalid shopId or key')

    resp = await client.post('/api/v1/billing/payments')
    assert resp.status_code == 503
    saved = next(iter(repo.payments.values()))
    assert saved.status == 'failed'
    assert saved.failure_reason is not None


@pytest.mark.asyncio
async def test_repeat_purchase_throttles_yookassa_get_calls_to_once_per_5_seconds(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-NFR-R-06: 10 повторных вызовов за 2 секунды делают ровно 1 GET в ЮKassa."""
    client, _, repo, provider, _ = purchase_test_env

    # Первое создание платежа (POST в ЮKassa)
    first = await client.post('/api/v1/billing/payments')
    assert first.status_code == 200
    assert len(provider.create_calls) == 1
    assert len(provider.get_calls) == 0

    # 10 повторных запросов подряд в пределах 5 секунд
    for _ in range(10):
        repeat = await client.post('/api/v1/billing/payments')
        assert repeat.status_code == 200
        assert repeat.json()['payment_id'] == first.json()['payment_id']

    # Первый из 10 повторов сделал GET и записал last_checked_at, остальные 9 взяли кэш
    assert len(provider.get_calls) == 1
    assert len(repo.payments) == 1


@pytest.mark.asyncio
async def test_repeat_purchase_redirects_to_result_when_yookassa_returns_succeeded(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """US-U-01: при succeeded в ЮKassa редиректит на экран результата платежа."""
    client, _, _, provider, _ = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    payment_id = first.json()['payment_id']

    provider.get_status = 'succeeded'
    second = await client.post('/billing/subscribe')
    assert second.status_code == 303
    assert second.headers['location'] == f'/billing/payments/{payment_id}'


@pytest.mark.asyncio
async def test_idempotent_post_replay_verifies_via_get_when_non_pending(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        FakePurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """Инвариант 6: при canceled на повторе POST статус сверяется через GET."""
    client, _, repo, provider, user = purchase_test_env

    # В БД есть pending-платёж без сохранённого provider_payment_id (после обрыва связи)
    existing, _ = await repo.get_or_create_pending_payment(user.user_id)
    assert existing.provider_payment_id is None

    # ЮKassa на идемпотентный POST возвращает canceled, смена статуса только после GET
    provider.get_status = 'failed'

    orig_create = provider.create_payment

    async def create_returning_canceled_first(
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        if len(provider.create_calls) == 0:
            provider.create_calls.append(request)
            return ProviderPaymentSnapshot(
                provider_code='yookassa',
                provider_payment_id='yk-old-canceled',
                domain_status='failed',
                amount_kopeks=request.amount_kopeks,
                currency=request.currency,
                confirmation_url=None,
                failure_reason='expired_on_confirmation',
                metadata_payment_id=str(request.payment_id),
                provider_details={'id': 'yk-old-canceled', 'status': 'canceled'},
            )
        return await orig_create(request)

    provider.create_payment = create_returning_canceled_first  # type: ignore[method-assign]

    resp = await client.post('/billing/subscribe')
    assert resp.status_code == 303
    assert 'yk-pay-1' in resp.headers['location']
    assert 'yk-old-canceled' in provider.get_calls
    assert repo.payments[existing.id].status == 'failed'
    assert len(repo.payments) == 2
