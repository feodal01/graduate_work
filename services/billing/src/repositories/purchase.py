from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import exists, select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from api.errors import BillingDomainError
from db.sql.errors import DatabaseUnavailableError
from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    OperationEvent,
    Payment,
    PaymentProvider,
    Refund,
    Subscription,
    Tariff,
)


class PaymentRecordProtocol(Protocol):
    id: UUID
    user_id: UUID
    tariff_id: UUID
    provider_code: str
    amount_kopeks: int
    currency: str
    status: str
    idempotency_key: UUID
    provider_payment_id: str | None
    confirmation_url: str | None
    failure_reason: str | None
    last_checked_at: datetime | None
    created_at: datetime
    updated_at: datetime


class PurchaseRepository(Protocol):
    async def check_purchase_allowed(
        self,
        user_id: UUID,
    ) -> tuple[bool, str | None]: ...

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[PaymentRecordProtocol, bool]: ...

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, Any],
    ) -> PaymentRecordProtocol: ...

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> PaymentRecordProtocol: ...

    async def mark_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: str,
        provider_details: dict[str, Any] | None = None,
    ) -> PaymentRecordProtocol: ...


class SQLAlchemyPurchaseRepository:
    """SQLAlchemy-репозиторий создания и проверки платежей покупки (T-10)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    async def _acquire_user_lock(session: AsyncSession, user_id: UUID) -> None:
        await session.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:uid))'),
            {'uid': str(user_id)},
        )

    @staticmethod
    async def _check_allowed_in_session(
        session: AsyncSession,
        user_id: UUID,
    ) -> tuple[bool, str | None]:
        now_utc = datetime.now(UTC)
        # 1. Сначала проверяем активную подписку (ends_at > now(), нет возврата)
        succeeded_refund_exists = (
            select(Refund.id)
            .where(
                Refund.subscription_id == Subscription.id,
                Refund.status == 'succeeded',
            )
            .exists()
        )
        active_sub_stmt = select(
            exists().where(
                Subscription.user_id == user_id,
                Subscription.ends_at > now_utc,
                ~succeeded_refund_exists,
            )
        )
        if await session.scalar(active_sub_stmt):
            return False, 'subscription_already_active'

        # 2. Затем проверяем наличие возврата в состоянии processing
        processing_refund_stmt = select(
            exists().where(
                Refund.payment_id == Payment.id,
                Payment.user_id == user_id,
                Refund.status == 'processing',
            )
        )
        if await session.scalar(processing_refund_stmt):
            return False, 'refund_in_processing'

        return True, None

    async def check_purchase_allowed(self, user_id: UUID) -> tuple[bool, str | None]:
        """Проверяет отсутствие активной подписки и возврата в обработке (BIL-FR-03)."""
        try:
            async with self._session_factory() as session:
                return await self._check_allowed_in_session(session, user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[Payment, bool]:
        """Возвращает существующий pending-платёж или создаёт новый (инвариант 1)."""
        try:
            async with self._session_factory() as session:
                await self._acquire_user_lock(session, user_id)
                allowed, block_code = await self._check_allowed_in_session(
                    session,
                    user_id,
                )
                if not allowed:
                    if block_code == 'subscription_already_active':
                        raise BillingDomainError(
                            'Подписка уже активна',
                            error_code='subscription_already_active',
                            status_code=409,
                        )
                    raise BillingDomainError(
                        'Нельзя оформить новую подписку, '
                        'пока возврат находится в обработке',
                        error_code='refund_in_processing',
                        status_code=409,
                    )

                existing = await session.scalar(
                    select(Payment).where(
                        Payment.user_id == user_id,
                        Payment.status == 'pending',
                    )
                )
                if existing is not None:
                    return existing, False

                tariff = await session.scalar(
                    select(Tariff).where(
                        Tariff.code == DEFAULT_TARIFF_CODE,
                        Tariff.is_active.is_(True),
                    )
                )
                if tariff is None:
                    raise RuntimeError(
                        'Active default tariff not found in tariffs table'
                    )

                provider = await session.scalar(
                    select(PaymentProvider).where(
                        PaymentProvider.is_active.is_(True),
                    )
                )
                provider_code = (
                    provider.code if provider is not None else DEFAULT_PROVIDER_CODE
                )

                payment_id = uuid4()
                payment = Payment(
                    id=payment_id,
                    user_id=user_id,
                    tariff_id=tariff.id,
                    provider_code=provider_code,
                    amount_kopeks=tariff.price_kopeks,
                    currency=tariff.currency.strip(),
                    status='pending',
                    idempotency_key=payment_id,
                )
                event = OperationEvent(
                    operation_type='payment',
                    operation_id=payment_id,
                    from_status=None,
                    to_status='pending',
                    source='continue',
                    details={'created': True},
                )
                session.add(payment)
                session.add(event)
                try:
                    await session.commit()
                    return payment, True
                except IntegrityError:
                    await session.rollback()

                concurrent = await session.scalar(
                    select(Payment).where(
                        Payment.user_id == user_id,
                        Payment.status == 'pending',
                    )
                )
                if concurrent is None:
                    raise RuntimeError(
                        'Failed to load concurrent pending payment after IntegrityError'
                    )
                return concurrent, False
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, Any],
    ) -> Payment:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await session.get(
                    Payment,
                    payment_id,
                    with_for_update=True,
                )
                if payment is None:
                    raise RuntimeError(f'Payment {payment_id} not found')
                if payment.status == 'pending':
                    payment.provider_payment_id = provider_payment_id
                    if confirmation_url is not None:
                        payment.confirmation_url = confirmation_url
                    payment.provider_details = provider_details
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> Payment:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await session.get(
                    Payment,
                    payment_id,
                    with_for_update=True,
                )
                if payment is None:
                    raise RuntimeError(f'Payment {payment_id} not found')
                if payment.status == 'pending':
                    if confirmation_url:
                        payment.confirmation_url = confirmation_url
                    if provider_details is not None:
                        payment.provider_details = provider_details
                    payment.last_checked_at = now_utc
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def mark_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: str,
        provider_details: dict[str, Any] | None = None,
    ) -> Payment:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await session.get(
                    Payment,
                    payment_id,
                    with_for_update=True,
                )
                if payment is None:
                    raise RuntimeError(f'Payment {payment_id} not found')
                if payment.status == 'pending':
                    old_status = payment.status
                    payment.status = 'failed'
                    payment.failure_reason = failure_reason
                    if provider_details is not None:
                        payment.provider_details = provider_details
                    payment.last_checked_at = now_utc
                    payment.updated_at = now_utc
                    session.add(
                        OperationEvent(
                            operation_type='payment',
                            operation_id=payment_id,
                            from_status=old_status,
                            to_status='failed',
                            source=source,
                            details={'failure_reason': failure_reason},
                        )
                    )
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
