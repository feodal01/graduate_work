from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from api.errors import BillingDomainError
from db.sql.errors import DatabaseUnavailableError
from domain.subscription import ReconcileSource, deterministic_id, format_msk_datetime
from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    OperationEvent,
    OutboxTask,
    Payment,
    PaymentProvider,
    Refund,
    Subscription,
    Tariff,
    UserAccessVersion,
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


@dataclass(frozen=True, slots=True)
class LocalPaymentResultData:
    payment_id: UUID
    user_id: UUID
    tariff_title: str
    amount_kopeks: int
    currency: str
    status: str
    failure_reason: str | None
    subscription_ends_at: datetime | None
    auth_access_delivered: bool


class PurchaseRepository(Protocol):
    async def check_purchase_allowed(
        self,
        user_id: UUID,
    ) -> tuple[bool, str | None]: ...

    async def get_payment(
        self,
        payment_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_payment_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None: ...

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> PaymentRecordProtocol | None: ...

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

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...


class SQLAlchemyPurchaseRepository:
    """SQLAlchemy-репозиторий создания и атомарной фиксации платежей (T-10, T-11)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    async def _acquire_user_lock(session: AsyncSession, user_id: UUID) -> None:
        await session.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:uid))'),
            {'uid': str(user_id)},
        )

    @classmethod
    async def _lock_user_and_payment(
        cls,
        session: AsyncSession,
        payment_id: UUID,
    ) -> Payment:
        """Захватывает user advisory lock и FOR UPDATE на платеже в одном порядке."""
        user_id = await session.scalar(
            select(Payment.user_id).where(Payment.id == payment_id)
        )
        if user_id is None:
            raise RuntimeError(f'Payment {payment_id} not found')
        await cls._acquire_user_lock(session, user_id)
        payment = await session.get(
            Payment,
            payment_id,
            with_for_update=True,
        )
        if payment is None:
            raise RuntimeError(f'Payment {payment_id} not found')
        return payment

    @staticmethod
    async def _increment_access_version(session: AsyncSession, user_id: UUID) -> int:
        now_utc = datetime.now(UTC)
        stmt = (
            pg_insert(UserAccessVersion)
            .values(user_id=user_id, last_version=1, updated_at=now_utc)
            .on_conflict_do_update(
                index_elements=[UserAccessVersion.user_id],
                set_={
                    'last_version': UserAccessVersion.last_version + 1,
                    'updated_at': now_utc,
                },
            )
            .returning(UserAccessVersion.last_version)
        )
        version = await session.scalar(stmt)
        if version is None:
            raise RuntimeError(f'Failed to increment access_version for user {user_id}')
        return int(version)

    @staticmethod
    async def _check_allowed_in_session(
        session: AsyncSession,
        user_id: UUID,
    ) -> tuple[bool, str | None]:
        now_utc = datetime.now(UTC)
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

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        """Возвращает платёж по его внутреннему UUID."""
        try:
            async with self._session_factory() as session:
                return await session.get(Payment, payment_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_payment_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> Payment | None:
        """Возвращает платёж пользователя по UUID или None (BIL-NFR-SEC-03)."""
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Payment).where(
                        Payment.id == payment_id,
                        Payment.user_id == user_id,
                    )
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None:
        """Читает локальный статус платежа и подписки одним запросом (BIL-FR-04)."""
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(
                        Payment.id,
                        Payment.user_id,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Payment.status,
                        Payment.failure_reason,
                        Tariff.title,
                        Subscription.ends_at,
                        OutboxTask.status,
                    )
                    .join(Tariff, Tariff.id == Payment.tariff_id)
                    .outerjoin(
                        Subscription,
                        Subscription.payment_id == Payment.id,
                    )
                    .outerjoin(
                        OutboxTask,
                        and_(
                            OutboxTask.payment_id == Payment.id,
                            OutboxTask.target_service == 'auth',
                            OutboxTask.task_type == 'sync_access',
                        ),
                    )
                    .where(
                        Payment.id == payment_id,
                        Payment.user_id == user_id,
                    )
                )
                row = (await session.execute(stmt)).one_or_none()
                if row is None:
                    return None

                (
                    row_payment_id,
                    row_user_id,
                    amount_kopeks,
                    currency,
                    status,
                    failure_reason,
                    tariff_title,
                    sub_ends_at,
                    auth_task_status,
                ) = row

                return LocalPaymentResultData(
                    payment_id=row_payment_id,
                    user_id=row_user_id,
                    tariff_title=tariff_title,
                    amount_kopeks=amount_kopeks,
                    currency=currency.strip(),
                    status=status,
                    failure_reason=failure_reason,
                    subscription_ends_at=sub_ends_at,
                    auth_access_delivered=auth_task_status == 'completed',
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> Payment | None:
        """Возвращает платёж по коду провайдера и внешнему идентификатору."""
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Payment).where(
                        Payment.provider_code == provider_code,
                        Payment.provider_payment_id == provider_payment_id,
                    )
                )
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
                payment = await self._lock_user_and_payment(session, payment_id)
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
                payment = await self._lock_user_and_payment(session, payment_id)
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

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Payment, bool]:
        """Атомарно фиксирует оплату (BIL-FR-08, BIL-FR-11, BIL-FR-13, BIL-NFR-R-03)."""
        now_utc = datetime.now(UTC)
        base_url = public_base_url.rstrip('/')
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status != 'pending':
                    return payment, False

                tariff = await session.get(Tariff, payment.tariff_id)
                duration_days = tariff.duration_days if tariff is not None else 30

                old_status = payment.status
                payment.status = 'succeeded'
                if provider_details is not None:
                    payment.provider_details = provider_details
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc

                starts_at = now_utc
                ends_at = starts_at + timedelta(days=duration_days)
                sub_id = uuid4()
                subscription = Subscription(
                    id=sub_id,
                    user_id=payment.user_id,
                    tariff_id=payment.tariff_id,
                    payment_id=payment.id,
                    starts_at=starts_at,
                    ends_at=ends_at,
                )
                session.add(subscription)

                access_version = await self._increment_access_version(
                    session,
                    payment.user_id,
                )

                session.add(
                    OperationEvent(
                        operation_type='payment',
                        operation_id=payment.id,
                        from_status=old_status,
                        to_status='succeeded',
                        source=source,
                        details=provider_details or {'status': 'succeeded'},
                    )
                )

                ends_msk = format_msk_datetime(ends_at)
                my_sub_url = f'{base_url}/billing/'
                paid_campaign_id = deterministic_id(
                    'payment_succeeded_email', payment.id
                )
                paid_batch_id = deterministic_id('payment_succeeded_batch', payment.id)
                exp_campaign_id = deterministic_id('subscription_expiration', sub_id)
                exp_batch_id = deterministic_id('subscription_expiration_batch', sub_id)

                session.add_all(
                    [
                        OutboxTask(
                            dedup_key=f'payment_succeeded:{payment.id}:auth_access',
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
                            payload={
                                'campaign_id': str(paid_campaign_id),
                                'batch_id': str(paid_batch_id),
                                'user_id': str(payment.user_id),
                                'template_id': 'billing_notification',
                                'subject': 'Подписка «Премиум» оплачена',
                                'text': (
                                    f'Оплата подписки «Премиум» подтверждена. '
                                    f'Доступ к новинкам открыт до {ends_msk}. '
                                    f'Страница подписки: {my_sub_url}'
                                ),
                                'available_at': None,
                                'subscription_id': str(sub_id),
                                'ends_at': ends_at.isoformat(),
                            },
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'subscription_expiration:{sub_id}:notify_expiring',
                            target_service='notifications',
                            task_type='subscription_expiration_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload={
                                'campaign_id': str(exp_campaign_id),
                                'batch_id': str(exp_batch_id),
                                'user_id': str(payment.user_id),
                                'template_id': 'billing_notification',
                                'subject': 'Срок подписки «Премиум» закончился',
                                'text': (
                                    f'Срок действия вашей подписки «Премиум» истёк '
                                    f'({ends_msk}). Вы можете оформить новую подписку '
                                    f'на странице: {my_sub_url}'
                                ),
                                'available_at': ends_at.isoformat(),
                                'subscription_id': str(sub_id),
                            },
                            next_retry_at=now_utc,
                        ),
                    ]
                )
                await session.commit()
                return payment, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Payment, bool]:
        """Атомарно переводит платёж в failed и ставит письмо (BIL-FR-06, BIL-FR-11)."""
        now_utc = datetime.now(UTC)
        base_url = public_base_url.rstrip('/')
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status != 'pending':
                    return payment, False

                old_status = payment.status
                payment.status = 'failed'
                payment.failure_reason = failure_reason
                if provider_details is not None:
                    payment.provider_details = provider_details
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc

                event_details: dict[str, Any] = {
                    **(provider_details or {}),
                    'failure_reason': failure_reason,
                }
                session.add(
                    OperationEvent(
                        operation_type='payment',
                        operation_id=payment.id,
                        from_status=old_status,
                        to_status='failed',
                        source=source,
                        details=event_details,
                    )
                )

                retry_url = f'{base_url}/billing/'
                failed_campaign_id = deterministic_id(
                    'payment_failed_email', payment.id
                )
                failed_batch_id = deterministic_id('payment_failed_batch', payment.id)
                session.add(
                    OutboxTask(
                        dedup_key=f'payment_failed:{payment.id}:notify_failed',
                        target_service='notifications',
                        task_type='payment_failed_email',
                        payment_id=payment.id,
                        user_id=payment.user_id,
                        access_version=None,
                        payload={
                            'campaign_id': str(failed_campaign_id),
                            'batch_id': str(failed_batch_id),
                            'user_id': str(payment.user_id),
                            'template_id': 'billing_notification',
                            'subject': 'Оплата подписки «Премиум» не прошла',
                            'text': (
                                f'Не удалось провести оплату подписки «Премиум» '
                                f'(причина: {failure_reason}). '
                                f'Попробовать снова можно на странице: {retry_url}'
                            ),
                            'available_at': None,
                            'failure_reason': failure_reason,
                        },
                        next_retry_at=now_utc,
                    )
                )
                await session.commit()
                return payment, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
