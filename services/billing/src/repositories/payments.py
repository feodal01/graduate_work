from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from db.sql.errors import DatabaseUnavailableError
from domain.errors import RuleConflictError
from domain.subscription import (
    ReconcileSource,
    build_payment_failed_email_payload,
    build_payment_succeeded_email_payload,
    build_subscription_expiration_email_payload,
)
from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    OperationEvent,
    OutboxTask,
    Payment,
    PaymentProvider,
    Subscription,
    Tariff,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.records import (
    PaymentRecordProtocol,
)


def payment_access_task_key(payment_id: UUID) -> str:
    """Ключ задачи «открыть доступ» после оплаты; у платежа она ровно одна.

    После возврата у того же платежа появляется вторая задача `sync_access`
    (закрыть доступ), поэтому экраны ищут задачу оплаты по этому ключу.
    """
    return f'payment_succeeded:{payment_id}:auth_access'


class PaymentRepository(Protocol):
    """Платежи: создание, сверка и атомарная фиксация итога (T-10, T-11, T-17)."""

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

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> PaymentRecordProtocol | None: ...

    async def list_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
        limit: int = 20,
    ) -> Sequence[PaymentRecordProtocol]: ...

    async def claim_next_due_pending_payment(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> PaymentRecordProtocol | None: ...

    async def record_payment_reconcile_error(
        self,
        payment_id: UUID,
        *,
        error_message: str,
    ) -> PaymentRecordProtocol: ...

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
        *,
        provider_code: str = DEFAULT_PROVIDER_CODE,
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


class SQLAlchemyPaymentRepository(SQLAlchemyRepository):
    """Платежи: создание, сверка и атомарная фиксация итога (T-10, T-11, T-17)."""

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

    async def list_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
        limit: int = 20,
    ) -> list[Payment]:
        """Возвращает pending-платежи старше min_age_seconds (BIL-FR-05)."""
        now_utc = datetime.now(UTC)
        created_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        checked_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Payment)
                    .where(
                        Payment.status == 'pending',
                        Payment.created_at <= created_cutoff,
                        or_(
                            Payment.last_checked_at.is_(None),
                            Payment.last_checked_at <= checked_cutoff,
                        ),
                    )
                    .order_by(
                        Payment.last_checked_at.asc().nulls_first(),
                        Payment.created_at.asc(),
                    )
                    .limit(limit)
                )
                return list((await session.scalars(stmt)).all())
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def claim_next_due_pending_payment(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> Payment | None:
        """Атомарно захватывает pending-платёж через SKIP LOCKED (BIL-NFR-S-01)."""
        now_utc = datetime.now(UTC)
        created_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        checked_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Payment)
                    .where(
                        Payment.status == 'pending',
                        Payment.created_at <= created_cutoff,
                        or_(
                            Payment.last_checked_at.is_(None),
                            Payment.last_checked_at <= checked_cutoff,
                        ),
                    )
                    .order_by(
                        Payment.last_checked_at.asc().nulls_first(),
                        Payment.created_at.asc(),
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                payment = await session.scalar(stmt)
                if payment is None:
                    return None
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc
                await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_payment_reconcile_error(
        self,
        payment_id: UUID,
        *,
        error_message: str,
    ) -> Payment:
        """Фиксирует ошибку фоновой сверки с backoff (US-S-01, BIL-NFR-R-05)."""
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status == 'pending':
                    details = dict(payment.provider_details or {})
                    prev_errors = details.get('_reconcile_errors', 0)
                    err_count = (
                        int(prev_errors) + 1
                        if isinstance(prev_errors, (int, str))
                        else 1
                    )
                    backoff_sec = min(300, 5 * (2 ** min(err_count - 1, 6)))
                    next_eligible_at = now_utc + timedelta(
                        seconds=max(0, backoff_sec - 5)
                    )
                    details['_reconcile_errors'] = err_count
                    details['_last_reconcile_error'] = error_message[:250]
                    payment.provider_details = details
                    payment.last_checked_at = next_eligible_at
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
        *,
        provider_code: str = DEFAULT_PROVIDER_CODE,
    ) -> tuple[Payment, bool]:
        """Возвращает существующий pending-платёж или создаёт новый (инвариант 1).

        Новый платёж закрепляется за `provider_code` — активным провайдером
        из конфигурации. Код должен быть в справочнике `payment_providers`
        с `is_active=true`, иначе это ошибка конфигурации (BIL-NFR-S-02).
        """
        try:
            async with self._session_factory() as session:
                await self._acquire_user_lock(session, user_id)
                allowed, block_code = await self._check_allowed_in_session(
                    session,
                    user_id,
                )
                if not allowed:
                    if block_code == 'subscription_already_active':
                        raise RuleConflictError(
                            'Подписка уже активна',
                            error_code='subscription_already_active',
                        )
                    raise RuleConflictError(
                        'Нельзя оформить новую подписку, '
                        'пока возврат находится в обработке',
                        error_code='refund_in_processing',
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
                        PaymentProvider.code == provider_code,
                        PaymentProvider.is_active.is_(True),
                    )
                )
                if provider is None:
                    raise RuntimeError(
                        f'Payment provider {provider_code!r} is not active '
                        'in payment_providers table'
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

                session.add_all(
                    [
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
                                subscription_id=sub_id,
                                user_id=payment.user_id,
                                ends_at=ends_at,
                                public_base_url=public_base_url,
                            ),
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'subscription_expiration:{sub_id}:notify_expiring',
                            target_service='notifications',
                            task_type='subscription_expiration_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_subscription_expiration_email_payload(
                                subscription_id=sub_id,
                                user_id=payment.user_id,
                                ends_at=ends_at,
                                public_base_url=public_base_url,
                            ),
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

                session.add(
                    OutboxTask(
                        dedup_key=f'payment_failed:{payment.id}:notify_failed',
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
                        ),
                        next_retry_at=now_utc,
                    )
                )
                await session.commit()
                return payment, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
