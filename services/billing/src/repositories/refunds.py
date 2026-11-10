from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from db.sql.errors import DatabaseUnavailableError
from domain.errors import EntityNotFoundError, RuleConflictError
from domain.subscription import (
    ReconcileSource,
    build_cancel_expiration_email_payload,
    build_refund_succeeded_email_payload,
    compute_subscription_state,
)
from models import (
    AdminAction,
    OperationEvent,
    OutboxTask,
    Refund,
    Subscription,
    UserAccessVersion,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.records import (
    RefundRecordProtocol,
)


class RefundRepository(Protocol):
    """Возвраты: создание, сверка и атомарная фиксация итога (T-29)."""

    async def get_refund(
        self,
        refund_id: UUID,
    ) -> RefundRecordProtocol | None: ...

    async def get_refund_by_provider_id(
        self,
        provider_code: str,
        provider_refund_id: str,
    ) -> RefundRecordProtocol | None: ...

    async def claim_next_due_processing_refund(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> RefundRecordProtocol | None: ...

    async def get_or_create_processing_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        refund_id: UUID | None = None,
        now_utc: datetime | None = None,
    ) -> tuple[RefundRecordProtocol, bool]: ...

    async def save_provider_refund_created(
        self,
        refund_id: UUID,
        *,
        provider_refund_id: str,
        provider_details: dict[str, Any],
    ) -> RefundRecordProtocol: ...

    async def record_refund_checked(
        self,
        refund_id: UUID,
        *,
        provider_details: dict[str, Any] | None = None,
    ) -> RefundRecordProtocol: ...

    async def record_refund_reconcile_error(
        self,
        refund_id: UUID,
        *,
        error_message: str,
    ) -> RefundRecordProtocol: ...

    async def finalize_refund_succeeded(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[RefundRecordProtocol, bool]: ...

    async def finalize_refund_rejected(
        self,
        refund_id: UUID,
        *,
        rejection_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
    ) -> tuple[RefundRecordProtocol, bool]: ...


class SQLAlchemyRefundRepository(SQLAlchemyRepository):
    """Возвраты: создание, сверка и атомарная фиксация итога (T-29)."""

    async def get_refund(self, refund_id: UUID) -> Refund | None:
        try:
            async with self._session_factory() as session:
                return await session.get(Refund, refund_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_refund_by_provider_id(
        self,
        provider_code: str,
        provider_refund_id: str,
    ) -> Refund | None:
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Refund).where(
                        Refund.provider_code == provider_code,
                        Refund.provider_refund_id == provider_refund_id,
                    )
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def claim_next_due_processing_refund(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> Refund | None:
        now_utc = datetime.now(UTC)
        age_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        throttle_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Refund)
                    .where(
                        Refund.status == 'processing',
                        Refund.created_at <= age_cutoff,
                        or_(
                            Refund.last_checked_at.is_(None),
                            Refund.last_checked_at <= throttle_cutoff,
                        ),
                    )
                    .order_by(
                        Refund.last_checked_at.asc().nulls_first(),
                        Refund.created_at.asc(),
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                refund = await session.scalar(stmt)
                if refund is None:
                    return None
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc
                await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_or_create_processing_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        refund_id: UUID | None = None,
        now_utc: datetime | None = None,
    ) -> tuple[Refund, bool]:
        """Создаёт попытку возврата или возвращает уже существующую (инв. 4).

        `refund_id` — UUID попытки, который форма админки создаёт при показе.
        Повтор той же формы (двойной клик, F5) находит попытку по этому UUID
        в любом статусе, поэтому после отказа новая попытка появляется только
        из новой формы (BIL-NFR-R-02). Выполненный возврат тоже возвращается
        как есть, а не ошибкой 409.
        """
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if refund_id is not None:
                    same_attempt = await session.get(Refund, refund_id)
                    if same_attempt is not None:
                        if same_attempt.payment_id != payment_id:
                            raise RuleConflictError(
                                'UUID попытки возврата принадлежит другому платежу',
                                error_code='refund_id_conflict',
                            )
                        return same_attempt, False
                existing_active = await session.scalar(
                    select(Refund).where(
                        Refund.payment_id == payment_id,
                        Refund.status.in_(('processing', 'succeeded')),
                    )
                )
                if existing_active is not None:
                    return existing_active, False

                if payment.status != 'succeeded':
                    raise RuleConflictError(
                        'Возврат возможен только для оплаченного платежа '
                        'активной подписки',
                        error_code='refund_not_allowed',
                    )

                sub = await session.scalar(
                    select(Subscription).where(Subscription.payment_id == payment_id)
                )
                if (
                    sub is None
                    or compute_subscription_state(
                        ends_at=sub.ends_at,
                        has_succeeded_refund=False,
                        now_utc=now,
                    )
                    != 'active'
                ):
                    raise RuleConflictError(
                        'Возврат возможен только для платежа текущей активной подписки',
                        error_code='refund_not_allowed',
                    )

                refund_id = refund_id or uuid4()
                refund = Refund(
                    id=refund_id,
                    payment_id=payment.id,
                    subscription_id=sub.id,
                    provider_code=payment.provider_code,
                    amount_kopeks=payment.amount_kopeks,
                    currency=payment.currency.strip(),
                    status='processing',
                    idempotency_key=refund_id,
                    admin_user_id=admin_user_id,
                    reason=cleaned_reason,
                )
                event = OperationEvent(
                    operation_type='refund',
                    operation_id=refund_id,
                    from_status=None,
                    to_status='processing',
                    source='admin',
                    details={
                        'payment_id': str(payment.id),
                        'admin_user_id': str(admin_user_id),
                        'reason': cleaned_reason,
                    },
                )
                action = AdminAction(
                    admin_user_id=admin_user_id,
                    action_type='refund_requested',
                    target_type='refund',
                    target_id=refund_id,
                    reason=cleaned_reason,
                )
                session.add_all([refund, event, action])
                try:
                    await session.commit()
                    return refund, True
                except IntegrityError:
                    await session.rollback()

                if refund_id is not None:
                    # Параллельный запрос успел вставить попытку с тем же UUID
                    # (PK). Для чужого платежа это конфликт UUID, а не «возврат
                    # уже оформлен».
                    same_attempt = await session.get(Refund, refund_id)
                    if same_attempt is not None:
                        if same_attempt.payment_id != payment_id:
                            raise RuleConflictError(
                                'UUID попытки возврата принадлежит другому платежу',
                                error_code='refund_id_conflict',
                            )
                        return same_attempt, False
                concurrent = await session.scalar(
                    select(Refund).where(
                        Refund.payment_id == payment_id,
                        Refund.status.in_(('processing', 'succeeded')),
                    )
                )
                if concurrent is None:
                    raise RuleConflictError(
                        'Возврат по данному платежу уже оформлен',
                        error_code='refund_already_succeeded',
                    )
                return concurrent, False
        except LookupError as exc:
            raise EntityNotFoundError(
                'Payment not found',
                error_code='payment_not_found',
            ) from exc
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def save_provider_refund_created(
        self,
        refund_id: UUID,
        *,
        provider_refund_id: str,
        provider_details: dict[str, Any],
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    refund.provider_refund_id = provider_refund_id
                    refund.provider_details = provider_details
                    refund.updated_at = now_utc
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_refund_checked(
        self,
        refund_id: UUID,
        *,
        provider_details: dict[str, Any] | None = None,
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    refund.last_checked_at = now_utc
                    refund.updated_at = now_utc
                    if provider_details is not None:
                        refund.provider_details = provider_details
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_refund_reconcile_error(
        self,
        refund_id: UUID,
        *,
        error_message: str,
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    details = dict(refund.provider_details or {})
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
                    refund.provider_details = details
                    refund.last_checked_at = next_eligible_at
                    refund.updated_at = now_utc
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_refund_succeeded(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Refund, bool]:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, payment = await self._lock_user_and_refund(session, refund_id)
                if refund.status != 'processing':
                    return refund, False

                old_status = refund.status
                refund.status = 'succeeded'
                if provider_details is not None:
                    refund.provider_details = provider_details
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc

                session.add(
                    OperationEvent(
                        operation_type='refund',
                        operation_id=refund.id,
                        from_status=old_status,
                        to_status='succeeded',
                        source=source,
                        details=provider_details,
                    )
                )

                version_stmt = (
                    pg_insert(UserAccessVersion)
                    .values(
                        user_id=payment.user_id,
                        last_version=1,
                        updated_at=now_utc,
                    )
                    .on_conflict_do_update(
                        index_elements=[UserAccessVersion.user_id],
                        set_={
                            'last_version': UserAccessVersion.last_version + 1,
                            'updated_at': now_utc,
                        },
                    )
                    .returning(UserAccessVersion.last_version)
                )
                new_version = int((await session.execute(version_stmt)).scalar_one())

                tariff_title = await self._tariff_title(session, payment.tariff_id)
                session.add_all(
                    [
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:sync_access',
                            target_service='auth',
                            task_type='sync_access',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=new_version,
                            payload={
                                'version': new_version,
                                'expires_at': None,
                            },
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:cancel_expiration',
                            target_service='notifications',
                            task_type='cancel_expiration_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_cancel_expiration_email_payload(
                                subscription_id=refund.subscription_id,
                                user_id=payment.user_id,
                            ),
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:notify_refund',
                            target_service='notifications',
                            task_type='refund_succeeded_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_refund_succeeded_email_payload(
                                refund_id=refund.id,
                                payment_id=payment.id,
                                user_id=payment.user_id,
                                amount_kopeks=refund.amount_kopeks,
                                tariff_title=tariff_title,
                                currency=refund.currency.strip(),
                                public_base_url=public_base_url,
                            ),
                            next_retry_at=now_utc,
                        ),
                    ]
                )
                await session.commit()
                return refund, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_refund_rejected(
        self,
        refund_id: UUID,
        *,
        rejection_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
    ) -> tuple[Refund, bool]:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status != 'processing':
                    return refund, False

                old_status = refund.status
                refund.status = 'rejected'
                refund.rejection_reason = rejection_reason
                if provider_details is not None:
                    refund.provider_details = provider_details
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc

                event_details: dict[str, Any] = {
                    **(provider_details or {}),
                    'rejection_reason': rejection_reason,
                }
                session.add(
                    OperationEvent(
                        operation_type='refund',
                        operation_id=refund.id,
                        from_status=old_status,
                        to_status='rejected',
                        source=source,
                        details=event_details,
                    )
                )
                await session.commit()
                return refund, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
