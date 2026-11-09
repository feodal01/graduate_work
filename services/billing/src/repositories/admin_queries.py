from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.exc import SQLAlchemyError

from db.sql.errors import DatabaseUnavailableError
from domain.subscription import (
    compute_subscription_state,
)
from models import (
    AdminAction,
    OperationEvent,
    OutboxTask,
    Payment,
    Refund,
    Subscription,
    Tariff,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.outbox import admin_can_retry_or_close
from repositories.read_models import (
    InternalAdminActionFilter,
    InternalAdminActionItemData,
    InternalOperationEventItemData,
    InternalOutboxTaskItemData,
    InternalPaymentCardData,
    InternalPaymentFilter,
    InternalPaymentItemData,
    InternalRefundItemData,
    InternalSubscriptionFilter,
    InternalSubscriptionItemData,
)


class AdminQueries(Protocol):
    """Чтение для internal API админки: списки и карточка платежа (T-26, T-28)."""

    async def list_internal_subscriptions(
        self,
        filters: InternalSubscriptionFilter,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalSubscriptionItemData]]: ...

    async def list_internal_payments(
        self,
        filters: InternalPaymentFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalPaymentItemData]]: ...

    async def get_internal_payment_card(
        self,
        payment_id: UUID,
        *,
        now_utc: datetime | None = None,
    ) -> InternalPaymentCardData | None: ...

    async def list_internal_admin_actions(
        self,
        filters: InternalAdminActionFilter,
    ) -> tuple[int, list[InternalAdminActionItemData]]: ...


class SQLAlchemyAdminQueries(SQLAlchemyRepository):
    """Чтение для internal API админки: списки и карточка платежа (T-26, T-28)."""

    async def list_internal_subscriptions(
        self,
        filters: InternalSubscriptionFilter,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalSubscriptionItemData]]:
        now = now_utc or datetime.now(UTC)
        succeeded_refund_exists = exists(
            select(1).where(
                Refund.subscription_id == Subscription.id,
                Refund.status == 'succeeded',
            )
        )
        conditions: list[Any] = []
        if filters.user_id is not None:
            conditions.append(Subscription.user_id == filters.user_id)
        if filters.created_from is not None:
            conditions.append(Subscription.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(Subscription.created_at <= filters.created_to)
        if filters.status == 'refunded':
            conditions.append(succeeded_refund_exists)
        elif filters.status == 'active':
            conditions.append(~succeeded_refund_exists)
            conditions.append(Subscription.ends_at > now)
        elif filters.status == 'expired':
            conditions.append(~succeeded_refund_exists)
            conditions.append(Subscription.ends_at <= now)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(Subscription.id))
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                stmt = (
                    select(
                        Subscription.id,
                        Subscription.user_id,
                        Subscription.payment_id,
                        Tariff.code,
                        Tariff.title,
                        Subscription.starts_at,
                        Subscription.ends_at,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Subscription.created_at,
                        succeeded_refund_exists.label('has_succeeded_refund'),
                    )
                    .join(Payment, Payment.id == Subscription.payment_id)
                    .join(Tariff, Tariff.id == Subscription.tariff_id)
                )
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(
                        Subscription.created_at.desc(),
                        Subscription.id.desc(),
                    )
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = (await session.execute(stmt)).all()
                items: list[InternalSubscriptionItemData] = []
                for (
                    sub_id,
                    uid,
                    pay_id,
                    t_code,
                    t_title,
                    starts_at,
                    ends_at,
                    amount_kopeks,
                    currency,
                    created_at,
                    has_succeeded_refund,
                ) in rows:
                    state_enum = compute_subscription_state(
                        ends_at=ends_at,
                        has_succeeded_refund=bool(has_succeeded_refund),
                        now_utc=now,
                    )
                    computed_status: Literal['active', 'expired', 'refunded'] = (
                        'refunded'
                        if state_enum == 'refunded'
                        else 'active'
                        if state_enum == 'active'
                        else 'expired'
                    )
                    items.append(
                        InternalSubscriptionItemData(
                            id=sub_id,
                            user_id=uid,
                            payment_id=pay_id,
                            tariff_code=t_code,
                            tariff_name=t_title,
                            starts_at=starts_at,
                            ends_at=ends_at,
                            status=computed_status,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            created_at=created_at,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_internal_payments(
        self,
        filters: InternalPaymentFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalPaymentItemData]]:
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)
        any_refund_exists = exists(select(1).where(Refund.payment_id == Payment.id))
        stuck_refund_exists = exists(
            select(1).where(
                Refund.payment_id == Payment.id,
                Refund.status == 'processing',
                Refund.created_at <= stuck_cutoff,
            )
        )
        stuck_condition = or_(
            and_(Payment.status == 'pending', Payment.created_at <= stuck_cutoff),
            stuck_refund_exists,
        )

        conditions: list[Any] = []
        if filters.status is not None:
            conditions.append(Payment.status == filters.status)
        if filters.has_refund is True:
            conditions.append(any_refund_exists)
        elif filters.has_refund is False:
            conditions.append(~any_refund_exists)
        if filters.stuck:
            conditions.append(stuck_condition)
        if filters.user_id is not None:
            conditions.append(Payment.user_id == filters.user_id)
        if filters.payment_id is not None:
            conditions.append(Payment.id == filters.payment_id)
        if filters.provider_payment_id is not None:
            conditions.append(
                Payment.provider_payment_id == filters.provider_payment_id
            )
        if filters.created_from is not None:
            conditions.append(Payment.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(Payment.created_at <= filters.created_to)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(Payment.id))
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                stmt = select(
                    Payment.id,
                    Payment.user_id,
                    Tariff.code,
                    Tariff.title,
                    Payment.provider_code,
                    Payment.provider_payment_id,
                    Payment.amount_kopeks,
                    Payment.currency,
                    Payment.status,
                    Payment.failure_reason,
                    Payment.created_at,
                    Payment.updated_at,
                ).join(Tariff, Tariff.id == Payment.tariff_id)
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(Payment.created_at.desc(), Payment.id.desc())
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = (await session.execute(stmt)).all()
                payment_ids = [row[0] for row in rows]
                refunds_by_payment: dict[UUID, list[Refund]] = {}
                if payment_ids:
                    refund_rows = list(
                        await session.scalars(
                            select(Refund)
                            .where(Refund.payment_id.in_(payment_ids))
                            .order_by(Refund.created_at.desc(), Refund.id.desc())
                        )
                    )
                    for ref in refund_rows:
                        refunds_by_payment.setdefault(ref.payment_id, []).append(ref)

                items: list[InternalPaymentItemData] = []
                for (
                    pid,
                    uid,
                    t_code,
                    t_title,
                    prov_code,
                    prov_pid,
                    amount_kopeks,
                    currency,
                    p_status,
                    failure_reason,
                    created_at,
                    updated_at,
                ) in rows:
                    p_refunds = refunds_by_payment.get(pid, [])
                    has_ref = len(p_refunds) > 0
                    active_or_latest = next(
                        (
                            r
                            for r in p_refunds
                            if r.status in ('processing', 'succeeded')
                        ),
                        p_refunds[0] if p_refunds else None,
                    )
                    ref_status: (
                        Literal['processing', 'succeeded', 'rejected'] | None
                    ) = None
                    active_ref_id: UUID | None = None
                    if active_or_latest is not None:
                        if active_or_latest.status == 'succeeded':
                            ref_status = 'succeeded'
                            active_ref_id = active_or_latest.id
                        elif active_or_latest.status == 'processing':
                            ref_status = 'processing'
                            active_ref_id = active_or_latest.id
                        else:
                            ref_status = 'rejected'

                    is_payment_stuck = (
                        p_status == 'pending' and created_at <= stuck_cutoff
                    ) or any(
                        r.status == 'processing' and r.created_at <= stuck_cutoff
                        for r in p_refunds
                    )
                    normalized_status: Literal['pending', 'succeeded', 'failed'] = (
                        'succeeded'
                        if p_status == 'succeeded'
                        else 'failed'
                        if p_status == 'failed'
                        else 'pending'
                    )
                    items.append(
                        InternalPaymentItemData(
                            id=pid,
                            user_id=uid,
                            tariff_code=t_code,
                            tariff_name=t_title,
                            provider_code=prov_code,
                            provider_payment_id=prov_pid,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            status=normalized_status,
                            failure_reason=failure_reason,
                            has_refund=has_ref,
                            refund_status=ref_status,
                            active_refund_id=active_ref_id,
                            is_stuck=is_payment_stuck,
                            created_at=created_at,
                            updated_at=updated_at,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_internal_payment_card(
        self,
        payment_id: UUID,
        *,
        now_utc: datetime | None = None,
    ) -> InternalPaymentCardData | None:
        now = now_utc or datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                pay_row = (
                    await session.execute(
                        select(Payment, Tariff.code, Tariff.title)
                        .join(Tariff, Tariff.id == Payment.tariff_id)
                        .where(Payment.id == payment_id)
                    )
                ).one_or_none()
                if pay_row is None:
                    return None
                payment, t_code, t_title = pay_row

                sub_row = await session.scalar(
                    select(Subscription).where(Subscription.payment_id == payment_id)
                )
                refund_rows = list(
                    await session.scalars(
                        select(Refund)
                        .where(Refund.payment_id == payment_id)
                        .order_by(Refund.created_at.desc(), Refund.id.desc())
                    )
                )
                has_succeeded_refund = any(r.status == 'succeeded' for r in refund_rows)
                has_active_or_succeeded_refund = any(
                    r.status in ('processing', 'succeeded') for r in refund_rows
                )

                sub_data: InternalSubscriptionItemData | None = None
                if sub_row is not None:
                    state_enum = compute_subscription_state(
                        ends_at=sub_row.ends_at,
                        has_succeeded_refund=has_succeeded_refund,
                        now_utc=now,
                    )
                    sub_status: Literal['active', 'expired', 'refunded'] = (
                        'refunded'
                        if state_enum == 'refunded'
                        else 'active'
                        if state_enum == 'active'
                        else 'expired'
                    )
                    sub_data = InternalSubscriptionItemData(
                        id=sub_row.id,
                        user_id=sub_row.user_id,
                        payment_id=sub_row.payment_id,
                        tariff_code=t_code,
                        tariff_name=t_title,
                        starts_at=sub_row.starts_at,
                        ends_at=sub_row.ends_at,
                        status=sub_status,
                        amount_kopeks=payment.amount_kopeks,
                        currency=payment.currency.strip(),
                        created_at=sub_row.created_at,
                    )

                refund_ids = [r.id for r in refund_rows]
                event_cond: Any = and_(
                    OperationEvent.operation_type == 'payment',
                    OperationEvent.operation_id == payment_id,
                )
                if refund_ids:
                    event_cond = or_(
                        event_cond,
                        and_(
                            OperationEvent.operation_type == 'refund',
                            OperationEvent.operation_id.in_(refund_ids),
                        ),
                    )
                event_rows = list(
                    await session.scalars(
                        select(OperationEvent)
                        .where(event_cond)
                        .order_by(
                            OperationEvent.created_at.asc(),
                            OperationEvent.id.asc(),
                        )
                    )
                )
                task_rows = list(
                    await session.scalars(
                        select(OutboxTask)
                        .where(OutboxTask.payment_id == payment_id)
                        .order_by(OutboxTask.created_at.asc(), OutboxTask.id.asc())
                    )
                )

                can_refund = (
                    payment.status == 'succeeded'
                    and sub_data is not None
                    and sub_data.status == 'active'
                    and not has_active_or_succeeded_refund
                )
                normalized_p_status: Literal['pending', 'succeeded', 'failed'] = (
                    'succeeded'
                    if payment.status == 'succeeded'
                    else 'failed'
                    if payment.status == 'failed'
                    else 'pending'
                )
                refund_items: list[InternalRefundItemData] = []
                for r in refund_rows:
                    r_status: Literal['processing', 'succeeded', 'rejected'] = (
                        'succeeded'
                        if r.status == 'succeeded'
                        else 'processing'
                        if r.status == 'processing'
                        else 'rejected'
                    )
                    refund_items.append(
                        InternalRefundItemData(
                            id=r.id,
                            payment_id=r.payment_id,
                            subscription_id=r.subscription_id,
                            provider_code=r.provider_code,
                            provider_refund_id=r.provider_refund_id,
                            amount_kopeks=r.amount_kopeks,
                            currency=r.currency.strip(),
                            status=r_status,
                            reason=r.reason,
                            rejection_reason=r.rejection_reason,
                            admin_user_id=r.admin_user_id,
                            created_at=r.created_at,
                            updated_at=r.updated_at,
                            can_reconcile=(r_status == 'processing'),
                        )
                    )

                event_items: list[InternalOperationEventItemData] = [
                    InternalOperationEventItemData(
                        id=ev.id,
                        operation_type=(
                            'refund' if ev.operation_type == 'refund' else 'payment'
                        ),
                        operation_id=ev.operation_id,
                        from_status=ev.from_status,
                        to_status=ev.to_status,
                        source=ev.source,
                        details=ev.details,
                        created_at=ev.created_at,
                    )
                    for ev in event_rows
                ]

                task_items: list[InternalOutboxTaskItemData] = []
                for t in task_rows:
                    t_status: Literal[
                        'pending', 'completed', 'requires_intervention', 'closed'
                    ] = (
                        'completed'
                        if t.status == 'completed'
                        else 'requires_intervention'
                        if t.status == 'requires_intervention'
                        else 'closed'
                        if t.status == 'closed'
                        else 'pending'
                    )
                    task_items.append(
                        InternalOutboxTaskItemData(
                            id=t.id,
                            target_service=(
                                'notifications'
                                if t.target_service == 'notifications'
                                else 'auth'
                            ),
                            task_type=t.task_type,
                            access_version=t.access_version,
                            status=t_status,
                            attempts=t.attempts,
                            last_error=t.last_error,
                            created_at=t.created_at,
                            updated_at=t.updated_at,
                            can_retry_or_close=admin_can_retry_or_close(
                                t.status, t.task_type, t.attempts
                            ),
                            payment_id=t.payment_id,
                            user_id=t.user_id,
                        )
                    )

                return InternalPaymentCardData(
                    id=payment.id,
                    user_id=payment.user_id,
                    tariff_code=t_code,
                    tariff_name=t_title,
                    provider_code=payment.provider_code,
                    provider_payment_id=payment.provider_payment_id,
                    amount_kopeks=payment.amount_kopeks,
                    currency=payment.currency.strip(),
                    status=normalized_p_status,
                    failure_reason=payment.failure_reason,
                    created_at=payment.created_at,
                    updated_at=payment.updated_at,
                    can_refund=can_refund,
                    can_reconcile_payment=(normalized_p_status == 'pending'),
                    subscription=sub_data,
                    refunds=refund_items,
                    operation_events=event_items,
                    outbox_tasks=task_items,
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_internal_admin_actions(
        self,
        filters: InternalAdminActionFilter,
    ) -> tuple[int, list[InternalAdminActionItemData]]:
        conditions: list[Any] = []
        if filters.admin_user_id is not None:
            conditions.append(AdminAction.admin_user_id == filters.admin_user_id)
        if filters.action_type is not None:
            conditions.append(AdminAction.action_type == filters.action_type)
        if filters.created_from is not None:
            conditions.append(AdminAction.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(AdminAction.created_at <= filters.created_to)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(AdminAction.id))
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                stmt = select(AdminAction)
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = list(await session.scalars(stmt))

                refund_ids = [r.target_id for r in rows if r.target_type == 'refund']
                task_ids = [r.target_id for r in rows if r.target_type == 'outbox_task']
                refund_info: dict[UUID, tuple[UUID, str]] = {}
                if refund_ids:
                    ref_rows = (
                        await session.execute(
                            select(Refund.id, Refund.payment_id, Refund.status).where(
                                Refund.id.in_(refund_ids)
                            )
                        )
                    ).all()
                    for rid, pid, rst in ref_rows:
                        refund_info[rid] = (pid, rst)

                task_info: dict[UUID, tuple[UUID, str]] = {}
                if task_ids:
                    t_rows = (
                        await session.execute(
                            select(
                                OutboxTask.id,
                                OutboxTask.payment_id,
                                OutboxTask.status,
                            ).where(OutboxTask.id.in_(task_ids))
                        )
                    ).all()
                    for tid, pid, tst in t_rows:
                        task_info[tid] = (pid, tst)

                items: list[InternalAdminActionItemData] = []
                for row in rows:
                    act_type: Literal[
                        'refund_requested', 'outbox_retried', 'outbox_closed'
                    ] = (
                        'outbox_retried'
                        if row.action_type == 'outbox_retried'
                        else 'outbox_closed'
                        if row.action_type == 'outbox_closed'
                        else 'refund_requested'
                    )
                    tgt_type: Literal['refund', 'outbox_task'] = (
                        'outbox_task' if row.target_type == 'outbox_task' else 'refund'
                    )
                    resolved = (
                        task_info.get(row.target_id)
                        if tgt_type == 'outbox_task'
                        else refund_info.get(row.target_id)
                    )
                    items.append(
                        InternalAdminActionItemData(
                            id=row.id,
                            admin_user_id=row.admin_user_id,
                            action_type=act_type,
                            target_type=tgt_type,
                            target_id=row.target_id,
                            payment_id=resolved[0] if resolved is not None else None,
                            target_status=resolved[1] if resolved is not None else None,
                            reason=row.reason,
                            created_at=row.created_at,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
