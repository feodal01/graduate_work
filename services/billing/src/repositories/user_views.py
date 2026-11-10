from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError

from db.sql.errors import DatabaseUnavailableError
from models import (
    DEFAULT_TARIFF_CODE,
    OutboxTask,
    Payment,
    Refund,
    Subscription,
    Tariff,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.payments import payment_access_task_key
from repositories.read_models import (
    LocalPaymentResultData,
    UserBillingOverviewData,
    UserPaymentHistoryItemData,
)


class UserBillingViews(Protocol):
    """Чтение для экранов пользователя: результат оплаты и «Моя подписка»."""

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None: ...

    async def get_user_billing_overview(
        self,
        user_id: UUID,
    ) -> UserBillingOverviewData: ...


class SQLAlchemyUserBillingViews(SQLAlchemyRepository):
    """Чтение для экранов пользователя: результат оплаты и «Моя подписка»."""

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
                        OutboxTask.dedup_key == payment_access_task_key(payment_id),
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

    async def get_user_billing_overview(
        self,
        user_id: UUID,
    ) -> UserBillingOverviewData:
        """Читает сводные данные для экрана Э2 «Моя подписка» (T-23, BIL-FR-09)."""
        try:
            async with self._session_factory() as session:
                tariff = await session.scalar(
                    select(Tariff)
                    .where(
                        Tariff.code == DEFAULT_TARIFF_CODE,
                        Tariff.is_active.is_(True),
                    )
                    .limit(1)
                )
                # Нет активного тарифа — цены нет, покупка недоступна.
                tariff_price_kopeks = (
                    tariff.price_kopeks if tariff is not None else None
                )

                latest_sub_stmt = (
                    select(Subscription.payment_id, Subscription.ends_at)
                    .where(Subscription.user_id == user_id)
                    .order_by(
                        Subscription.ends_at.desc(), Subscription.created_at.desc()
                    )
                    .limit(1)
                )
                latest_sub_row = (await session.execute(latest_sub_stmt)).one_or_none()

                latest_ends_at: datetime | None = None
                has_succeeded_refund = False
                has_processing_refund = False
                auth_delivered = False

                if latest_sub_row is not None:
                    latest_sub_payment_id, latest_ends_at = latest_sub_row
                    auth_task_status = await session.scalar(
                        select(OutboxTask.status).where(
                            OutboxTask.dedup_key
                            == payment_access_task_key(latest_sub_payment_id)
                        )
                    )
                    auth_delivered = auth_task_status == 'completed'

                    active_refund_status = await session.scalar(
                        select(Refund.status)
                        .where(
                            Refund.payment_id == latest_sub_payment_id,
                            Refund.status.in_(('processing', 'succeeded')),
                        )
                        .limit(1)
                    )
                    has_succeeded_refund = active_refund_status == 'succeeded'
                    has_processing_refund = active_refund_status == 'processing'

                payments_stmt = (
                    select(
                        Payment.id,
                        Payment.created_at,
                        Tariff.title,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Payment.status,
                        Payment.failure_reason,
                        Refund.status,
                    )
                    .join(Tariff, Tariff.id == Payment.tariff_id)
                    .outerjoin(
                        Refund,
                        and_(
                            Refund.payment_id == Payment.id,
                            Refund.status.in_(('processing', 'succeeded')),
                        ),
                    )
                    .where(Payment.user_id == user_id)
                    .order_by(Payment.created_at.desc(), Payment.id.desc())
                )
                payment_rows = (await session.execute(payments_stmt)).all()

                pending_payment_id: UUID | None = None
                history_items: list[UserPaymentHistoryItemData] = []
                for (
                    pid,
                    created_at,
                    t_title,
                    amount_kopeks,
                    currency,
                    p_status,
                    failure_reason,
                    r_status,
                ) in payment_rows:
                    if p_status == 'pending' and pending_payment_id is None:
                        pending_payment_id = pid
                    normalized_refund: Literal['processing', 'succeeded'] | None = (
                        'succeeded'
                        if r_status == 'succeeded'
                        else 'processing'
                        if r_status == 'processing'
                        else None
                    )
                    history_items.append(
                        UserPaymentHistoryItemData(
                            payment_id=pid,
                            created_at=created_at,
                            tariff_title=t_title,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            status=p_status,
                            failure_reason=failure_reason,
                            refund_status=normalized_refund,
                        )
                    )

                return UserBillingOverviewData(
                    pending_payment_id=pending_payment_id,
                    latest_subscription_ends_at=latest_ends_at,
                    latest_subscription_has_succeeded_refund=has_succeeded_refund,
                    latest_subscription_has_processing_refund=has_processing_refund,
                    latest_subscription_auth_delivered=auth_delivered,
                    tariff_price_kopeks=tariff_price_kopeks,
                    payments=history_items,
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
