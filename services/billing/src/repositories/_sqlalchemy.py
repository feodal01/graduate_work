from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import exists, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.sql.errors import DatabaseUnavailableError
from domain.subscription import ReconcileSource
from models import (
    OperationEvent,
    Payment,
    Refund,
    Subscription,
    Tariff,
    UserAccessVersion,
)
from repositories.read_models import TariffData


def tariff_data(tariff: Tariff) -> TariffData:
    return TariffData(
        id=tariff.id,
        code=tariff.code,
        title=tariff.title,
        price_kopeks=tariff.price_kopeks,
        currency=tariff.currency.strip(),
        duration_days=tariff.duration_days,
    )


class SQLAlchemyRepository:
    """Общая основа SQL-репозиториев: фабрика сессий и блокировки (инв. 7, 11)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    async def _tariff_title(session: AsyncSession, tariff_id: UUID) -> str:
        """Название тарифа для писем: берётся из БД, а не из кода."""
        title = await session.scalar(select(Tariff.title).where(Tariff.id == tariff_id))
        if title is None:
            raise LookupError(f'Tariff {tariff_id} not found')
        return title

    @staticmethod
    def _add_unchanged_check_event(
        session: AsyncSession,
        operation_type: str,
        operation_id: UUID,
        status: str,
        source: ReconcileSource,
    ) -> None:
        """Проверка в ЮKassa без смены статуса тоже видна в истории (US-A-06)."""
        session.add(
            OperationEvent(
                operation_type=operation_type,
                operation_id=operation_id,
                from_status=status,
                to_status=status,
                source=source,
                details={'check': 'status_unchanged'},
            )
        )

    async def _set_marker_once(
        self,
        operation_type: str,
        operation_id: UUID,
        setter: Callable[[Any], bool],
    ) -> bool:
        """Ставит отметку «сигнал поднят» под блокировкой строки.

        Сигнал остаётся единственным даже при гонке вебхуков или сверок.
        """
        try:
            async with self._session_factory() as session:
                record: Payment | Refund
                if operation_type == 'payment':
                    record = await self._lock_user_and_payment(session, operation_id)
                else:
                    record, _ = await self._lock_user_and_refund(session, operation_id)
                marked = setter(record)
                if marked:
                    await session.commit()
                return marked
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

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
            raise LookupError(f'Payment {payment_id} not found')
        await cls._acquire_user_lock(session, user_id)
        payment = await session.get(
            Payment,
            payment_id,
            with_for_update=True,
        )
        if payment is None:
            raise LookupError(f'Payment {payment_id} not found')
        return payment

    async def _lock_user_and_refund(
        self,
        session: AsyncSession,
        refund_id: UUID,
    ) -> tuple[Refund, Payment]:
        ref_row = (
            await session.execute(
                select(Refund.id, Payment.user_id)
                .join(Payment, Payment.id == Refund.payment_id)
                .where(Refund.id == refund_id)
            )
        ).one_or_none()
        if ref_row is None:
            raise LookupError(f'Refund {refund_id} not found')
        _, user_id = ref_row
        await self._acquire_user_lock(session, user_id)

        refund = await session.scalar(
            select(Refund).where(Refund.id == refund_id).with_for_update()
        )
        if refund is None:
            raise LookupError(f'Refund {refund_id} not found')
        payment = await session.scalar(
            select(Payment).where(Payment.id == refund.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError(f'Payment {refund.payment_id} not found')
        return refund, payment

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
