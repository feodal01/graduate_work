import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID

import sentry_sdk
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.exc import SQLAlchemyError

from core.observability import correlation_request_id
from db.sql.errors import DatabaseUnavailableError
from domain.errors import EntityNotFoundError, RuleConflictError
from models import (
    AdminAction,
    OutboxTask,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.read_models import (
    InternalOutboxFilter,
    InternalOutboxTaskItemData,
)
from repositories.records import (
    OutboxTaskRecordProtocol,
)

logger = logging.getLogger(__name__)


DeliveryOutcome = Literal['completed', 'transient_error', 'permanent_error']


OutboxDeliverFn = Callable[
    [OutboxTaskRecordProtocol],
    Awaitable[tuple[DeliveryOutcome, str | None]],
]


# Задачи, которые нельзя бросать по лимиту попыток. Доставка доступа в Auth
# идемпотентна и монотонна по версии: повтор безопасен, а остановка оставляет
# возвращённую подписку открытой (или оплаченную закрытой) до ручного
# вмешательства. Отмена письма об окончании после возврата обязана дойти до
# момента T, иначе пользователь с возвращёнными деньгами получит письмо
# «срок подписки закончился» (ТЗ 6.3: «повторяется до восстановления»).
RETRY_UNTIL_DELIVERED_TASK_TYPES = frozenset({'sync_access', 'cancel_expiration_email'})


def transient_failure_transition(
    task_type: str,
    attempts: int,
    max_attempts: int,
) -> tuple[Literal['retry', 'requires_intervention'], bool]:
    """Решает судьбу задачи после временной ошибки: повтор или вмешательство.

    `attempts` — число попыток уже с учётом текущей. Второй элемент — нужен ли
    алерт: для бессрочных задач он поднимается один раз, когда лимит пройден,
    а задача продолжает повторяться с потолком backoff.
    """
    if task_type in RETRY_UNTIL_DELIVERED_TASK_TYPES:
        return 'retry', attempts == max_attempts
    if attempts >= max_attempts:
        return 'requires_intervention', True
    return 'retry', False


def compute_outbox_backoff_seconds(attempts: int) -> int:
    """Вычисляет паузу экспоненциального backoff (2^attempt с потолком 300 с)."""
    normalized = max(1, attempts)
    return int(min(300, 2 ** min(normalized, 9)))


class OutboxRepository(Protocol):
    """Обработка задач outbox воркером (T-19, US-S-02)."""

    async def process_due_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        target_services: Sequence[str] = ('auth', 'notifications'),
        payment_id: UUID | None = None,
        task_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int: ...

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int: ...


class OutboxAdminRepository(Protocol):
    """Просмотр и ручные действия админа над outbox (T-31, T-32)."""

    async def list_internal_outbox_tasks(
        self,
        filters: InternalOutboxFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalOutboxTaskItemData]]: ...

    async def get_internal_outbox_task(
        self,
        task_id: UUID,
    ) -> InternalOutboxTaskItemData | None: ...

    async def retry_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData: ...

    async def close_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData: ...


class SQLAlchemyOutboxRepository(SQLAlchemyRepository):
    """Outbox: доставка воркером и ручные действия админа (T-19, T-31, T-32)."""

    async def process_due_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        target_services: Sequence[str] = ('auth', 'notifications'),
        payment_id: UUID | None = None,
        task_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Выбирает и обрабатывает задачи outbox под SKIP LOCKED (T-19, BIL-FR-11)."""
        if not target_services:
            return 0
        processed = 0
        try:
            for _ in range(limit):
                if should_stop is not None and should_stop():
                    break
                async with self._session_factory() as session:
                    now_utc = datetime.now(UTC)
                    stmt = (
                        select(OutboxTask)
                        .where(
                            OutboxTask.target_service.in_(tuple(target_services)),
                            OutboxTask.status == 'pending',
                            OutboxTask.next_retry_at <= now_utc,
                        )
                        .order_by(
                            OutboxTask.next_retry_at.asc(),
                            OutboxTask.created_at.asc(),
                        )
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    if payment_id is not None:
                        stmt = stmt.where(OutboxTask.payment_id == payment_id)
                    if task_id is not None:
                        stmt = stmt.where(OutboxTask.id == task_id)

                    task = await session.scalar(stmt)
                    if task is None:
                        break

                    # US-S-02 шаг 3: если уже доставлена более новая версия,
                    # закрываем устаревшую задачу без вызова Auth.
                    if (
                        task.target_service == 'auth'
                        and task.access_version is not None
                    ):
                        max_delivered = await session.scalar(
                            select(func.max(OutboxTask.access_version)).where(
                                OutboxTask.user_id == task.user_id,
                                OutboxTask.target_service == 'auth',
                                OutboxTask.status == 'completed',
                            )
                        )
                        if (
                            max_delivered is not None
                            and int(max_delivered) >= task.access_version
                        ):
                            task.status = 'completed'
                            task.last_error = None
                            task.updated_at = now_utc
                            await session.commit()
                            processed += 1
                            continue

                    try:
                        outcome, error_msg = await deliver_fn(
                            task  # pyright: ignore[reportArgumentType]  # Mapped[T]
                        )
                    except Exception as exc:
                        logger.exception(
                            'Unexpected error delivering outbox task %s',
                            task.id,
                        )
                        outcome = 'transient_error'
                        error_msg = f'Unexpected delivery error: {exc}'

                    now_after = datetime.now(UTC)
                    alert_msg: str | None = None
                    if outcome == 'completed':
                        task.status = 'completed'
                        task.last_error = None
                        task.updated_at = now_after
                    elif outcome == 'permanent_error':
                        task.attempts += 1
                        task.status = 'requires_intervention'
                        task.last_error = error_msg
                        task.updated_at = now_after
                        alert_msg = (
                            f'Outbox task {task.id} requires intervention '
                            f'(permanent error): {error_msg}'
                        )
                    else:
                        task.attempts += 1
                        task.last_error = error_msg
                        task.updated_at = now_after
                        transition, alert = transient_failure_transition(
                            task.task_type, task.attempts, max_attempts
                        )
                        if transition == 'requires_intervention':
                            task.status = 'requires_intervention'
                            alert_msg = (
                                f'Outbox task {task.id} exhausted {max_attempts} '
                                f'attempts and requires intervention: {error_msg}'
                            )
                        else:
                            backoff_sec = compute_outbox_backoff_seconds(task.attempts)
                            task.next_retry_at = now_after + timedelta(
                                seconds=backoff_sec
                            )
                            if alert:
                                alert_msg = (
                                    f'Outbox task {task.id} ({task.task_type}) failed '
                                    f'{max_attempts} attempts and keeps retrying '
                                    f'until delivered: {error_msg}'
                                )
                    await session.commit()
                    if alert_msg is not None:
                        with correlation_request_id(f'outbox-{task.id}'):
                            logger.error(alert_msg)
                            sentry_sdk.capture_message(alert_msg, level='error')
                    processed += 1
            return processed
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Выбирает и обрабатывает задачи Auth под SKIP LOCKED (BIL-FR-11)."""
        return await self.process_due_outbox_tasks(
            deliver_fn,
            target_services=('auth',),
            payment_id=payment_id,
            limit=limit,
            max_attempts=max_attempts,
            should_stop=should_stop,
        )

    async def list_internal_outbox_tasks(
        self,
        filters: InternalOutboxFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalOutboxTaskItemData]]:
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)

        conditions: list[Any] = []
        if filters.status == 'requires_intervention':
            conditions.append(OutboxTask.status == 'requires_intervention')
        elif filters.status == 'pending':
            conditions.append(
                and_(
                    OutboxTask.status == 'pending',
                    OutboxTask.created_at <= stuck_cutoff,
                )
            )
        else:
            conditions.append(
                or_(
                    OutboxTask.status == 'requires_intervention',
                    and_(
                        OutboxTask.status == 'pending',
                        OutboxTask.created_at <= stuck_cutoff,
                    ),
                )
            )
        if filters.target_service is not None:
            conditions.append(OutboxTask.target_service == filters.target_service)
        if filters.user_id is not None:
            conditions.append(OutboxTask.user_id == filters.user_id)
        if filters.payment_id is not None:
            conditions.append(OutboxTask.payment_id == filters.payment_id)
        if filters.created_from is not None:
            conditions.append(OutboxTask.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(OutboxTask.created_at <= filters.created_to)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(OutboxTask.id)).where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                priority_order = case(
                    (OutboxTask.status == 'requires_intervention', 0),
                    else_=1,
                )
                stmt = (
                    select(OutboxTask)
                    .where(*conditions)
                    .order_by(
                        priority_order.asc(),
                        OutboxTask.created_at.desc(),
                        OutboxTask.id.desc(),
                    )
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = list(await session.scalars(stmt))
                items: list[InternalOutboxTaskItemData] = []
                for t in rows:
                    t_status: Literal[
                        'pending', 'completed', 'requires_intervention', 'closed'
                    ] = (
                        'requires_intervention'
                        if t.status == 'requires_intervention'
                        else 'pending'
                    )
                    items.append(
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
                            can_retry_or_close=(t_status == 'requires_intervention'),
                            payment_id=t.payment_id,
                            user_id=t.user_id,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_internal_outbox_task(
        self,
        task_id: UUID,
    ) -> InternalOutboxTaskItemData | None:
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id)
                if task is None:
                    return None
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def retry_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        now = now_utc or datetime.now(UTC)
        cleaned_reason = (
            reason.strip()
            if reason.strip()
            else 'Ручной повтор отправки администратором'
        )
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id, with_for_update=True)
                if task is None:
                    raise EntityNotFoundError(
                        'Outbox task not found',
                        error_code='outbox_task_not_found',
                    )
                if task.status in ('completed', 'closed'):
                    raise RuleConflictError(
                        'Задача уже завершена или закрыта',
                        error_code='outbox_task_not_retryable',
                    )
                task.status = 'pending'
                task.attempts = 0
                task.next_retry_at = now
                task.updated_at = now
                session.add(
                    AdminAction(
                        admin_user_id=admin_user_id,
                        action_type='outbox_retried',
                        target_type='outbox_task',
                        target_id=task.id,
                        reason=cleaned_reason,
                    )
                )
                await session.commit()
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def close_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id, with_for_update=True)
                if task is None:
                    raise EntityNotFoundError(
                        'Outbox task not found',
                        error_code='outbox_task_not_found',
                    )
                if task.status != 'requires_intervention':
                    raise RuleConflictError(
                        'Закрытие доступно только для задачи в состоянии '
                        'requires_intervention',
                        error_code='outbox_task_not_requires_intervention',
                    )
                task.status = 'closed'
                task.updated_at = now
                session.add(
                    AdminAction(
                        admin_user_id=admin_user_id,
                        action_type='outbox_closed',
                        target_type='outbox_task',
                        target_id=task.id,
                        reason=cleaned_reason,
                    )
                )
                await session.commit()
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    @staticmethod
    def _to_internal_outbox_item(t: OutboxTask) -> InternalOutboxTaskItemData:
        t_status: Literal['pending', 'completed', 'requires_intervention', 'closed'] = (
            'completed'
            if t.status == 'completed'
            else 'requires_intervention'
            if t.status == 'requires_intervention'
            else 'closed'
            if t.status == 'closed'
            else 'pending'
        )
        return InternalOutboxTaskItemData(
            id=t.id,
            target_service=(
                'notifications' if t.target_service == 'notifications' else 'auth'
            ),
            task_type=t.task_type,
            access_version=t.access_version,
            status=t_status,
            attempts=t.attempts,
            last_error=t.last_error,
            created_at=t.created_at,
            updated_at=t.updated_at,
            can_retry_or_close=(t_status == 'requires_intervention'),
            payment_id=t.payment_id,
            user_id=t.user_id,
        )
