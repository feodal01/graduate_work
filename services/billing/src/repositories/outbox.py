import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

import sentry_sdk
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from core.observability import correlation_request_id
from db.sql.errors import DatabaseUnavailableError
from domain.errors import EntityNotFoundError, RuleConflictError
from models import (
    AdminAction,
    OutboxTask,
    UserAccessVersion,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.outbox_rules import (
    LATEST_ACCESS_TASK_CLOSE_MESSAGE,
    DeliveryOutcome,
    admin_can_close,
    admin_can_retry_or_close,
    compute_outbox_backoff_seconds,
    is_latest_access_task,
    transient_failure_transition,
)
from repositories.read_models import (
    InternalOutboxFilter,
    InternalOutboxTaskItemData,
)
from repositories.records import (
    OutboxTaskRecordProtocol,
)

logger = logging.getLogger(__name__)


# Срок аренды задачи на время доставки. Должен быть заметно больше тайм-аутов
# клиентов Auth и Notifications: иначе задачу заберёт второй обработчик, пока
# первый ещё ждёт ответа.
DEFAULT_OUTBOX_LEASE_SECONDS = 60.0


@dataclass(slots=True)
class ClaimedOutboxTask:
    """Снимок задачи, закреплённой за обработчиком на время доставки.

    Доставка идёт уже без сессии БД, поэтому обработчик получает копию полей,
    а не ORM-объект. `lease_token` подтверждает владельца при записи исхода.
    """

    id: UUID
    dedup_key: str
    target_service: str
    task_type: str
    payment_id: UUID
    user_id: UUID
    access_version: int | None
    payload: dict[str, Any]
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: datetime
    lease_token: UUID


OutboxDeliverFn = Callable[
    [OutboxTaskRecordProtocol],
    Awaitable[tuple[DeliveryOutcome, str | None]],
]


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
        lease_seconds: float = DEFAULT_OUTBOX_LEASE_SECONDS,
        should_stop: Callable[[], bool] | None = None,
    ) -> int: ...

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        lease_seconds: float = DEFAULT_OUTBOX_LEASE_SECONDS,
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
        lease_seconds: float = DEFAULT_OUTBOX_LEASE_SECONDS,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Доставляет готовые задачи outbox (T-19, BIL-FR-11).

        Сетевой вызов идёт без соединения с БД. Задача сначала закрепляется за
        обработчиком короткой транзакцией (аренда до `lease_until`), а исход
        доставки записывает вторая короткая транзакция с проверкой владельца.
        Если обработчик завершится между ними, аренда истечёт и задачу
        доставят повторно с теми же ключами из `payload` и `access_version`.
        """
        if not target_services:
            return 0
        processed = 0
        try:
            for _ in range(limit):
                if should_stop is not None and should_stop():
                    break
                claim = await self._claim_next_outbox_task(
                    target_services=target_services,
                    payment_id=payment_id,
                    task_id=task_id,
                    lease_seconds=lease_seconds,
                )
                if claim is None:
                    break
                if claim == 'superseded':
                    processed += 1
                    continue

                try:
                    outcome, error_msg = await deliver_fn(claim)
                except Exception as exc:
                    logger.exception(
                        'Unexpected error delivering outbox task %s',
                        claim.id,
                    )
                    outcome = 'transient_error'
                    error_msg = f'Unexpected delivery error: {exc}'

                if await self._record_delivery_outcome(
                    claim,
                    outcome=outcome,
                    error_msg=error_msg,
                    max_attempts=max_attempts,
                ):
                    processed += 1
            return processed
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def _claim_next_outbox_task(
        self,
        *,
        target_services: Sequence[str],
        payment_id: UUID | None,
        task_id: UUID | None,
        lease_seconds: float,
    ) -> 'ClaimedOutboxTask | Literal["superseded"] | None':
        """Короткая транзакция: закрепляет за обработчиком одну готовую задачу.

        Возвращает снимок задачи с номером аренды, `'superseded'`, если задача
        закрыта без вызова получателя, или `None`, если готовых задач нет.
        """
        async with self._session_factory() as session:
            now_utc = datetime.now(UTC)
            stmt = (
                select(OutboxTask)
                .where(
                    OutboxTask.target_service.in_(tuple(target_services)),
                    OutboxTask.status == 'pending',
                    OutboxTask.next_retry_at <= now_utc,
                    # Задачу с действующей арендой сейчас доставляет другой
                    # обработчик; истёкшая аренда — обработчик не дописал исход.
                    or_(
                        OutboxTask.lease_until.is_(None),
                        OutboxTask.lease_until <= now_utc,
                    ),
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
                return None

            # US-S-02 шаг 3: если уже доставлена более новая версия,
            # закрываем устаревшую задачу без вызова Auth.
            if task.target_service == 'auth' and task.access_version is not None:
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
                    task.lease_token = None
                    task.lease_until = None
                    task.updated_at = now_utc
                    await session.commit()
                    return 'superseded'

            lease_token = uuid4()
            task.lease_token = lease_token
            task.lease_until = now_utc + timedelta(seconds=lease_seconds)
            claimed = ClaimedOutboxTask(
                id=task.id,
                dedup_key=task.dedup_key,
                target_service=task.target_service,
                task_type=task.task_type,
                payment_id=task.payment_id,
                user_id=task.user_id,
                access_version=task.access_version,
                payload=dict(task.payload),
                status=task.status,
                attempts=task.attempts,
                last_error=task.last_error,
                next_retry_at=task.next_retry_at,
                lease_token=lease_token,
            )
            await session.commit()
            return claimed

    @staticmethod
    async def _complete_superseded_access_tasks(
        session: AsyncSession,
        delivered: OutboxTask,
        now_utc: datetime,
    ) -> None:
        """Доставлена версия V — более старые задачи доступа в ТВ больше не нужны.

        Auth применяет только версию больше сохранённой (инв. 11), поэтому
        старую задачу доставлять незачем: она выполнена без вызова Auth, в той
        же транзакции, что и запись доставки, и уходит из «Не доставлено».
        Ожидающие старые задачи по-прежнему закрывает взятие в работу.
        """
        if delivered.task_type != 'sync_access' or delivered.access_version is None:
            return
        superseded = await session.scalars(
            select(OutboxTask)
            .where(
                OutboxTask.user_id == delivered.user_id,
                OutboxTask.task_type == 'sync_access',
                OutboxTask.status == 'requires_intervention',
                OutboxTask.access_version < delivered.access_version,
            )
            .with_for_update()
        )
        for old in superseded:
            old.status = 'completed'
            old.last_error = (
                f'superseded by access version {delivered.access_version} '
                f'(task {delivered.id})'
            )
            old.updated_at = now_utc

    async def _record_delivery_outcome(
        self,
        claimed: 'ClaimedOutboxTask',
        *,
        outcome: DeliveryOutcome,
        error_msg: str | None,
        max_attempts: int,
    ) -> bool:
        """Короткая транзакция: записывает исход доставки, если аренда ещё наша.

        Возвращает `False`, когда задачу уже забрал другой обработчик (аренда
        истекла) или её закрыл администратор: чужой исход не перезаписывается.
        """
        async with self._session_factory() as session:
            task = await session.scalar(
                select(OutboxTask)
                .where(
                    OutboxTask.id == claimed.id,
                    OutboxTask.lease_token == claimed.lease_token,
                    OutboxTask.status == 'pending',
                )
                .with_for_update()
            )
            if task is None:
                logger.warning(
                    'Outbox task %s is no longer leased by this handler; '
                    'delivery outcome %s is not recorded',
                    claimed.id,
                    outcome,
                )
                return False

            now_after = datetime.now(UTC)
            alert_msg: str | None = None
            task.lease_token = None
            task.lease_until = None
            task.updated_at = now_after
            if outcome == 'completed':
                task.status = 'completed'
                task.last_error = None
                await self._complete_superseded_access_tasks(session, task, now_after)
            elif outcome == 'permanent_error':
                task.attempts += 1
                task.status = 'requires_intervention'
                task.last_error = error_msg
                alert_msg = (
                    f'Outbox task {task.id} requires intervention '
                    f'(permanent error): {error_msg}'
                )
            else:
                task.attempts += 1
                task.last_error = error_msg
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
                    task.next_retry_at = now_after + timedelta(seconds=backoff_sec)
                    if alert:
                        alert_msg = (
                            f'Outbox task {task.id} ({task.task_type}) failed '
                            f'{max_attempts} attempts and keeps retrying '
                            f'until delivered: {error_msg}'
                        )
            await session.commit()
        if alert_msg is not None:
            with correlation_request_id(f'outbox-{claimed.id}'):
                logger.error(alert_msg)
                sentry_sdk.capture_message(alert_msg, level='error')
        return True

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        lease_seconds: float = DEFAULT_OUTBOX_LEASE_SECONDS,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Выбирает и обрабатывает задачи Auth под SKIP LOCKED (BIL-FR-11)."""
        return await self.process_due_outbox_tasks(
            deliver_fn,
            target_services=('auth',),
            payment_id=payment_id,
            limit=limit,
            max_attempts=max_attempts,
            lease_seconds=lease_seconds,
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
                latest = await self._latest_access_versions(
                    session, {t.user_id for t in rows}
                )
                items = [
                    self._to_internal_outbox_item(t, latest.get(t.user_id))
                    for t in rows
                ]
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
                latest = await self._latest_access_versions(session, {task.user_id})
                return self._to_internal_outbox_item(task, latest.get(task.user_id))
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
                if not admin_can_retry_or_close(
                    task.status, task.task_type, task.attempts
                ):
                    raise RuleConflictError(
                        'Повтор доступен только для задачи, требующей вмешательства: '
                        'pending-задачу до лимита уже повторяет воркер, '
                        'а завершённую или закрытую повторять нельзя',
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
                latest = await self._latest_access_versions(session, {task.user_id})
                await session.commit()
                return self._to_internal_outbox_item(task, latest.get(task.user_id))
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
                if not admin_can_retry_or_close(
                    task.status, task.task_type, task.attempts
                ):
                    raise RuleConflictError(
                        'Закрытие доступно только для задачи, требующей вмешательства',
                        error_code='outbox_task_not_requires_intervention',
                    )
                latest = await self._latest_access_versions(session, {task.user_id})
                if is_latest_access_task(
                    task.task_type,
                    access_version=task.access_version,
                    latest_access_version=latest.get(task.user_id),
                ):
                    raise RuleConflictError(
                        LATEST_ACCESS_TASK_CLOSE_MESSAGE,
                        error_code='outbox_task_latest_access_version',
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
                return self._to_internal_outbox_item(task, latest.get(task.user_id))
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    @staticmethod
    async def _latest_access_versions(
        session: AsyncSession, user_ids: set[UUID]
    ) -> dict[UUID, int]:
        """Последняя выданная версия доступа пользователей (инв. 11)."""
        if not user_ids:
            return {}
        rows = await session.execute(
            select(UserAccessVersion.user_id, UserAccessVersion.last_version).where(
                UserAccessVersion.user_id.in_(user_ids)
            )
        )
        return {user_id: int(version) for user_id, version in rows.all()}

    @staticmethod
    def _to_internal_outbox_item(
        t: OutboxTask, latest_access_version: int | None
    ) -> InternalOutboxTaskItemData:
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
            can_retry_or_close=admin_can_retry_or_close(
                t.status, t.task_type, t.attempts
            ),
            payment_id=t.payment_id,
            user_id=t.user_id,
            can_close=admin_can_close(
                t.status,
                t.task_type,
                t.attempts,
                access_version=t.access_version,
                latest_access_version=latest_access_version,
            ),
        )
