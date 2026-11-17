"""Аренда задачи outbox на время доставки (BIL-NFR-R-04)."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from repositories.outbox import DeliveryOutcome
from repositories.records import OutboxTaskRecordProtocol
from tests.fakes import FakeOutboxTaskRow, InMemoryPurchaseRepository


def _task() -> FakeOutboxTaskRow:
    payment_id = uuid4()
    return FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=uuid4(),
        access_version=1,
        payload={'version': 1, 'expires_at': None},
    )


async def test_leased_task_is_not_taken_by_second_handler() -> None:
    """Пока идёт доставка, второй обработчик эту задачу не берёт."""
    repo = InMemoryPurchaseRepository()
    task = _task()
    repo.outbox_tasks[task.dedup_key] = task
    second_handler_results: list[int] = []
    leases_during_delivery: list[tuple[UUID | None, datetime | None]] = []

    async def unreachable(
        _: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        raise AssertionError('second handler must not deliver a leased task')

    async def deliver(
        _: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        leases_during_delivery.append((task.lease_token, task.lease_until))
        second_handler_results.append(await repo.process_due_outbox_tasks(unreachable))
        return 'completed', None

    assert await repo.process_due_outbox_tasks(deliver, lease_seconds=30) == 1

    assert second_handler_results == [0]
    lease_token, lease_until = leases_during_delivery[0]
    assert lease_token is not None
    assert lease_until is not None
    assert lease_until > datetime.now(UTC) + timedelta(seconds=20)
    assert task.status == 'completed'
    assert (task.lease_token, task.lease_until) == (None, None)


async def test_expired_lease_is_redelivered_and_late_outcome_is_dropped() -> None:
    """Истёкшую аренду забирает другой обработчик; опоздавший исход не пишется."""
    repo = InMemoryPurchaseRepository()
    task = _task()
    repo.outbox_tasks[task.dedup_key] = task
    delivered_versions: list[int | None] = []

    async def second_deliver(
        claimed: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        delivered_versions.append(claimed.access_version)
        return 'completed', None

    async def slow_deliver(
        claimed: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        delivered_versions.append(claimed.access_version)
        # Первый обработчик завис дольше срока аренды: задачу берёт второй.
        task.lease_until = datetime.now(UTC) - timedelta(seconds=1)
        assert await repo.process_due_outbox_tasks(second_deliver) == 1
        return 'permanent_error', 'late answer of the first handler'

    assert await repo.process_due_outbox_tasks(slow_deliver) == 0

    # Повтор ушёл с тем же ключом (версией доступа), итог записал второй.
    assert delivered_versions == [1, 1]
    assert task.status == 'completed'
    assert task.attempts == 0
    assert task.last_error is None


async def test_outcome_is_not_recorded_for_task_closed_during_delivery() -> None:
    """Задачу, закрытую администратором во время доставки, исход не меняет."""
    repo = InMemoryPurchaseRepository()
    task = _task()
    repo.outbox_tasks[task.dedup_key] = task

    async def deliver(
        _: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        task.status = 'closed'
        return 'transient_error', 'Auth returned 503'

    assert await repo.process_due_outbox_tasks(deliver) == 0

    assert task.status == 'closed'
    assert task.attempts == 0
