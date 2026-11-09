import asyncio
import os
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import PostgresSettings
from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    DEFAULT_TARIFF_ID,
    DEFAULT_TARIFF_PRICE_KOPEKS,
    AdminAction,
    OperationEvent,
    OutboxTask,
    Payment,
    PaymentProvider,
    Refund,
    Subscription,
    Tariff,
    UserAccessVersion,
)

if TYPE_CHECKING:
    from repositories.bundle import BillingRepositories
    from repositories.records import PaymentRecordProtocol

pytestmark = pytest.mark.integration

SERVICE_ROOT = Path(__file__).resolve().parents[1]


async def _run_alembic(db_name: str, command: str, revision: str) -> None:
    env = dict(os.environ, POSTGRES_DB=db_name)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'alembic',
        '-c',
        str(SERVICE_ROOT / 'alembic.ini'),
        command,
        revision,
        cwd=str(SERVICE_ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, (stdout + stderr).decode()


@pytest.fixture
async def migrated_billing_db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Создаёт изолированную БД в PostgreSQL и накатывает миграции Alembic."""
    settings = PostgresSettings()
    admin_db = (
        'notifications_test_admin'
        if settings.user == 'notifications_test'
        else 'postgres'
    )
    database = f'billing_test_{uuid4().hex}'
    admin = await asyncpg.connect(
        user=settings.user,
        password=settings.password.get_secret_value(),
        host=settings.host,
        port=settings.port,
        database=admin_db,
    )
    await admin.execute(f'CREATE DATABASE "{database}"')
    engine: AsyncEngine = create_async_engine(
        settings.model_copy(update={'db': database}).dsn,
        connect_args={'server_settings': {'synchronous_commit': 'on'}},
    )
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        await _run_alembic(database, 'upgrade', 'head')
        yield session_factory
    finally:
        await engine.dispose()
        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        await admin.close()


async def test_migration_seeds_tariffs_and_providers_and_enforces_synchronous_commit(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """Миграция обратима (downgrade -> upgrade) и включает synchronous_commit=on."""
    session_factory = migrated_billing_db
    async with session_factory() as session:
        db_name = str(await session.scalar(text('SELECT current_database()')))

    # Проверяем обратимость миграции: downgrade base -> 0 таблиц -> upgrade head
    await _run_alembic(db_name, 'downgrade', 'base')
    async with session_factory() as session:
        tables_after_down = list(
            (
                await session.scalars(
                    text(
                        'SELECT tablename FROM pg_tables '
                        "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
                    )
                )
            ).all()
        )
        assert tables_after_down == []

    await _run_alembic(db_name, 'upgrade', 'head')

    async with session_factory() as session:
        sync_commit = await session.scalar(text('SHOW synchronous_commit'))
        assert sync_commit == 'on'

        provider = await session.get(PaymentProvider, DEFAULT_PROVIDER_CODE)
        assert provider is not None
        assert provider.title == 'ЮKassa'
        assert provider.is_active is True

        tariff = await session.scalar(
            select(Tariff).where(Tariff.code == DEFAULT_TARIFF_CODE)
        )
        assert tariff is not None
        assert tariff.id == DEFAULT_TARIFF_ID
        assert tariff.price_kopeks == DEFAULT_TARIFF_PRICE_KOPEKS
        assert tariff.currency.strip() == 'RUB'
        assert tariff.duration_days == 30
        assert tariff.is_active is True


async def test_invariant_1_single_pending_payment_per_user_enforced_by_db(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """Инвариант 1 и BIL-NFR-R-01: не более одного pending-платежа на пользователя."""
    session_factory = migrated_billing_db
    user_id = uuid4()
    first_id = uuid4()

    async with session_factory() as session:
        first = Payment(
            id=first_id,
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='pending',
            idempotency_key=uuid4(),
        )
        session.add(first)
        await session.commit()

        # Второй pending-платёж отклоняется индексом uq_payments_user_pending
        duplicate_pending = Payment(
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='pending',
            idempotency_key=uuid4(),
        )
        session.add(duplicate_pending)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # Переводим первый платёж в failed -> теперь новый pending-платёж разрешён
        first_in_db = await session.get(Payment, first_id)
        assert first_in_db is not None
        first_in_db.status = 'failed'
        first_in_db.failure_reason = 'canceled_by_provider'
        await session.commit()

        second_pending = Payment(
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='pending',
            idempotency_key=uuid4(),
        )
        session.add(second_pending)
        await session.commit()


async def test_invariant_4_single_active_or_succeeded_refund_and_provider_fk(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """Инвариант 4, BIL-NFR-R-02 и BIL-NFR-S-02: единственный возврат и составной FK."""
    session_factory = migrated_billing_db
    user_id = uuid4()
    admin_id = uuid4()
    payment_id = uuid4()
    subscription_id = uuid4()
    refund_1_id = uuid4()
    starts_at = datetime.now(UTC)
    ends_at = starts_at + timedelta(days=30)

    async with session_factory() as session:
        session.add(
            PaymentProvider(code='other_provider', title='Other', is_active=False)
        )
        payment = Payment(
            id=payment_id,
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='succeeded',
            idempotency_key=uuid4(),
            provider_payment_id='yk-pay-1',
        )
        session.add(payment)
        await session.flush()

        subscription = Subscription(
            id=subscription_id,
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            payment_id=payment_id,
            starts_at=starts_at,
            ends_at=ends_at,
        )
        session.add(subscription)
        await session.commit()

        # 1. Возврат через другого провайдера отклоняется fk_refunds_payment_provider
        wrong_provider_refund = Refund(
            payment_id=payment_id,
            subscription_id=subscription_id,
            provider_code='other_provider',
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='processing',
            idempotency_key=uuid4(),
            admin_user_id=admin_id,
            reason='Тест неверного провайдера',
        )
        session.add(wrong_provider_refund)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # 2. Создаём штатный возврат в статусе processing
        refund_1 = Refund(
            id=refund_1_id,
            payment_id=payment_id,
            subscription_id=subscription_id,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='processing',
            idempotency_key=uuid4(),
            admin_user_id=admin_id,
            reason='Первая попытка возврата',
        )
        session.add(refund_1)
        await session.commit()

        # 3. Вторая попытка при processing отклоняется частичным уникальным индексом
        duplicate_refund = Refund(
            payment_id=payment_id,
            subscription_id=subscription_id,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='processing',
            idempotency_key=uuid4(),
            admin_user_id=admin_id,
            reason='Дублирующая попытка',
        )
        session.add(duplicate_refund)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # 4. Переводим первую попытку в rejected -> теперь новая попытка разрешена
        r1 = await session.get(Refund, refund_1_id)
        assert r1 is not None
        r1.status = 'rejected'
        r1.rejection_reason = 'provider_rejected'
        await session.commit()

        refund_2 = Refund(
            payment_id=payment_id,
            subscription_id=subscription_id,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='succeeded',
            idempotency_key=uuid4(),
            admin_user_id=admin_id,
            reason='Повторная успешная попытка возврата',
        )
        session.add(refund_2)
        await session.commit()

        # 5. После succeeded ещё один возврат снова запрещён
        after_succeeded_refund = Refund(
            payment_id=payment_id,
            subscription_id=subscription_id,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='processing',
            idempotency_key=uuid4(),
            admin_user_id=admin_id,
            reason='Попытка после успешного возврата',
        )
        session.add(after_succeeded_refund)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()


async def test_invariant_8_on_delete_restrict_and_check_constraints_enforced(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """Инвариант 8 и BIL-NFR-R-07: ON DELETE RESTRICT и CHECK-ограничения."""
    session_factory = migrated_billing_db
    user_id = uuid4()
    payment_id = uuid4()

    async with session_factory() as session:
        payment = Payment(
            id=payment_id,
            user_id=user_id,
            tariff_id=DEFAULT_TARIFF_ID,
            provider_code=DEFAULT_PROVIDER_CODE,
            amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
            status='pending',
            idempotency_key=uuid4(),
        )
        session.add(payment)
        await session.commit()

        # Удаление тарифа/провайдера при наличии платежа отклоняется ON DELETE RESTRICT
        with pytest.raises(IntegrityError):
            await session.execute(delete(Tariff).where(Tariff.id == DEFAULT_TARIFF_ID))
            await session.commit()
        await session.rollback()

        with pytest.raises(IntegrityError):
            await session.execute(
                delete(PaymentProvider).where(
                    PaymentProvider.code == DEFAULT_PROVIDER_CODE
                )
            )
            await session.commit()
        await session.rollback()

        # Неположительная сумма платежа отклоняется ck_payments_amount_kopeks
        session.add(
            Payment(
                user_id=uuid4(),
                tariff_id=DEFAULT_TARIFF_ID,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=0,
                status='pending',
                idempotency_key=uuid4(),
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # Недопустимый источник ('user_return') отклоняется ck_operation_events_source
        session.add(
            OperationEvent(
                operation_type='payment',
                operation_id=payment_id,
                from_status=None,
                to_status='pending',
                source='user_return',
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # Пустая причина действия администратора отклоняется ck_admin_actions_reason
        session.add(
            AdminAction(
                admin_user_id=uuid4(),
                action_type='outbox_closed',
                target_type='outbox_task',
                target_id=uuid4(),
                reason='   ',
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # Версия доступа < 1 отклоняется ck_user_access_versions_last_version
        session.add(UserAccessVersion(user_id=uuid4(), last_version=0))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        # Дубликат dedup_key в outbox_tasks отклоняется uq_outbox_tasks_dedup_key
        task_1 = OutboxTask(
            dedup_key='payment_succeeded:1',
            target_service='auth',
            task_type='sync_access',
            payment_id=payment_id,
            user_id=user_id,
            access_version=1,
            payload={'ok': True},
        )
        session.add(task_1)
        await session.commit()

        task_dup = OutboxTask(
            dedup_key='payment_succeeded:1',
            target_service='auth',
            task_type='sync_access',
            payment_id=payment_id,
            user_id=user_id,
            access_version=1,
            payload={'ok': True},
        )
        session.add(task_dup)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()


async def test_sqlalchemy_purchase_repository_concurrent_and_blocking_rules(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-10: проверка SQLAlchemyPurchaseRepository на реальном PostgreSQL."""
    from repositories.bundle import BillingRepositories

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()

    # 1. Изначально покупка разрешена
    allowed, code = await repos.payments.check_purchase_allowed(user_id)
    assert allowed is True and code is None

    # 2. Два конкурентных вызова возвращают один и тот же pending-платёж
    res_a, res_b = await asyncio.gather(
        repos.payments.get_or_create_pending_payment(user_id),
        repos.payments.get_or_create_pending_payment(user_id),
    )
    pay_a, is_new_a = res_a
    pay_b, is_new_b = res_b
    assert pay_a.id == pay_b.id
    assert {is_new_a, is_new_b} == {True, False}
    assert pay_a.last_checked_at is None

    # 3. save_provider_payment_created не ставит last_checked_at
    updated = await repos.payments.save_provider_payment_created(
        pay_a.id,
        provider_payment_id='yk-pg-1',
        confirmation_url='https://yoomoney.ru/checkout/1',
        provider_details={'id': 'yk-pg-1'},
    )
    assert updated.provider_payment_id == 'yk-pg-1'
    assert updated.last_checked_at is None

    checked = await repos.payments.record_payment_checked(
        pay_a.id,
        confirmation_url='https://yoomoney.ru/checkout/1',
        provider_details={'id': 'yk-pg-1', 'status': 'pending'},
    )
    assert checked.last_checked_at is not None

    # 4. Два параллельных finalize_payment_failed пишут ровно одно событие
    await asyncio.gather(
        repos.payments.finalize_payment_failed(
            pay_a.id,
            failure_reason='canceled_by_provider',
            source='continue',
        ),
        repos.payments.finalize_payment_failed(
            pay_a.id,
            failure_reason='canceled_by_provider',
            source='continue',
        ),
    )
    async with session_factory() as session:
        events = list(
            (
                await session.scalars(
                    select(OperationEvent).where(
                        OperationEvent.operation_id == pay_a.id,
                        OperationEvent.to_status == 'failed',
                    )
                )
            ).all()
        )
        assert len(events) == 1

    # 5. При активной подписке и возврате в processing приоритет у подписки
    now_utc = datetime.now(UTC)
    sub_id = uuid4()
    succeeded_pay_id = uuid4()
    async with session_factory() as session:
        session.add(
            Payment(
                id=succeeded_pay_id,
                user_id=user_id,
                tariff_id=DEFAULT_TARIFF_ID,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                status='succeeded',
                idempotency_key=uuid4(),
                provider_payment_id='yk-pg-succeeded',
            )
        )
        await session.flush()
        session.add(
            Subscription(
                id=sub_id,
                user_id=user_id,
                tariff_id=DEFAULT_TARIFF_ID,
                payment_id=succeeded_pay_id,
                starts_at=now_utc,
                ends_at=now_utc + timedelta(days=30),
            )
        )
        await session.flush()
        session.add(
            Refund(
                id=uuid4(),
                payment_id=succeeded_pay_id,
                subscription_id=sub_id,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                status='processing',
                idempotency_key=uuid4(),
                admin_user_id=uuid4(),
                reason='Тест приоритета блокировки',
            )
        )
        await session.commit()

    allowed_sub, code_sub = await repos.payments.check_purchase_allowed(user_id)
    assert allowed_sub is False
    assert code_sub == 'subscription_already_active'

    # И внутри транзакции get_or_create_pending_payment под lock выбрасывается 409
    from domain.errors import RuleConflictError

    with pytest.raises(RuleConflictError) as exc_info:
        await repos.payments.get_or_create_pending_payment(user_id)
    assert exc_info.value.error_code == 'subscription_already_active'


async def test_t11_atomic_reconciliation_succeeded_failed_and_concurrent_race(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-11 (BIL-FR-04, BIL-FR-08, BIL-FR-13, инв. 6, 7, 11): фиксация в БД."""
    from domain.subscription import SubscriptionState, compute_subscription_state
    from repositories.bundle import BillingRepositories

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()

    # 1. Создаём pending-платёж и сохраняем provider_payment_id
    payment, is_new = await repos.payments.get_or_create_pending_payment(user_id)
    assert is_new is True
    await repos.payments.save_provider_payment_created(
        payment.id,
        provider_payment_id='yk-race-succeeded-1',
        confirmation_url='https://yoomoney.ru/checkout/race-1',
        provider_details={'id': 'yk-race-succeeded-1', 'status': 'pending'},
    )

    # 2. Запускаем 4 параллельных процесса фиксации оплаты
    results = await asyncio.gather(
        repos.payments.finalize_payment_succeeded(
            payment.id,
            source='webhook',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repos.payments.finalize_payment_succeeded(
            payment.id,
            source='reconcile',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repos.payments.finalize_payment_succeeded(
            payment.id,
            source='continue',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repos.payments.finalize_payment_succeeded(
            payment.id,
            source='admin',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
    )

    transition_flags = [transitioned for _, transitioned in results]
    assert transition_flags.count(True) == 1
    assert transition_flags.count(False) == 3

    # 3. Проверяем таблицы в PostgreSQL: 1 подписка, версия 1, 3 задачи outbox
    async with session_factory() as session:
        subs = list(
            (
                await session.scalars(
                    select(Subscription).where(Subscription.payment_id == payment.id)
                )
            ).all()
        )
        assert len(subs) == 1
        sub = subs[0]
        assert sub.ends_at - sub.starts_at == timedelta(days=30)
        assert (
            compute_subscription_state(
                ends_at=sub.ends_at,
                has_succeeded_refund=False,
            )
            == SubscriptionState.ACTIVE
        )

        ver = await session.get(UserAccessVersion, user_id)
        assert ver is not None
        assert ver.last_version == 1

        tasks = list(
            (
                await session.scalars(
                    select(OutboxTask).where(OutboxTask.payment_id == payment.id)
                )
            ).all()
        )
        assert len(tasks) == 3
        task_types = {t.task_type for t in tasks}
        assert task_types == {
            'sync_access',
            'payment_succeeded_email',
            'subscription_expiration_email',
        }
        auth_task = next(t for t in tasks if t.task_type == 'sync_access')
        assert auth_task.access_version == 1
        assert auth_task.payload['version'] == 1
        assert auth_task.payload['expires_at'] == sub.ends_at.isoformat()

        succeeded_events = list(
            (
                await session.scalars(
                    select(OperationEvent).where(
                        OperationEvent.operation_id == payment.id,
                        OperationEvent.to_status == 'succeeded',
                    )
                )
            ).all()
        )
        assert len(succeeded_events) == 1

    # 4. Попытка перевести уже оплаченный платёж в failed игнорируется (инвариант 7 и 8)
    after_fail, failed_transitioned = await repos.payments.finalize_payment_failed(
        payment.id,
        failure_reason='late_cancel',
        source='reconcile',
    )
    assert failed_transitioned is False
    assert after_fail.status == 'succeeded'

    # 5. Проверяем атомарную фиксацию неуспешной оплаты (failed + письмо отказа)
    user_2 = uuid4()
    pay_2, _ = await repos.payments.get_or_create_pending_payment(user_2)
    res_fail_1, res_fail_2 = await asyncio.gather(
        repos.payments.finalize_payment_failed(
            pay_2.id,
            failure_reason='insufficient_funds',
            source='webhook',
        ),
        repos.payments.finalize_payment_failed(
            pay_2.id,
            failure_reason='insufficient_funds',
            source='reconcile',
        ),
    )
    assert {res_fail_1[1], res_fail_2[1]} == {True, False}
    async with session_factory() as session:
        fail_tasks = list(
            (
                await session.scalars(
                    select(OutboxTask).where(OutboxTask.payment_id == pay_2.id)
                )
            ).all()
        )
        assert len(fail_tasks) == 1
        assert fail_tasks[0].task_type == 'payment_failed_email'
        assert fail_tasks[0].payload['failure_reason'] == 'insufficient_funds'


async def test_t12_t13_t16_and_t17_repository_queries_and_skip_locked(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-12, T-13, T-16, T-17: локальный результат и SKIP LOCKED в PostgreSQL."""
    from repositories.bundle import BillingRepositories
    from repositories.outbox import DeliveryOutcome
    from repositories.records import OutboxTaskRecordProtocol

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()
    payment, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.finalize_payment_succeeded(payment.id, source='webhook')

    # 1. T-12: проверяем get_local_payment_result_for_user
    local_own = await repos.user_views.get_local_payment_result_for_user(
        payment.id, user_id
    )
    assert local_own is not None
    assert local_own.status == 'succeeded'
    assert local_own.subscription_ends_at is not None
    assert local_own.auth_access_delivered is False

    # Помечаем задачу sync_access выполненной -> auth_access_delivered == True
    async with session_factory() as session:
        task_row = await session.scalar(
            select(OutboxTask).where(
                OutboxTask.payment_id == payment.id,
                OutboxTask.task_type == 'sync_access',
            )
        )
        assert task_row is not None
        task_row.status = 'completed'
        await session.commit()

    local_delivered = await repos.user_views.get_local_payment_result_for_user(
        payment.id, user_id
    )
    assert local_delivered is not None
    assert local_delivered.auth_access_delivered is True

    # Чужой пользователь получает None (защита BIL-NFR-SEC-03)
    local_foreign = await repos.user_views.get_local_payment_result_for_user(
        payment.id, uuid4()
    )
    assert local_foreign is None

    # 2. T-13: проверяем get_payment_for_user для своего и чужого пользователя
    own_pay = await repos.payments.get_payment_for_user(payment.id, user_id)
    assert own_pay is not None and own_pay.id == payment.id
    foreign_pay = await repos.payments.get_payment_for_user(payment.id, uuid4())
    assert foreign_pay is None

    # 3. T-16: проверяем FOR UPDATE SKIP LOCKED в process_due_auth_outbox_tasks
    user_3 = uuid4()
    pay_3, _ = await repos.payments.get_or_create_pending_payment(user_3)
    await repos.payments.finalize_payment_succeeded(pay_3.id, source='webhook')

    delivered_calls: list[int] = []

    async def deliver_ok(
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        delivered_calls.append(int(task.access_version or 0))
        return 'completed', None

    # Два параллельных воркера обрабатывают одну задачу без дублей благодаря SKIP LOCKED
    c1, c2 = await asyncio.gather(
        repos.outbox.process_due_auth_outbox_tasks(deliver_ok, payment_id=pay_3.id),
        repos.outbox.process_due_auth_outbox_tasks(deliver_ok, payment_id=pay_3.id),
    )
    assert c1 + c2 == 1
    assert delivered_calls == [1]

    local_after = await repos.user_views.get_local_payment_result_for_user(
        pay_3.id, user_3
    )
    assert local_after is not None and local_after.auth_access_delivered is True

    # 4. T-17: проверяем list_due_pending_payments с отступом min_age_seconds
    now_t17 = datetime.now(UTC)
    young_pay_id = uuid4()
    mature_pay_id = uuid4()
    throttled_pay_id = uuid4()
    async with session_factory() as session:
        session.add_all(
            [
                Payment(
                    id=young_pay_id,
                    user_id=uuid4(),
                    tariff_id=DEFAULT_TARIFF_ID,
                    provider_code=DEFAULT_PROVIDER_CODE,
                    amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                    status='pending',
                    idempotency_key=young_pay_id,
                    created_at=now_t17 - timedelta(seconds=1),
                ),
                Payment(
                    id=mature_pay_id,
                    user_id=uuid4(),
                    tariff_id=DEFAULT_TARIFF_ID,
                    provider_code=DEFAULT_PROVIDER_CODE,
                    amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                    status='pending',
                    idempotency_key=mature_pay_id,
                    created_at=now_t17 - timedelta(seconds=15),
                ),
                Payment(
                    id=throttled_pay_id,
                    user_id=uuid4(),
                    tariff_id=DEFAULT_TARIFF_ID,
                    provider_code=DEFAULT_PROVIDER_CODE,
                    amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                    status='pending',
                    idempotency_key=throttled_pay_id,
                    created_at=now_t17 - timedelta(seconds=20),
                    last_checked_at=now_t17 - timedelta(seconds=1),
                ),
            ]
        )
        await session.commit()

    due_rows = await repos.payments.list_due_pending_payments(
        min_age_seconds=5.0,
        throttle_seconds=5.0,
        limit=10,
    )
    due_ids = {r.id for r in due_rows}
    assert mature_pay_id in due_ids
    assert young_pay_id not in due_ids
    assert throttled_pay_id not in due_ids

    # Два параллельных воркера через claim_next_due_pending_payment (SKIP LOCKED)
    # захватывают mature_pay_id ровно один раз без дублей (BIL-NFR-S-01)
    claimed_1, claimed_2 = await asyncio.gather(
        repos.payments.claim_next_due_pending_payment(
            min_age_seconds=5.0, throttle_seconds=5.0
        ),
        repos.payments.claim_next_due_pending_payment(
            min_age_seconds=5.0, throttle_seconds=5.0
        ),
    )
    claimed_ids = [c.id for c in (claimed_1, claimed_2) if c is not None]
    assert claimed_ids == [mature_pay_id]

    # Проверяем record_payment_reconcile_error с экспоненциальным backoff
    err_1 = await repos.payments.record_payment_reconcile_error(
        mature_pay_id,
        error_message='YooKassa timeout #1',
    )
    assert err_1.provider_details is not None
    assert err_1.provider_details['_reconcile_errors'] == 1
    err_2 = await repos.payments.record_payment_reconcile_error(
        mature_pay_id,
        error_message='YooKassa timeout #2',
    )
    assert err_2.provider_details is not None
    assert err_2.provider_details['_reconcile_errors'] == 2
    assert err_2.next_reconcile_at is not None
    assert err_2.next_reconcile_at > datetime.now(UTC)


async def test_t19_outbox_worker_skip_locked_backoff_intervention_and_stale_versions(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-19 (BIL-FR-11, BIL-FR-14, BIL-NFR-R-04, BIL-NFR-S-01): воркер outbox в БД."""
    from repositories.bundle import BillingRepositories
    from repositories.outbox import DeliveryOutcome
    from repositories.records import OutboxTaskRecordProtocol

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()
    pay_1, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.finalize_payment_succeeded(pay_1.id, source='webhook')

    # 1. Проверяем SKIP LOCKED сразу по двум target_service ('auth', 'notifications')
    delivered_keys: list[str] = []

    async def deliver_all_ok(
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        await asyncio.sleep(0.01)
        delivered_keys.append(f'{task.target_service}:{task.task_type}')
        return 'completed', None

    w1, w2 = await asyncio.gather(
        repos.outbox.process_due_outbox_tasks(
            deliver_all_ok,
            target_services=('auth', 'notifications'),
            payment_id=pay_1.id,
        ),
        repos.outbox.process_due_outbox_tasks(
            deliver_all_ok,
            target_services=('auth', 'notifications'),
            payment_id=pay_1.id,
        ),
    )
    # 1 задача auth + 2 задачи notifications = 3 задачи, каждая обработана ровно 1 раз
    assert w1 + w2 == 3
    assert sorted(delivered_keys) == [
        'auth:sync_access',
        'notifications:payment_succeeded_email',
        'notifications:subscription_expiration_email',
    ]

    # 2. Проверяем пропуск устаревшей версии доступа (US-S-02 шаг 3)
    stale_task_id = uuid4()
    newer_task_id = uuid4()
    async with session_factory() as session:
        session.add_all(
            [
                OutboxTask(
                    id=newer_task_id,
                    dedup_key=f't19_newer:{pay_1.id}',
                    target_service='auth',
                    task_type='sync_access',
                    payment_id=pay_1.id,
                    user_id=user_id,
                    access_version=5,
                    payload={'version': 5, 'expires_at': None},
                    status='completed',
                ),
                OutboxTask(
                    id=stale_task_id,
                    dedup_key=f't19_stale:{pay_1.id}',
                    target_service='auth',
                    task_type='sync_access',
                    payment_id=pay_1.id,
                    user_id=user_id,
                    access_version=2,
                    payload={'version': 2, 'expires_at': None},
                    status='pending',
                    attempts=1,
                    last_error='Previous 503',
                    next_retry_at=datetime.now(UTC) - timedelta(seconds=1),
                ),
            ]
        )
        await session.commit()

    called_for_stale = 0

    async def should_not_be_called(
        _task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        nonlocal called_for_stale
        called_for_stale += 1
        return 'completed', None

    processed_stale = await repos.outbox.process_due_outbox_tasks(
        should_not_be_called,
        task_id=stale_task_id,
    )
    assert processed_stale == 1
    assert called_for_stale == 0
    async with session_factory() as session:
        stale_row = await session.get(OutboxTask, stale_task_id)
        assert stale_row is not None
        assert stale_row.status == 'completed'
        assert stale_row.last_error is None

    # 3. Проверяем backoff и переход в requires_intervention после лимита
    flaky_task_id = uuid4()
    async with session_factory() as session:
        session.add(
            OutboxTask(
                id=flaky_task_id,
                dedup_key=f't19_flaky:{pay_1.id}',
                target_service='notifications',
                task_type='payment_succeeded_email',
                payment_id=pay_1.id,
                user_id=user_id,
                access_version=None,
                payload={'subject': 'Test'},
                status='pending',
                attempts=0,
                next_retry_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        await session.commit()

    async def deliver_transient_fail(
        _task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        return 'transient_error', 'Notifications 503 Service Unavailable'

    await repos.outbox.process_due_outbox_tasks(
        deliver_transient_fail,
        task_id=flaky_task_id,
        max_attempts=2,
    )
    async with session_factory() as session:
        row_after_1 = await session.get(OutboxTask, flaky_task_id)
        assert row_after_1 is not None
        assert row_after_1.status == 'pending'
        assert row_after_1.attempts == 1
        assert row_after_1.next_retry_at > datetime.now(UTC)
        # Сдвигаем next_retry_at в прошлое для 2-й (финальной) попытки
        row_after_1.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    await repos.outbox.process_due_outbox_tasks(
        deliver_transient_fail,
        task_id=flaky_task_id,
        max_attempts=2,
    )
    async with session_factory() as session:
        row_after_2 = await session.get(OutboxTask, flaky_task_id)
        assert row_after_2 is not None
        assert row_after_2.status == 'requires_intervention'
        assert row_after_2.attempts == 2
        assert row_after_2.last_error == 'Notifications 503 Service Unavailable'


async def test_t23_get_user_billing_overview_in_real_postgres(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-23 (BIL-FR-09, BIL-API-02): get_user_billing_overview в PostgreSQL."""
    from repositories.bundle import BillingRepositories

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()
    other_user_id = uuid4()

    # 1. Пустой пользователь -> нет подписки, пустая история
    empty_ov = await repos.user_views.get_user_billing_overview(user_id)
    assert empty_ov.pending_payment_id is None
    assert empty_ov.latest_subscription_ends_at is None
    assert empty_ov.payments == []

    # 2. Чужой платёж не попадает в выборку
    other_pay, _ = await repos.payments.get_or_create_pending_payment(other_user_id)
    await repos.payments.finalize_payment_succeeded(other_pay.id, source='webhook')

    # 3. Неуспешный платёж, затем успешный платёж с подпиской и возвратом в обработке
    pay_fail, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.finalize_payment_failed(
        pay_fail.id,
        failure_reason='insufficient_funds',
        source='webhook',
    )
    pay_ok, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.finalize_payment_succeeded(pay_ok.id, source='webhook')

    ov_activating = await repos.user_views.get_user_billing_overview(user_id)
    assert ov_activating.pending_payment_id is None
    assert ov_activating.latest_subscription_ends_at is not None
    assert ov_activating.latest_subscription_auth_delivered is False
    assert len(ov_activating.payments) == 2
    assert ov_activating.payments[0].payment_id == pay_ok.id
    assert ov_activating.payments[1].payment_id == pay_fail.id

    # Доставляем задачу Auth и добавляем возврат processing
    async with session_factory() as session:
        sub_row = await session.scalar(
            select(Subscription).where(Subscription.payment_id == pay_ok.id)
        )
        assert sub_row is not None
        auth_task = await session.scalar(
            select(OutboxTask).where(
                OutboxTask.payment_id == pay_ok.id,
                OutboxTask.target_service == 'auth',
            )
        )
        assert auth_task is not None
        auth_task.status = 'completed'
        refund_id = uuid4()
        session.add(
            Refund(
                id=refund_id,
                payment_id=pay_ok.id,
                subscription_id=sub_row.id,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                status='processing',
                reason='Проверка отметки возврата в истории',
                admin_user_id=uuid4(),
                idempotency_key=refund_id,
            )
        )
        await session.commit()

    ov_active_ref = await repos.user_views.get_user_billing_overview(user_id)
    assert ov_active_ref.latest_subscription_auth_delivered is True
    assert ov_active_ref.latest_subscription_has_processing_refund is True
    assert ov_active_ref.latest_subscription_has_succeeded_refund is False
    assert ov_active_ref.payments[0].refund_status == 'processing'

    # Переводим возврат в succeeded -> отражается как succeeded
    async with session_factory() as session:
        ref_row = await session.get(Refund, refund_id)
        assert ref_row is not None
        ref_row.status = 'succeeded'
        await session.commit()

    ov_refunded = await repos.user_views.get_user_billing_overview(user_id)
    assert ov_refunded.latest_subscription_has_succeeded_refund is True
    assert ov_refunded.latest_subscription_has_processing_refund is False
    assert ov_refunded.payments[0].refund_status == 'succeeded'


async def test_t26_internal_subscriptions_and_payments_lists_in_real_postgres(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-26 (BIL-API-06, BIL-API-07): списки подписок и платежей в PostgreSQL."""
    from repositories.bundle import BillingRepositories
    from repositories.read_models import (
        InternalPaymentFilter,
        InternalSubscriptionFilter,
    )

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    now_utc = datetime.now(UTC)

    user_active = uuid4()
    user_refunded = uuid4()
    user_stuck = uuid4()

    # 1. Активная подписка
    pay_active, _ = await repos.payments.get_or_create_pending_payment(user_active)
    await repos.payments.save_provider_payment_created(
        pay_active.id,
        provider_payment_id='yk_t26_pg_active',
        confirmation_url='https://yookassa.test/confirm/1',
        provider_details={'id': 'yk_t26_pg_active'},
    )
    await repos.payments.finalize_payment_succeeded(pay_active.id, source='webhook')

    # 2. Возвращённая подписка
    pay_ref, _ = await repos.payments.get_or_create_pending_payment(user_refunded)
    await repos.payments.finalize_payment_succeeded(pay_ref.id, source='webhook')
    async with session_factory() as session:
        sub_ref = await session.scalar(
            select(Subscription).where(Subscription.payment_id == pay_ref.id)
        )
        assert sub_ref is not None
        ref_id = uuid4()
        session.add(
            Refund(
                id=ref_id,
                payment_id=pay_ref.id,
                subscription_id=sub_ref.id,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                status='succeeded',
                reason='T-26 PG test refund',
                admin_user_id=uuid4(),
                idempotency_key=ref_id,
            )
        )
        # 3. Зависший pending-платёж (старше 10 минут)
        stuck_pay_id = uuid4()
        session.add(
            Payment(
                id=stuck_pay_id,
                user_id=user_stuck,
                tariff_id=DEFAULT_TARIFF_ID,
                provider_code=DEFAULT_PROVIDER_CODE,
                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
                status='pending',
                idempotency_key=stuck_pay_id,
                provider_payment_id='yk_t26_pg_stuck',
                created_at=now_utc - timedelta(minutes=25),
                updated_at=now_utc - timedelta(minutes=25),
            )
        )
        await session.commit()

    # Проверяем фильтрацию подписок по user_id и вычисленному статусу
    (
        sub_total_act,
        sub_items_act,
    ) = await repos.admin_queries.list_internal_subscriptions(
        InternalSubscriptionFilter(user_id=user_active, status='active'),
        now_utc=now_utc,
    )
    assert sub_total_act == 1
    assert sub_items_act[0].payment_id == pay_active.id
    assert sub_items_act[0].status == 'active'

    (
        sub_total_ref,
        sub_items_ref,
    ) = await repos.admin_queries.list_internal_subscriptions(
        InternalSubscriptionFilter(user_id=user_refunded, status='refunded'),
        now_utc=now_utc,
    )
    assert sub_total_ref == 1
    assert sub_items_ref[0].payment_id == pay_ref.id
    assert sub_items_ref[0].status == 'refunded'

    # Проверяем фильтрацию платежей (по provider_payment_id, has_refund и stuck=True)
    pay_total_prov, pay_items_prov = await repos.admin_queries.list_internal_payments(
        InternalPaymentFilter(provider_payment_id='yk_t26_pg_active'),
        now_utc=now_utc,
    )
    assert pay_total_prov == 1
    assert pay_items_prov[0].id == pay_active.id
    assert pay_items_prov[0].has_refund is False

    pay_total_ref, pay_items_ref = await repos.admin_queries.list_internal_payments(
        InternalPaymentFilter(user_id=user_refunded, has_refund=True),
        now_utc=now_utc,
    )
    assert pay_total_ref == 1
    assert pay_items_ref[0].id == pay_ref.id
    assert pay_items_ref[0].refund_status == 'succeeded'

    pay_total_stuck, pay_items_stuck = await repos.admin_queries.list_internal_payments(
        InternalPaymentFilter(user_id=user_stuck, stuck=True),
        stuck_threshold_seconds=600,
        now_utc=now_utc,
    )
    assert pay_total_stuck == 1
    assert pay_items_stuck[0].id == stuck_pay_id
    assert pay_items_stuck[0].is_stuck is True

    # T-28 (BIL-API-08): полная карточка платежа в PostgreSQL
    card_act = await repos.admin_queries.get_internal_payment_card(
        pay_active.id, now_utc=now_utc
    )
    assert card_act is not None
    assert card_act.id == pay_active.id
    assert card_act.can_refund is True
    assert card_act.subscription is not None
    assert card_act.subscription.status == 'active'
    assert len(card_act.operation_events) >= 2
    assert len(card_act.outbox_tasks) == 3

    card_ref = await repos.admin_queries.get_internal_payment_card(
        pay_ref.id, now_utc=now_utc
    )
    assert card_ref is not None
    assert card_ref.can_refund is False
    assert card_ref.subscription is not None
    assert card_ref.subscription.status == 'refunded'
    assert len(card_ref.refunds) == 1
    assert card_ref.refunds[0].status == 'succeeded'

    assert (
        await repos.admin_queries.get_internal_payment_card(uuid4(), now_utc=now_utc)
        is None
    )


async def test_t29_refund_lifecycle_and_atomic_outbox_in_real_postgres(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-29 (BIL-FR-10, BIL-NFR-R-02..03, инв. 4, 5, 7, 9, 11): возврат в БД."""
    from domain.errors import EntityNotFoundError, RuleConflictError
    from repositories.bundle import BillingRepositories

    session_factory = migrated_billing_db
    repos = BillingRepositories.sqlalchemy(session_factory)
    user_id = uuid4()
    admin_id = uuid4()

    payment, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.save_provider_payment_created(
        payment.id,
        provider_payment_id='yk_t29_pg_pay',
        confirmation_url='https://yookassa.test/confirm',
        provider_details={'id': 'yk_t29_pg_pay'},
    )
    await repos.payments.finalize_payment_succeeded(payment.id, source='webhook')

    # 1. Две параллельные попытки создать возврат дают 1 возврат processing (инв. 4)
    res_1, res_2 = await asyncio.gather(
        repos.refunds.get_or_create_processing_refund(
            payment.id,
            admin_user_id=admin_id,
            reason='Первая попытка возврата',
        ),
        repos.refunds.get_or_create_processing_refund(
            payment.id,
            admin_user_id=admin_id,
            reason='Первая попытка возврата',
        ),
    )
    assert res_1[0].id == res_2[0].id
    assert {res_1[1], res_2[1]} == {True, False}
    ref_1 = res_1[0]

    # 2. Первая попытка отменяется (rejected), после чего разрешена новая (инв. 4)
    await repos.refunds.finalize_refund_rejected(
        ref_1.id,
        rejection_reason='rejected_by_provider',
        source='reconcile',
    )
    ref_2, created_2 = await repos.refunds.get_or_create_processing_refund(
        payment.id,
        admin_user_id=admin_id,
        reason='Вторая попытка возврата',
    )
    assert created_2 is True
    assert ref_2.id != ref_1.id

    # 3. Атомарная фиксация succeeded при гонке двух процессов (инв. 7)
    await repos.refunds.save_provider_refund_created(
        ref_2.id,
        provider_refund_id='yk_t29_pg_ref_2',
        provider_details={'id': 'yk_t29_pg_ref_2'},
    )
    fin_1, fin_2 = await asyncio.gather(
        repos.refunds.finalize_refund_succeeded(ref_2.id, source='admin'),
        repos.refunds.finalize_refund_succeeded(ref_2.id, source='webhook'),
    )
    assert {fin_1[1], fin_2[1]} == {True, False}

    # Проверяем версию доступа (2) и 3 новые задачи outbox для возврата
    async with session_factory() as session:
        ver = await session.get(UserAccessVersion, user_id)
        assert ver is not None and ver.last_version == 2
        ref_tasks = list(
            await session.scalars(
                select(OutboxTask).where(
                    OutboxTask.payment_id == payment.id,
                    OutboxTask.dedup_key.like(f'refund_succeeded:{ref_2.id}:%'),
                )
            )
        )
        assert len(ref_tasks) == 3
        actions = list(
            await session.scalars(
                select(AdminAction).where(AdminAction.admin_user_id == admin_id)
            )
        )
        assert len(actions) == 2

    # 4. Повтор после succeeded возвращает выполненный возврат без новой
    #    попытки (инв. 4, BIL-NFR-R-02), а для несуществующего payment_id
    #    возвращается 404 payment_not_found
    again, created_again = await repos.refunds.get_or_create_processing_refund(
        payment.id,
        admin_user_id=admin_id,
        reason='Третья попытка',
    )
    assert (again.id, again.status, created_again) == (ref_2.id, 'succeeded', False)
    with pytest.raises(EntityNotFoundError) as not_found_exc:
        await repos.refunds.get_or_create_processing_refund(
            uuid4(),
            admin_user_id=admin_id,
            reason='Неизвестный платёж',
        )
    assert not_found_exc.value.error_code == 'payment_not_found'

    # 5. T-31 (BIL-API-11): выборка проблемных задач outbox в PostgreSQL
    from repositories.read_models import InternalOutboxFilter

    now_utc = datetime.now(UTC)
    intervene_task_id = uuid4()
    old_pending_task_id = uuid4()
    fresh_pending_task_id = uuid4()
    async with session_factory() as session:
        session.add_all(
            [
                OutboxTask(
                    id=intervene_task_id,
                    dedup_key=f't31_intervene:{intervene_task_id}',
                    target_service='auth',
                    task_type='sync_access',
                    payment_id=payment.id,
                    user_id=user_id,
                    access_version=3,
                    payload={'version': 3},
                    status='requires_intervention',
                    attempts=20,
                    last_error='Auth HTTP 404',
                    created_at=now_utc - timedelta(seconds=10),
                    updated_at=now_utc - timedelta(seconds=10),
                ),
                OutboxTask(
                    id=old_pending_task_id,
                    dedup_key=f't31_old:{old_pending_task_id}',
                    target_service='notifications',
                    task_type='payment_succeeded_email',
                    payment_id=payment.id,
                    user_id=user_id,
                    access_version=None,
                    payload={'subject': 'Test'},
                    status='pending',
                    attempts=4,
                    last_error='Notifications HTTP 503',
                    created_at=now_utc - timedelta(minutes=15),
                    updated_at=now_utc - timedelta(minutes=1),
                ),
                OutboxTask(
                    id=fresh_pending_task_id,
                    dedup_key=f't31_fresh:{fresh_pending_task_id}',
                    target_service='notifications',
                    task_type='payment_succeeded_email',
                    payment_id=payment.id,
                    user_id=user_id,
                    access_version=None,
                    payload={'subject': 'Fresh'},
                    status='pending',
                    attempts=0,
                    last_error=None,
                    created_at=now_utc - timedelta(minutes=1),
                    updated_at=now_utc - timedelta(minutes=1),
                ),
            ]
        )
        await session.commit()

    ob_total, ob_items = await repos.outbox_admin.list_internal_outbox_tasks(
        InternalOutboxFilter(user_id=user_id),
        stuck_threshold_seconds=600,
        now_utc=now_utc,
    )
    assert ob_total == 2
    assert [t.id for t in ob_items] == [intervene_task_id, old_pending_task_id]

    # 6. T-32 (BIL-API-12, BIL-API-13): retry и close задач outbox в PostgreSQL
    retried_item = await repos.outbox_admin.retry_outbox_task_by_admin(
        intervene_task_id,
        admin_user_id=admin_id,
        reason='Повторная отправка',
        now_utc=now_utc,
    )
    assert retried_item.status == 'pending'
    assert retried_item.attempts == 0

    # Возвращаем в requires_intervention и закрываем с причиной
    async with session_factory() as session:
        t_row = await session.get(OutboxTask, intervene_task_id)
        assert t_row is not None
        t_row.status = 'requires_intervention'
        await session.commit()

    closed_item = await repos.outbox_admin.close_outbox_task_by_admin(
        intervene_task_id,
        admin_user_id=admin_id,
        reason='Закрыто в PG тесте T-32',
        now_utc=now_utc,
    )
    assert closed_item.status == 'closed'

    with pytest.raises(RuleConflictError) as close_409:
        await repos.outbox_admin.close_outbox_task_by_admin(
            old_pending_task_id,
            admin_user_id=admin_id,
            reason='Pending не закрывается напрямую',
            now_utc=now_utc,
        )

    assert close_409.value.error_code == 'outbox_task_not_requires_intervention'
    # 7. T-33 (BIL-API-14): журнал действий администраторов в PostgreSQL
    from repositories.read_models import InternalAdminActionFilter

    act_total, act_items = await repos.admin_queries.list_internal_admin_actions(
        InternalAdminActionFilter(admin_user_id=admin_id)
    )
    assert act_total == 4
    assert {a.action_type for a in act_items} == {
        'refund_requested',
        'outbox_retried',
        'outbox_closed',
    }
    assert all(a.payment_id == payment.id for a in act_items)

    # 8. T-35 (раздел 6.4): операционные метрики в PostgreSQL
    stuck_pay_pg = uuid4()
    intervene_pg = uuid4()
    async with session_factory() as session:
        session.add_all(
            [
                Payment(
                    id=stuck_pay_pg,
                    user_id=uuid4(),
                    tariff_id=payment.tariff_id,
                    provider_code='yookassa',
                    amount_kopeks=29900,
                    currency='RUB',
                    status='pending',
                    idempotency_key=stuck_pay_pg,
                    created_at=now_utc - timedelta(minutes=25),
                    updated_at=now_utc - timedelta(minutes=25),
                ),
                OutboxTask(
                    id=intervene_pg,
                    dedup_key=f't35_intervene:{intervene_pg}',
                    target_service='auth',
                    task_type='sync_access',
                    payment_id=payment.id,
                    user_id=user_id,
                    access_version=4,
                    payload={'version': 4},
                    status='requires_intervention',
                    attempts=20,
                    last_error='Auth HTTP 422',
                    created_at=now_utc - timedelta(minutes=2),
                    updated_at=now_utc - timedelta(minutes=2),
                ),
            ]
        )
        await session.commit()

    metrics_snap = await repos.metrics.get_operational_metrics(
        stuck_threshold_seconds=600,
        now_utc=now_utc,
    )
    assert metrics_snap.stuck_payments_count >= 1
    assert metrics_snap.stuck_refunds_count == 0
    assert metrics_snap.stuck_operations_count == (
        metrics_snap.stuck_payments_count + metrics_snap.stuck_refunds_count
    )
    assert metrics_snap.outbox_stuck_pending_count >= 1
    assert metrics_snap.outbox_requires_intervention_count >= 1
    assert metrics_snap.undelivered_outbox_count == (
        metrics_snap.outbox_requires_intervention_count
        + metrics_snap.outbox_stuck_pending_count
    )


async def test_payment_result_and_overview_survive_refund_access_task(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """После возврата у платежа две задачи sync_access; экраны не падают в 503.

    Регрессия: JOIN по payment_id находил обе задачи, `.one_or_none()` бросал
    MultipleResultsFound, и страница результата оплаты отвечала 503.
    """
    from repositories.bundle import BillingRepositories

    repos = BillingRepositories.sqlalchemy(migrated_billing_db)
    user_id = uuid4()
    payment, _ = await repos.payments.get_or_create_pending_payment(user_id)
    await repos.payments.save_provider_payment_created(
        payment.id,
        provider_payment_id='yk_refund_view_pay',
        confirmation_url='https://yookassa.test/confirm',
        provider_details={'id': 'yk_refund_view_pay'},
    )
    await repos.payments.finalize_payment_succeeded(payment.id, source='webhook')
    async with migrated_billing_db() as session:
        task = await session.scalar(
            select(OutboxTask).where(
                OutboxTask.payment_id == payment.id,
                OutboxTask.task_type == 'sync_access',
            )
        )
        assert task is not None
        task.status = 'completed'
        await session.commit()

    refund, _ = await repos.refunds.get_or_create_processing_refund(
        payment.id,
        admin_user_id=uuid4(),
        reason='Проверка экранов после возврата',
    )
    await repos.refunds.finalize_refund_succeeded(refund.id, source='admin')

    result = await repos.user_views.get_local_payment_result_for_user(
        payment.id, user_id
    )
    overview = await repos.user_views.get_user_billing_overview(user_id)

    assert result is not None
    # Флаг описывает задачу оплаты, а не pending-задачу закрытия доступа.
    assert result.auth_access_delivered is True
    assert overview.latest_subscription_auth_delivered is True


async def test_sql_outbox_keeps_retrying_access_sync_after_attempt_limit(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """SQL-outbox: `sync_access` после лимита попыток остаётся в повторе (ТЗ 6.3)."""
    from repositories.bundle import BillingRepositories
    from repositories.outbox import DeliveryOutcome
    from repositories.records import OutboxTaskRecordProtocol

    repos = BillingRepositories.sqlalchemy(migrated_billing_db)
    payment, _ = await repos.payments.get_or_create_pending_payment(uuid4())
    task_id = uuid4()
    async with migrated_billing_db() as session:
        session.add(
            OutboxTask(
                id=task_id,
                dedup_key=f'retry_forever:{task_id}',
                target_service='auth',
                task_type='sync_access',
                payment_id=payment.id,
                user_id=payment.user_id,
                access_version=1,
                payload={'version': 1, 'expires_at': None},
                status='pending',
                attempts=1,
                next_retry_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        await session.commit()

    async def auth_down(
        _task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        return 'transient_error', 'Auth 503'

    await repos.outbox.process_due_outbox_tasks(
        auth_down, task_id=task_id, max_attempts=2
    )

    async with migrated_billing_db() as session:
        row = await session.get(OutboxTask, task_id)
        assert row is not None
        assert row.status == 'pending'
        assert row.attempts == 2
        assert row.next_retry_at > datetime.now(UTC)

    # «Повторить» для pending-задачи запрещён: её уже повторяет воркер.
    from domain.errors import RuleConflictError

    with pytest.raises(RuleConflictError) as exc_info:
        await repos.outbox_admin.retry_outbox_task_by_admin(
            task_id, admin_user_id=uuid4(), reason='Повтор'
        )
    assert exc_info.value.error_code == 'outbox_task_not_retryable'

    # После лимита бессрочную задачу администратор может закрыть (6.3).
    from core.config import settings

    async with migrated_billing_db() as session:
        row = await session.get(OutboxTask, task_id)
        assert row is not None
        row.attempts = settings.worker.billing_outbox_max_attempts
        await session.commit()
    closed = await repos.outbox_admin.close_outbox_task_by_admin(
        task_id, admin_user_id=uuid4(), reason='Получатель удалён'
    )
    assert closed.status == 'closed'
    assert closed.can_retry_or_close is False


async def test_claim_payment_check_is_atomic_and_backoff_has_own_column(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """BIL-NFR-R-06 в PostgreSQL: из 10 параллельных проверок проходит одна.

    Backoff фоновой сверки пишется в next_reconcile_at, а не в будущее
    время last_checked_at, поэтому не блокирует пользовательскую проверку.
    """
    from repositories.bundle import BillingRepositories

    repos = BillingRepositories.sqlalchemy(migrated_billing_db)
    payment, _ = await repos.payments.get_or_create_pending_payment(uuid4())

    claims = await asyncio.gather(
        *(
            repos.payments.claim_payment_check(payment.id, throttle_seconds=5)
            for _ in range(10)
        )
    )
    assert claims.count(True) == 1

    await repos.payments.record_payment_reconcile_error(
        payment.id, error_message='YooKassa 503'
    )
    async with migrated_billing_db() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.next_reconcile_at is not None
        assert row.next_reconcile_at > datetime.now(UTC)
        assert row.last_checked_at is not None
        assert row.last_checked_at <= datetime.now(UTC)


async def _paid_payment_with_subscription(
    repos: 'BillingRepositories',
) -> 'PaymentRecordProtocol':
    payment, _ = await repos.payments.get_or_create_pending_payment(uuid4())
    await repos.payments.save_provider_payment_created(
        payment.id,
        provider_payment_id=f'yk_{payment.id.hex[:12]}',
        confirmation_url='https://yookassa.test/confirm',
        provider_details={'id': 'x'},
    )
    paid, _ = await repos.payments.finalize_payment_succeeded(
        payment.id, source='webhook'
    )
    return paid


async def test_refund_attempt_uuid_is_idempotent_in_postgres(
    migrated_billing_db: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BIL-NFR-R-02 в PostgreSQL: повтор формы находит попытку по её UUID."""
    from domain.errors import RuleConflictError
    from repositories.bundle import BillingRepositories

    repos = BillingRepositories.sqlalchemy(migrated_billing_db)
    first = await _paid_payment_with_subscription(repos)
    second = await _paid_payment_with_subscription(repos)
    attempt = uuid4()
    admin = uuid4()

    created, is_new = await repos.refunds.get_or_create_processing_refund(
        first.id, admin_user_id=admin, reason='Жалоба', refund_id=attempt
    )
    assert (created.id, is_new) == (attempt, True)

    # Отказ ЮKassa: повтор той же формы возвращает отказ, а не новую попытку.
    await repos.refunds.finalize_refund_rejected(
        attempt, rejection_reason='rejected_by_provider', source='admin'
    )
    again, again_new = await repos.refunds.get_or_create_processing_refund(
        first.id, admin_user_id=admin, reason='Жалоба', refund_id=attempt
    )
    assert (again.id, again.status, again_new) == (attempt, 'rejected', False)

    # Тот же UUID для другого платежа — конфликт, попытка не «угоняется».
    with pytest.raises(RuleConflictError) as exc_info:
        await repos.refunds.get_or_create_processing_refund(
            second.id, admin_user_id=admin, reason='Жалоба', refund_id=attempt
        )
    assert exc_info.value.error_code == 'refund_id_conflict'

    # Гонка: попытку с тем же UUID для другого платежа вставили параллельно,
    # и ранняя проверка её ещё не видела. Вставка падает по PK; это должен
    # быть refund_id_conflict, а не «возврат уже оформлен».
    shared = uuid4()
    third = await _paid_payment_with_subscription(repos)
    await repos.refunds.get_or_create_processing_refund(
        second.id, admin_user_id=admin, reason='A', refund_id=shared
    )
    original_get = AsyncSession.get
    hidden = {'left': 1}

    async def get_hiding_first_lookup(self, entity, ident, **kwargs):  # type: ignore[no-untyped-def]
        if ident == shared and hidden['left']:
            hidden['left'] -= 1
            return None
        return await original_get(self, entity, ident, **kwargs)

    monkeypatch.setattr(AsyncSession, 'get', get_hiding_first_lookup)
    with pytest.raises(RuleConflictError) as race_exc:
        await repos.refunds.get_or_create_processing_refund(
            third.id, admin_user_id=admin, reason='B', refund_id=shared
        )
    assert race_exc.value.error_code == 'refund_id_conflict'


async def test_next_reconcile_at_migration_moves_backoff_both_ways(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """Миграция 20261109_0002 переносит backoff на непустых данных.

    До неё backoff хранился как будущее время `last_checked_at`. Upgrade
    переносит такое значение в `next_reconcile_at`, а обычную отметку проверки
    не трогает; downgrade возвращает backoff обратно.
    """
    engine = migrated_billing_db.kw['bind']
    database = engine.url.database
    backoff_pid, checked_pid = uuid4(), uuid4()

    await _run_alembic(database, 'downgrade', '20261103_0001')
    async with engine.begin() as conn:
        for pid, checked in (
            (backoff_pid, "now() + interval '4 minutes'"),
            (checked_pid, "now() - interval '1 minute'"),
        ):
            await conn.execute(
                text(
                    'INSERT INTO payments (id, user_id, tariff_id, provider_code, '
                    'amount_kopeks, currency, status, idempotency_key, '
                    f'last_checked_at) VALUES (:id, :uid, '
                    "'00000000-0000-0000-0000-000000000001', 'yookassa', 29900, "
                    f"'RUB', 'pending', :id, {checked})"
                ),
                {'id': pid, 'uid': uuid4()},
            )

    await _run_alembic(database, 'upgrade', 'head')
    async with engine.connect() as conn:
        rows = {
            r.id: r
            for r in await conn.execute(
                text(
                    'SELECT id, last_checked_at <= now() AS checked_not_future, '
                    'next_reconcile_at > now() AS backoff_future, '
                    'next_reconcile_at IS NULL AS no_backoff FROM payments'
                )
            )
        }
    assert rows[backoff_pid].checked_not_future and rows[backoff_pid].backoff_future
    assert rows[checked_pid].checked_not_future and rows[checked_pid].no_backoff

    await _run_alembic(database, 'downgrade', '20261103_0001')
    async with engine.connect() as conn:
        restored = await conn.scalar(
            text('SELECT last_checked_at > now() FROM payments WHERE id = :id'),
            {'id': backoff_pid},
        )
    assert restored is True
    await _run_alembic(database, 'upgrade', 'head')
