import asyncio
import os
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
    """Миграция заполняет справочники и включает synchronous_commit=on."""
    session_factory = migrated_billing_db
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
    from repositories.purchase import SQLAlchemyPurchaseRepository

    session_factory = migrated_billing_db
    repo = SQLAlchemyPurchaseRepository(session_factory)
    user_id = uuid4()

    # 1. Изначально покупка разрешена
    allowed, code = await repo.check_purchase_allowed(user_id)
    assert allowed is True and code is None

    # 2. Два конкурентных вызова возвращают один и тот же pending-платёж
    res_a, res_b = await asyncio.gather(
        repo.get_or_create_pending_payment(user_id),
        repo.get_or_create_pending_payment(user_id),
    )
    pay_a, is_new_a = res_a
    pay_b, is_new_b = res_b
    assert pay_a.id == pay_b.id
    assert {is_new_a, is_new_b} == {True, False}
    assert pay_a.last_checked_at is None

    # 3. save_provider_payment_created не ставит last_checked_at
    updated = await repo.save_provider_payment_created(
        pay_a.id,
        provider_payment_id='yk-pg-1',
        confirmation_url='https://yoomoney.ru/checkout/1',
        provider_details={'id': 'yk-pg-1'},
    )
    assert updated.provider_payment_id == 'yk-pg-1'
    assert updated.last_checked_at is None

    checked = await repo.record_payment_checked(
        pay_a.id,
        confirmation_url='https://yoomoney.ru/checkout/1',
        provider_details={'id': 'yk-pg-1', 'status': 'pending'},
    )
    assert checked.last_checked_at is not None

    # 4. Два параллельных finalize_payment_failed пишут ровно одно событие
    await asyncio.gather(
        repo.finalize_payment_failed(
            pay_a.id,
            failure_reason='canceled_by_provider',
            source='continue',
        ),
        repo.finalize_payment_failed(
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

    allowed_sub, code_sub = await repo.check_purchase_allowed(user_id)
    assert allowed_sub is False
    assert code_sub == 'subscription_already_active'

    # И внутри транзакции get_or_create_pending_payment под lock выбрасывается 409
    from api.errors import BillingDomainError

    with pytest.raises(BillingDomainError) as exc_info:
        await repo.get_or_create_pending_payment(user_id)
    assert exc_info.value.status_code == 409
    assert exc_info.value.error_code == 'subscription_already_active'


async def test_t11_atomic_reconciliation_succeeded_failed_and_concurrent_race(
    migrated_billing_db: async_sessionmaker[AsyncSession],
) -> None:
    """T-11 (BIL-FR-04, BIL-FR-08, BIL-FR-13, инв. 6, 7, 11): фиксация в БД."""
    from domain.subscription import SubscriptionState, compute_subscription_state
    from repositories.purchase import SQLAlchemyPurchaseRepository

    session_factory = migrated_billing_db
    repo = SQLAlchemyPurchaseRepository(session_factory)
    user_id = uuid4()

    # 1. Создаём pending-платёж и сохраняем provider_payment_id
    payment, is_new = await repo.get_or_create_pending_payment(user_id)
    assert is_new is True
    await repo.save_provider_payment_created(
        payment.id,
        provider_payment_id='yk-race-succeeded-1',
        confirmation_url='https://yoomoney.ru/checkout/race-1',
        provider_details={'id': 'yk-race-succeeded-1', 'status': 'pending'},
    )

    # 2. Запускаем 4 параллельных процесса фиксации оплаты
    results = await asyncio.gather(
        repo.finalize_payment_succeeded(
            payment.id,
            source='webhook',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repo.finalize_payment_succeeded(
            payment.id,
            source='reconcile',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repo.finalize_payment_succeeded(
            payment.id,
            source='continue',
            provider_details={'id': 'yk-race-succeeded-1', 'status': 'succeeded'},
            public_base_url='http://localhost',
        ),
        repo.finalize_payment_succeeded(
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
    after_fail, failed_transitioned = await repo.finalize_payment_failed(
        payment.id,
        failure_reason='late_cancel',
        source='reconcile',
    )
    assert failed_transitioned is False
    assert after_fail.status == 'succeeded'

    # 5. Проверяем атомарную фиксацию неуспешной оплаты (failed + письмо отказа)
    user_2 = uuid4()
    pay_2, _ = await repo.get_or_create_pending_payment(user_2)
    res_fail_1, res_fail_2 = await asyncio.gather(
        repo.finalize_payment_failed(
            pay_2.id,
            failure_reason='insufficient_funds',
            source='webhook',
        ),
        repo.finalize_payment_failed(
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

    # 6. T-12: проверяем get_local_payment_result_for_user
    local_own = await repo.get_local_payment_result_for_user(payment.id, user_id)
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

    local_delivered = await repo.get_local_payment_result_for_user(payment.id, user_id)
    assert local_delivered is not None
    assert local_delivered.auth_access_delivered is True

    # Чужой пользователь получает None (защита BIL-NFR-SEC-03)
    local_foreign = await repo.get_local_payment_result_for_user(payment.id, uuid4())
    assert local_foreign is None

    # 7. T-13: проверяем get_payment_for_user для своего и чужого пользователя
    own_pay = await repo.get_payment_for_user(payment.id, user_id)
    assert own_pay is not None and own_pay.id == payment.id
    foreign_pay = await repo.get_payment_for_user(payment.id, uuid4())
    assert foreign_pay is None

    # 8. T-16: проверяем FOR UPDATE SKIP LOCKED в process_due_auth_outbox_tasks
    user_3 = uuid4()
    pay_3, _ = await repo.get_or_create_pending_payment(user_3)
    await repo.finalize_payment_succeeded(pay_3.id, source='webhook')

    delivered_calls: list[int] = []

    from repositories.purchase import DeliveryOutcome, OutboxTaskRecordProtocol

    async def deliver_ok(
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        delivered_calls.append(int(task.access_version or 0))
        return 'completed', None

    # Два параллельных воркера обрабатывают одну задачу без дублей благодаря SKIP LOCKED
    c1, c2 = await asyncio.gather(
        repo.process_due_auth_outbox_tasks(deliver_ok, payment_id=pay_3.id),
        repo.process_due_auth_outbox_tasks(deliver_ok, payment_id=pay_3.id),
    )
    assert c1 + c2 == 1
    assert delivered_calls == [1]

    local_after = await repo.get_local_payment_result_for_user(pay_3.id, user_3)
    assert local_after is not None and local_after.auth_access_delivered is True

    # 9. T-17: проверяем list_due_pending_payments с отступом min_age_seconds
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

    due_rows = await repo.list_due_pending_payments(
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
        repo.claim_next_due_pending_payment(min_age_seconds=5.0, throttle_seconds=5.0),
        repo.claim_next_due_pending_payment(min_age_seconds=5.0, throttle_seconds=5.0),
    )
    claimed_ids = [c.id for c in (claimed_1, claimed_2) if c is not None]
    assert claimed_ids == [mature_pay_id]

    # Проверяем record_payment_reconcile_error с экспоненциальным backoff
    err_1 = await repo.record_payment_reconcile_error(
        mature_pay_id,
        error_message='YooKassa timeout #1',
    )
    assert err_1.provider_details is not None
    assert err_1.provider_details['_reconcile_errors'] == 1
    err_2 = await repo.record_payment_reconcile_error(
        mature_pay_id,
        error_message='YooKassa timeout #2',
    )
    assert err_2.provider_details is not None
    assert err_2.provider_details['_reconcile_errors'] == 2
    assert err_2.last_checked_at is not None
    assert err_2.last_checked_at > datetime.now(UTC)
