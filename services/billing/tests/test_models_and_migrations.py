import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint

from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    DEFAULT_TARIFF_PRICE_KOPEKS,
    AdminAction,
    Base,
    OperationEvent,
    OutboxTask,
    Payment,
    PaymentProvider,
    Refund,
    Subscription,
    Tariff,
    UserAccessVersion,
)

SERVICE_ROOT = Path(__file__).resolve().parents[1]


def test_all_billing_tables_are_registered_in_metadata() -> None:
    """Все 9 таблиц из раздела 5.2 ТЗ зарегистрированы в Base.metadata."""
    expected_tables = {
        'payment_providers',
        'tariffs',
        'payments',
        'subscriptions',
        'refunds',
        'user_access_versions',
        'outbox_tasks',
        'operation_events',
        'admin_actions',
    }
    assert set(Base.metadata.tables) == expected_tables


def test_foreign_keys_use_restrict_on_delete() -> None:
    """Все внешние ключи финансовых таблиц имеют ON DELETE RESTRICT (BIL-NFR-R-07)."""
    for table_name in ('payments', 'subscriptions', 'refunds', 'outbox_tasks'):
        table = Base.metadata.tables[table_name]
        fk_constraints = [
            constraint
            for constraint in table.constraints
            if isinstance(constraint, ForeignKeyConstraint)
        ]
        assert fk_constraints, f'{table_name} must define foreign keys'
        for fk in fk_constraints:
            assert fk.ondelete == 'RESTRICT', (
                f'Foreign key {fk.name} on {table_name} must use ON DELETE RESTRICT'
            )


def test_partial_unique_indexes_for_pending_payment_and_active_refund() -> None:
    """Частичные уникальные индексы защищают инварианты 1 и 4."""
    payments_indexes = {
        str(idx.name): idx for idx in Base.metadata.tables['payments'].indexes
    }
    assert 'uq_payments_user_pending' in payments_indexes
    pending_idx = payments_indexes['uq_payments_user_pending']
    assert pending_idx.unique is True
    assert [col.name for col in pending_idx.columns] == ['user_id']
    assert "status = 'pending'" in str(
        pending_idx.dialect_options['postgresql']['where']
    )

    refunds_indexes = {
        str(idx.name): idx for idx in Base.metadata.tables['refunds'].indexes
    }
    assert 'uq_refunds_payment_active_or_succeeded' in refunds_indexes
    refund_idx = refunds_indexes['uq_refunds_payment_active_or_succeeded']
    assert refund_idx.unique is True
    assert [col.name for col in refund_idx.columns] == ['payment_id']
    assert "status IN ('processing', 'succeeded')" in str(
        refund_idx.dialect_options['postgresql']['where']
    )


def test_provider_composite_foreign_key_between_refunds_and_payments() -> None:
    """Возврат привязан к провайдеру платежа через составной FK."""
    payments_uniques = {
        tuple(col.name for col in constraint.columns)
        for constraint in Base.metadata.tables['payments'].constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ('id', 'provider_code') in payments_uniques
    assert ('provider_code', 'provider_payment_id') in payments_uniques
    assert ('idempotency_key',) in payments_uniques

    refunds_fks = [
        constraint
        for constraint in Base.metadata.tables['refunds'].constraints
        if isinstance(constraint, ForeignKeyConstraint)
    ]
    composite_fk = next(
        (fk for fk in refunds_fks if fk.name == 'fk_refunds_payment_provider'),
        None,
    )
    assert composite_fk is not None
    assert [col.name for col in composite_fk.columns] == ['payment_id', 'provider_code']


def test_operation_events_source_and_check_constraints() -> None:
    """В operation_events.source разрешены webhook, reconcile, continue, admin."""
    op_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in Base.metadata.tables['operation_events'].constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert 'ck_operation_events_source' in op_checks
    source_sql = op_checks['ck_operation_events_source']
    for source in ('webhook', 'reconcile', 'continue', 'admin'):
        assert f"'{source}'" in source_sql
    assert 'user_return' not in source_sql

    op_indexes = {
        str(idx.name): [col.name for col in idx.columns]
        for idx in Base.metadata.tables['operation_events'].indexes
    }
    assert op_indexes['ix_operation_events_operation_created_at'] == [
        'operation_id',
        'operation_type',
        'created_at',
    ]


def test_orm_models_can_be_instantiated_with_defaults() -> None:
    """ORM-модели корректно инстанциируются с целочисленными суммами в копейках."""
    provider = PaymentProvider(code=DEFAULT_PROVIDER_CODE, title='ЮKassa')
    tariff = Tariff(
        code=DEFAULT_TARIFF_CODE,
        title='Премиум',
        price_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,
        duration_days=30,
    )
    user_id = uuid4()
    payment = Payment(
        user_id=user_id,
        tariff_id=uuid4(),
        provider_code=provider.code,
        amount_kopeks=tariff.price_kopeks,
        idempotency_key=uuid4(),
    )
    subscription = Subscription(
        user_id=user_id,
        tariff_id=payment.tariff_id,
        payment_id=uuid4(),
    )
    refund = Refund(
        payment_id=uuid4(),
        subscription_id=uuid4(),
        provider_code=provider.code,
        amount_kopeks=payment.amount_kopeks,
        idempotency_key=uuid4(),
        admin_user_id=uuid4(),
        reason='Возврат по обращению пользователя',
    )
    version = UserAccessVersion(user_id=user_id, last_version=1)
    task = OutboxTask(
        dedup_key='auth:sub:1',
        target_service='auth',
        task_type='sync_subscription_access',
        payment_id=uuid4(),
        user_id=user_id,
        access_version=1,
        payload={'has_active_subscription': True},
    )
    event = OperationEvent(
        operation_type='payment',
        operation_id=uuid4(),
        from_status='pending',
        to_status='succeeded',
        source='webhook',
    )
    action = AdminAction(
        admin_user_id=uuid4(),
        action_type='refund_requested',
        target_type='refund',
        target_id=uuid4(),
        reason='Запрос клиента',
    )

    assert payment.amount_kopeks == 29_900
    assert subscription.user_id == user_id
    assert refund.reason == 'Возврат по обращению пользователя'
    assert version.last_version == 1
    assert task.target_service == 'auth'
    assert event.source == 'webhook'
    assert action.action_type == 'refund_requested'


def test_alembic_offline_sql_generates_tables_indexes_and_seeds() -> None:
    """Миграция 20261103_0001 генерирует DDL и начальные записи справочников."""
    result = subprocess.run(
        [sys.executable, '-m', 'alembic', 'upgrade', 'head', '--sql'],
        cwd=SERVICE_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    sql_output = result.stdout

    expected_snippets = (
        'CREATE TABLE payment_providers',
        'CREATE TABLE tariffs',
        'CREATE TABLE payments',
        'CREATE TABLE subscriptions',
        'CREATE TABLE refunds',
        'CREATE TABLE user_access_versions',
        'CREATE TABLE outbox_tasks',
        'CREATE TABLE operation_events',
        'CREATE TABLE admin_actions',
        'CREATE UNIQUE INDEX uq_payments_user_pending ON payments (user_id)',
        'CREATE UNIQUE INDEX uq_refunds_payment_active_or_succeeded',
        'INSERT INTO payment_providers (code, title, is_active)',
        'INSERT INTO tariffs',
        "'yookassa'",
        "'premium'",
        '29900',
    )
    for expected_snippet in expected_snippets:
        assert expected_snippet in sql_output
