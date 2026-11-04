"""Create billing core, outbox, and audit tables with initial reference data."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '20261103_0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'payment_providers',
        sa.Column('code', sa.String(length=32), nullable=False),
        sa.Column('title', sa.String(length=128), nullable=False),
        sa.Column(
            'is_active',
            sa.Boolean(),
            server_default='true',
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(code) <> ''",
            name='ck_payment_providers_code',
        ),
        sa.CheckConstraint(
            "btrim(title) <> ''",
            name='ck_payment_providers_title',
        ),
        sa.PrimaryKeyConstraint('code'),
    )

    op.create_table(
        'tariffs',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('code', sa.String(length=64), nullable=False),
        sa.Column('title', sa.String(length=128), nullable=False),
        sa.Column('price_kopeks', sa.Integer(), nullable=False),
        sa.Column(
            'currency',
            sa.CHAR(length=3),
            server_default='RUB',
            nullable=False,
        ),
        sa.Column('duration_days', sa.Integer(), nullable=False),
        sa.Column(
            'is_active',
            sa.Boolean(),
            server_default='true',
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint('price_kopeks > 0', name='ck_tariffs_price_kopeks'),
        sa.CheckConstraint('duration_days > 0', name='ck_tariffs_duration_days'),
        sa.CheckConstraint("currency = 'RUB'", name='ck_tariffs_currency'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('code', name='uq_tariffs_code'),
    )

    op.create_table(
        'payments',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('tariff_id', sa.Uuid(), nullable=False),
        sa.Column('provider_code', sa.String(length=32), nullable=False),
        sa.Column('amount_kopeks', sa.Integer(), nullable=False),
        sa.Column(
            'currency',
            sa.CHAR(length=3),
            server_default='RUB',
            nullable=False,
        ),
        sa.Column(
            'status',
            sa.String(length=16),
            server_default='pending',
            nullable=False,
        ),
        sa.Column('idempotency_key', sa.Uuid(), nullable=False),
        sa.Column('provider_payment_id', sa.String(length=128), nullable=True),
        sa.Column('confirmation_url', sa.Text(), nullable=True),
        sa.Column('failure_reason', sa.Text(), nullable=True),
        sa.Column(
            'provider_details',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column('last_checked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint('amount_kopeks > 0', name='ck_payments_amount_kopeks'),
        sa.CheckConstraint("currency = 'RUB'", name='ck_payments_currency'),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name='ck_payments_status',
        ),
        sa.ForeignKeyConstraint(
            ['provider_code'],
            ['payment_providers.code'],
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['tariff_id'],
            ['tariffs.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('id', 'provider_code', name='uq_payments_id_provider_code'),
        sa.UniqueConstraint('idempotency_key', name='uq_payments_idempotency_key'),
        sa.UniqueConstraint(
            'provider_code',
            'provider_payment_id',
            name='uq_payments_provider_payment_id',
        ),
    )
    op.create_index(
        'uq_payments_user_pending',
        'payments',
        ['user_id'],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        'ix_payments_user_id_created_at',
        'payments',
        ['user_id', 'created_at'],
        unique=False,
    )
    op.create_index(
        'ix_payments_status_created_at',
        'payments',
        ['status', 'created_at'],
        unique=False,
    )
    op.create_index(
        'ix_payments_created_at',
        'payments',
        ['created_at'],
        unique=False,
    )

    op.create_table(
        'subscriptions',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('tariff_id', sa.Uuid(), nullable=False),
        sa.Column('payment_id', sa.Uuid(), nullable=False),
        sa.Column('starts_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('ends_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            'ends_at > starts_at',
            name='ck_subscriptions_period_order',
        ),
        sa.ForeignKeyConstraint(
            ['payment_id'],
            ['payments.id'],
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['tariff_id'],
            ['tariffs.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('payment_id', name='uq_subscriptions_payment_id'),
    )
    op.create_index(
        'ix_subscriptions_user_id_ends_at',
        'subscriptions',
        ['user_id', 'ends_at'],
        unique=False,
    )
    op.create_index(
        'ix_subscriptions_created_at',
        'subscriptions',
        ['created_at'],
        unique=False,
    )

    op.create_table(
        'refunds',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('payment_id', sa.Uuid(), nullable=False),
        sa.Column('subscription_id', sa.Uuid(), nullable=False),
        sa.Column('provider_code', sa.String(length=32), nullable=False),
        sa.Column('amount_kopeks', sa.Integer(), nullable=False),
        sa.Column(
            'currency',
            sa.CHAR(length=3),
            server_default='RUB',
            nullable=False,
        ),
        sa.Column(
            'status',
            sa.String(length=16),
            server_default='processing',
            nullable=False,
        ),
        sa.Column('idempotency_key', sa.Uuid(), nullable=False),
        sa.Column('provider_refund_id', sa.String(length=128), nullable=True),
        sa.Column('rejection_reason', sa.Text(), nullable=True),
        sa.Column(
            'provider_details',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column('admin_user_id', sa.Uuid(), nullable=False),
        sa.Column('reason', sa.Text(), nullable=False),
        sa.Column('last_checked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint('amount_kopeks > 0', name='ck_refunds_amount_kopeks'),
        sa.CheckConstraint("currency = 'RUB'", name='ck_refunds_currency'),
        sa.CheckConstraint(
            "status IN ('processing', 'succeeded', 'rejected')",
            name='ck_refunds_status',
        ),
        sa.CheckConstraint("btrim(reason) <> ''", name='ck_refunds_reason_not_empty'),
        sa.ForeignKeyConstraint(
            ['payment_id'],
            ['payments.id'],
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['payment_id', 'provider_code'],
            ['payments.id', 'payments.provider_code'],
            name='fk_refunds_payment_provider',
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['provider_code'],
            ['payment_providers.code'],
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['subscription_id'],
            ['subscriptions.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idempotency_key', name='uq_refunds_idempotency_key'),
        sa.UniqueConstraint(
            'provider_code',
            'provider_refund_id',
            name='uq_refunds_provider_refund_id',
        ),
    )
    op.create_index(
        'uq_refunds_payment_active_or_succeeded',
        'refunds',
        ['payment_id'],
        unique=True,
        postgresql_where=sa.text("status IN ('processing', 'succeeded')"),
    )
    op.create_index(
        'ix_refunds_payment_id_created_at',
        'refunds',
        ['payment_id', 'created_at'],
        unique=False,
    )
    op.create_index(
        'ix_refunds_subscription_id',
        'refunds',
        ['subscription_id'],
        unique=False,
    )
    op.create_index(
        'ix_refunds_status_created_at',
        'refunds',
        ['status', 'created_at'],
        unique=False,
    )

    op.create_table(
        'user_access_versions',
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column(
            'last_version',
            sa.BigInteger(),
            server_default='1',
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            'last_version >= 1',
            name='ck_user_access_versions_last_version',
        ),
        sa.PrimaryKeyConstraint('user_id'),
    )

    op.create_table(
        'outbox_tasks',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('dedup_key', sa.String(length=128), nullable=False),
        sa.Column('target_service', sa.String(length=16), nullable=False),
        sa.Column('task_type', sa.String(length=32), nullable=False),
        sa.Column('payment_id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('access_version', sa.BigInteger(), nullable=True),
        sa.Column(
            'payload',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            'status',
            sa.String(length=24),
            server_default='pending',
            nullable=False,
        ),
        sa.Column(
            'attempts',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column(
            'next_retry_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint('attempts >= 0', name='ck_outbox_tasks_attempts'),
        sa.CheckConstraint(
            "status IN ('pending', 'completed', 'requires_intervention', 'closed')",
            name='ck_outbox_tasks_status',
        ),
        sa.CheckConstraint(
            "target_service IN ('auth', 'notifications')",
            name='ck_outbox_tasks_target_service',
        ),
        sa.ForeignKeyConstraint(
            ['payment_id'],
            ['payments.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dedup_key', name='uq_outbox_tasks_dedup_key'),
    )
    op.create_index(
        'ix_outbox_tasks_status_next_retry_at',
        'outbox_tasks',
        ['status', 'next_retry_at'],
        unique=False,
    )
    op.create_index(
        'ix_outbox_tasks_user_id',
        'outbox_tasks',
        ['user_id'],
        unique=False,
    )
    op.create_index(
        'ix_outbox_tasks_payment_id',
        'outbox_tasks',
        ['payment_id'],
        unique=False,
    )

    op.create_table(
        'operation_events',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('operation_type', sa.String(length=16), nullable=False),
        sa.Column('operation_id', sa.Uuid(), nullable=False),
        sa.Column('from_status', sa.String(length=16), nullable=True),
        sa.Column('to_status', sa.String(length=16), nullable=False),
        sa.Column('source', sa.String(length=16), nullable=False),
        sa.Column(
            'details',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "operation_type IN ('payment', 'refund')",
            name='ck_operation_events_operation_type',
        ),
        sa.CheckConstraint(
            "source IN ('webhook', 'reconcile', 'continue', 'admin')",
            name='ck_operation_events_source',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_operation_events_operation_created_at',
        'operation_events',
        ['operation_id', 'operation_type', 'created_at'],
        unique=False,
    )

    op.create_table(
        'admin_actions',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('admin_user_id', sa.Uuid(), nullable=False),
        sa.Column('action_type', sa.String(length=24), nullable=False),
        sa.Column('target_type', sa.String(length=16), nullable=False),
        sa.Column('target_id', sa.Uuid(), nullable=False),
        sa.Column('reason', sa.Text(), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action_type IN ('refund_requested', 'outbox_retried', 'outbox_closed')",
            name='ck_admin_actions_action_type',
        ),
        sa.CheckConstraint("btrim(reason) <> ''", name='ck_admin_actions_reason'),
        sa.CheckConstraint(
            "target_type IN ('refund', 'outbox_task')",
            name='ck_admin_actions_target_type',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_admin_actions_admin_user_created_at',
        'admin_actions',
        ['admin_user_id', 'created_at'],
        unique=False,
    )
    op.create_index(
        'ix_admin_actions_target',
        'admin_actions',
        ['target_type', 'target_id'],
        unique=False,
    )

    # Seed reference tables: payment_providers ('yookassa') and tariffs ('premium').
    op.execute(
        sa.text(
            """
            INSERT INTO payment_providers (code, title, is_active)
            VALUES ('yookassa', 'ЮKassa', true)
            ON CONFLICT (code) DO NOTHING
            """
        )
    )
    op.execute(
        sa.text(
            """
            INSERT INTO tariffs (
                id, code, title, price_kopeks, currency, duration_days, is_active
            )
            VALUES (
                '00000000-0000-0000-0000-000000000001',
                'premium',
                'Премиум',
                29900,
                'RUB',
                30,
                true
            )
            ON CONFLICT (code) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_index('ix_admin_actions_target', table_name='admin_actions')
    op.drop_index('ix_admin_actions_admin_user_created_at', table_name='admin_actions')
    op.drop_table('admin_actions')

    op.drop_index(
        'ix_operation_events_operation_created_at',
        table_name='operation_events',
    )
    op.drop_table('operation_events')

    op.drop_index('ix_outbox_tasks_payment_id', table_name='outbox_tasks')
    op.drop_index('ix_outbox_tasks_user_id', table_name='outbox_tasks')
    op.drop_index('ix_outbox_tasks_status_next_retry_at', table_name='outbox_tasks')
    op.drop_table('outbox_tasks')

    op.drop_table('user_access_versions')

    op.drop_index('ix_refunds_status_created_at', table_name='refunds')
    op.drop_index('ix_refunds_subscription_id', table_name='refunds')
    op.drop_index('ix_refunds_payment_id_created_at', table_name='refunds')
    op.drop_index('uq_refunds_payment_active_or_succeeded', table_name='refunds')
    op.drop_table('refunds')

    op.drop_index('ix_subscriptions_created_at', table_name='subscriptions')
    op.drop_index('ix_subscriptions_user_id_ends_at', table_name='subscriptions')
    op.drop_table('subscriptions')

    op.drop_index('ix_payments_created_at', table_name='payments')
    op.drop_index('ix_payments_status_created_at', table_name='payments')
    op.drop_index('ix_payments_user_id_created_at', table_name='payments')
    op.drop_index('uq_payments_user_pending', table_name='payments')
    op.drop_table('payments')

    op.drop_table('tariffs')
    op.drop_table('payment_providers')
