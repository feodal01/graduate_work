from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.functions import now

from models.base import Base

DEFAULT_TARIFF_ID = UUID('00000000-0000-0000-0000-000000000001')
DEFAULT_TARIFF_CODE = 'premium'
DEFAULT_TARIFF_TITLE = 'Премиум'
DEFAULT_TARIFF_PRICE_KOPEKS = 29_900
DEFAULT_TARIFF_CURRENCY = 'RUB'
DEFAULT_TARIFF_DURATION_DAYS = 30
DEFAULT_PROVIDER_CODE = 'yookassa'
DEFAULT_PROVIDER_TITLE = 'ЮKassa'


def _utc_now() -> datetime:
    return datetime.now(UTC)


class PaymentProvider(Base):
    __tablename__ = 'payment_providers'
    __table_args__ = (
        CheckConstraint("btrim(code) <> ''", name='ck_payment_providers_code'),
        CheckConstraint("btrim(title) <> ''", name='ck_payment_providers_title'),
    )

    code: Mapped[str] = mapped_column(String(32), primary_key=True, nullable=False)
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default='true',
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )


class Tariff(Base):
    __tablename__ = 'tariffs'
    __table_args__ = (
        UniqueConstraint('code', name='uq_tariffs_code'),
        CheckConstraint('price_kopeks > 0', name='ck_tariffs_price_kopeks'),
        CheckConstraint('duration_days > 0', name='ck_tariffs_duration_days'),
        CheckConstraint("currency = 'RUB'", name='ck_tariffs_currency'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    price_kopeks: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        default='RUB',
        server_default='RUB',
        nullable=False,
    )
    duration_days: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default='true',
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )


class Payment(Base):
    __tablename__ = 'payments'
    __table_args__ = (
        UniqueConstraint('idempotency_key', name='uq_payments_idempotency_key'),
        UniqueConstraint(
            'provider_code',
            'provider_payment_id',
            name='uq_payments_provider_payment_id',
        ),
        UniqueConstraint('id', 'provider_code', name='uq_payments_id_provider_code'),
        CheckConstraint('amount_kopeks > 0', name='ck_payments_amount_kopeks'),
        CheckConstraint("currency = 'RUB'", name='ck_payments_currency'),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name='ck_payments_status',
        ),
        Index(
            'uq_payments_user_pending',
            'user_id',
            unique=True,
            postgresql_where=text("status = 'pending'"),
        ),
        Index('ix_payments_user_id_created_at', 'user_id', 'created_at'),
        Index('ix_payments_status_created_at', 'status', 'created_at'),
        Index('ix_payments_created_at', 'created_at'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    tariff_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('tariffs.id', ondelete='RESTRICT'),
        nullable=False,
    )
    provider_code: Mapped[str] = mapped_column(
        String(32),
        ForeignKey('payment_providers.code', ondelete='RESTRICT'),
        nullable=False,
    )
    amount_kopeks: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        default='RUB',
        server_default='RUB',
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default='pending',
        server_default='pending',
        nullable=False,
    )
    idempotency_key: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    provider_payment_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    confirmation_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_details: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        server_default=now(),
        nullable=False,
    )

    tariff: Mapped[Tariff] = relationship(lazy='selectin')
    subscription: Mapped['Subscription | None'] = relationship(
        back_populates='payment',
        uselist=False,
        lazy='selectin',
    )
    refunds: Mapped[list['Refund']] = relationship(
        back_populates='payment',
        foreign_keys='[Refund.payment_id]',
        primaryjoin='Payment.id == Refund.payment_id',
        lazy='selectin',
    )


class Subscription(Base):
    __tablename__ = 'subscriptions'
    __table_args__ = (
        UniqueConstraint('payment_id', name='uq_subscriptions_payment_id'),
        CheckConstraint(
            'ends_at > starts_at',
            name='ck_subscriptions_period_order',
        ),
        Index('ix_subscriptions_user_id_ends_at', 'user_id', 'ends_at'),
        Index('ix_subscriptions_created_at', 'created_at'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    tariff_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('tariffs.id', ondelete='RESTRICT'),
        nullable=False,
    )
    payment_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('payments.id', ondelete='RESTRICT'),
        nullable=False,
    )
    starts_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    ends_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )

    tariff: Mapped[Tariff] = relationship(lazy='selectin')
    payment: Mapped[Payment] = relationship(
        back_populates='subscription',
        lazy='selectin',
    )
    refunds: Mapped[list['Refund']] = relationship(
        back_populates='subscription',
        lazy='selectin',
    )


class Refund(Base):
    __tablename__ = 'refunds'
    __table_args__ = (
        UniqueConstraint('idempotency_key', name='uq_refunds_idempotency_key'),
        UniqueConstraint(
            'provider_code',
            'provider_refund_id',
            name='uq_refunds_provider_refund_id',
        ),
        ForeignKeyConstraint(
            ['payment_id', 'provider_code'],
            ['payments.id', 'payments.provider_code'],
            name='fk_refunds_payment_provider',
            ondelete='RESTRICT',
        ),
        CheckConstraint('amount_kopeks > 0', name='ck_refunds_amount_kopeks'),
        CheckConstraint("currency = 'RUB'", name='ck_refunds_currency'),
        CheckConstraint(
            "status IN ('processing', 'succeeded', 'rejected')",
            name='ck_refunds_status',
        ),
        CheckConstraint("btrim(reason) <> ''", name='ck_refunds_reason_not_empty'),
        Index(
            'uq_refunds_payment_active_or_succeeded',
            'payment_id',
            unique=True,
            postgresql_where=text("status IN ('processing', 'succeeded')"),
        ),
        Index('ix_refunds_payment_id_created_at', 'payment_id', 'created_at'),
        Index('ix_refunds_subscription_id', 'subscription_id'),
        Index('ix_refunds_status_created_at', 'status', 'created_at'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    payment_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('payments.id', ondelete='RESTRICT'),
        nullable=False,
    )
    subscription_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('subscriptions.id', ondelete='RESTRICT'),
        nullable=False,
    )
    provider_code: Mapped[str] = mapped_column(
        String(32),
        ForeignKey('payment_providers.code', ondelete='RESTRICT'),
        nullable=False,
    )
    amount_kopeks: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        default='RUB',
        server_default='RUB',
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default='processing',
        server_default='processing',
        nullable=False,
    )
    idempotency_key: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    provider_refund_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_details: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    admin_user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        server_default=now(),
        nullable=False,
    )

    payment: Mapped[Payment] = relationship(
        back_populates='refunds',
        foreign_keys=[payment_id],
        primaryjoin='Refund.payment_id == Payment.id',
        lazy='selectin',
    )
    subscription: Mapped[Subscription] = relationship(
        back_populates='refunds',
        lazy='selectin',
    )


class UserAccessVersion(Base):
    __tablename__ = 'user_access_versions'
    __table_args__ = (
        CheckConstraint(
            'last_version >= 1',
            name='ck_user_access_versions_last_version',
        ),
    )

    user_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, nullable=False)
    last_version: Mapped[int] = mapped_column(
        BigInteger,
        default=1,
        server_default='1',
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        server_default=now(),
        nullable=False,
    )


class OutboxTask(Base):
    __tablename__ = 'outbox_tasks'
    __table_args__ = (
        UniqueConstraint('dedup_key', name='uq_outbox_tasks_dedup_key'),
        CheckConstraint(
            "target_service IN ('auth', 'notifications')",
            name='ck_outbox_tasks_target_service',
        ),
        CheckConstraint(
            "status IN ('pending', 'completed', 'requires_intervention', 'closed')",
            name='ck_outbox_tasks_status',
        ),
        CheckConstraint('attempts >= 0', name='ck_outbox_tasks_attempts'),
        Index('ix_outbox_tasks_status_next_retry_at', 'status', 'next_retry_at'),
        Index('ix_outbox_tasks_user_id', 'user_id'),
        Index('ix_outbox_tasks_payment_id', 'payment_id'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    dedup_key: Mapped[str] = mapped_column(String(128), nullable=False)
    target_service: Mapped[str] = mapped_column(String(16), nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    payment_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('payments.id', ondelete='RESTRICT'),
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    access_version: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24),
        default='pending',
        server_default='pending',
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default='0',
        nullable=False,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        server_default=now(),
        nullable=False,
    )


class OperationEvent(Base):
    __tablename__ = 'operation_events'
    __table_args__ = (
        CheckConstraint(
            "operation_type IN ('payment', 'refund')",
            name='ck_operation_events_operation_type',
        ),
        CheckConstraint(
            "source IN ('webhook', 'reconcile', 'continue', 'admin')",
            name='ck_operation_events_source',
        ),
        Index(
            'ix_operation_events_operation_created_at',
            'operation_id',
            'operation_type',
            'created_at',
        ),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    operation_type: Mapped[str] = mapped_column(String(16), nullable=False)
    operation_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    from_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_status: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )


class AdminAction(Base):
    __tablename__ = 'admin_actions'
    __table_args__ = (
        CheckConstraint(
            "action_type IN ('refund_requested', 'outbox_retried', 'outbox_closed')",
            name='ck_admin_actions_action_type',
        ),
        CheckConstraint(
            "target_type IN ('refund', 'outbox_task')",
            name='ck_admin_actions_target_type',
        ),
        CheckConstraint("btrim(reason) <> ''", name='ck_admin_actions_reason'),
        Index('ix_admin_actions_admin_user_created_at', 'admin_user_id', 'created_at'),
        Index('ix_admin_actions_target', 'target_type', 'target_id'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    admin_user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    action_type: Mapped[str] = mapped_column(String(24), nullable=False)
    target_type: Mapped[str] = mapped_column(String(16), nullable=False)
    target_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
