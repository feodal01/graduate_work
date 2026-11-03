from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Index, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.functions import now

from models.base import Base


class NotificationOutbox(Base):
    __tablename__ = 'notification_outbox'
    __table_args__ = (
        CheckConstraint(
            'user_version >= 1', name='ck_notification_outbox_user_version'
        ),
        CheckConstraint(
            "event_type IN ('user.registered', 'user.profile_updated', 'user.deleted')",
            name='ck_notification_outbox_event_type',
        ),
        Index(
            'uq_notification_outbox_load_user',
            'load_id',
            'user_id',
            unique=True,
            postgresql_where=text('load_id IS NOT NULL'),
        ),
        Index(
            'ix_notification_outbox_unpublished',
            'created_at',
            'event_id',
            postgresql_where=text('published_at IS NULL'),
        ),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    user_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, str | int | None]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    load_id: Mapped[str | None] = mapped_column(String(255), nullable=True)


class NotificationLoadMarker(Base):
    __tablename__ = 'notification_load_markers'

    load_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=now(), nullable=False
    )
