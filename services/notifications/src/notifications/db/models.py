"""Durable projections and workflow state. All timestamps are timezone-aware UTC.

Handlers must lock/version-guard rows inside the caller's transaction. Delivery
leases use a fresh token per claim; completion must compare that token. Releasing
review reservations and recording a terminal delivery is one transaction.
"""

from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Profile(Base):
    __tablename__ = "profiles"
    user_id: Mapped[UUID] = mapped_column(primary_key=True)
    user_version: Mapped[int] = mapped_column(BigInteger)
    tz: Mapped[str | None] = mapped_column(Text)
    likes: Mapped[bool] = mapped_column(Boolean)
    new_films: Mapped[bool] = mapped_column(Boolean)
    marketing: Mapped[bool] = mapped_column(Boolean)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("user_version >= 1", name="version_positive"),
        CheckConstraint(
            "deleted_at IS NULL OR NOT (likes OR new_films OR marketing)", name="deleted_flags_off"
        ),
        Index(
            "ix_profiles_active_likes",
            "user_id",
            postgresql_where=text("deleted_at IS NULL AND likes"),
        ),
        Index(
            "ix_profiles_active_films",
            "user_id",
            postgresql_where=text("deleted_at IS NULL AND new_films"),
        ),
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    __table_args__ = (Index("ix_processed_events_cleanup", "processed_at"),)


class KafkaOffset(Base):
    __tablename__ = "kafka_offsets"
    topic: Mapped[str] = mapped_column(Text, primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    next_offset: Mapped[int] = mapped_column(BigInteger)
    owner_id: Mapped[UUID | None]
    generation: Mapped[int] = mapped_column(BigInteger, server_default="0")
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint(
            "partition >= 0 AND next_offset >= 0 AND generation >= 0", name="nonnegative_position"
        ),
        CheckConstraint("(owner_id IS NULL) = (lease_until IS NULL)", name="owner_lease_pair"),
    )


class ReviewCounter(Base):
    __tablename__ = "review_counters"
    review_id: Mapped[str] = mapped_column(String(24), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("profiles.user_id"))
    likes_count: Mapped[int] = mapped_column(BigInteger)
    likes_version: Mapped[int] = mapped_column(BigInteger)
    last_sent_likes_count: Mapped[int] = mapped_column(BigInteger, server_default="0")
    last_notification_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("review_id ~ '^[0-9a-fA-F]{24}$'", name="review_id_format"),
        CheckConstraint(
            "likes_count >= 0 AND likes_version >= 0 AND last_sent_likes_count >= 0",
            name="nonnegative_counts",
        ),
        Index("ix_review_counters_user", "user_id"),
        Index("ix_review_counters_delta", text("(likes_count - last_sent_likes_count)")),
    )


class BootstrapReceipt(Base):
    """Never cascade from processed_events/outbox: survives their retention cleanup."""

    __tablename__ = "bootstrap_receipts"
    load_id: Mapped[str] = mapped_column(Text, primary_key=True)
    entity_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[UUID]
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        Index("ix_bootstrap_receipts_entity", "entity_id"),
        Index(
            "ix_bootstrap_receipts_pending",
            "load_id",
            "entity_id",
            postgresql_where=text("applied_at IS NULL"),
        ),
    )


class LoadState(Base):
    __tablename__ = "load_state"
    load_id: Mapped[str] = mapped_column(Text, primary_key=True)
    expected_count: Mapped[int] = mapped_column(BigInteger)
    manifest_hash: Mapped[str] = mapped_column(String(64))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("expected_count >= 0", name="expected_nonnegative"),
        CheckConstraint("manifest_hash ~ '^[0-9a-f]{64}$'", name="hash_format"),
    )


class PendingReview(Base):
    __tablename__ = "pending_reviews"
    review_id: Mapped[str] = mapped_column(String(24), primary_key=True)
    user_id: Mapped[UUID]  # Deliberately no FK: Auth can arrive after UGC.
    likes_count: Mapped[int] = mapped_column(BigInteger)
    likes_version: Mapped[int] = mapped_column(BigInteger)
    baseline_likes_count: Mapped[int | None] = mapped_column(BigInteger)
    baseline_load_id: Mapped[str | None] = mapped_column(Text)
    __table_args__ = (
        CheckConstraint("review_id ~ '^[0-9a-fA-F]{24}$'", name="review_id_format"),
        CheckConstraint(
            "likes_count >= 0 AND likes_version >= 0 AND "
            "(baseline_likes_count IS NULL OR baseline_likes_count >= 0)",
            name="nonnegative_counts",
        ),
        CheckConstraint(
            "(baseline_likes_count IS NULL) = (baseline_load_id IS NULL)", name="baseline_pair"
        ),
        Index("ix_pending_reviews_user", "user_id"),
    )


class FilmCard(Base):
    __tablename__ = "film_cards"
    film_id: Mapped[UUID] = mapped_column(primary_key=True)
    film_version: Mapped[int] = mapped_column(BigInteger)
    catalog_published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    catalog_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    creation_date: Mapped[date | None] = mapped_column(Date)
    imdb_rating: Mapped[float | None]
    genres: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    __table_args__ = (CheckConstraint("film_version >= 1", name="version_positive"),)


class ScheduleRun(Base):
    __tablename__ = "schedule_runs"
    run_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    kind: Mapped[str] = mapped_column(Text)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    films_per_digest: Mapped[int | None]
    status: Mapped[str] = mapped_column(Text, server_default="pending")
    cursor_user_id: Mapped[UUID | None]
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("kind", "period_start"),
        CheckConstraint("kind IN ('likes','new_films')", name="kind"),
        CheckConstraint(
            "films_per_digest IS NULL OR (kind = 'new_films' AND films_per_digest >= 1)",
            name="films_per_digest",
        ),
        CheckConstraint("status IN ('pending','expanding','completed','failed')", name="status"),
        CheckConstraint("(lease_token IS NULL) = (lease_until IS NULL)", name="lease_pair"),
        Index("ix_schedule_runs_pending", "status", "lease_until"),
    )


class FilmAnnouncement(Base):
    __tablename__ = "film_announcements"
    film_id: Mapped[UUID] = mapped_column(ForeignKey("film_cards.film_id"), primary_key=True)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    assigned_run_id: Mapped[UUID | None] = mapped_column(ForeignKey("schedule_runs.run_id"))
    included_in_digest: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    __table_args__ = (
        Index(
            "ix_film_announcements_unassigned",
            "detected_at",
            "film_id",
            postgresql_where=text("assigned_run_id IS NULL"),
        ),
    )


class ScheduleRecipient(Base):
    """Frozen audience: profiles created after the run cannot enter it on resume."""

    __tablename__ = "schedule_recipients"
    run_id: Mapped[UUID] = mapped_column(ForeignKey("schedule_runs.run_id"), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("profiles.user_id"), primary_key=True)


class ScheduleItem(Base):
    """Frozen likes candidates, reserved before fan-out creates deliveries."""

    __tablename__ = "schedule_items"
    run_id: Mapped[UUID] = mapped_column(ForeignKey("schedule_runs.run_id"), primary_key=True)
    review_id: Mapped[str] = mapped_column(
        ForeignKey("review_counters.review_id"), primary_key=True
    )
    user_id: Mapped[UUID] = mapped_column(ForeignKey("profiles.user_id"))
    likes_count: Mapped[int] = mapped_column(BigInteger)
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    __table_args__ = (
        CheckConstraint("likes_count >= 0", name="count_nonnegative"),
        Index(
            "uq_schedule_items_active_review",
            "review_id",
            unique=True,
            postgresql_where=text("active"),
        ),
        Index("ix_schedule_items_recipient", "run_id", "user_id"),
    )


class Delivery(Base):
    __tablename__ = "deliveries"
    delivery_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    logical_key: Mapped[str] = mapped_column(Text, unique=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("profiles.user_id"))
    run_id: Mapped[UUID | None] = mapped_column(ForeignKey("schedule_runs.run_id"))
    kind: Mapped[str] = mapped_column(Text)
    external_send_class: Mapped[str | None] = mapped_column(Text)
    required_subscription: Mapped[str | None] = mapped_column(Text)
    terminal_reason: Mapped[str | None] = mapped_column(Text)
    send_class: Mapped[str] = mapped_column(
        Text,
        Computed(
            "CASE kind WHEN 'welcome' THEN 'transactional' "
            "WHEN 'likes' THEN 'service' WHEN 'new_films' THEN 'bulk' "
            "WHEN 'external' THEN external_send_class END",
            persisted=True,
        ),
    )
    status: Mapped[str] = mapped_column(Text, server_default="pending")
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    message_id: Mapped[str | None] = mapped_column(Text, unique=True)
    prepared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    attempt_count: Mapped[int] = mapped_column(Integer, server_default="0")
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        Index("ix_deliveries_send_class", "send_class", "delivery_id"),
        UniqueConstraint("run_id", "user_id"),
        CheckConstraint("kind IN ('welcome','likes','new_films','external')", name="kind"),
        CheckConstraint(
            "(kind = 'external' AND external_send_class IS NOT NULL "
            "AND external_send_class IN ('transactional','service','bulk') AND run_id IS NULL) "
            "OR (kind <> 'external' AND external_send_class IS NULL "
            "AND required_subscription IS NULL)",
            name="external_policy",
        ),
        CheckConstraint(
            "required_subscription IN ('likes','new_films','marketing')",
            name="required_subscription",
        ),
        CheckConstraint(
            "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
            "'contact_unavailable','invalid_timezone','smtp_permanent','attempts_exhausted',"
            "'smtp_uncertain','cancelled_by_sender')",
            name="terminal_reason",
        ),
        CheckConstraint(
            "status IN ('pending','prepared','sending','retry','sent','cancelled',"
            "'skipped','failed')",
            name="status",
        ),
        CheckConstraint("attempt_count >= 0", name="attempts_nonnegative"),
        CheckConstraint("(lease_token IS NULL) = (lease_until IS NULL)", name="lease_pair"),
        CheckConstraint(
            "(prepared_at IS NULL AND payload IS NULL AND message_id IS NULL) OR "
            "(prepared_at IS NOT NULL AND payload IS NOT NULL AND message_id IS NOT NULL) OR "
            "(status IN ('sent','cancelled','skipped','failed') AND payload IS NULL "
            "AND prepared_at IS NOT NULL AND message_id IS NOT NULL)",
            name="snapshot_complete",
        ),
        CheckConstraint("(status = 'sent') = (sent_at IS NOT NULL)", name="sent_timestamp"),
        Index(
            "uq_deliveries_welcome_user",
            "user_id",
            unique=True,
            postgresql_where=text("kind = 'welcome'"),
        ),
        Index(
            "ix_deliveries_ready",
            "next_attempt_at",
            "lease_until",
            postgresql_where=text("status IN ('pending','prepared','sending','retry')"),
        ),
    )


class Campaign(Base):
    __tablename__ = "campaigns"
    campaign_id: Mapped[UUID] = mapped_column(primary_key=True)
    created_by_user_id: Mapped[UUID | None] = mapped_column()
    intake_status: Mapped[str] = mapped_column(Text, server_default="open")
    expected_batches: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("intake_status IN ('open','closed')", name="intake_status"),
        CheckConstraint("expected_batches > 0", name="expected_batches_positive"),
        CheckConstraint(
            "(intake_status = 'open' AND closed_at IS NULL AND expected_batches IS NULL) OR "
            "(intake_status = 'closed' AND closed_at IS NOT NULL AND expected_batches IS NOT NULL)",
            name="closure_complete",
        ),
    )


class CancelledCampaign(Base):
    __tablename__ = "cancelled_campaigns"
    campaign_id: Mapped[UUID] = mapped_column(primary_key=True)
    cancelled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class CampaignBatch(Base):
    __tablename__ = "campaign_batches"
    campaign_id: Mapped[UUID] = mapped_column(ForeignKey("campaigns.campaign_id"), primary_key=True)
    batch_id: Mapped[UUID] = mapped_column(primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    item_count: Mapped[int] = mapped_column(Integer)
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    __table_args__ = (
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="hash_format"),
        CheckConstraint("item_count >= 1", name="item_count_range"),
    )


class CampaignRecipient(Base):
    __tablename__ = "campaign_recipients"
    campaign_id: Mapped[UUID] = mapped_column(primary_key=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("profiles.user_id"), primary_key=True)
    batch_id: Mapped[UUID]
    delivery_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("deliveries.delivery_id"), unique=True
    )
    template_id: Mapped[str] = mapped_column(String(64))
    channel: Mapped[str] = mapped_column(Text)
    subject: Mapped[str | None] = mapped_column(Text)
    text: Mapped[str | None] = mapped_column(Text)
    source_available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effective_available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    skip_reason: Mapped[str | None] = mapped_column(Text)
    __table_args__ = (
        ForeignKeyConstraint(
            ["campaign_id", "batch_id"],
            ["campaign_batches.campaign_id", "campaign_batches.batch_id"],
        ),
        CheckConstraint("channel = 'email'", name="channel"),
        CheckConstraint("(subject IS NULL) = (text IS NULL)", name="content_pair"),
        CheckConstraint(
            "skip_reason IN ('profile_deleted','subscription_disabled','no_email',"
            "'invalid_timezone')",
            name="skip_reason",
        ),
        CheckConstraint("(delivery_id IS NULL) = (skip_reason IS NOT NULL)", name="outcome"),
        Index("ix_campaign_recipients_batch", "campaign_id", "batch_id"),
    )


class DeliveryItem(Base):
    __tablename__ = "delivery_items"
    item_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    delivery_id: Mapped[UUID] = mapped_column(ForeignKey("deliveries.delivery_id"))
    review_id: Mapped[str | None] = mapped_column(ForeignKey("review_counters.review_id"))
    likes_count: Mapped[int | None] = mapped_column(BigInteger)
    film_id: Mapped[UUID | None] = mapped_column(ForeignKey("film_cards.film_id"))
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    __table_args__ = (
        UniqueConstraint("delivery_id", "review_id", name="uq_delivery_items_review"),
        UniqueConstraint("delivery_id", "film_id", name="uq_delivery_items_film"),
        CheckConstraint(
            "(review_id IS NOT NULL AND likes_count IS NOT NULL AND likes_count >= 0 "
            "AND film_id IS NULL) OR "
            "(review_id IS NULL AND likes_count IS NULL AND film_id IS NOT NULL)",
            name="item_type",
        ),
        Index(
            "uq_delivery_items_active_review",
            "review_id",
            unique=True,
            postgresql_where=text("active AND review_id IS NOT NULL"),
        ),
    )


class Outbox(Base):
    __tablename__ = "outbox"
    outbox_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    delivery_id: Mapped[UUID] = mapped_column(ForeignKey("deliveries.delivery_id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("(lease_token IS NULL) = (lease_until IS NULL)", name="lease_pair"),
        Index(
            "uq_outbox_pending_delivery",
            "delivery_id",
            unique=True,
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "ix_outbox_ready",
            "available_at",
            "lease_until",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    attempt_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    delivery_id: Mapped[UUID] = mapped_column(ForeignKey("deliveries.delivery_id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    duplicate_risk: Mapped[bool] = mapped_column(Boolean, server_default="false")
    __table_args__ = (
        UniqueConstraint("delivery_id", "attempt_number"),
        CheckConstraint("attempt_number >= 1", name="attempt_positive"),
        CheckConstraint(
            "outcome IN ('started','accepted','retry','uncertain','failed','cancelled','skipped')",
            name="outcome",
        ),
    )
