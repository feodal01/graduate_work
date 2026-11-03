"""PG owns retries; a session lock serializes live senders beyond lease expiry.

The lock cannot fence an already transmitted SMTP request if PostgreSQL disconnects.
Such failures remain uncertain and may duplicate on retry with the same Message-ID.
"""

import argparse
import asyncio
import logging
import math
import signal
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from errno import EHOSTUNREACH, ENETUNREACH
from socket import EAI_AGAIN, gaierror
from typing import cast
from uuid import UUID, uuid4

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.selectable import Exists

from notifications.clients.auth_contacts import ActiveContact, AuthContactsClient, ContactsError
from notifications.config import Settings, WorkerSettings
from notifications.contracts import DeliverySnapshot
from notifications.db.models import (
    Delivery,
    DeliveryAttempt,
    DeliveryItem,
    Outbox,
    Profile,
    ReviewCounter,
    ScheduleItem,
)
from notifications.db.session import Database
from notifications.delivery.limiter_config import LimiterSettings
from notifications.delivery.ownership import delivery_ownership
from notifications.delivery.policy import skip_reason
from notifications.delivery.publisher import Broker
from notifications.delivery.rate_limit import Limiter, PermitDecision, RedisRateLimiter
from notifications.delivery.render import render
from notifications.delivery.routing import SEND_CLASSES, SendClass, class_for_kind
from notifications.delivery.smtp import DeliveryError, transmit
from notifications.scheduling.windows import next_send_at
from notifications.template_registry import get_template_spec

LOG = logging.getLogger(__name__)
TERMINAL = {"sent", "cancelled", "skipped", "failed"}


def transient_recovery_error(error: Exception) -> bool:
    """Retry transport/timeout failures and explicit transient PostgreSQL states."""
    if isinstance(error, (ConnectionError, TimeoutError, DatabaseTimeoutError)):
        return True
    if isinstance(error, gaierror):
        return error.errno == EAI_AGAIN
    if isinstance(error, OSError):
        return error.errno in {EHOSTUNREACH, ENETUNREACH}
    if isinstance(error, DBAPIError):
        return error.connection_invalidated or getattr(error.orig, "sqlstate", None) in {
            "08000",  # connection_exception
            "08001",  # sqlclient_unable_to_establish_sqlconnection
            "08003",  # connection_does_not_exist
            "08006",  # connection_failure
            "08007",  # transaction_resolution_unknown
            "40001",  # serialization_failure
            "40P01",  # deadlock_detected
            "53300",  # too_many_connections
            "55P03",  # lock_not_available
            "57014",  # query_canceled (for example statement_timeout)
            "57P01",  # admin_shutdown
            "57P02",  # crash_shutdown
            "57P03",  # cannot_connect_now
        }
    return False


class DeliveryWorker:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        *,
        send_class: SendClass,
        limiter: Limiter,
        max_concurrency: int,
        unavailable_retry_seconds: float,
        retry_seconds: float,
        max_attempts: int,
        lease_seconds: float = 120,
        smtp_timeout: float = 30,
        contacts: AuthContactsClient | None = None,
    ):
        if send_class not in SEND_CLASSES:
            raise ValueError(f"Unknown send class: {send_class}")
        self.send_class: SendClass = send_class
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(cast(object, max_concurrency), int)
            or max_concurrency < 1
        ):
            raise ValueError("max_concurrency must be a positive integer")
        for pause in (unavailable_retry_seconds,):
            if isinstance(pause, bool) or not math.isfinite(pause) or pause <= 0:
                raise ValueError("retry pauses must be finite and positive")
        self.limiter = limiter
        self.max_concurrency = max_concurrency
        self._slots = asyncio.Semaphore(max_concurrency)
        self.unavailable_retry_seconds = unavailable_retry_seconds
        if retry_seconds <= 0 or max_attempts < 1 or smtp_timeout <= 0:
            raise ValueError("retry, attempts and SMTP timeout must be positive")
        if lease_seconds <= smtp_timeout:
            raise ValueError("lease must exceed the entire SMTP transaction timeout")
        self.database, self.settings = database, settings
        if contacts is None:
            assert settings.auth_contacts_url is not None
            assert settings.auth_contacts_api_key is not None
            assert settings.auth_contacts_timeout_seconds is not None
            assert settings.auth_contacts_batch_size is not None
            contacts = AuthContactsClient(
                settings.auth_contacts_url,
                settings.auth_contacts_api_key.get_secret_value(),
                settings.auth_contacts_timeout_seconds,
                settings.auth_contacts_batch_size,
            )
        self.contacts = contacts
        self.retry_seconds, self.max_attempts = retry_seconds, max_attempts
        self.lease_seconds, self.smtp_timeout = lease_seconds, smtp_timeout

    def ownership(self, did: UUID) -> AbstractAsyncContextManager[AsyncConnection | None]:
        return delivery_ownership(self.database, did)

    @staticmethod
    def permission(delivery: Delivery, profile: Profile | None) -> str | None:
        subscription = (
            delivery.required_subscription
            if delivery.kind == "external"
            else get_template_spec(delivery.kind).required_subscription
        )
        send_class = (
            cast(SendClass, delivery.external_send_class)
            if delivery.kind == "external"
            else class_for_kind(delivery.kind)
        )
        return skip_reason(profile, subscription, send_class=send_class)

    async def terminal(
        self, s: AsyncSession, delivery: Delivery, status: str, *, reason: str | None = None
    ) -> None:
        delivery.status = status
        delivery.terminal_reason = reason
        delivery.terminal_at = await s.scalar(select(func.clock_timestamp()))
        delivery.lease_token = delivery.lease_until = None
        await s.execute(
            update(DeliveryItem)
            .where(DeliveryItem.delivery_id == delivery.delivery_id)
            .values(active=False)
        )
        if delivery.run_id is not None:
            await s.execute(
                update(ScheduleItem)
                .where(
                    ScheduleItem.run_id == delivery.run_id, ScheduleItem.user_id == delivery.user_id
                )
                .values(active=False)
            )

    async def snapshot(self, s: AsyncSession, delivery: Delivery) -> DeliverySnapshot | None:
        profile = await s.get(Profile, delivery.user_id, with_for_update=True)
        denied = self.permission(delivery, profile)
        if denied:
            await self.terminal(
                s,
                delivery,
                "skipped"
                if denied in {"no_email", "contact_unavailable", "invalid_timezone"}
                else "cancelled",
                reason=denied,
            )
            return None
        if delivery.payload is None:
            return None
        return DeliverySnapshot(
            delivery_id=delivery.delivery_id,
            user_id=delivery.user_id,
            kind=delivery.kind,
            payload=delivery.payload,
        )

    async def prepare_owned(
        self, conn: AsyncConnection, delivery_id: UUID, *, require_due: bool = False
    ) -> DeliverySnapshot | None:
        # The advisory connection owns the delivery throughout HTTP; each row-lock
        # transaction is committed before crossing the network boundary.
        async with AsyncSession(bind=conn, expire_on_commit=False) as s, s.begin():
            delivery = await s.get(Delivery, delivery_id, with_for_update=True)
            if delivery is None or delivery.status in TERMINAL:
                return None
            if delivery.send_class != self.send_class:
                if require_due:
                    await self.enqueue(s, delivery)
                return None
            now: datetime = (await s.execute(select(func.clock_timestamp()))).scalar_one()
            if require_due and delivery.next_attempt_at > now:
                return None
            if delivery.lease_until is not None and delivery.lease_until > now:
                return None
            if delivery.status == "sending":
                await self.recover_one(s, delivery)
                return None
            profile = await s.get(Profile, delivery.user_id, with_for_update=True)
            denied = self.permission(delivery, profile)
            if denied:
                await self.terminal(
                    s,
                    delivery,
                    "skipped" if denied == "invalid_timezone" else "cancelled",
                    reason=denied,
                )
                return None
            if delivery.payload is not None:
                return await self.snapshot(s, delivery)
            user_id = delivery.user_id
        try:
            contact = (await self.contacts.fetch([user_id]))[user_id]
        except ContactsError as error:
            async with AsyncSession(bind=conn) as s, s.begin():
                delivery = await s.get(Delivery, delivery_id, with_for_update=True)
                if delivery is not None and delivery.status not in TERMINAL:
                    profile = await s.get(Profile, delivery.user_id, with_for_update=True)
                    denied = self.permission(delivery, profile)
                    if denied:
                        await self.terminal(
                            s,
                            delivery,
                            "skipped" if denied == "invalid_timezone" else "cancelled",
                            reason=denied,
                        )
                    else:
                        assert self.settings.auth_contacts_retry_seconds is not None
                        await self.defer(
                            s,
                            delivery,
                            self.due_at(
                                delivery,
                                profile,
                                await self.window_now(s)
                                + timedelta(seconds=self.settings.auth_contacts_retry_seconds),
                            ),
                        )
            LOG.warning("delivery=%s contacts=%s", delivery_id, error.code)
            return None
        async with AsyncSession(bind=conn, expire_on_commit=False) as s, s.begin():
            delivery = await s.get(Delivery, delivery_id, with_for_update=True)
            if delivery is None or delivery.status in TERMINAL:
                return None
            profile = await s.get(Profile, delivery.user_id, with_for_update=True)
            denied = self.permission(delivery, profile)
            if denied:
                await self.terminal(
                    s,
                    delivery,
                    "skipped" if denied == "invalid_timezone" else "cancelled",
                    reason=denied,
                )
                return None
            if not isinstance(contact, ActiveContact) or not contact.email:
                await self.terminal(
                    s,
                    delivery,
                    "skipped",
                    reason=(
                        "contact_unavailable"
                        if not isinstance(contact, ActiveContact)
                        else "no_email"
                    ),
                )
                return None
            assert profile is not None
            if delivery.payload is None:
                prepared_at = await self.window_now(s)
                delivery.payload = await render(s, delivery, profile, self.settings, contact)
                delivery.message_id = f"<{delivery.delivery_id}@notifications.local>"
                delivery.prepared_at = prepared_at
                delivery.status = "prepared"
            return await self.snapshot(s, delivery)

    async def prepare_delivery(self, delivery_id: UUID) -> DeliverySnapshot | None:
        async with self.ownership(delivery_id) as conn:
            if conn is None:
                return None
            return await self.prepare_owned(conn, delivery_id)

    async def enqueue(self, s: AsyncSession, delivery: Delivery) -> None:
        await s.execute(
            insert(Outbox)
            .values(delivery_id=delivery.delivery_id, available_at=delivery.next_attempt_at)
            .on_conflict_do_update(
                index_elements=[Outbox.delivery_id],
                index_where=Outbox.published_at.is_(None),
                # Rescheduling replaces the claimed work, even when the due time
                # is unchanged. A late publisher result must lose ownership.
                set_={
                    "available_at": delivery.next_attempt_at,
                    "lease_token": None,
                    "lease_until": None,
                },
            )
        )

    async def window_now(self, s: AsyncSession) -> datetime:
        """Business-clock seam for window boundary tests; production uses PostgreSQL."""
        return (await s.execute(select(func.clock_timestamp()))).scalar_one()

    def due_at(self, delivery: Delivery, profile: Profile | None, earliest: datetime) -> datetime:
        # UPDATE expires generated send_class; use its persisted source without lazy IO.
        if delivery.kind == "external":
            if delivery.external_send_class not in SEND_CLASSES:
                raise ValueError("Unknown external send class")
            # mypy does not narrow strings through tuple membership as Pyright does.
            send_class = cast(SendClass, delivery.external_send_class)  # pyright: ignore[reportUnnecessaryCast]
        else:
            send_class = class_for_kind(delivery.kind)
        return next_send_at(
            send_class,
            profile.tz if profile is not None else None,
            earliest,
            timing=self.settings,
        )

    async def defer(self, s: AsyncSession, delivery: Delivery, due: datetime) -> None:
        delivery.next_attempt_at = due
        delivery.lease_token = delivery.lease_until = None
        await self.enqueue(s, delivery)

    async def recover_one(self, s: AsyncSession, delivery: Delivery) -> bool:
        if delivery.send_class != self.send_class:
            return False
        now: datetime = (await s.execute(select(func.clock_timestamp()))).scalar_one()
        if delivery.lease_until is not None and delivery.lease_until > now:
            return False
        if delivery.status == "sending":
            attempt = await s.scalar(
                select(DeliveryAttempt)
                .where(
                    DeliveryAttempt.delivery_id == delivery.delivery_id,
                    DeliveryAttempt.attempt_number == delivery.attempt_count,
                )
                .with_for_update()
            )
            if attempt is not None and attempt.outcome == "started":
                attempt.outcome, attempt.duplicate_risk = "uncertain", True
                attempt.finished_at, attempt.error = (
                    now,
                    "Lease expired before persisted SMTP result",
                )
            profile = await s.get(Profile, delivery.user_id, with_for_update=True)
            denied = self.permission(delivery, profile)
            if denied:
                await self.terminal(
                    s,
                    delivery,
                    "skipped" if denied == "invalid_timezone" else "cancelled",
                    reason=denied,
                )
                return True
            delivery.next_attempt_at = self.due_at(
                delivery, profile, await self.window_now(s) + timedelta(seconds=self.retry_seconds)
            )
            if delivery.attempt_count >= self.max_attempts:
                await self.terminal(s, delivery, "failed", reason="smtp_uncertain")
            else:
                delivery.status = "retry"
                delivery.lease_token = delivery.lease_until = None
            if delivery.status not in TERMINAL:
                await self.enqueue(s, delivery)
            return True
        if delivery.status not in TERMINAL:
            # Give a confirmed broker handoff time to reach a worker. Existing
            # unpublished rows belong to the publisher, including future retries.
            waiting = await s.scalar(select(self.dispatch_pending(delivery.delivery_id)))
            if waiting:
                return False
            profile = await s.get(Profile, delivery.user_id, with_for_update=True)
            denied = self.permission(delivery, profile)
            if denied:
                await self.terminal(
                    s,
                    delivery,
                    "skipped" if denied == "invalid_timezone" else "cancelled",
                    reason=denied,
                )
                return True
            await self.defer(
                s,
                delivery,
                self.due_at(
                    delivery, profile, max(delivery.next_attempt_at, await self.window_now(s))
                ),
            )
            return True
        return False

    def dispatch_pending(self, did: UUID | InstrumentedAttribute[UUID]) -> Exists:
        return (
            select(Outbox.outbox_id)
            .where(
                Outbox.delivery_id == did,
                or_(
                    Outbox.published_at.is_(None),
                    Outbox.published_at > func.now() - timedelta(seconds=self.lease_seconds),
                ),
            )
            .exists()
        )

    async def recover(self) -> int:
        async with self.database.transaction() as s:
            ids = list(
                await s.scalars(
                    select(Delivery.delivery_id)
                    .where(
                        Delivery.send_class == self.send_class,
                        Delivery.status.not_in(TERMINAL),
                        Delivery.next_attempt_at <= func.now(),
                        or_(Delivery.lease_until.is_(None), Delivery.lease_until <= func.now()),
                        or_(
                            Delivery.status == "sending",
                            ~self.dispatch_pending(Delivery.delivery_id),
                        ),
                    )
                    .order_by(Delivery.next_attempt_at)
                    .limit(self.settings.recovery_batch_size)
                )
            )
        count = 0
        for did in ids:
            async with self.ownership(did) as conn:
                if conn is not None:
                    async with AsyncSession(bind=conn) as s, s.begin():
                        delivery = await s.get(Delivery, did, with_for_update=True)
                        if delivery is not None:
                            count += await self.recover_one(s, delivery)
        return count

    async def send_delivery(self, delivery_id: UUID) -> None:
        # Bound direct callers too, before opening a connection or taking ownership.
        async with self._slots:
            await self._send_delivery(delivery_id)

    async def _send_delivery(self, delivery_id: UUID) -> None:
        async with self.ownership(delivery_id) as conn:
            if conn is None:
                return
            if await self.prepare_owned(conn, delivery_id, require_due=True) is None:
                return
            token = uuid4()
            async with AsyncSession(bind=conn, expire_on_commit=False) as s, s.begin():
                delivery = await s.get(Delivery, delivery_id, with_for_update=True)
                if delivery is None or delivery.status in TERMINAL:
                    return
                if delivery.send_class != self.send_class:
                    await self.enqueue(s, delivery)
                    return
                now: datetime = (await s.execute(select(func.clock_timestamp()))).scalar_one()
                if delivery.next_attempt_at > now:
                    return
                if delivery.lease_until is not None and delivery.lease_until > now:
                    return
                if delivery.status == "sending":
                    await self.recover_one(s, delivery)
                    return
                snapshot = await self.snapshot(s, delivery)
                if snapshot is None:
                    return
                profile = await s.get(Profile, delivery.user_id, with_for_update=True)
                window_now = await self.window_now(s)
                due = self.due_at(delivery, profile, window_now)
                if due > window_now:
                    await self.defer(s, delivery, due)
                    return
                delivery.lease_token = token
                delivery.lease_until = now + timedelta(seconds=self.lease_seconds)
            # No row locks or open transaction during Redis. The session ownership
            # remains held; an uncertain reply is never retried or refunded.
            try:
                async with asyncio.timeout(self.smtp_timeout):
                    decision = await self.limiter.acquire(self.send_class)
            except (OSError, TimeoutError):
                decision = PermitDecision(False, self.unavailable_retry_seconds, "unavailable")
            async with AsyncSession(bind=conn) as s, s.begin():
                delivery = await s.get(Delivery, delivery_id, with_for_update=True)
                checked_at: datetime = (
                    await s.execute(select(func.clock_timestamp()))
                ).scalar_one()
                if (
                    delivery is None
                    or delivery.lease_token != token
                    or delivery.lease_until is None
                    or delivery.lease_until <= checked_at
                ):
                    raise RuntimeError("Delivery ownership lost before SMTP")
                profile = await s.get(Profile, delivery.user_id, with_for_update=True)
                denied = self.permission(delivery, profile)
                if denied:
                    await self.terminal(
                        s,
                        delivery,
                        "skipped"
                        if denied in {"no_email", "contact_unavailable", "invalid_timezone"}
                        else "cancelled",
                        reason=denied,
                    )
                    return
                window_now = await self.window_now(s)
                if not decision.allowed:
                    delay = decision.retry_after_seconds
                    if not math.isfinite(delay) or delay <= 0:
                        delay = self.unavailable_retry_seconds
                    await self.defer(
                        s,
                        delivery,
                        self.due_at(delivery, profile, window_now + timedelta(seconds=delay)),
                    )
                    return
                due = self.due_at(delivery, profile, window_now)
                if due > window_now:
                    await self.defer(s, delivery, due)
                    return
                delivery.status = "sending"
                delivery.attempt_count += 1
                delivery.lease_until = checked_at + timedelta(seconds=self.lease_seconds)
                s.add(
                    DeliveryAttempt(
                        delivery_id=delivery_id,
                        attempt_number=delivery.attempt_count,
                        outcome="started",
                    )
                )
                payload, message_id = delivery.payload, delivery.message_id
            assert payload is not None and message_id is not None
            error = None
            try:
                await transmit(self.settings, payload, message_id, self.smtp_timeout)
            except DeliveryError as exc:
                error = exc
            # Failure here intentionally leaves 'started': recovery records uncertainty.
            await self.finish(conn, delivery_id, token, error)

    async def finish(
        self, conn: AsyncConnection, did: UUID, token: UUID, error: DeliveryError | None
    ) -> None:
        async with AsyncSession(bind=conn) as s, s.begin():
            delivery = await s.get(Delivery, did, with_for_update=True)
            now: datetime = (await s.execute(select(func.clock_timestamp()))).scalar_one()
            if (
                delivery is None
                or delivery.lease_token != token
                or delivery.lease_until is None
                or delivery.lease_until <= now
            ):
                raise RuntimeError("Delivery ownership lost before finalization")
            attempt = await s.scalar(
                select(DeliveryAttempt)
                .where(
                    DeliveryAttempt.delivery_id == did,
                    DeliveryAttempt.attempt_number == delivery.attempt_count,
                )
                .with_for_update()
            )
            assert attempt is not None
            attempt.finished_at = now
            if error is None:
                attempt.outcome = "accepted"
                delivery.sent_at = now
                delivery.status = "sent"
                assert delivery.payload is not None
                for review in delivery.payload["reviews"]:
                    await s.execute(
                        update(ReviewCounter)
                        .where(ReviewCounter.review_id == review["review_id"])
                        .values(
                            last_sent_likes_count=review["likes_count"],
                            last_notification_sent_at=now,
                        )
                    )
                await self.terminal(s, delivery, "sent")
            else:
                attempt.error, attempt.duplicate_risk = str(error), error.uncertain
                attempt.outcome = "uncertain" if error.uncertain else "retry"
                if error.permanent or delivery.attempt_count >= self.max_attempts:
                    if not error.uncertain:
                        attempt.outcome = "failed"
                    # A later rejection cannot rule out an earlier uncertain acceptance.
                    uncertain = error.uncertain or bool(
                        await s.scalar(
                            select(
                                select(DeliveryAttempt.delivery_id)
                                .where(
                                    DeliveryAttempt.delivery_id == did,
                                    DeliveryAttempt.duplicate_risk.is_(True),
                                )
                                .exists()
                            )
                        )
                    )
                    reason = (
                        "smtp_uncertain"
                        if uncertain
                        else "smtp_permanent"
                        if error.permanent
                        else "attempts_exhausted"
                    )
                    await self.terminal(s, delivery, "failed", reason=reason)
                else:
                    delivery.status = "retry"
                    profile = await s.get(Profile, delivery.user_id, with_for_update=True)
                    denied = self.permission(delivery, profile)
                    if denied:
                        await self.terminal(
                            s,
                            delivery,
                            "skipped" if denied == "invalid_timezone" else "cancelled",
                            reason=denied,
                        )
                        return
                    await self.defer(
                        s,
                        delivery,
                        self.due_at(
                            delivery,
                            profile,
                            await self.window_now(s) + timedelta(seconds=self.retry_seconds),
                        ),
                    )
            LOG.info(
                "delivery=%s outcome=%s duplicate_risk=%s",
                did,
                attempt.outcome,
                attempt.duplicate_risk,
            )

    async def handle(self, message: AbstractIncomingMessage) -> None:
        try:
            did = UUID(message.body.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            await message.reject(requeue=False)
            return
        try:
            await self.send_delivery(did)
            async with self.database.transaction() as s:
                delivery = await s.get(Delivery, did)
                failed = delivery is not None and delivery.status == "failed"
            if failed:
                await message.reject(requeue=False)
            else:
                await message.ack()
        except Exception as error:
            LOG.error("delivery=%s processing failed: %s", did, type(error).__name__)
            await message.nack(requeue=True)

    async def consume(
        self, broker: Broker, poll_seconds: float, stop: asyncio.Event | None = None
    ) -> None:
        async def recovery() -> None:
            while True:
                try:
                    count = await self.recover()
                except Exception as error:
                    if not transient_recovery_error(error):
                        raise
                    LOG.warning("recovery temporarily unavailable: %s", type(error).__name__)
                else:
                    LOG.info("recovered=%d", count)
                await asyncio.sleep(poll_seconds)

        active: set[asyncio.Task[None]] = set()
        errors: list[Exception] = []
        handler_failed = asyncio.Event()
        # Only guarded handlers belong to this group. Failures are raised after
        # intake has stopped and every active handler has finished.
        # External cancellation (including run()'s shutdown deadline) still cancels sends.
        async with asyncio.TaskGroup() as group:

            async def handle(message: AbstractIncomingMessage) -> None:
                try:
                    await self.handle(message)
                except Exception as error:
                    errors.append(error)
                    LOG.error("worker handler failed: %s", type(error).__name__)
                    handler_failed.set()

            async def intake() -> None:
                async with broker.queues[self.send_class].iterator() as messages:
                    async for message in messages:
                        while len(active) >= self.max_concurrency:
                            await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                        if (
                            recovering.done()
                            or handler_failed.is_set()
                            or (stop is not None and stop.is_set())
                        ):
                            break
                        task = group.create_task(handle(message))
                        active.add(task)
                        task.add_done_callback(active.discard)

            recovering = asyncio.create_task(recovery())
            consuming = asyncio.create_task(intake())
            background: list[asyncio.Task[None] | asyncio.Task[bool]] = [
                recovering,
                consuming,
                asyncio.create_task(handler_failed.wait()),
            ]
            if stop is not None:
                background.append(asyncio.create_task(stop.wait()))
            try:
                # Normal iterator completion must also return to reconnect.
                await asyncio.wait(background, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in background:
                    task.cancel()
                results = await asyncio.gather(*background, return_exceptions=True)
                for error in results:
                    if isinstance(error, Exception):
                        errors.append(error)
                        LOG.error("worker background task failed: %s", type(error).__name__)
        if errors:
            raise ExceptionGroup("worker tasks failed", errors)


def configured_worker(settings: WorkerSettings | None = None) -> DeliveryWorker:
    if settings is None:
        settings = WorkerSettings()  # type: ignore[call-arg]
    limiter_settings = LimiterSettings()  # type: ignore[call-arg]
    return DeliveryWorker(
        Database(settings.database_url),
        settings,
        send_class=settings.send_class,
        limiter=RedisRateLimiter(limiter_settings),
        max_concurrency=settings.max_concurrency,
        unavailable_retry_seconds=limiter_settings.unavailable_retry_seconds,
        retry_seconds=settings.retry_seconds,
        max_attempts=settings.max_attempts,
        lease_seconds=settings.lease_seconds,
        smtp_timeout=settings.smtp_timeout,
    )


async def prepare_delivery(delivery_id: UUID) -> DeliverySnapshot | None:
    worker = configured_worker()
    try:
        return await worker.prepare_delivery(delivery_id)
    finally:
        try:
            await cast(RedisRateLimiter, worker.limiter).aclose()
        finally:
            await worker.database.close()


async def send_delivery(delivery_id: UUID) -> None:
    worker = configured_worker()
    try:
        await worker.send_delivery(delivery_id)
    finally:
        try:
            await cast(RedisRateLimiter, worker.limiter).aclose()
        finally:
            await worker.database.close()


async def run(args: argparse.Namespace) -> None:
    overrides = {name: value for name, value in vars(args).items() if value is not None}
    settings = WorkerSettings(**overrides)
    worker = configured_worker(settings)
    limiter = cast(RedisRateLimiter, worker.limiter)
    db = worker.database
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)

    async def serve() -> None:
        while not stop.is_set():
            try:
                async with Broker(str(settings.rabbitmq_url), settings.namespace) as broker:
                    await broker.channel.set_qos(
                        prefetch_count=min(settings.prefetch, worker.max_concurrency)
                    )
                    await worker.consume(broker, settings.poll_seconds, stop)
            except Exception as error:
                LOG.error("worker loop failed: %s", type(error).__name__)
                try:
                    await asyncio.wait_for(stop.wait(), settings.poll_seconds)
                except TimeoutError:
                    pass

    serving = asyncio.create_task(serve())
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait((serving, stopping), return_when=asyncio.FIRST_COMPLETED)
        try:
            await asyncio.wait_for(serving, settings.shutdown_timeout)
        except TimeoutError:
            # Cancellation awaits callbacks and ownership cleanup. An interrupted
            # SMTP attempt remains started; lease recovery records it as uncertain.
            LOG.warning("worker shutdown deadline reached; active callbacks cancelled")
    finally:
        serving.cancel()
        stopping.cancel()
        await asyncio.gather(serving, stopping, return_exceptions=True)
        try:
            await limiter.aclose()
        finally:
            try:
                await db.close()
            finally:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    loop.remove_signal_handler(signum)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry-seconds", type=float)
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--max-concurrency", type=int)
    parser.add_argument("--lease-seconds", type=float, default=None)
    parser.add_argument("--smtp-timeout", type=float, default=None)
    parser.add_argument("--shutdown-timeout", type=float, default=None)
    parser.add_argument("--poll-seconds", type=float, default=None)
    parser.add_argument("--prefetch", type=int, default=None)
    parser.add_argument("--namespace", default=None)
    parser.add_argument("--send-class", choices=SEND_CLASSES)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
