"""Standalone scheduler: atomic issue snapshots and leased transactional fan-out.

Run with ``python -m notifications.scheduling.scheduler``. ``--once`` performs
one scheduling/recovery cycle; intervals and bootstrap identity come from Settings.
"""

import argparse
import asyncio
import logging
import signal
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from uuid import UUID, uuid4

from sqlalchemy import exists, func, insert, literal, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.config import Settings, TimingSettings
from notifications.db.models import (
    Delivery,
    DeliveryItem,
    FilmAnnouncement,
    LoadState,
    Outbox,
    Profile,
    ScheduleItem,
    ScheduleRecipient,
    ScheduleRun,
)
from notifications.db.session import Database, get_database
from notifications.delivery.policy import valid_timezone
from notifications.delivery.routing import class_for_kind
from notifications.scheduling.films import snapshot_films
from notifications.scheduling.likes import snapshot_likes
from notifications.scheduling.windows import next_send_at

logger = logging.getLogger(__name__)


class LeaseLost(RuntimeError):
    """Another owner took over, or this batch exceeded its lease."""


class Scheduler:
    def __init__(
        self,
        database: Database,
        *,
        likes_min_delta: int,
        likes_bootstrap_load_id: str,
        timing: TimingSettings,
        lease_seconds: float = 60,
    ) -> None:
        if type(likes_min_delta) is not int:
            raise ValueError("likes_min_delta must be an integer >= 1")
        if likes_min_delta < 1 or not likes_bootstrap_load_id or lease_seconds <= 0:
            raise ValueError("positive threshold/lease and explicit bootstrap load are required")
        self.timing = timing
        self.database = database
        self.likes_min_delta = likes_min_delta
        self.likes_bootstrap_load_id = likes_bootstrap_load_id
        self.lease_seconds = lease_seconds

    async def schedule_likes(self, period_start: datetime) -> UUID | None:
        return await self._schedule("likes", period_start)

    async def schedule_films(self, period_start: datetime) -> UUID | None:
        return await self._schedule("new_films", period_start)

    async def _schedule(self, kind: str, period_start: datetime) -> UUID | None:
        if period_start.utcoffset() != timedelta(0):
            raise ValueError("period_start must be timezone-aware UTC")
        async with self.database.transaction() as session:
            # One transaction lock per kind serializes snapshots across periods,
            # including the empty-set case where there is no row to lock yet.
            await session.execute(
                text("SELECT pg_advisory_xact_lock(748301, :kind)"),
                {"kind": 1 if kind == "likes" else 2},
            )
            if kind == "likes":
                ready = await session.scalar(
                    select(
                        exists().where(
                            LoadState.load_id == self.likes_bootstrap_load_id,
                            LoadState.completed_at.is_not(None),
                        )
                    )
                )
                if not ready:
                    return None
            existing = await session.scalar(
                select(ScheduleRun.run_id).where(
                    ScheduleRun.kind == kind, ScheduleRun.period_start == period_start
                )
            )
            if existing is not None:
                return existing
            run_id = uuid4()
            session.add(
                ScheduleRun(
                    run_id=run_id,
                    kind=kind,
                    period_start=period_start,
                    films_per_digest=self.timing.films_per_digest if kind == "new_films" else None,
                )
            )
            await session.flush()
            if kind == "likes":
                await snapshot_likes(session, run_id, self.likes_min_delta)
            else:
                await snapshot_films(session, run_id, period_start, self.timing.films_per_digest)
            return run_id

    async def claim_run(self, run_id: UUID) -> UUID | None:
        """Commit a fresh fencing token; crashed owners become reclaimable on expiry."""
        async with self.database.transaction() as session:
            run = await session.scalar(
                select(ScheduleRun)
                .where(
                    ScheduleRun.run_id == run_id,
                    ScheduleRun.status.in_(("pending", "expanding")),
                )
                .with_for_update(skip_locked=True)
            )
            if run is None:
                return None
            now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            if run.lease_until is not None and run.lease_until > now:
                return None
            token = uuid4()
            run.status = "expanding"
            run.lease_token = token
            run.lease_until = now + timedelta(seconds=self.lease_seconds)
            return token

    async def expand_run(self, run_id: UUID, limit: int) -> int:
        """Process up to limit frozen recipients; includes terminal cancellations.

        Zero means completed, owned elsewhere, or missing. A batch exception
        leaves the claim to expire, and all batch writes roll back together.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        token = await self.claim_run(run_id)
        if token is None:
            return 0
        return await self.expand_claim(run_id, token, limit)

    async def expand_claim(self, run_id: UUID, token: UUID, limit: int) -> int:
        """Fence both before work and before committing delivery/outbox/cursor."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        async with self.database.transaction() as session:
            run = await session.scalar(
                select(ScheduleRun).where(ScheduleRun.run_id == run_id).with_for_update()
            )
            await self._check_lease(session, run, token)
            assert run is not None
            query = select(ScheduleRecipient.user_id).where(ScheduleRecipient.run_id == run_id)
            if run.cursor_user_id is not None:
                query = query.where(ScheduleRecipient.user_id > run.cursor_user_id)
            users = (
                await session.scalars(query.order_by(ScheduleRecipient.user_id).limit(limit))
            ).all()
            for user_id in users:
                await self._expand_recipient(session, run, user_id)
                run.cursor_user_id = user_id
            remaining = select(exists().where(ScheduleRecipient.run_id == run_id))
            if run.cursor_user_id is not None:
                remaining = select(
                    exists().where(
                        ScheduleRecipient.run_id == run_id,
                        ScheduleRecipient.user_id > run.cursor_user_id,
                    )
                )
            run.status = "expanding" if await session.scalar(remaining) else "completed"
            # Force every write (including faulting triggers) before the final
            # wall-clock check: transaction now() would hide mid-batch expiry.
            await session.flush()
            await self._check_lease(session, run, token)
            run.lease_token = None
            run.lease_until = None
            return len(users)

    @staticmethod
    async def _check_lease(session: AsyncSession, run: ScheduleRun | None, token: UUID) -> None:
        now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if (
            run is None
            or run.lease_token != token
            or run.lease_until is None
            or run.lease_until <= now
        ):
            raise LeaseLost("schedule run lease expired or changed owner")

    async def _expand_recipient(
        self, session: AsyncSession, run: ScheduleRun, user_id: UUID
    ) -> None:
        # Auth and preferences lock the profile too. Holding that lock prevents
        # deletion/opt-out from racing this batch's cancellation decision.
        profile = await session.scalar(
            select(Profile).where(Profile.user_id == user_id).with_for_update()
        )
        assert profile is not None  # frozen recipients retain their profile FK
        if not valid_timezone(profile.tz):
            await session.execute(
                update(ScheduleItem)
                .where(ScheduleItem.run_id == run.run_id, ScheduleItem.user_id == user_id)
                .values(active=False)
            )
            return
        enabled = profile.likes if run.kind == "likes" else profile.new_films
        active = profile.deleted_at is None and enabled
        due = next_send_at(
            class_for_kind(run.kind),
            profile.tz,
            datetime.now(UTC),
            timing=self.timing,
        )
        delivery_id = uuid4()
        session.add(
            Delivery(
                delivery_id=delivery_id,
                logical_key=f"{run.kind}:{run.run_id}:{user_id}",
                user_id=user_id,
                run_id=run.run_id,
                kind=run.kind,
                status="pending" if active else "cancelled",
                terminal_at=None if active else datetime.now(UTC),
                next_attempt_at=due,
            )
        )
        await session.flush()
        if run.kind == "likes":
            await session.execute(
                insert(DeliveryItem).from_select(
                    ["item_id", "delivery_id", "review_id", "likes_count", "active"],
                    select(
                        func.gen_random_uuid(),
                        literal(delivery_id),
                        ScheduleItem.review_id,
                        ScheduleItem.likes_count,
                        literal(active),
                    ).where(ScheduleItem.run_id == run.run_id, ScheduleItem.user_id == user_id),
                    include_defaults=False,
                )
            )
            if not active:
                await session.execute(
                    update(ScheduleItem)
                    .where(
                        ScheduleItem.run_id == run.run_id,
                        ScheduleItem.user_id == user_id,
                    )
                    .values(active=False)
                )
        else:
            await session.execute(
                insert(DeliveryItem).from_select(
                    ["item_id", "delivery_id", "film_id", "active"],
                    select(
                        func.gen_random_uuid(),
                        literal(delivery_id),
                        FilmAnnouncement.film_id,
                        literal(active),
                    ).where(
                        FilmAnnouncement.assigned_run_id == run.run_id,
                        FilmAnnouncement.included_in_digest.is_(True),
                    ),
                    include_defaults=False,
                )
            )
        if active:
            session.add(Outbox(delivery_id=delivery_id, available_at=due))

    async def recover(self, limit: int) -> None:
        """Drain available runs, including snapshots created by a previous process."""
        async with self.database.transaction() as session:
            run_ids = (
                await session.scalars(
                    select(ScheduleRun.run_id)
                    .where(
                        ScheduleRun.status.in_(("pending", "expanding")),
                        or_(
                            ScheduleRun.lease_until.is_(None),
                            ScheduleRun.lease_until <= func.clock_timestamp(),
                        ),
                    )
                    .order_by(ScheduleRun.period_start, ScheduleRun.run_id)
                )
            ).all()
        for run_id in run_ids:
            while await self.expand_run(run_id, limit):
                pass


@lru_cache(maxsize=1)
def get_scheduler() -> Scheduler:
    settings = Settings()  # type: ignore[call-arg]
    if settings.process != "scheduler":
        raise ValueError("scheduler requires NOTIFICATIONS_PROCESS=scheduler")
    assert settings.likes_min_delta is not None
    assert settings.likes_bootstrap_load_id is not None
    return Scheduler(
        get_database(),
        likes_min_delta=settings.likes_min_delta,
        likes_bootstrap_load_id=settings.likes_bootstrap_load_id,
        timing=settings,
    )


async def schedule_likes(period_start: datetime) -> UUID | None:
    return await get_scheduler().schedule_likes(period_start)


async def schedule_films(period_start: datetime) -> UUID | None:
    return await get_scheduler().schedule_films(period_start)


async def expand_run(run_id: UUID, limit: int) -> int:
    return await get_scheduler().expand_run(run_id, limit)


def period_start(now: datetime, interval_seconds: int, anchor: datetime) -> datetime | None:
    """Latest configured UTC boundary; a future anchor delays the first run."""
    if now.utcoffset() != timedelta(0) or anchor.utcoffset() != timedelta(0):
        raise ValueError("schedule timestamps must be timezone-aware UTC")
    if interval_seconds < 1:
        raise ValueError("schedule interval must be positive")
    if now < anchor:
        return None
    interval = timedelta(seconds=interval_seconds)
    return anchor + ((now - anchor) // interval) * interval


async def serve(args: argparse.Namespace) -> None:
    settings = Settings()  # type: ignore[call-arg]
    if settings.process != "scheduler":
        raise ValueError("scheduler requires NOTIFICATIONS_PROCESS=scheduler")
    assert settings.likes_min_delta is not None
    assert settings.likes_bootstrap_load_id is not None
    assert settings.likes_interval_seconds is not None
    assert settings.films_interval_seconds is not None
    database = Database(settings.database_url)
    scheduler = Scheduler(
        database,
        likes_min_delta=settings.likes_min_delta,
        likes_bootstrap_load_id=settings.likes_bootstrap_load_id,
        timing=settings,
        lease_seconds=args.lease_seconds,
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    try:
        while not stopped.is_set():
            try:
                now = datetime.now(UTC)
                likes_period = period_start(
                    now, settings.likes_interval_seconds, settings.likes_schedule_anchor_utc
                )
                films_period = period_start(
                    now, settings.films_interval_seconds, settings.films_schedule_anchor_utc
                )
                if likes_period is not None:
                    await scheduler.schedule_likes(likes_period)
                if films_period is not None:
                    await scheduler.schedule_films(films_period)
                await scheduler.recover(settings.scheduler_batch_size)
            except Exception:
                if args.once:
                    raise
                logger.exception("Scheduler cycle failed; durable work will resume")
            if args.once:
                break
            try:
                await asyncio.wait_for(stopped.wait(), timeout=args.poll_seconds)
            except TimeoutError:
                pass
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--lease-seconds", type=float, default=60)
    parser.add_argument("--poll-seconds", type=float, default=1)
    args = parser.parse_args()
    if min(args.lease_seconds, args.poll_seconds) <= 0:
        parser.error("lease and poll interval must be positive")
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(args))


if __name__ == "__main__":
    main()
