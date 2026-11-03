"""Real PostgreSQL checks for snapshot, fan-out, lease and restart boundaries."""

import asyncio
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from contact_fixtures import contact_profile
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from notifications.config import TimingSettings
from notifications.db.models import (
    Delivery,
    DeliveryItem,
    FilmAnnouncement,
    FilmCard,
    LoadState,
    Outbox,
    Profile,
    ReviewCounter,
    ScheduleItem,
    ScheduleRecipient,
    ScheduleRun,
)
from notifications.db.session import Database

PERIOD = datetime(2026, 1, 1, tzinfo=UTC)
LOAD = "historical-reviews"


@pytest.fixture
async def db(database_url):
    database = Database(database_url)
    try:
        yield database
    finally:
        await database.close()


def scheduler(db, **kwargs):
    from notifications.scheduling.scheduler import Scheduler

    return Scheduler(
        db,
        timing=kwargs.pop("timing", TimingSettings()),
        likes_min_delta=3,
        likes_bootstrap_load_id=LOAD,
        **kwargs,
    )


async def seed(db, *, users=1, completed=True):
    ids = [UUID(int=i + 1) for i in range(users)]
    async with db.transaction() as session:
        session.add_all(
            contact_profile(
                user_id=user_id,
                user_version=1,
                email=f"{user_id}@example.test",
                tz="UTC",
                likes=True,
                new_films=True,
                marketing=False,
            )
            for user_id in ids
        )
        if completed is not None:
            session.add(
                LoadState(
                    load_id=LOAD,
                    expected_count=0,
                    manifest_hash="0" * 64,
                    completed_at=PERIOD if completed else None,
                )
            )
    return ids


async def reviews(db, user_id, values):
    async with db.transaction() as session:
        session.add_all(
            ReviewCounter(
                review_id=f"{number:024x}",
                user_id=user_id,
                likes_count=current,
                likes_version=99,
                last_sent_likes_count=baseline,
            )
            for number, current, baseline in values
        )


async def film(db, *, detected_at=PERIOD):
    film_id = uuid4()
    async with db.transaction() as session:
        session.add(
            FilmCard(
                film_id=film_id,
                film_version=1,
                catalog_published_at=PERIOD,
                catalog_updated_at=PERIOD,
                title="Premiere",
                genres=[],
            )
        )
        await session.flush()
        session.add(FilmAnnouncement(film_id=film_id, detected_at=detected_at))
    return film_id


async def count(db, model):
    async with db.transaction() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def trigger(db, table, body):
    async with db.transaction() as session:
        await session.execute(
            text(
                f"CREATE FUNCTION fail_write() RETURNS trigger LANGUAGE plpgsql AS $$ "
                f"BEGIN {body}; RETURN NEW; END $$"
            )
        )
        await session.execute(
            text(
                f"CREATE TRIGGER fail_write BEFORE INSERT ON {table} "
                "FOR EACH ROW EXECUTE FUNCTION fail_write()"
            )
        )


@pytest.mark.parametrize("completed", [None, False])
async def test_likes_require_configured_completed_bootstrap(db, completed):
    users = await seed(db, completed=completed)
    await reviews(db, users[0], [(1, 10, 0)])
    async with db.transaction() as session:
        session.add(
            LoadState(
                load_id="another", expected_count=0, manifest_hash="0" * 64, completed_at=PERIOD
            )
        )
    assert await scheduler(db).schedule_likes(PERIOD) is None
    assert await count(db, ScheduleRun) == 0
    assert await count(db, ScheduleItem) == 0
    async with db.transaction() as session:
        state = await session.get(LoadState, LOAD)
        if state is None:
            session.add(
                LoadState(
                    load_id=LOAD, expected_count=0, manifest_hash="0" * 64, completed_at=PERIOD
                )
            )
        else:
            state.completed_at = PERIOD
    assert await scheduler(db).schedule_likes(PERIOD) is not None


async def test_likes_threshold_grouping_and_baseline_remain_unchanged(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 12, 10), (2, 13, 10), (3, 9, 10), (4, 10, 10), (5, 4, 0)])
    worker = scheduler(db)
    run = await worker.schedule_likes(PERIOD)
    assert run is not None
    async with db.transaction() as session:
        # Later live projection changes must not change the selected email counts.
        await session.execute(
            update(ReviewCounter)
            .where(ReviewCounter.review_id == f"{2:024x}")
            .values(likes_count=20)
        )
    assert await worker.expand_run(run, 10) == 1
    assert await worker.expand_run(run, 10) == 0
    async with db.transaction() as session:
        assert (await session.scalars(select(Delivery.user_id))).all() == users
        items = (
            await session.execute(
                select(DeliveryItem.review_id, DeliveryItem.likes_count).order_by(
                    DeliveryItem.review_id
                )
            )
        ).all()
        assert items == [(f"{2:024x}", 13), (f"{5:024x}", 4)]
        counters = (
            await session.scalars(select(ReviewCounter).order_by(ReviewCounter.review_id))
        ).all()
        assert [r.last_sent_likes_count for r in counters] == [10, 10, 10, 10, 0]
        assert all(r.last_notification_sent_at is None for r in counters)
        assert (await session.get(ScheduleRun, run)).status == "completed"
    assert await count(db, Outbox) == 1


async def test_two_schedulers_same_period_and_distinct_period_reservations(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    first, second = scheduler(db), scheduler(db)
    runs = await asyncio.gather(first.schedule_likes(PERIOD), second.schedule_likes(PERIOD))
    assert runs[0] == runs[1] and runs[0] is not None
    assert await count(db, ScheduleRun) == 1
    next_run = await second.schedule_likes(PERIOD + timedelta(days=1))
    assert next_run is not None
    assert await count(db, ScheduleItem) == 1
    await asyncio.gather(first.expand_run(runs[0], 1), second.expand_run(runs[0], 1))
    assert await count(db, Delivery) == 1
    assert await count(db, Outbox) == 1
    assert await first.expand_run(next_run, 1) == 0
    # Completing fan-out does not release either reservation until terminal delivery.
    third = await first.schedule_likes(PERIOD + timedelta(days=2))
    assert third is not None
    assert await first.expand_run(third, 1) == 0
    async with db.transaction() as session:
        assert (await session.scalar(select(ScheduleItem.active))) is True
        assert (await session.scalar(select(DeliveryItem.active))) is True


async def test_concurrent_different_periods_reserve_review_once(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    await asyncio.gather(
        scheduler(db).schedule_likes(PERIOD),
        scheduler(db).schedule_likes(PERIOD + timedelta(days=1)),
    )
    assert await count(db, ScheduleItem) == 1
    assert await count(db, ScheduleRecipient) == 1


@pytest.mark.parametrize("kind", ["likes", "new_films"])
async def test_snapshot_failure_rolls_back_run_and_candidates(db, kind):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    film_id = await film(db)
    await trigger(db, "schedule_recipients", "RAISE EXCEPTION 'snapshot fault'")
    worker = scheduler(db)
    method = worker.schedule_likes if kind == "likes" else worker.schedule_films
    with pytest.raises(DBAPIError, match="snapshot fault"):
        await method(PERIOD)
    assert await count(db, ScheduleRun) == 0
    assert await count(db, ScheduleItem) == 0
    async with db.transaction() as session:
        assert (await session.get(FilmAnnouncement, film_id)).assigned_run_id is None
        await session.execute(text("DROP TRIGGER fail_write ON schedule_recipients"))
    assert await method(PERIOD) is not None


@pytest.mark.parametrize("kind", ["likes", "new_films"])
async def test_snapshot_excludes_opt_out_deleted_and_later_profiles(db, kind):
    users = await seed(db, users=3)
    for i, user in enumerate(users):
        await reviews(db, user, [(i + 1, 3, 0)])
    await film(db)
    async with db.transaction() as session:
        await session.execute(
            update(Profile).where(Profile.user_id == users[1]).values(likes=False, new_films=False)
        )
        await session.execute(
            update(Profile)
            .where(Profile.user_id == users[2])
            .values(likes=False, new_films=False, marketing=False, deleted_at=PERIOD)
        )
    worker = scheduler(db)
    run = await (
        worker.schedule_likes(PERIOD) if kind == "likes" else worker.schedule_films(PERIOD)
    )
    assert run is not None
    async with db.transaction() as session:
        session.add(
            contact_profile(
                user_id=UUID(int=4), user_version=1, likes=True, new_films=True, marketing=False
            )
        )
        await session.execute(
            update(Profile).where(Profile.user_id == users[1]).values(likes=True, new_films=True)
        )
    await worker.expand_run(run, 10)
    async with db.transaction() as session:
        assert (await session.scalars(select(Delivery.user_id))).all() == [users[0]]


@pytest.mark.parametrize("kind", ["likes", "new_films"])
@pytest.mark.parametrize("deleted", [False, True])
async def test_opt_out_after_snapshot_cancels_and_releases(db, kind, deleted):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    await film(db)
    worker = scheduler(db)
    run = await (
        worker.schedule_likes(PERIOD) if kind == "likes" else worker.schedule_films(PERIOD)
    )
    assert run is not None
    async with db.transaction() as session:
        await session.execute(
            update(Profile).values(
                likes=False,
                new_films=False,
                marketing=False,
                deleted_at=PERIOD if deleted else None,
            )
        )
    assert await worker.expand_run(run, 10) == 1
    assert await count(db, Outbox) == 0
    async with db.transaction() as session:
        assert (await session.scalar(select(Delivery.status))) == "cancelled"
        assert not (await session.scalars(select(ScheduleItem).where(ScheduleItem.active))).all()
        assert not (await session.scalars(select(DeliveryItem).where(DeliveryItem.active))).all()
        assert (await session.scalar(select(ReviewCounter.last_sent_likes_count))) == 0


async def test_films_allocated_once_with_detected_cutoff_and_frozen_audience(db):
    users = await seed(db, completed=False)
    old = await film(db, detected_at=PERIOD - timedelta(days=1))
    due = await film(db)
    future = await film(db, detected_at=PERIOD + timedelta(seconds=1))
    first, second = scheduler(db), scheduler(db)
    runs = await asyncio.gather(first.schedule_films(PERIOD), second.schedule_films(PERIOD))
    assert runs[0] == runs[1] and runs[0] is not None
    later = await first.schedule_films(PERIOD + timedelta(days=1))
    assert later is not None
    await first.expand_run(runs[0], 10)
    await second.expand_run(later, 10)
    async with db.transaction() as session:
        assigned = dict(
            (
                await session.execute(
                    select(FilmAnnouncement.film_id, FilmAnnouncement.assigned_run_id)
                )
            ).all()
        )
        assert assigned == {old: runs[0], due: runs[0], future: later}
        deliveries = (await session.scalars(select(Delivery))).all()
        assert len(deliveries) == 2 and all(d.user_id == users[0] for d in deliveries)
        assert await session.scalar(select(func.count()).select_from(DeliveryItem)) == 3


@pytest.mark.parametrize("kind", ["likes", "new_films"])
async def test_empty_issue_is_durable_and_has_no_deliveries(db, kind):
    await seed(db)
    worker = scheduler(db)
    method = worker.schedule_likes if kind == "likes" else worker.schedule_films
    run = await method(PERIOD)
    assert run is not None
    assert await worker.expand_run(run, 10) == 0
    assert await method(PERIOD) == run
    assert await count(db, Delivery) == 0
    async with db.transaction() as session:
        assert (await session.get(ScheduleRun, run)).status == "completed"


@pytest.mark.parametrize("kind", ["likes", "new_films"])
async def test_fanout_crash_rolls_back_entire_batch_and_restart_resumes_cursor(db, kind):
    users = await seed(db, users=3)
    for i, user in enumerate(users):
        await reviews(db, user, [(i + 1, 3, 0)])
    await film(db)
    worker = scheduler(db)
    run = await (
        worker.schedule_likes(PERIOD) if kind == "likes" else worker.schedule_films(PERIOD)
    )
    assert run is not None
    assert await worker.expand_run(run, 1) == 1
    await trigger(
        db,
        "outbox",
        "IF (SELECT user_id FROM deliveries "
        "WHERE delivery_id = NEW.delivery_id) = "
        f"'{users[2]}'::uuid THEN RAISE EXCEPTION 'batch fault'; END IF",
    )
    with pytest.raises(DBAPIError, match="batch fault"):
        await worker.expand_run(run, 10)
    assert await count(db, Delivery) == 1
    assert await count(db, DeliveryItem) == 1
    assert await count(db, Outbox) == 1
    async with db.transaction() as session:
        state = await session.get(ScheduleRun, run)
        assert state.cursor_user_id == users[0]
        await session.execute(text("DROP TRIGGER fail_write ON outbox"))
        state.lease_until = datetime.now(UTC) - timedelta(seconds=1)
    restarted = scheduler(db)
    assert await restarted.expand_run(run, 10) == 2
    assert await restarted.expand_run(run, 10) == 0
    assert await count(db, Delivery) == 3
    assert await count(db, DeliveryItem) == 3
    assert await count(db, Outbox) == 3


async def test_stale_owner_cannot_expand_after_takeover(db):
    from notifications.scheduling.scheduler import LeaseLost

    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    first, second = scheduler(db), scheduler(db)
    run = await first.schedule_likes(PERIOD)
    assert run is not None
    old = await first.claim_run(run)
    assert old is not None
    assert await second.claim_run(run) is None
    async with db.transaction() as session:
        await session.execute(
            update(ScheduleRun)
            .where(ScheduleRun.run_id == run)
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    new = await second.claim_run(run)
    assert new is not None and new != old
    with pytest.raises(LeaseLost):
        await first.expand_claim(run, old, 10)
    assert await count(db, Delivery) == 0
    assert await second.expand_claim(run, new, 10) == 1


async def test_expiry_during_fanout_rolls_back_cursor_and_writes(db):
    from notifications.scheduling.scheduler import LeaseLost

    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    await trigger(db, "outbox", "PERFORM pg_sleep(0.3)")
    worker = scheduler(db, lease_seconds=0.2)
    run = await worker.schedule_likes(PERIOD)
    assert run is not None
    with pytest.raises(LeaseLost):
        await worker.expand_run(run, 10)
    assert await count(db, Delivery) == 0
    assert await count(db, DeliveryItem) == 0
    assert await count(db, Outbox) == 0
    async with db.transaction() as session:
        assert (await session.get(ScheduleRun, run)).cursor_user_id is None
        await session.execute(text("DROP TRIGGER fail_write ON outbox"))
    assert await scheduler(db).expand_run(run, 10) == 1


async def test_standalone_once_schedules_and_recovers_existing_run(db, database_url):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    run = await scheduler(db).schedule_likes(PERIOD)
    assert run is not None
    token = await scheduler(db).claim_run(run)
    assert token is not None
    async with db.transaction() as session:
        await session.execute(
            update(ScheduleRun)
            .where(ScheduleRun.run_id == run)
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "notifications.scheduling.scheduler",
        "--once",
        env={
            **os.environ,
            "NOTIFICATIONS_PROCESS": "scheduler",
            "NOTIFICATIONS_SCHEDULER_BATCH_SIZE": "1",
            "NOTIFICATIONS_DATABASE_URL": database_url,
            "NOTIFICATIONS_LIKES_MIN_DELTA": "3",
            "NOTIFICATIONS_LIKES_BOOTSTRAP_LOAD_ID": LOAD,
            "NOTIFICATIONS_LIKES_INTERVAL_SECONDS": "3600",
            "NOTIFICATIONS_FILMS_INTERVAL_SECONDS": "3600",
            "NOTIFICATIONS_INVALID_TZ_RETRY_SECONDS": "120",
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
    assert process.returncode == 0, (stdout, stderr)
    assert await count(db, Delivery) == 1
    async with db.transaction() as session:
        assert (await session.get(ScheduleRun, run)).status == "completed"


async def test_non_utc_period_rejected_before_database_work(db):
    worker = scheduler(db)
    with pytest.raises(ValueError, match="UTC"):
        await worker.schedule_likes(datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="UTC"):
        await worker.schedule_films(datetime.fromisoformat("2026-01-01T00:00:00+03:00"))
    assert await count(db, ScheduleRun) == 0


async def test_issue_without_audience_consumes_films_without_retroactive_delivery(db):
    await seed(db, users=0)
    first_film = await film(db)
    worker = scheduler(db)
    run = await worker.schedule_films(PERIOD)
    assert run is not None
    assert await worker.expand_run(run, 10) == 0
    async with db.transaction() as session:
        session.add(
            contact_profile(
                user_id=UUID(int=1), user_version=1, likes=True, new_films=True, marketing=False
            )
        )
    later = await worker.schedule_films(PERIOD + timedelta(days=1))
    assert later is not None
    assert await worker.expand_run(later, 10) == 0
    assert await count(db, Delivery) == 0
    async with db.transaction() as session:
        assert (await session.get(FilmAnnouncement, first_film)).assigned_run_id == run


async def test_active_delivery_alone_also_reserves_review(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 10, 0)])
    async with db.transaction() as session:
        delivery_id = uuid4()
        session.add(
            Delivery(
                delivery_id=delivery_id,
                user_id=users[0],
                kind="likes",
                logical_key="already-pending",
            )
        )
        await session.flush()
        session.add(DeliveryItem(delivery_id=delivery_id, review_id=f"{1:024x}", likes_count=5))
    run = await scheduler(db).schedule_likes(PERIOD)
    assert run is not None
    assert await scheduler(db).expand_run(run, 10) == 0
    assert await count(db, ScheduleItem) == 0
    assert await count(db, Delivery) == 1


async def test_terminal_release_uses_sent_snapshot_as_next_delta_baseline(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    worker = scheduler(db)
    run = await worker.schedule_likes(PERIOD)
    assert run is not None
    await worker.expand_run(run, 10)
    # Model Task16 terminal transaction with an independently advanced live count.
    async with db.transaction() as session:
        await session.execute(
            update(ReviewCounter).values(
                likes_count=6, last_sent_likes_count=3, last_notification_sent_at=PERIOD
            )
        )
        await session.execute(update(Delivery).values(status="sent", sent_at=PERIOD))
        await session.execute(update(DeliveryItem).values(active=False))
        await session.execute(update(ScheduleItem).values(active=False))
    later = await worker.schedule_likes(PERIOD + timedelta(days=1))
    assert later is not None
    assert await worker.expand_run(later, 10) == 1
    async with db.transaction() as session:
        active = (await session.scalars(select(DeliveryItem).where(DeliveryItem.active))).one()
        assert active.likes_count == 6
        counter = await session.get(ReviewCounter, f"{1:024x}")
        assert counter.last_sent_likes_count == 3
        assert counter.last_notification_sent_at == PERIOD


@pytest.mark.parametrize("kind", ["likes", "new_films"])
@pytest.mark.parametrize(
    ("tz", "due"),
    [
        ("Europe/Moscow", PERIOD + timedelta(hours=7)),
        (None, PERIOD + timedelta(seconds=120)),
        ("invalid", PERIOD + timedelta(seconds=120)),
    ],
)
@pytest.mark.parametrize("window_start", ["10:00", "08:30"])
async def test_scheduler_outbox_uses_recipient_window(db, monkeypatch, kind, tz, due, window_start):
    from notifications.scheduling import scheduler as scheduler_module

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return PERIOD

    monkeypatch.setattr(scheduler_module, "datetime", FixedDatetime)
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    await film(db)
    worker = scheduler(db, timing=TimingSettings(send_window_start=window_start))
    if tz == "Europe/Moscow" and window_start == "08:30":
        due = PERIOD + timedelta(hours=5, minutes=30)
    run = await (
        worker.schedule_likes(PERIOD) if kind == "likes" else worker.schedule_films(PERIOD)
    )
    assert run is not None
    # The current profile at expansion determines the window, not snapshot-time tz.
    async with db.transaction() as session:
        await session.execute(update(Profile).values(tz=tz))
    assert await worker.expand_run(run, 10) == 1
    async with db.transaction() as session:
        if tz != "Europe/Moscow":
            assert await session.scalar(select(func.count()).select_from(Delivery)) == 0
            assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
            assert not await session.scalar(select(ScheduleItem.active))
            return
        delivery = (await session.scalars(select(Delivery))).one()
        outbox = (await session.scalars(select(Outbox))).one()
        assert delivery.next_attempt_at == outbox.available_at == due
        assert delivery.status == "pending"
        assert await session.scalar(select(func.count()).select_from(DeliveryItem)) == 1
        if kind == "likes":
            assert (await session.scalars(select(ScheduleItem.active))).one()
        assert (await session.get(ScheduleRun, run)).cursor_user_id == users[0]


async def test_per_comment_threshold_accumulates_across_daily_issues(db):
    users = await seed(db)
    await reviews(db, users[0], [(1, 2, 0), (2, 2, 0)])
    worker = scheduler(db)
    first = await worker.schedule_likes(PERIOD)
    assert first is not None
    assert await worker.expand_run(first, 10) == 0
    assert await count(db, ScheduleItem) == 0
    async with db.transaction() as session:
        counters = (await session.scalars(select(ReviewCounter))).all()
        assert all(counter.last_sent_likes_count == 0 for counter in counters)
        assert all(counter.last_notification_sent_at is None for counter in counters)
        await session.execute(
            update(ReviewCounter)
            .where(ReviewCounter.review_id == f"{1:024x}")
            .values(likes_count=3)
        )
    second = await worker.schedule_likes(PERIOD + timedelta(days=1))
    assert second is not None
    assert await worker.expand_run(second, 10) == 1
    async with db.transaction() as session:
        item = (await session.scalars(select(DeliveryItem))).one()
        assert item.review_id == f"{1:024x}"
        assert item.likes_count == 3
        assert (await session.get(ReviewCounter, f"{1:024x}")).last_sent_likes_count == 0


@pytest.mark.parametrize(
    ("interval", "now", "expected"),
    [
        (86400, datetime(2026, 10, 10, 23, 59, tzinfo=UTC), datetime(2026, 10, 10, tzinfo=UTC)),
        (604800, datetime(2026, 10, 14, 23, 59, tzinfo=UTC), datetime(2026, 10, 8, tzinfo=UTC)),
        (604800, datetime(2026, 10, 15, tzinfo=UTC), datetime(2026, 10, 15, tzinfo=UTC)),
    ],
)
def test_daily_and_weekly_periods_are_anchored_to_epoch(interval, now, expected):
    from notifications.scheduling.scheduler import period_start

    assert period_start(now, interval, datetime(1970, 1, 1, tzinfo=UTC)) == expected


@pytest.mark.parametrize("tz", [None, "", "invalid"])
async def test_invalid_timezone_excluded_before_review_reservation(db, tz):
    users = await seed(db)
    await reviews(db, users[0], [(1, 3, 0)])
    async with db.transaction() as session:
        await session.execute(update(Profile).values(tz=tz))
    worker = scheduler(db)
    for day in range(2):
        run = await worker.schedule_likes(PERIOD + timedelta(days=day))
        assert run is not None
        await worker.expand_run(run, 10)
    async with db.transaction() as session:
        for model in (ScheduleItem, ScheduleRecipient, Delivery, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
