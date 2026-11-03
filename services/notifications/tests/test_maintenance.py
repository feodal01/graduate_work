from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from notifications.db.models import Delivery, Outbox, Profile
from notifications.db.session import Database
from notifications.maintenance import Maintenance, MaintenanceSettings


def test_settings_require_explicit_periods():
    with pytest.raises(ValidationError):
        MaintenanceSettings(database_url="postgresql+asyncpg://localhost/test")  # type: ignore[call-arg]


async def test_sweep_and_retention(database_url):
    db = Database(database_url)
    now = datetime.now(UTC)
    async with db.transaction() as session:
        uid = uuid4()
        session.add(
            Profile(
                user_id=uid, user_version=1, tz=None, likes=True, new_films=True, marketing=True
            )
        )
        await session.flush()
        invalid = Delivery(
            user_id=uid,
            logical_key="invalid",
            kind="external",
            external_send_class="service",
            next_attempt_at=now + timedelta(days=10),
        )
        active = Delivery(
            user_id=uid,
            logical_key="active",
            kind="external",
            external_send_class="bulk",
            lease_token=uuid4(),
            lease_until=now + timedelta(hours=1),
        )
        old = Delivery(
            user_id=uid,
            logical_key="old",
            kind="welcome",
            status="skipped",
            terminal_at=now - timedelta(days=2),
            payload={"email": "old@example.test"},
            prepared_at=now - timedelta(days=3),
            message_id="<old@example.test>",
        )
        session.add_all([invalid, active, old])
        await session.flush()
        session.add(Outbox(delivery_id=invalid.delivery_id))
        ids = invalid.delivery_id, active.delivery_id, old.delivery_id
    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    assert await maintenance.run_once() == (1, 1)
    assert await maintenance.run_once() == (0, 0)
    async with db.transaction() as session:
        invalid, active, old = [await session.get(Delivery, did) for did in ids]
        assert invalid is not None and active is not None and old is not None
        assert invalid.status == "skipped"
        assert invalid.terminal_reason == "invalid_timezone"
        assert invalid.terminal_at is not None
        assert invalid.sent_at is None
        assert active.status == "pending"
        assert old.payload is None
        assert old.message_id == "<old@example.test>"
        assert old.prepared_at is not None
        assert await session.scalar(select(Outbox.outbox_id)) is None
    await db.close()


async def test_sweep_respects_advisory_owner_and_retains_uncertainty(database_url):
    from notifications.db.models import (
        DeliveryAttempt,
        DeliveryItem,
        ReviewCounter,
        ScheduleItem,
        ScheduleRun,
    )
    from notifications.delivery.ownership import delivery_ownership

    db = Database(database_url)
    now = datetime.now(UTC)
    async with db.transaction() as session:
        uid, run_id = uuid4(), uuid4()
        session.add(
            Profile(
                user_id=uid,
                user_version=1,
                tz="invalid",
                likes=True,
                new_films=True,
                marketing=True,
            )
        )
        session.add(ScheduleRun(run_id=run_id, kind="likes", period_start=now))
        await session.flush()
        session.add(
            ReviewCounter(
                review_id="a" * 24,
                user_id=uid,
                likes_count=4,
                likes_version=1,
                last_sent_likes_count=0,
            )
        )
        await session.flush()
        delivery = Delivery(
            user_id=uid,
            run_id=run_id,
            logical_key="sending",
            kind="likes",
            status="sending",
            payload={"email": "retained@example.test"},
            prepared_at=now,
            message_id="<retained@example.test>",
            attempt_count=1,
            lease_token=uuid4(),
            lease_until=now - timedelta(seconds=1),
        )
        session.add(delivery)
        await session.flush()
        did = delivery.delivery_id
        session.add(DeliveryAttempt(delivery_id=did, attempt_number=1, outcome="started"))
        session.add(DeliveryItem(delivery_id=did, review_id="a" * 24, likes_count=4))
        session.add(ScheduleItem(run_id=run_id, review_id="a" * 24, user_id=uid, likes_count=4))
    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    async with delivery_ownership(db, did) as connection:
        assert connection is not None
        assert await maintenance.run_once() == (0, 0)
    assert await maintenance.run_once() == (1, 0)
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        assert delivery is not None
        assert delivery.payload == {"email": "retained@example.test"}
        assert delivery.lease_token is None
        attempt = await session.scalar(select(DeliveryAttempt))
        assert attempt is not None
        assert attempt.outcome == "uncertain"
        assert attempt.duplicate_risk
        assert attempt.finished_at is not None
        item = await session.scalar(select(DeliveryItem))
        schedule_item = await session.scalar(select(ScheduleItem))
        counter = await session.get(ReviewCounter, "a" * 24)
        assert item is not None and schedule_item is not None and counter is not None
        assert not item.active
        assert not schedule_item.active
        assert counter.last_sent_likes_count == 0
    await db.close()


async def test_valid_pages_cannot_starve_invalid_or_clear_active_retry(database_url):
    from uuid import UUID

    db = Database(database_url)
    now = datetime.now(UTC)
    async with db.transaction() as session:
        good, bad = uuid4(), uuid4()
        for uid, tz in [(good, "UTC"), (bad, None)]:
            session.add(
                Profile(
                    user_id=uid, user_version=1, tz=tz, likes=True, new_films=True, marketing=True
                )
            )
        await session.flush()
        for number in range(1, 4):
            session.add(
                Delivery(
                    delivery_id=UUID(int=number),
                    user_id=good,
                    logical_key=str(number),
                    kind="external",
                    external_send_class="service",
                )
            )
        session.add(
            Delivery(
                delivery_id=UUID(int=4),
                user_id=bad,
                logical_key="bad",
                kind="external",
                external_send_class="bulk",
            )
        )
        session.add(
            Delivery(
                delivery_id=UUID(int=5),
                user_id=good,
                logical_key="retry",
                kind="welcome",
                status="retry",
                prepared_at=now - timedelta(days=10),
                payload={"email": "active@example.test"},
                message_id="<active@example.test>",
            )
        )
    assert await Maintenance(db, batch_size=1, retention_seconds=1).run_once() == (1, 0)
    async with db.transaction() as session:
        invalid = await session.get(Delivery, UUID(int=4))
        active = await session.get(Delivery, UUID(int=5))
        assert invalid is not None and active is not None
        assert invalid.status == "skipped"
        assert active.payload is not None
    await db.close()


@pytest.mark.parametrize(
    "field",
    ["maintenance_batch_size", "maintenance_poll_seconds", "delivery_payload_retention_seconds"],
)
@pytest.mark.parametrize("value", [0, -1])
def test_settings_reject_nonpositive_values(field, value):
    values = {
        "database_url": "postgresql+asyncpg://localhost/test",
        "maintenance_poll_seconds": 5,
        "delivery_payload_retention_seconds": 60,
        field: value,
    }
    with pytest.raises(ValidationError):
        MaintenanceSettings(**values)
