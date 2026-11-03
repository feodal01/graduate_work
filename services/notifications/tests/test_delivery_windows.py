"""Fixed business clock at the worker boundary; PG, Redis and SMTP remain real."""

from datetime import UTC, datetime, time, timedelta

import pytest
from sqlalchemy import select
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import expire, mail_messages
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup

from notifications.db.models import Delivery, DeliveryAttempt, Outbox, Profile


async def clock(worker, monkeypatch, moment):
    async def now(session):
        return moment

    monkeypatch.setattr(worker, "window_now", now)


@pytest.mark.parametrize("kind,send_class", [("likes", "service"), ("new_films", "bulk")])
@pytest.mark.parametrize("hour,allowed", [(9, False), (10, True), (19, True), (20, False)])
async def test_window_before_redis(setup, endpoints, monkeypatch, kind, send_class, hour, allowed):
    db, worker, did, uid, _ = setup
    worker.send_class = send_class
    now = datetime(2026, 10, 10, hour, tzinfo=UTC)
    await clock(worker, monkeypatch, now)
    async with db.transaction() as s:
        (await s.get(Delivery, did)).kind = kind
        (await s.get(Profile, uid)).tz = "UTC"
    client = worker.limiter._redis
    before = await client.hgetall(worker.limiter.settings.key)
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == int(allowed)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.attempt_count == int(allowed)
        if not allowed:
            assert await s.scalar(select(DeliveryAttempt)) is None
            due = now.replace(hour=10) + timedelta(days=int(hour >= 20))
            assert delivery.next_attempt_at == due
            assert (await s.scalar(select(Outbox))).available_at == due
            assert delivery.lease_token is delivery.lease_until is None
            assert await client.hgetall(worker.limiter.settings.key) == before


@pytest.mark.parametrize("tz", [None, "", "Not/AZone"])
@pytest.mark.parametrize("kind,send_class", [("likes", "service"), ("welcome", "transactional")])
async def test_unknown_zone_has_no_fallback_but_welcome_is_immediate(
    setup, endpoints, monkeypatch, tz, kind, send_class
):
    db, worker, did, uid, _ = setup
    worker.send_class = send_class
    now = datetime(2026, 10, 10, 23, tzinfo=UTC)
    await clock(worker, monkeypatch, now)
    async with db.transaction() as s:
        (await s.get(Delivery, did)).kind = kind
        (await s.get(Profile, uid)).tz = tz
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        if kind == "welcome":
            assert delivery.status == "sent" and delivery.attempt_count == 1
        else:
            assert delivery.status == "skipped"
            assert delivery.terminal_reason == "invalid_timezone"
            assert delivery.attempt_count == 0
            assert await s.scalar(select(DeliveryAttempt)) is None
            assert await mail_messages(endpoints, uid) == []


@pytest.mark.parametrize("change", ["timezone", "missing", "closed"])
async def test_fresh_window_after_permit(setup, endpoints, monkeypatch, change):
    db, worker, did, uid, _ = setup
    now = datetime(2026, 10, 10, 19, 59, 59, tzinfo=UTC)
    await clock(worker, monkeypatch, now)
    async with db.transaction() as s:
        (await s.get(Profile, uid)).tz = "UTC"
    acquire = worker.limiter.acquire

    async def changed(send_class):
        decision = await acquire(send_class)
        assert decision.allowed
        if change == "closed":
            await clock(worker, monkeypatch, now + timedelta(seconds=1))
        else:
            async with db.transaction() as s:
                (await s.get(Profile, uid)).tz = "Asia/Tokyo" if change == "timezone" else None
        return decision

    monkeypatch.setattr(worker.limiter, "acquire", changed)
    await worker.send_delivery(did)
    assert await mail_messages(endpoints, uid) == []
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        if change == "missing":
            assert delivery.status == "skipped"
            assert delivery.terminal_reason == "invalid_timezone"
            return
        expected = {
            "closed": datetime(2026, 10, 11, 10, tzinfo=UTC),
            "timezone": datetime(2026, 10, 11, 1, tzinfo=UTC),
            "missing": now + timedelta(seconds=60),
        }[change]
        assert delivery.next_attempt_at == expected
        assert (await s.scalar(select(Outbox))).available_at == expected
        assert delivery.lease_token is delivery.lease_until is None
        assert delivery.attempt_count == 0
        assert await s.scalar(select(DeliveryAttempt)) is None


@pytest.mark.parametrize("path", ["denial", "smtp_retry", "uncertain_recovery"])
@pytest.mark.parametrize("start,end", [(time(10), time(20)), (time(8, 30), time(18, 45))])
async def test_every_retry_due_is_normalized_to_window(setup, monkeypatch, path, start, end):
    db, worker, did, uid, _ = setup
    worker.settings.send_window_start = start
    worker.settings.send_window_end = end
    now = datetime.combine(datetime(2026, 10, 10).date(), end, tzinfo=UTC) - timedelta(seconds=1)
    await clock(worker, monkeypatch, now)
    async with db.transaction() as s:
        (await s.get(Profile, uid)).tz = "UTC"
    worker.retry_seconds = 2
    if path == "denial":
        await worker.limiter._redis.delete(worker.limiter.settings.key)
    elif path == "smtp_retry":
        worker.settings.smtp_port = 1
    else:

        async def crash(*args):
            raise RuntimeError("lost final commit")

        monkeypatch.setattr(worker, "finish", crash)
    if path == "uncertain_recovery":
        with pytest.raises(RuntimeError, match="lost final commit"):
            await worker.send_delivery(did)
        await expire(db, did)
        assert await worker.recover() == 1
    else:
        await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.next_attempt_at == datetime.combine(
            datetime(2026, 10, 11).date(), start, tzinfo=UTC
        )
        assert (await s.scalar(select(Outbox))).available_at == delivery.next_attempt_at
        assert delivery.attempt_count == int(path != "denial")


async def test_worker_sends_in_configured_early_window(setup, endpoints, monkeypatch):
    db, worker, did, uid, _ = setup
    worker.settings.send_window_start = time(8, 30)
    worker.settings.send_window_end = time(18, 45)
    await clock(worker, monkeypatch, datetime(2026, 10, 10, 8, 30, tzinfo=UTC))
    async with db.transaction() as session:
        (await session.get(Profile, uid)).tz = "UTC"
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        assert delivery.status == "sent"
        assert delivery.attempt_count == 1
