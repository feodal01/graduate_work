"""Real PG/Redis/SMTP permits and precise lost-reply/commit fault boundaries."""

import asyncio
from datetime import UTC, datetime

import pytest
from contact_fixtures import contact_profile
from pydantic import SecretStr
from sqlalchemy import select, update
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import mail_messages, make_due
from test_delivery import setup as setup

from notifications.db.models import Delivery, DeliveryAttempt, Outbox, Profile


async def assert_deferred(db, did):
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        outbox = await s.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
        assert delivery.attempt_count == 0
        assert await s.scalar(select(DeliveryAttempt)) is None
        assert delivery.next_attempt_at == outbox.available_at
        assert delivery.next_attempt_at > datetime.now(UTC)
        assert delivery.lease_token is delivery.lease_until is None
        assert delivery.status != "sending"
        return delivery.payload, delivery.message_id


@pytest.mark.parametrize("failure", ["denial", "missing", "lost_reply", "outage"])
async def test_fail_closed_without_attempt_and_recovery(setup, endpoints, monkeypatch, failure):
    db, worker, did, uid, _ = setup
    limiter = worker.limiter
    config, client = limiter.settings, limiter._redis
    original = limiter._script
    if failure == "denial":
        # Real depleted bucket; its slow refill leaves a deterministic retry interval.
        worker.limiter = type(limiter)(
            config.model_copy(
                update={
                    "total_rate": 0.003,
                    "transactional_rate": 0.001,
                    "service_rate": 0.001,
                    "bulk_rate": 0.001,
                }
            )
        )
        await client.delete(config.key)
        from test_rate_limit import seed

        await seed(worker.limiter.settings, client, service_tokens=0)
    elif failure == "missing":
        await client.delete(config.key)
    elif failure == "outage":
        worker.limiter = type(limiter)(
            config.model_copy(update={"redis_url": SecretStr("redis://127.0.0.1:1/0")})
        )
    else:

        async def lost_reply(**kwargs):
            result = await original(**kwargs)
            assert result[0] == 1
            raise ConnectionError("reply lost after debit")

        monkeypatch.setattr(limiter, "_script", lost_reply)
    try:
        await worker.send_delivery(did)
        snapshot, message_id = await assert_deferred(db, did)
        if failure == "missing":
            for _ in range(worker.max_attempts + 1):
                await make_due(db, did)
                await worker.send_delivery(did)
                assert await assert_deferred(db, did) == (snapshot, message_id)
            assert not await client.exists(config.key)
        assert await mail_messages(endpoints, uid) == []
        if failure == "lost_reply":
            assert float(await client.hget(config.key, "service_tokens")) < 1000
        monkeypatch.setattr(limiter, "_script", original)
        if worker.limiter is not limiter:
            await worker.limiter.aclose()
        worker.limiter = limiter
        await client.delete(config.key)
        from test_rate_limit import seed

        await seed(config, client)
        await make_due(db, did)
        await worker.send_delivery(did)
        assert len(await mail_messages(endpoints, uid)) == 1
        async with db.transaction() as s:
            delivery = await s.get(Delivery, did)
            assert delivery.attempt_count == 1 and delivery.status == "sent"
            assert (delivery.payload, delivery.message_id) == (snapshot, message_id)
    finally:
        if worker.limiter is not limiter:
            await worker.limiter.aclose()


async def test_optout_during_redis_holds_no_rows_and_consumes_no_attempt(
    setup, endpoints, monkeypatch
):
    db, worker, did, uid, _ = setup
    acquire = worker.limiter.acquire

    async def optout(send_class):
        async with asyncio.timeout(2):
            async with db.transaction() as s:
                await s.execute(update(Profile).where(Profile.user_id == uid).values(likes=False))
                delivery = await s.get(Delivery, did, with_for_update=True)
                assert delivery.attempt_count == 0 and delivery.status != "sending"
        return await acquire(send_class)

    monkeypatch.setattr(worker.limiter, "acquire", optout)
    await worker.send_delivery(did)
    assert await mail_messages(endpoints, uid) == []
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == "cancelled"
        assert (await s.get(Delivery, did)).attempt_count == 0
        assert await s.scalar(select(DeliveryAttempt)) is None


async def test_deferral_commit_failure_nacks_original(setup, endpoints, monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.orm import Session
    from test_delivery import expire
    from test_delivery_routing import isolated_broker

    db, worker, did, uid, _ = setup
    await worker.limiter._redis.delete(worker.limiter.settings.key)
    async with db.transaction() as s:
        await s.execute(update(Outbox).values(published_at=datetime.now(UTC)))
    defer = worker.defer

    async def mark_failure(session, delivery, due):
        await defer(session, delivery, due)
        session.info["fail_deferral_commit"] = True

    def failed_commit(session):
        if session.info.get("fail_deferral_commit"):
            raise RuntimeError("deferral COMMIT failed")

    event.listen(Session, "before_commit", failed_commit)
    monkeypatch.setattr(worker, "defer", mark_failure)
    try:
        async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
            await broker.publish(str(did), send_class="service")
            await worker.handle(await broker.queues["service"].get(timeout=5))
            repeated = await broker.queues["service"].get(timeout=5)
            assert repeated.redelivered
            async with db.transaction() as s:
                assert await s.scalar(select(Outbox).where(Outbox.published_at.is_(None))) is None
                assert (await s.get(Delivery, did)).attempt_count == 0
                assert await s.scalar(select(DeliveryAttempt)) is None
            monkeypatch.setattr(worker, "defer", defer)
            await expire(db, did)
            await worker.handle(repeated)
            await assert_deferred(db, did)
    finally:
        event.remove(Session, "before_commit", failed_commit)
    assert await mail_messages(endpoints, uid) == []


async def add_welcomes(db, worker, did, uid, count):
    from uuid import uuid4

    worker.send_class = "transactional"
    ids = [did]
    async with db.transaction() as s:
        (await s.get(Delivery, did)).kind = "welcome"
        for _ in range(count - 1):
            other, recipient = uuid4(), uuid4()
            s.add(
                contact_profile(
                    user_id=recipient,
                    user_version=1,
                    email=f"{uid}@example.org",
                    likes=True,
                    new_films=True,
                    marketing=False,
                )
            )
            await s.flush()
            s.add(
                Delivery(
                    delivery_id=other, logical_key=str(other), user_id=recipient, kind="welcome"
                )
            )
            await s.flush()
            s.add(Outbox(delivery_id=other))
            ids.append(other)
    return ids


async def test_direct_sends_bound_smtp_and_ownership(setup, endpoints, monkeypatch):
    from contextlib import asynccontextmanager

    import notifications.delivery.worker as module

    db, worker, did, uid, _ = setup
    ids = await add_welcomes(db, worker, did, uid, 8)
    original, ownership = module.transmit, worker.ownership
    active = peak = owned = peak_owned = 0
    reached, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def count_ownership(did):
        nonlocal owned, peak_owned
        owned += 1
        peak_owned = max(peak_owned, owned)
        try:
            async with ownership(did) as conn:
                yield conn
        finally:
            owned -= 1

    async def slow_smtp(*args):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == worker.max_concurrency:
            reached.set()
        try:
            await release.wait()
            await original(*args)
        finally:
            active -= 1

    monkeypatch.setattr(worker, "ownership", count_ownership)
    monkeypatch.setattr(module, "transmit", slow_smtp)
    tasks = [asyncio.create_task(worker.send_delivery(delivery_id)) for delivery_id in ids]
    try:
        await asyncio.wait_for(reached.wait(), 5)
        await asyncio.sleep(0.1)
        assert active == owned == worker.max_concurrency
    finally:
        release.set()
        await asyncio.gather(*tasks)
    assert peak == peak_owned == worker.max_concurrency
    assert len(await mail_messages(endpoints, uid)) == len(ids)


async def test_two_processes_share_one_account_quota(setup, endpoints):
    import os
    import sys

    from test_rate_limit import seed

    db, worker, did, uid, _ = setup
    ids = await add_welcomes(db, worker, did, uid, 2)
    for delivery_id in ids:
        await worker.prepare_delivery(delivery_id)
    limiter = worker.limiter
    config = limiter.settings.model_copy(
        update={
            "total_rate": 0.003,
            "transactional_rate": 0.001,
            "service_rate": 0.001,
            "bulk_rate": 0.001,
            "total_capacity": 3,
            "transactional_capacity": 1,
            "service_capacity": 1,
            "bulk_capacity": 1,
        }
    )
    await limiter._redis.delete(config.key)
    await seed(config, limiter._redis)
    env = {**os.environ}
    for name in (
        "process",
        "database_url",
        "rabbitmq_url",
        "smtp_host",
        "smtp_port",
        "smtp_from",
        "smtp_starttls",
        "fallback_locale",
        "fallback_tz",
    ):
        env["NOTIFICATIONS_" + name.upper()] = str(getattr(worker.settings, name))
    env.update(
        {
            "NOTIFICATIONS_SEND_CLASS": "transactional",
            "NOTIFICATIONS_MAX_CONCURRENCY": "1",
            "NOTIFICATIONS_RETRY_SECONDS": "1",
            "NOTIFICATIONS_MAX_ATTEMPTS": "3",
        }
    )
    for name, value in config.model_dump().items():
        env["NOTIFICATIONS_LIMITER_" + name.upper()] = (
            value.get_secret_value() if hasattr(value, "get_secret_value") else str(value)
        )
    code = (
        "import asyncio,sys; from uuid import UUID; "
        "from notifications.delivery.worker import send_delivery; "
        "asyncio.run(send_delivery(UUID(sys.argv[1])))"
    )
    processes = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            code,
            str(delivery_id),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        for delivery_id in ids
    ]
    results = await asyncio.gather(*(process.communicate() for process in processes))
    assert all(process.returncode == 0 for process in processes), results
    async with db.transaction() as s:
        deliveries = list(await s.scalars(select(Delivery)))
        assert sorted(delivery.attempt_count for delivery in deliveries) == [0, 1]
        assert len(list(await s.scalars(select(DeliveryAttempt)))) == 1
        delayed = next(delivery for delivery in deliveries if delivery.attempt_count == 0)
        outbox = await s.scalar(select(Outbox).where(Outbox.delivery_id == delayed.delivery_id))
        assert outbox.available_at == delayed.next_attempt_at > datetime.now(UTC)
        assert delayed.lease_token is delayed.lease_until is None
    assert len(await mail_messages(endpoints, uid)) == 1


async def test_next_real_attempt_requires_new_permit(setup, endpoints):
    db, worker, did, uid, _ = setup
    smtp_port = worker.settings.smtp_port
    worker.settings.smtp_port = 1
    await worker.send_delivery(did)
    await worker.limiter._redis.delete(worker.limiter.settings.key)
    await make_due(db, did)
    worker.settings.smtp_port = smtp_port
    await worker.send_delivery(did)
    assert await mail_messages(endpoints, uid) == []
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).attempt_count == 1
        assert len(list(await s.scalars(select(DeliveryAttempt)))) == 1
    from test_rate_limit import seed

    await seed(worker.limiter.settings, worker.limiter._redis)
    await make_due(db, did)
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).attempt_count == 2


async def test_permit_wait_has_deadline_without_attempt(setup, endpoints, monkeypatch):
    db, worker, did, uid, _ = setup
    worker.smtp_timeout = 0.05

    async def stalled(send_class):
        await asyncio.sleep(10)
        raise AssertionError("deadline did not cancel limiter")

    monkeypatch.setattr(worker.limiter, "acquire", stalled)
    async with asyncio.timeout(2):
        await worker.send_delivery(did)
    await assert_deferred(db, did)
    assert await mail_messages(endpoints, uid) == []


async def test_consumer_bounds_tasks_and_cancels_before_closing_resources(
    setup, endpoints, monkeypatch
):
    from test_delivery_routing import isolated_broker

    import notifications.delivery.worker as module

    db, worker, did, uid, _ = setup
    ids = await add_welcomes(db, worker, did, uid, 8)
    reached, release = asyncio.Event(), asyncio.Event()
    active = peak = 0

    async def smtp_blocked(*args):
        nonlocal active, peak
        active += 1
        peak = max(active, peak)
        if active == worker.max_concurrency:
            reached.set()
        try:
            await release.wait()
        finally:
            active -= 1

    monkeypatch.setattr(module, "transmit", smtp_blocked)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        await broker.channel.set_qos(prefetch_count=worker.max_concurrency)
        for delivery_id in ids:
            await broker.publish(str(delivery_id), send_class="transactional")
        running = asyncio.create_task(worker.consume(broker, 0.05))
        await asyncio.wait_for(reached.wait(), 5)
        await asyncio.sleep(0.1)
        assert active == peak == worker.max_concurrency
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, 5)
        assert active == 0
        # Cancelling active callbacks has released every session advisory lock.
        for delivery_id in ids:
            async with worker.ownership(delivery_id) as connection:
                assert connection is not None
    assert await mail_messages(endpoints, uid) == []


async def test_database_failure_after_grant_does_not_refund_or_create_attempt(
    setup, endpoints, monkeypatch
):
    from sqlalchemy import event
    from sqlalchemy.orm import Session
    from test_delivery import expire
    from test_rate_limit import seed

    from notifications.delivery.rate_limit import RedisRateLimiter

    db, worker, did, uid, _ = setup
    initial = worker.limiter
    config = initial.settings.model_copy(
        update={
            "total_rate": 0.003,
            "transactional_rate": 0.001,
            "service_rate": 0.001,
            "bulk_rate": 0.001,
            "total_capacity": 3,
            "transactional_capacity": 1,
            "service_capacity": 1,
            "bulk_capacity": 1,
        }
    )
    worker.limiter = RedisRateLimiter(config)
    await initial._redis.delete(config.key)
    await seed(config, initial._redis)

    def fail_attempt_commit(session):
        if any(isinstance(obj, DeliveryAttempt) for obj in session.new):
            raise RuntimeError("attempt COMMIT failed after grant")

    event.listen(Session, "before_commit", fail_attempt_commit)
    try:
        with pytest.raises(RuntimeError, match="after grant"):
            await worker.send_delivery(did)
        assert float(await initial._redis.hget(config.key, "service_tokens")) < 1
        async with db.transaction() as s:
            assert (await s.get(Delivery, did)).attempt_count == 0
            assert await s.scalar(select(DeliveryAttempt)) is None
        await expire(db, did)
        await worker.send_delivery(did)
        await assert_deferred(db, did)
        assert await mail_messages(endpoints, uid) == []
    finally:
        event.remove(Session, "before_commit", fail_attempt_commit)
        await worker.limiter.aclose()
        worker.limiter = initial
