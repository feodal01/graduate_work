"""Real PostgreSQL/RabbitMQ coverage for class isolation and fair publication."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import psycopg
import pytest
from contact_fixtures import contact_profile
from sqlalchemy import select, text, update
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup

from notifications.db.models import Delivery, DeliveryAttempt, Outbox
from notifications.db.session import Database
from notifications.delivery.routing import SendClass


@asynccontextmanager
async def isolated_broker(url):
    from notifications.delivery.publisher import Broker

    async with Broker(url, namespace="test_" + uuid4().hex) as broker:
        try:
            yield broker
        finally:
            for queue in broker.queues.values():
                await queue.delete(if_unused=False, if_empty=False)
            await broker.dead_queue.delete(if_unused=False, if_empty=False)
            await broker.exchange.delete()
            await broker.dead_exchange.delete()


async def seed(db, kinds):
    ids = []
    async with db.transaction() as s:
        for kind in kinds:
            uid, did = uuid4(), uuid4()
            s.add(
                contact_profile(
                    user_id=uid, user_version=1, likes=True, new_films=True, marketing=False
                )
            )
            await s.flush()
            s.add(Delivery(delivery_id=did, user_id=uid, kind=kind, logical_key=str(did)))
            await s.flush()
            s.add(Outbox(delivery_id=did))
            ids.append(did)
    return ids


def test_existing_deliveries_gain_class(database_url):
    from conftest import migrate

    migrate(database_url, "downgrade", "002")
    ids = [uuid4() for _ in range(3)]
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        for i, kind in enumerate(("welcome", "likes", "new_films")):
            uid = uuid4()
            conn.execute(
                "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
                "VALUES (%s,1,true,true,false)",
                (uid,),
            )
            conn.execute(
                "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind) VALUES (%s,%s,%s,%s)",
                (ids[i], str(ids[i]), uid, kind),
            )
            conn.execute(
                "INSERT INTO outbox(outbox_id,delivery_id,available_at,published_at) "
                "VALUES (%s,%s,now()+(%s * interval '1 day'),CASE WHEN %s THEN now() END)",
                (uuid4(), ids[i], int(i == 1), i == 2),
            )
        before = conn.execute("SELECT * FROM outbox ORDER BY delivery_id").fetchall()
    migrate(database_url, "upgrade", "head")
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        classes = []
        for did in ids:
            row = conn.execute(
                "SELECT send_class FROM deliveries WHERE delivery_id=%s", (did,)
            ).fetchone()
            assert row is not None
            classes.append(row[0])
        assert classes == ["transactional", "service", "bulk"]
        assert conn.execute("SELECT * FROM outbox ORDER BY delivery_id").fetchall() == before
        generated = conn.execute(
            "SELECT is_generated,generation_expression FROM information_schema.columns "
            "WHERE table_name='deliveries' AND column_name='send_class'"
        ).fetchone()
        assert generated is not None
        assert generated[0] == "ALWAYS"
        with pytest.raises(psycopg.errors.GeneratedAlways):
            conn.execute("UPDATE deliveries SET send_class='bulk' WHERE delivery_id=%s", (ids[0],))


async def test_three_queues_and_fair_batches(database_url, endpoints):
    from notifications.delivery.publisher import Publisher
    from notifications.delivery.routing import SEND_CLASSES, Topology

    topology = Topology()
    assert topology.exchange == "notifications.email"
    assert [topology.queue(c) for c in SEND_CLASSES] == [
        "email.transactional",
        "email.service",
        "email.bulk",
    ]
    db = Database(database_url)
    try:
        ids = await seed(db, ["new_films"] * 8 + ["welcome", "likes", "welcome"])
        async with db.transaction() as s:
            await s.execute(
                update(Outbox)
                .where(Outbox.delivery_id == ids[-1])
                .values(available_at=datetime.now(UTC) + timedelta(days=1))
            )
        async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
            published_classes = []
            original = broker.publish

            async def record(delivery_id, *, send_class, dead=False):
                await original(delivery_id, send_class=send_class, dead=dead)
                published_classes.append(send_class)

            broker.publish = record
            limits = dict.fromkeys(SEND_CLASSES, 1)
            assert await Publisher(db, broker).publish_ready(limits) == 3
            assert published_classes == list(SEND_CLASSES)
            for send_class in SEND_CLASSES:
                message = await broker.queues[send_class].get(timeout=5)
                assert message.routing_key == send_class
                assert UUID(message.body.decode()) != ids[-1]
                assert message.delivery_mode.value == 2
                await message.ack()
            async with db.transaction() as s:
                delayed = await s.scalar(select(Outbox).where(Outbox.delivery_id == ids[-1]))
                assert delayed is not None
                assert delayed.published_at is None
    finally:
        await db.close()


async def test_wrong_class_requeues_without_smtp(setup, endpoints, monkeypatch):
    db, worker, did, uid, review = setup
    worker.send_class = "bulk"
    async with db.transaction() as s:
        await s.execute(update(Outbox).values(published_at=datetime.now(UTC)))
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        await broker.publish(str(did), send_class="bulk")
        message = await broker.queues["bulk"].get(timeout=5)
        await worker.handle(message)
        assert message.processed
        async with db.transaction() as s:
            delivery = await s.get(Delivery, did)
            assert delivery.attempt_count == 0 and delivery.payload is None
            assert await s.scalar(select(DeliveryAttempt)) is None
            outbox = await s.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
            assert outbox.delivery_id == did
            assert delivery.send_class == "service"


async def test_two_publishers_skip_locked_and_preserve_classes(database_url, endpoints):
    from notifications.delivery.publisher import Publisher
    from notifications.delivery.routing import SEND_CLASSES, class_for_kind

    db = Database(database_url)
    try:
        ids = await seed(db, ["welcome", "likes", "new_films"] * 4)
        async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
            limits = dict.fromkeys(SEND_CLASSES, 2)
            # A locked outbox must not block either process; locking deliveries
            # would unnecessarily serialize independent outbox claims.
            async with db.transaction() as locked:
                await locked.execute(
                    select(Outbox).where(Outbox.delivery_id == ids[0]).with_for_update()
                )
                results = await asyncio.wait_for(
                    asyncio.gather(
                        Publisher(db, broker).publish_ready(limits),
                        Publisher(db, broker).publish_ready(limits),
                    ),
                    timeout=15,
                )
                assert sum(results) == len(ids) - 1
            assert await Publisher(db, broker).publish_ready(limits) == 1
            observed = []
            for send_class in SEND_CLASSES:
                for _ in range(4):
                    message = await broker.queues[send_class].get(timeout=5)
                    did = UUID(message.body.decode())
                    async with db.transaction() as s:
                        delivery = await s.get(Delivery, did)
                        assert delivery is not None
                        assert class_for_kind(delivery.kind) == send_class
                    observed.append(did)
                    await message.ack()
            assert set(observed) == set(ids) and len(observed) == len(ids)
    finally:
        await db.close()


async def test_broken_class_does_not_starve_other_routes(database_url, endpoints):
    from notifications.delivery.publisher import Publisher
    from notifications.delivery.routing import SEND_CLASSES

    db = Database(database_url)
    try:
        ids = await seed(db, ["welcome", "likes", "new_films"])
        async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
            await broker.queues["transactional"].unbind(
                broker.exchange, routing_key="transactional"
            )
            with pytest.raises(ExceptionGroup):
                await Publisher(db, broker).publish_ready(dict.fromkeys(SEND_CLASSES, 1))
            async with db.transaction() as s:
                rows = {row.delivery_id: row for row in await s.scalars(select(Outbox))}
                assert rows[ids[0]].published_at is None
                assert rows[ids[0]].lease_token is None
                assert all(rows[did].published_at is not None for did in ids[1:])
            for send_class in ("service", "bulk"):
                await (await broker.queues[send_class].get(timeout=5)).ack()
            await broker.queues["transactional"].bind(broker.exchange, routing_key="transactional")
            assert await Publisher(db, broker).publish_ready(dict.fromkeys(SEND_CLASSES, 1)) == 1
            await (await broker.queues["transactional"].get(timeout=5)).ack()
    finally:
        await db.close()


async def test_wrong_class_commit_failure_requeues_original_message(setup, endpoints, monkeypatch):
    db, worker, did, uid, review = setup
    worker.send_class = "bulk"
    async with db.transaction() as s:
        await s.execute(update(Outbox).values(published_at=datetime.now(UTC)))
    enqueue = worker.enqueue

    async def fail_after_insert(session, delivery):
        await enqueue(session, delivery)
        raise RuntimeError("cannot commit reroute")

    monkeypatch.setattr(worker, "enqueue", fail_after_insert)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        await broker.publish(str(did), send_class="bulk")
        await worker.handle(await broker.queues["bulk"].get(timeout=5))
        repeated = await broker.queues["bulk"].get(timeout=5)
        assert repeated.redelivered and repeated.body == str(did).encode()
        async with db.transaction() as s:
            assert await s.scalar(select(Outbox).where(Outbox.published_at.is_(None))) is None
            assert (await s.get(Delivery, did)).attempt_count == 0
        monkeypatch.setattr(worker, "enqueue", enqueue)
        await worker.handle(repeated)
        async with db.transaction() as s:
            assert await s.scalar(select(Outbox).where(Outbox.published_at.is_(None))) is not None


async def test_recovery_only_handles_own_class(setup):
    db, worker, did, uid, review = setup
    async with db.transaction() as s:
        await s.execute(text("DELETE FROM outbox"))
    worker.send_class = "bulk"
    assert await worker.recover() == 0
    async with db.transaction() as s:
        assert not await worker.recover_one(s, await s.get(Delivery, did))
        assert await s.scalar(select(Outbox)) is None
    worker.send_class = "service"
    assert await worker.recover() == 1


async def test_recovery_batch_size_from_settings(setup):
    db, worker, *_ = setup
    await seed(db, ["likes", "likes"])
    async with db.transaction() as session:
        await session.execute(text("DELETE FROM outbox"))
    worker.settings = worker.settings.model_copy(update={"recovery_batch_size": 2})
    assert await worker.recover() == 2
    assert await worker.recover() == 1


async def test_publisher_batch_sizes_from_settings(database_url, endpoints, monkeypatch):
    from notifications.config import Settings
    from notifications.delivery.publisher import Publisher, configured_limits

    for send_class, size in (("transactional", 1), ("service", 2), ("bulk", 3)):
        monkeypatch.setenv(f"NOTIFICATIONS_{send_class.upper()}_BATCH_SIZE", str(size))
    settings = Settings(process="publisher", database_url=database_url, rabbitmq_url="amqp://local")
    db = Database(database_url)
    try:
        await seed(db, [kind for kind in ("welcome", "likes", "new_films") for _ in range(4)])
        async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
            assert await Publisher(db, broker).publish_ready(configured_limits(settings)) == 6
            expected_sizes: tuple[tuple[SendClass, int], ...] = (
                ("transactional", 1),
                ("service", 2),
                ("bulk", 3),
            )
            for send_class, expected in expected_sizes:
                count = 0
                while message := await broker.queues[send_class].get(fail=False):
                    await message.ack()
                    count += 1
                assert count == expected
    finally:
        await db.close()


async def test_retry_keeps_class_and_waits_until_due(setup, endpoints):
    from notifications.delivery.publisher import Publisher
    from notifications.delivery.routing import SEND_CLASSES

    db, worker, did, uid, review = setup
    limits = dict.fromkeys(SEND_CLASSES, 1)
    worker.settings.smtp_port = 1
    worker.retry_seconds = 600
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        assert await Publisher(db, broker).publish_ready(limits) == 1
        await worker.handle(await broker.queues["service"].get(timeout=5))
        assert await Publisher(db, broker).publish_ready(limits) == 0
        async with db.transaction() as s:
            delivery = await s.get(Delivery, did)
            assert delivery.send_class == "service" and delivery.status == "retry"
            retry = await s.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
            assert retry.available_at == delivery.next_attempt_at
            retry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        assert await Publisher(db, broker).publish_ready(limits) == 1
        message = await broker.queues["service"].get(timeout=5)
        assert message.body == str(did).encode() and message.routing_key == "service"
        await message.ack()


def test_unknown_kind_and_class_fail_explicitly():
    from notifications.delivery.routing import SEND_CLASSES, Topology, class_for_kind

    assert [class_for_kind(kind) for kind in ("welcome", "likes", "new_films")] == list(
        SEND_CLASSES
    )
    with pytest.raises(ValueError):
        class_for_kind("unknown")
    with pytest.raises(ValueError):
        # Deliberately bypass the literal type to exercise runtime input validation.
        Topology().queue(cast(SendClass, "unknown"))


async def test_confirmed_handoff_with_lost_lease_repeats_same_class(setup, endpoints, monkeypatch):
    from notifications.delivery.publisher import Publisher
    from notifications.delivery.routing import SEND_CLASSES

    db, worker, did, uid, review = setup
    limits = dict.fromkeys(SEND_CLASSES, 1)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        publish = broker.publish

        async def lose_lease(delivery_id, *, send_class, dead=False):
            await publish(delivery_id, send_class=send_class, dead=dead)
            async with db.transaction() as s:
                await s.execute(
                    update(Outbox).values(
                        lease_token=uuid4(), lease_until=datetime.now(UTC) - timedelta(seconds=1)
                    )
                )

        monkeypatch.setattr(broker, "publish", lose_lease)
        assert await Publisher(db, broker).publish_ready(limits) == 0
        async with db.transaction() as s:
            assert (await s.scalar(select(Outbox))).published_at is None
        monkeypatch.setattr(broker, "publish", publish)
        assert await Publisher(db, broker).publish_ready(limits) == 1
        for _ in range(2):
            message = await broker.queues["service"].get(timeout=5)
            assert message.body == str(did).encode() and message.routing_key == "service"
            await message.ack()
