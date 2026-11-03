"""A publisher result must apply only to the outbox work it claimed."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup
from test_delivery_routing import isolated_broker

from notifications.db.models import Delivery, DeliveryAttempt, Outbox
from notifications.delivery.publisher import Publisher


async def test_worker_retry_survives_confirmation_of_previous_publication(
    setup, endpoints, monkeypatch
):
    db, worker, did, _, _ = setup
    smtp_port = worker.settings.smtp_port
    worker.settings.smtp_port = 1
    worker.retry_seconds = 600
    published, release = asyncio.Event(), asyncio.Event()
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        publish = broker.publish

        async def hold_confirmation(delivery_id, *, send_class, dead=False):
            await publish(delivery_id, send_class=send_class, dead=dead)
            published.set()
            await release.wait()

        monkeypatch.setattr(broker, "publish", hold_confirmation)
        publisher = Publisher(db, broker)
        publishing = asyncio.create_task(publisher.publish_class("service", 1))
        try:
            await asyncio.wait_for(published.wait(), 5)
            message = await broker.queues["service"].get(timeout=5)
            await worker.handle(message)
            assert message.processed
            async with db.transaction() as session:
                delivery = await session.get(Delivery, did)
                assert delivery.status == "retry" and delivery.attempt_count == 1
                due = delivery.next_attempt_at
                assert due > datetime.now(UTC)

            release.set()
            await asyncio.wait_for(publishing, 5)
            async with db.transaction() as session:
                pending = await session.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
                assert pending is not None, "An old confirmation completed the future retry"
                assert pending.available_at == due

            monkeypatch.setattr(broker, "publish", publish)
            assert await publisher.publish_class("service", 1) == 0
            assert await broker.queues["service"].get(fail=False) is None
            # Advance the due time without running recovery or expiring the publisher lease.
            now = datetime.now(UTC) - timedelta(seconds=1)
            async with db.transaction() as session:
                await session.execute(update(Delivery).values(next_attempt_at=now))
                await session.execute(
                    update(Outbox).where(Outbox.published_at.is_(None)).values(available_at=now)
                )
            assert await publisher.publish_class("service", 1) == 1
            worker.settings.smtp_port = smtp_port
            await worker.handle(await broker.queues["service"].get(timeout=5))
            async with db.transaction() as session:
                delivery = await session.get(Delivery, did)
                attempts = list(
                    await session.scalars(
                        select(DeliveryAttempt).order_by(DeliveryAttempt.attempt_number)
                    )
                )
                assert delivery.status == "sent" and delivery.attempt_count == 2
                assert [attempt.outcome for attempt in attempts] == ["retry", "accepted"]
                assert all(not attempt.duplicate_risk for attempt in attempts)
            assert await broker.queues["service"].get(fail=False) is None
        finally:
            release.set()
            publishing.cancel()
            await asyncio.gather(publishing, return_exceptions=True)


@pytest.mark.parametrize("old_result", ["confirm", "error", "cancel"])
async def test_old_publisher_result_does_not_touch_a_reclaimed_retry(
    setup, endpoints, monkeypatch, old_result
):
    db, worker, did, _, _ = setup
    published = [asyncio.Event(), asyncio.Event()]
    release = [asyncio.Event(), asyncio.Event()]
    calls = 0
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        publish = broker.publish

        async def hold_confirmation(delivery_id, *, send_class, dead=False):
            nonlocal calls
            index = calls
            calls += 1
            await publish(delivery_id, send_class=send_class, dead=dead)
            published[index].set()
            await release[index].wait()
            if index == 0 and old_result == "error":
                raise ConnectionError("confirmation lost after broker accepted the message")

        monkeypatch.setattr(broker, "publish", hold_confirmation)
        first = asyncio.create_task(Publisher(db, broker).publish_class("service", 1))
        tasks = [first]
        try:
            await asyncio.wait_for(published[0].wait(), 5)
            message = await broker.queues["service"].get(timeout=5)
            async with db.transaction() as session:
                delivery = await session.get(Delivery, did, with_for_update=True)
                # Enqueueing work must invalidate the claim even if its due time is unchanged.
                await worker.defer(session, delivery, delivery.next_attempt_at)
            await message.ack()
            second = asyncio.create_task(Publisher(db, broker).publish_class("service", 1))
            received = asyncio.create_task(published[1].wait())
            tasks.extend((second, received))
            await asyncio.wait((second, received), timeout=5, return_when=asyncio.FIRST_COMPLETED)
            assert published[1].is_set(), "A rescheduled row still belongs to the old publisher"
            async with db.transaction() as session:
                row = await session.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
                token, until = row.lease_token, row.lease_until
                assert token is not None and until > datetime.now(UTC)

            if old_result == "cancel":
                first.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await first
            else:
                release[0].set()
                if old_result == "error":
                    with pytest.raises(ConnectionError):
                        await asyncio.wait_for(first, 5)
                else:
                    await asyncio.wait_for(first, 5)
            async with db.transaction() as session:
                row = await session.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
                assert row is not None, "An old confirmation completed the reclaimed retry"
                assert (row.lease_token, row.lease_until) == (token, until)

            release[1].set()
            assert await asyncio.wait_for(second, 5) == 1
            await (await broker.queues["service"].get(timeout=5)).ack()
            async with db.transaction() as session:
                assert (
                    await session.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
                    is None
                )
            assert await broker.queues["service"].get(fail=False) is None
        finally:
            for gate in release:
                gate.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
