"""Confirmed, mandatory RabbitMQ publication backed by PostgreSQL outbox."""

import argparse
import asyncio
import logging
import math
from collections.abc import Mapping
from datetime import datetime, timedelta
from types import TracebackType
from uuid import uuid4

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue, AbstractRobustConnection
from pamqp.commands import Basic
from sqlalchemy import func, or_, select

from notifications.config import Settings
from notifications.db.models import Delivery, Outbox
from notifications.db.session import Database
from notifications.delivery.routing import SEND_CLASSES, SendClass, Topology

LOG = logging.getLogger(__name__)


class Broker:
    def __init__(self, url: str, namespace: str = "") -> None:
        self.url, self.topology = url, Topology(namespace)
        self.connection: AbstractRobustConnection
        self.channel: AbstractChannel
        self.exchange: AbstractExchange
        self.dead_exchange: AbstractExchange
        self.queues: dict[SendClass, AbstractQueue] = {}
        self.dead_queue: AbstractQueue

    async def __aenter__(self) -> "Broker":
        self.connection = await aio_pika.connect_robust(self.url, timeout=10)
        try:
            self.channel = await self.connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            self.exchange = await self.channel.declare_exchange(
                self.topology.exchange, aio_pika.ExchangeType.DIRECT, durable=True
            )
            self.dead_exchange = await self.channel.declare_exchange(
                self.topology.dead_exchange, aio_pika.ExchangeType.DIRECT, durable=True
            )
            self.dead_queue = await self.channel.declare_queue(
                self.topology.dead_queue, durable=True, arguments={"x-queue-type": "quorum"}
            )
            await self.dead_queue.bind(
                self.dead_exchange, routing_key=self.topology.dead_routing_key
            )
            for send_class in SEND_CLASSES:
                queue = await self.channel.declare_queue(
                    self.topology.queue(send_class),
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-dead-letter-exchange": self.topology.dead_exchange,
                        "x-dead-letter-routing-key": self.topology.dead_routing_key,
                        "x-dead-letter-strategy": "at-least-once",
                        "x-overflow": "reject-publish",
                    },
                )
                await queue.bind(self.exchange, routing_key=send_class)
                self.queues[send_class] = queue
        except BaseException:
            await self.connection.close()
            raise
        return self

    async def __aexit__(
        self,
        typ: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.connection.close()

    async def publish(self, delivery_id: str, *, send_class: SendClass, dead: bool = False) -> None:
        if send_class not in SEND_CLASSES:
            raise ValueError(f"Unknown send class: {send_class}")
        exchange = self.dead_exchange if dead else self.exchange
        confirmation = await exchange.publish(
            aio_pika.Message(
                delivery_id.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="text/plain",
                message_id=delivery_id,
            ),
            routing_key=self.topology.dead_routing_key if dead else send_class,
            mandatory=True,
            timeout=10,
        )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("RabbitMQ did not confirm publication")


class Publisher:
    def __init__(self, database: Database, broker: Broker, lease_seconds: int = 60):
        if lease_seconds <= 10:
            raise ValueError("publisher lease must exceed publish timeout (10 seconds)")
        self.database, self.broker, self.lease_seconds = database, broker, lease_seconds

    async def publish_ready(self, limits: Mapping[SendClass, int]) -> int:
        if set(limits) != set(SEND_CLASSES) or any(
            type(limit) is not int or limit < 1 for limit in limits.values()
        ):
            raise ValueError("a positive integer batch size is required for every send class")
        count = 0
        errors: list[Exception] = []
        for send_class in SEND_CLASSES:
            try:
                count += await self.publish_class(send_class, limits[send_class])
            except Exception as error:
                # A broken route must not prevent independent classes from advancing.
                errors.append(error)
        if errors:
            raise ExceptionGroup("RabbitMQ publication failed", errors)
        return count

    async def publish_class(self, send_class: SendClass, limit: int) -> int:
        # Claim one at a time: later rows never wait behind a whole batch's timeout.
        count = 0
        for _ in range(limit):
            token = uuid4()
            async with self.database.transaction() as s:
                row = await s.scalar(
                    select(Outbox)
                    .join(Delivery, Delivery.delivery_id == Outbox.delivery_id)
                    .where(
                        Delivery.send_class == send_class,
                        Outbox.published_at.is_(None),
                        Outbox.available_at <= func.now(),
                        or_(Outbox.lease_until.is_(None), Outbox.lease_until <= func.now()),
                    )
                    .order_by(Outbox.available_at, Outbox.outbox_id)
                    .limit(1)
                    .with_for_update(of=Outbox, skip_locked=True)
                )
                if row is None:
                    break
                now: datetime = (await s.execute(select(func.clock_timestamp()))).scalar_one()
                row.lease_token, row.lease_until = (
                    token,
                    now + timedelta(seconds=self.lease_seconds),
                )
                oid, did = row.outbox_id, row.delivery_id
            try:
                await self.broker.publish(str(did), send_class=send_class)
            except BaseException:
                async with self.database.transaction() as s:
                    row = await s.get(Outbox, oid, with_for_update=True)
                    if row is not None and row.lease_token == token:
                        row.lease_token = row.lease_until = None
                raise
            async with self.database.transaction() as s:
                row = await s.get(Outbox, oid, with_for_update=True)
                now = (await s.execute(select(func.clock_timestamp()))).scalar_one()
                if (
                    row is not None
                    and row.lease_token == token
                    and row.lease_until is not None
                    and row.lease_until > now
                ):
                    row.published_at = now
                    row.lease_token = row.lease_until = None
                    count += 1
        return count


async def publish_ready() -> int:
    settings = Settings()  # type: ignore[call-arg]
    db = Database(settings.database_url)
    try:
        async with Broker(str(settings.rabbitmq_url)) as broker:
            return await Publisher(db, broker).publish_ready(configured_limits(settings))
    finally:
        await db.close()


def configured_limits(settings: Settings) -> dict[SendClass, int]:
    return {
        send_class: getattr(settings, f"{send_class}_batch_size") for send_class in SEND_CLASSES
    }


async def run(args: argparse.Namespace) -> None:
    settings = Settings()  # type: ignore[call-arg]
    db = Database(settings.database_url)
    limits = configured_limits(settings)
    try:
        while True:
            try:
                async with Broker(str(settings.rabbitmq_url), args.namespace) as broker:
                    publisher = Publisher(db, broker, args.lease_seconds)
                    while True:
                        count = await publisher.publish_ready(limits)
                        LOG.info("published=%d", count)
                        if not count:
                            await asyncio.sleep(args.poll_seconds)
            except Exception:
                LOG.exception("publisher iteration failed")
                await asyncio.sleep(args.poll_seconds)
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--poll-seconds", type=float, required=True)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--namespace", default="")
    args = parser.parse_args()
    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0 or args.lease_seconds <= 10:
        parser.error("positive poll and lease > 10 are required")
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
