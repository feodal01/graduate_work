import asyncio
import json
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email import policy
from email.parser import Parser
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from contact_fixtures import contact, contact_profile
from sqlalchemy import select, update
from test_rate_limit import settings as _redis_settings

from notifications.config import Settings
from notifications.db.models import (
    Delivery,
    DeliveryAttempt,
    DeliveryItem,
    Outbox,
    Profile,
    ReviewCounter,
    ScheduleItem,
    ScheduleRun,
)
from notifications.db.session import Database
from notifications.delivery.routing import SEND_CLASSES

limiter_settings = _redis_settings


@asynccontextmanager
async def international_smtp(smtputf8):
    commands, messages = [], []
    connections = set()

    async def server(reader, writer):
        connections.add(writer)
        try:
            writer.write(b"220 local test\r\n")
            await writer.drain()
            utf8_enabled = False
            while line := await reader.readline():
                commands.append(line)
                if line.startswith(b"EHLO"):
                    writer.write(
                        b"250-local test\r\n250-8BITMIME\r\n250 SMTPUTF8\r\n"
                        if smtputf8
                        else b"250 local test\r\n"
                    )
                elif line.startswith(b"MAIL"):
                    utf8_enabled = smtputf8 and b"SMTPUTF8" in line
                    writer.write(
                        b"250 OK\r\n"
                        if utf8_enabled or line.isascii()
                        else b"553 UTF8 required\r\n"
                    )
                elif line.startswith(b"RCPT"):
                    writer.write(
                        b"250 OK\r\n"
                        if utf8_enabled or line.isascii()
                        else b"553 UTF8 required\r\n"
                    )
                elif line == b"DATA\r\n":
                    writer.write(b"354 send mail\r\n")
                    await writer.drain()
                    data = []
                    while (body_line := await reader.readline()) not in (b".\r\n", b""):
                        data.append(body_line[1:] if body_line.startswith(b"..") else body_line)
                    messages.append(b"".join(data))
                    writer.write(b"250 accepted\r\n")
                else:
                    writer.write(b"500 unsupported command\r\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            connections.discard(writer)

    smtp = await asyncio.start_server(server, "127.0.0.1", 0)
    try:
        yield smtp.sockets[0].getsockname()[1], commands, messages
    finally:
        smtp.close()
        await smtp.wait_closed()
        for writer in tuple(connections):
            writer.close()
            await writer.wait_closed()


@pytest.mark.parametrize(
    "sender,recipient,envelope_sender,envelope_recipient,smtputf8",
    [
        (
            "sender@example.org",
            "user@пример.рф",
            "sender@example.org",
            "user@xn--e1afmkfd.xn--p1ai",
            False,
        ),
        ("sender@faß.de", "user@example.org", "sender@xn--fa-hia.de", "user@example.org", False),
        (
            "sender@example.org",
            "почта@example.org",
            "sender@example.org",
            "почта@example.org",
            True,
        ),
        (
            "отправитель@пример.рф",
            "user@example.org",
            "отправитель@xn--e1afmkfd.xn--p1ai",
            "user@example.org",
            True,
        ),
    ],
)
async def test_international_smtp_acceptance_preserves_snapshot(
    setup, sender, recipient, envelope_sender, envelope_recipient, smtputf8
):
    db, worker, did, uid, review = setup
    worker.settings.smtp_from = sender
    async with db.transaction() as s:
        contact(uid).email = recipient
    snapshot = await worker.prepare_delivery(did)
    async with international_smtp(smtputf8) as (port, commands, messages):
        worker.settings.smtp_host, worker.settings.smtp_port = "127.0.0.1", port
        await worker.send_delivery(did)
    mail = next(line for line in commands if line.startswith(b"MAIL"))
    assert mail.startswith(f"MAIL FROM:<{envelope_sender}>".encode())
    assert (b"SMTPUTF8" in mail) == smtputf8
    assert f"RCPT TO:<{envelope_recipient}>\r\n".encode() in commands
    assert len(messages) == 1
    message = Parser(policy=policy.SMTPUTF8).parsestr(messages[0].decode("utf-8"))
    assert message["From"].addresses[0].addr_spec == envelope_sender
    assert message["To"].addresses[0].addr_spec == envelope_recipient
    assert message["Bcc"] is None
    assert message.get_content().strip() == snapshot.payload["html"].strip()
    if not smtputf8:
        assert messages[0].isascii()
    else:
        assert b"=?utf-8?" not in messages[0].split(b"Subject:")[0].lower()
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        assert delivery.status == "sent" and delivery.sent_at is not None
        assert delivery.payload == snapshot.payload
        assert delivery.payload["to"] == recipient and delivery.payload["from"] == sender
        assert str(message["Message-ID"]) == delivery.message_id
        assert attempt.outcome == "accepted" and not attempt.duplicate_risk
        assert attempt.finished_at is not None


@pytest.mark.parametrize("field", ["from", "to"])
async def test_smtputf8_unsupported_is_known_permanent_failure(setup, field):
    db, worker, did, uid, review = setup
    if field == "from":
        worker.settings.smtp_from = "почта@example.org"
    else:
        async with db.transaction() as s:
            contact(uid).email = "почта@example.org"
    async with international_smtp(False) as (port, commands, messages):
        worker.settings.smtp_host, worker.settings.smtp_port = "127.0.0.1", port
        await worker.send_delivery(did)
    assert not any(line.startswith((b"MAIL", b"RCPT", b"DATA")) for line in commands)
    assert messages == []
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        assert delivery.status == "failed" and delivery.sent_at is None
        assert attempt.outcome == "failed" and not attempt.duplicate_risk
        assert attempt.finished_at is not None
        assert not (await s.scalar(select(DeliveryItem))).active
        assert not (await s.scalar(select(ScheduleItem))).active
        assert (await s.get(ReviewCounter, review)).last_notification_sent_at is None


@pytest.mark.parametrize("field", ["from", "to"])
@pytest.mark.parametrize(
    "address",
    [
        "sender@example.org\r\nBcc: other@example.org",
        "first@example.org,second@example.org",
        "invalid-address",
        "user@invalid_domain.example",
    ],
)
async def test_invalid_smtp_envelope_is_known_failure_before_data(field, address):
    from notifications.delivery.smtp import DeliveryError, transmit

    payload = {
        "from": "sender@example.org",
        "to": "user@example.org",
        "subject": "Тема",
        "html": "<p>Текст</p>",
    }
    payload[field] = address
    async with international_smtp(True) as (port, commands, messages):
        settings = Settings(
            process="bootstrap",
            database_url="postgresql+asyncpg://unused/test",
            smtp_host="127.0.0.1",
            smtp_port=port,
            smtp_starttls=False,
        )
        with pytest.raises(DeliveryError) as error:
            await transmit(settings, payload, "<test@example.org>", 5)
    assert error.value.permanent and not error.value.uncertain
    assert not any(line.startswith(b"DATA") for line in commands)
    assert messages == []


@pytest.fixture(scope="module")
def endpoints():
    root = Path(__file__).resolve().parents[3]
    return json.loads(
        subprocess.check_output(
            [
                sys.executable,
                str(root / "tests/notifications/stack_endpoints.py"),
            ],
            text=True,
        )
    )


@pytest.fixture
async def delivery_limiter(limiter_settings):
    from test_rate_limit import seed

    from notifications.delivery.rate_limit import RedisRateLimiter

    config = limiter_settings.model_copy(
        update={
            "total_rate": 3000,
            "total_capacity": 3000,
            "transactional_rate": 1000,
            "transactional_capacity": 1000,
            "service_rate": 1000,
            "service_capacity": 1000,
            "bulk_rate": 1000,
            "bulk_capacity": 1000,
        }
    )
    limiter = RedisRateLimiter(config)
    await seed(config, limiter._redis)
    try:
        yield limiter
    finally:
        await limiter._redis.delete(config.key)
        await limiter.aclose()


@pytest.fixture
async def setup(database_url, endpoints, delivery_limiter):
    from notifications.delivery.worker import DeliveryWorker

    db = Database(database_url)
    settings = Settings(
        process="worker",
        database_url=database_url,
        rabbitmq_url=endpoints["RABBITMQ_URL"],
        smtp_host=endpoints["SMTP_HOST"],
        smtp_port=int(endpoints["SMTP_PORT"]),
        smtp_from="sender@example.org",
        smtp_starttls=False,
        fallback_locale="ru",
        fallback_tz="UTC",
    )
    worker = DeliveryWorker(
        db,
        settings,
        send_class="service",
        retry_seconds=1,
        max_attempts=3,
        limiter=delivery_limiter,
        max_concurrency=3,
        unavailable_retry_seconds=7,
    )
    uid, did, rid = uuid4(), uuid4(), uuid4()
    review = uuid4().hex[:24]
    async with db.transaction() as s:
        s.add(
            contact_profile(
                user_id=uid,
                user_version=1,
                email=f"{uid}@example.org",
                first_name="<script>name</script>",
                last_name=None,
                locale=None,
                tz=f"Etc/GMT{datetime.now(UTC).hour - 12:+d}",
                likes=True,
                new_films=True,
                marketing=False,
            )
        )
        await s.flush()
        s.add(ReviewCounter(review_id=review, user_id=uid, likes_count=15, likes_version=1))
        s.add(ScheduleRun(run_id=rid, kind="likes", period_start=datetime.now(UTC)))
        await s.flush()
        s.add(
            Delivery(delivery_id=did, logical_key=str(did), kind="likes", user_id=uid, run_id=rid)
        )
        s.add(ScheduleItem(run_id=rid, review_id=review, user_id=uid, likes_count=15))
        await s.flush()
        s.add(DeliveryItem(delivery_id=did, review_id=review, likes_count=15))
        s.add(Outbox(delivery_id=did))
    yield db, worker, did, uid, review
    await db.close()


async def mail_messages(endpoints, uid):
    async with httpx.AsyncClient() as client:
        result = await client.get(
            endpoints["SMTP_API_URL"] + "/api/v1/search", params={"query": f"to:{uid}@example.org"}
        )
        result.raise_for_status()
        return result.json()["messages"]


async def test_snapshot_acceptance_and_duplicate(setup, endpoints):
    db, worker, did, uid, review = setup
    snapshot = await worker.prepare_delivery(did)
    assert "&lt;script&gt;" in snapshot.payload["html"]
    assert "<script>" not in snapshot.payload["html"]
    async with db.transaction() as s:
        await s.execute(update(ReviewCounter).values(likes_count=18, likes_version=2))
        assert (await s.get(Delivery, did)).sent_at is None
    await worker.send_delivery(did)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        counter = await s.get(ReviewCounter, review)
        assert (counter.likes_count, counter.last_sent_likes_count) == (18, 15)
        assert counter.last_notification_sent_at is not None
        delivery = await s.get(Delivery, did)
        assert delivery.status == "sent" and delivery.sent_at is not None
        assert not (await s.scalar(select(DeliveryItem))).active
        assert not (await s.scalar(select(ScheduleItem))).active
        assert (await s.scalar(select(DeliveryAttempt))).outcome == "accepted"
    assert len(await mail_messages(endpoints, uid)) == 1


@pytest.mark.parametrize(
    "change,status", [("optout", "cancelled"), ("delete", "cancelled"), ("timezone", "skipped")]
)
async def test_recheck_before_smtp(setup, endpoints, change, status):
    db, worker, did, uid, review = setup
    await worker.prepare_delivery(did)
    async with db.transaction() as s:
        profile = await s.get(Profile, uid)
        if change == "optout":
            profile.likes = False
        elif change == "delete":
            profile.deleted_at = datetime.now(UTC)
            profile.likes = profile.new_films = profile.marketing = False
        else:
            profile.tz = None
    await worker.send_delivery(did)
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == status
        assert (await s.get(ReviewCounter, review)).last_notification_sent_at is None
        assert not (await s.scalar(select(ScheduleItem))).active
    assert await mail_messages(endpoints, uid) == []


async def test_refused_smtp_persists_retry(setup):
    db, worker, did, uid, review = setup
    worker.settings.smtp_port = 1
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        outbox = await s.scalar(select(Outbox).where(Outbox.published_at.is_(None)))
        assert delivery.status == "retry" and delivery.sent_at is None
        assert attempt.outcome == "retry" and not attempt.duplicate_risk
        assert outbox.available_at == delivery.next_attempt_at


async def test_broker_confirm_unroutable_and_dlq(setup, endpoints):
    from notifications.delivery.publisher import Broker, Publisher

    db, worker, did, uid, review = setup
    name = "test_" + uuid4().hex
    async with Broker(endpoints["RABBITMQ_URL"], name) as broker:
        try:
            publisher = Publisher(db, broker)
            await broker.queues["service"].unbind(broker.exchange, routing_key="service")
            with pytest.raises(Exception):
                await publisher.publish_ready(dict.fromkeys(SEND_CLASSES, 10))
            async with db.transaction() as s:
                assert (await s.scalar(select(Outbox))).published_at is None
            await broker.queues["service"].bind(broker.exchange, routing_key="service")
            assert await publisher.publish_ready(dict.fromkeys(SEND_CLASSES, 10)) == 1
            message = await broker.queues["service"].get(timeout=5)
            assert message.body == str(did).encode()
            assert message.delivery_mode.value == 2
            await message.reject(requeue=False)
            dead = None
            for _ in range(30):
                dead = await broker.dead_queue.get(fail=False)
                if dead:
                    break
                await asyncio.sleep(0.1)
            assert dead is not None and dead.body == str(did).encode()
            await dead.ack()
        finally:
            for queue in broker.queues.values():
                await queue.delete(if_unused=False, if_empty=False)
            await broker.dead_queue.delete(if_unused=False, if_empty=False)
            await broker.exchange.delete()
            await broker.dead_exchange.delete()


async def expire(db, did):
    async with db.transaction() as s:
        await s.execute(
            update(Delivery)
            .where(Delivery.delivery_id == did)
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )


async def make_due(db, did):
    async with db.transaction() as s:
        await s.execute(
            update(Delivery)
            .where(Delivery.delivery_id == did)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )


async def test_crash_after_real_acceptance_recovers_same_snapshot(setup, endpoints, monkeypatch):
    db, worker, did, uid, review = setup
    finish = worker.finish

    async def crash(*args):
        raise RuntimeError("process crash before PG result commit")

    monkeypatch.setattr(worker, "finish", crash)
    with pytest.raises(RuntimeError, match="process crash"):
        await worker.send_delivery(did)
    messages = await mail_messages(endpoints, uid)
    assert len(messages) == 1
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        old_payload, old_message_id = delivery.payload, delivery.message_id
        assert delivery.status == "sending" and delivery.sent_at is None
        assert (await s.get(ReviewCounter, review)).last_sent_likes_count == 0
    await expire(db, did)
    assert await worker.recover() == 1
    async with db.transaction() as s:
        attempt = await s.scalar(select(DeliveryAttempt))
        assert attempt.outcome == "uncertain" and attempt.duplicate_risk
        assert (await s.scalar(select(ScheduleItem))).active
        contact(uid).first_name = "new name"
    monkeypatch.setattr(worker, "finish", finish)
    await make_due(db, did)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.payload == old_payload and delivery.message_id == old_message_id
        assert delivery.status == "sent"
    messages = await mail_messages(endpoints, uid)
    assert len(messages) == 2
    assert len({message["MessageID"] for message in messages}) == 1


async def test_expired_lease_live_owner_prevents_parallel_smtp(setup, endpoints, monkeypatch):
    import notifications.delivery.worker as module

    db, worker, did, uid, review = setup
    transmit = module.transmit
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args):
        started.set()
        await release.wait()
        await transmit(*args)

    monkeypatch.setattr(module, "transmit", delayed)
    task = asyncio.create_task(worker.send_delivery(did))
    await asyncio.wait_for(started.wait(), 5)
    try:
        await expire(db, did)
        await worker.send_delivery(did)
        assert await worker.recover() == 0
        assert await mail_messages(endpoints, uid) == []
        async with db.transaction() as s:
            await s.execute(
                update(Delivery)
                .where(Delivery.delivery_id == did)
                .values(lease_until=datetime.now(UTC) + timedelta(seconds=60))
            )
    finally:
        release.set()
        await task
    assert len(await mail_messages(endpoints, uid)) == 1


@pytest.mark.parametrize(
    "response,outcome,status,risk",
    [
        (b"451 temporary failure\r\n", "retry", "retry", False),
        (b"550 permanent failure\r\n", "failed", "failed", False),
        (None, "uncertain", "retry", True),
    ],
)
async def test_real_smtp_negative_and_uncertain(setup, response, outcome, status, risk):
    db, worker, did, uid, review = setup

    async def server(reader, writer):
        writer.write(b"220 local test\r\n")
        await writer.drain()
        in_data = False
        while line := await reader.readline():
            if in_data:
                if line != b".\r\n":
                    continue
                if response is not None:
                    writer.write(response)
                    await writer.drain()
                break
            if line.upper().startswith(b"DATA"):
                in_data = True
                writer.write(b"354 send mail\r\n")
            else:
                writer.write(b"250 OK\r\n")
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    smtp = await asyncio.start_server(server, "127.0.0.1", 0)
    try:
        worker.settings.smtp_port = smtp.sockets[0].getsockname()[1]
        await worker.send_delivery(did)
    finally:
        smtp.close()
        await smtp.wait_closed()
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        assert delivery.status == status and delivery.sent_at is None
        assert attempt.outcome == outcome and attempt.duplicate_risk == risk
        assert (await s.get(ReviewCounter, review)).last_notification_sent_at is None
        assert (await s.scalar(select(ScheduleItem))).active == (status != "failed")


async def test_latest_film_at_prepare_then_immutable(setup):
    from notifications.db.models import FilmCard

    db, worker, did, uid, review = setup
    worker.send_class = "bulk"
    film = uuid4()
    async with db.transaction() as s:
        now = datetime.now(UTC)
        s.add(
            FilmCard(
                film_id=film,
                film_version=2,
                title="<b>Latest title</b>",
                description=None,
                creation_date=None,
                imdb_rating=None,
                genres=[],
                catalog_published_at=now,
                catalog_updated_at=now,
            )
        )
        delivery = await s.get(Delivery, did)
        delivery.kind = "new_films"
        await s.flush()
        s.add(DeliveryItem(delivery_id=did, film_id=film))
    snapshot = await worker.prepare_delivery(did)
    assert snapshot.payload["films"][0]["film_version"] == 2
    assert "&lt;b&gt;Latest title&lt;/b&gt;" in snapshot.payload["html"]
    assert "None" not in snapshot.payload["html"] and "<a " not in snapshot.payload["html"]
    async with db.transaction() as s:
        await s.execute(update(FilmCard).values(film_version=3, title="Later title"))
    assert (await worker.prepare_delivery(did)).payload == snapshot.payload


async def test_fenced_completion_cannot_change_result(setup):
    db, worker, did, uid, review = setup
    async with db.engine.connect() as conn:
        with pytest.raises(RuntimeError, match="ownership lost"):
            await worker.finish(conn, did, uuid4(), None)
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == "pending"


async def test_worker_terminal_failure_goes_to_real_dlq(setup, endpoints):
    from notifications.delivery.publisher import Broker, Publisher

    db, worker, did, uid, review = setup
    # Header injection is a permanent local validation failure; never SMTP acceptance.
    worker.settings.smtp_from = "sender@example.org\nBcc: other@example.org"
    name = "test_" + uuid4().hex
    async with Broker(endpoints["RABBITMQ_URL"], name) as broker:
        try:
            assert await Publisher(db, broker).publish_ready(dict.fromkeys(SEND_CLASSES, 10)) == 1
            message = await broker.queues["service"].get(timeout=5)
            await worker.handle(message)
            dead = None
            for _ in range(30):
                dead = await broker.dead_queue.get(fail=False)
                if dead:
                    break
                await asyncio.sleep(0.1)
            assert dead is not None and dead.body == str(did).encode()
            await dead.ack()
            async with db.transaction() as s:
                assert (await s.get(Delivery, did)).status == "failed"
                assert not (await s.scalar(select(DeliveryItem))).active
                assert not (await s.scalar(select(ScheduleItem))).active
                assert (await s.scalar(select(DeliveryAttempt))).outcome == "failed"
            assert await mail_messages(endpoints, uid) == []
        finally:
            for queue in broker.queues.values():
                await queue.delete(if_unused=False, if_empty=False)
            await broker.dead_queue.delete(if_unused=False, if_empty=False)
            await broker.exchange.delete()
            await broker.dead_exchange.delete()


async def test_database_session_lost_after_mailpit_acceptance(setup, endpoints, monkeypatch):
    from sqlalchemy import text

    db, worker, did, uid, review = setup
    finish = worker.finish

    async def kill_connection(conn, *args):
        pid = await conn.scalar(text("SELECT pg_backend_pid()"))
        await conn.commit()
        async with db.transaction() as s:
            assert await s.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        await finish(conn, *args)

    monkeypatch.setattr(worker, "finish", kill_connection)
    with pytest.raises(Exception):
        await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == "sending"
        assert (await s.get(ReviewCounter, review)).last_notification_sent_at is None
    await expire(db, did)
    assert await worker.recover() == 1
    async with db.transaction() as s:
        attempt = await s.scalar(select(DeliveryAttempt))
        assert attempt.outcome == "uncertain" and attempt.duplicate_risk


async def test_exhausted_uncertain_recovery_is_terminal_and_outboxed(setup, monkeypatch):
    db, worker, did, uid, review = setup
    worker.max_attempts = 1

    async def crash(*args):
        raise RuntimeError("crash")

    monkeypatch.setattr(worker, "finish", crash)
    with pytest.raises(RuntimeError):
        await worker.send_delivery(did)
    await expire(db, did)
    await worker.recover()
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == "failed"
        assert not (await s.scalar(select(ScheduleItem))).active
        assert (await s.scalar(select(DeliveryAttempt))).duplicate_risk
        assert await s.scalar(select(Outbox).where(Outbox.published_at.is_(None))) is not None


async def test_welcome_ignores_preferences(setup, endpoints):
    db, worker, did, uid, review = setup
    worker.send_class = "transactional"
    async with db.transaction() as s:
        await s.execute(update(Profile).values(likes=False, new_films=False, marketing=False))
        await s.execute(update(Delivery).values(kind="welcome"))
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1


async def test_recovery_does_not_republish_recent_broker_handoff(setup):
    db, worker, did, uid, review = setup
    async with db.transaction() as s:
        await s.execute(update(Outbox).values(published_at=datetime.now(UTC)))
    assert await worker.recover() == 0
    async with db.transaction() as s:
        assert await s.scalar(select(Outbox).where(Outbox.published_at.is_(None))) is None
        await s.execute(
            update(Outbox).values(
                published_at=datetime.now(UTC) - timedelta(seconds=worker.lease_seconds + 1)
            )
        )
    assert await worker.recover() == 1
    assert await worker.recover() == 0


async def test_acceptance_then_transaction_failure_rolls_back_receipts(
    setup, endpoints, monkeypatch
):
    db, worker, did, uid, review = setup
    terminal = worker.terminal

    async def crash_in_transaction(session, delivery, status):
        await terminal(session, delivery, status)
        if status == "sent":
            raise RuntimeError("crash after receipt writes before commit")

    monkeypatch.setattr(worker, "terminal", crash_in_transaction)
    with pytest.raises(RuntimeError, match="before commit"):
        await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "sending" and delivery.sent_at is None
        assert (await s.scalar(select(DeliveryAttempt))).outcome == "started"
        counter = await s.get(ReviewCounter, review)
        assert counter.last_sent_likes_count == 0 and counter.last_notification_sent_at is None
        assert (await s.scalar(select(DeliveryItem))).active
        assert (await s.scalar(select(ScheduleItem))).active


async def test_optout_between_preparation_and_final_smtp_gate(setup, endpoints, monkeypatch):
    db, worker, did, uid, review = setup
    snapshot = worker.snapshot

    async def optout_after_snapshot(session, delivery):
        result = await snapshot(session, delivery)
        profile = await session.get(Profile, uid)
        profile.likes = False
        return result

    monkeypatch.setattr(worker, "snapshot", optout_after_snapshot)
    await worker.send_delivery(did)
    assert await mail_messages(endpoints, uid) == []
    async with db.transaction() as s:
        assert (await s.get(Delivery, did)).status == "cancelled"
        assert await s.scalar(select(DeliveryAttempt)) is None
        assert (await s.get(Delivery, did)).attempt_count == 0
