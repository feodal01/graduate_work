"""Recovery failures while real SMTP DATA is awaiting its acceptance reply."""

import asyncio
from contextlib import asynccontextmanager
from errno import ENETUNREACH
from socket import EAI_AGAIN, EAI_NONAME, gaierror
from types import SimpleNamespace
from uuid import uuid4

import pytest
from asyncpg import exceptions as pg_errors
from sqlalchemy import select, text, update
from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup
from test_delivery_limits import add_welcomes
from test_delivery_routing import isolated_broker

from notifications.db.models import Delivery, DeliveryAttempt
from notifications.delivery.worker import transient_recovery_error


@pytest.fixture
async def held_smtp():
    received, release = asyncio.Event(), asyncio.Event()
    messages, connections, tasks = [], set(), set()
    arrivals, reply_gates = asyncio.Queue(), []

    async def accept(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        connections.add(writer)
        try:
            writer.write(b"220 local test\r\n")
            await writer.drain()
            body = None
            while line := await reader.readline():
                if body is not None:
                    if line != b".\r\n":
                        body.append(line)
                        continue
                    messages.append(b"".join(body))
                    received.set()
                    arrivals.put_nowait(len(messages))
                    gate = reply_gates[len(messages) - 1] if reply_gates else release
                    await gate.wait()
                    writer.write(b"250 accepted\r\n")
                    body = None
                elif line.upper().startswith(b"DATA"):
                    body = []
                    writer.write(b"354 send mail\r\n")
                else:
                    writer.write(b"250 OK\r\n")
                await writer.drain()
        except ConnectionError:
            pass  # The red regression closes the SMTP connection before acceptance.
        finally:
            writer.close()
            await writer.wait_closed()
            connections.discard(writer)
            tasks.discard(task)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        yield SimpleNamespace(
            port=server.sockets[0].getsockname()[1],
            received=received,
            release=release,
            messages=messages,
            arrivals=arrivals,
            reply_gates=reply_gates,
        )
    finally:
        release.set()
        for gate in reply_gates:
            gate.set()
        server.close()
        await server.wait_closed()
        for writer in tuple(connections):
            writer.close()
        if tasks:
            await asyncio.gather(*tasks)


@pytest.fixture
async def recovery_setup(setup, held_smtp):
    db, worker, did, _, _ = setup
    async with db.transaction() as session:
        await session.execute(
            update(Delivery).where(Delivery.delivery_id == did).values(kind="welcome")
        )
    worker.send_class = "transactional"
    worker.settings.smtp_host = "127.0.0.1"
    worker.settings.smtp_port = held_smtp.port
    return db, worker, did


async def assert_accepted_once(db, did, smtp):
    assert len(smtp.messages) == 1
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        attempts = list(await session.scalars(select(DeliveryAttempt)))
        assert delivery.status == "sent" and delivery.attempt_count == 1
        assert len(attempts) == 1
        assert attempts[0].outcome == "accepted" and not attempts[0].duplicate_risk
        assert attempts[0].finished_at is not None


@pytest.mark.parametrize("failure", ["serialization", "disconnect"])
async def test_transient_recovery_failure_does_not_cancel_smtp(
    recovery_setup, held_smtp, endpoints, monkeypatch, failure
):
    db, worker, did = recovery_setup
    resumed, handled, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    recover, handle = worker.recover, worker.handle
    calls, failed_at, retried_at = 0, None, None
    processed = []

    async def flaky_recover():
        nonlocal calls, failed_at, retried_at
        calls += 1
        if calls == 1:
            await held_smtp.received.wait()
            failed_at = asyncio.get_running_loop().time()
            async with db.transaction() as session:
                statement = (
                    "DO $$ BEGIN RAISE EXCEPTION 'serialization probe' "
                    "USING ERRCODE='40001'; END $$"
                    if failure == "serialization"
                    else "SELECT pg_terminate_backend(pg_backend_pid())"
                )
                await session.execute(text(statement))
        retried_at = asyncio.get_running_loop().time()
        count = await recover()
        resumed.set()
        return count

    async def observed_handle(message):
        await handle(message)
        processed.append(message.processed)
        handled.set()

    monkeypatch.setattr(worker, "recover", flaky_recover)
    monkeypatch.setattr(worker, "handle", observed_handle)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        await broker.channel.set_qos(prefetch_count=1)
        await broker.publish(str(did), send_class="transactional")
        running = asyncio.create_task(worker.consume(broker, 0.05, stop))
        recovered = asyncio.create_task(resumed.wait())
        try:
            await asyncio.wait_for(held_smtp.received.wait(), 5)
            await asyncio.wait((running, recovered), timeout=5, return_when=asyncio.FIRST_COMPLETED)
            assert resumed.is_set(), "Recovery failure cancelled the worker during SMTP DATA"
            assert not running.done()
            assert retried_at - failed_at >= 0.05
            held_smtp.release.set()
            await asyncio.wait_for(handled.wait(), 5)
            stop.set()
            await asyncio.wait_for(running, 5)
            assert processed == [True]
            assert await broker.queues["transactional"].get(fail=False) is None
            await assert_accepted_once(db, did, held_smtp)
        finally:
            held_smtp.release.set()
            stop.set()
            running.cancel()
            recovered.cancel()
            await asyncio.gather(running, recovered, return_exceptions=True)


@pytest.mark.parametrize("source", ["recovery", "intake"])
@pytest.mark.parametrize("with_stop", [False, True])
async def test_restart_waits_for_active_smtp_and_ack(
    recovery_setup, held_smtp, endpoints, monkeypatch, source, with_stop
):
    db, worker, did = recovery_setup
    intake_closed = asyncio.Event()
    processed = []
    handle = worker.handle
    failure = RuntimeError(f"{source} must restart")

    async def broken_recover():
        await held_smtp.received.wait()
        raise failure

    async def observed_handle(message):
        await handle(message)
        processed.append(message.processed)

    monkeypatch.setattr(worker, "handle", observed_handle)
    if source == "recovery":
        monkeypatch.setattr(worker, "recover", broken_recover)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        original_iterator = broker.queues["transactional"].iterator

        async def broken_intake(messages):
            async for message in messages:
                yield message
                await held_smtp.received.wait()
                raise failure

        @asynccontextmanager
        async def observed_iterator(_queue):
            try:
                async with original_iterator() as messages:
                    yield broken_intake(messages) if source == "intake" else messages
            finally:
                intake_closed.set()

        monkeypatch.setattr(type(broker.queues["transactional"]), "iterator", observed_iterator)
        await broker.channel.set_qos(prefetch_count=1)
        await broker.publish(str(did), send_class="transactional")
        running = asyncio.create_task(
            worker.consume(broker, 0.05, asyncio.Event() if with_stop else None)
        )
        try:
            await asyncio.wait_for(held_smtp.received.wait(), 5)
            await asyncio.wait_for(intake_closed.wait(), 5)
            # The connection must stay owned until SMTP and persistence finish.
            async with worker.ownership(did) as connection:
                assert connection is None, "Background failure cancelled the active SMTP send"
            assert not running.done()
            queued = str(uuid4())
            await broker.publish(queued, send_class="transactional")
            held_smtp.release.set()
            with pytest.raises(ExceptionGroup) as errors:
                await asyncio.wait_for(running, 5)
            assert errors.value.exceptions == (failure,)
            assert processed == [True]
            pending = await broker.queues["transactional"].get(timeout=5)
            assert pending.body.decode() == queued and not pending.redelivered
            await pending.ack()
            await assert_accepted_once(db, did, held_smtp)
        finally:
            held_smtp.release.set()
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


async def test_stop_interrupts_recovery_backoff(recovery_setup, endpoints, monkeypatch):
    _, worker, _ = recovery_setup
    failed, consuming, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def unavailable():
        nonlocal calls
        calls += 1
        failed.set()
        raise ConnectionRefusedError("PostgreSQL unavailable")

    monkeypatch.setattr(worker, "recover", unavailable)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        original_iterator = broker.queues["transactional"].iterator

        @asynccontextmanager
        async def observed_iterator(_queue):
            async with original_iterator() as messages:
                consuming.set()
                yield messages

        monkeypatch.setattr(type(broker.queues["transactional"]), "iterator", observed_iterator)
        running = asyncio.create_task(worker.consume(broker, 60, stop))
        try:
            await asyncio.wait_for(failed.wait(), 5)
            await asyncio.wait_for(consuming.wait(), 5)
            stop.set()
            await asyncio.wait_for(running, 1)
            assert calls == 1
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("intake_fails", [False, True])
async def test_failed_ack_does_not_cancel_another_smtp(
    setup, held_smtp, endpoints, monkeypatch, intake_fails
):
    db, worker, did, uid, _ = setup
    first, second = await add_welcomes(db, worker, did, uid, 2)
    worker.settings.smtp_host = "127.0.0.1"
    worker.settings.smtp_port = held_smtp.port
    first_reply, second_reply = asyncio.Event(), asyncio.Event()
    held_smtp.reply_gates.extend((first_reply, second_reply))
    both_sending, intake_closed, ack_failed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    intake_error, ack_error = (
        ConnectionError("intake channel failed"),
        ConnectionError("ack failed"),
    )
    handle = worker.handle

    async def broken_ack(**kwargs):
        ack_failed.set()
        raise ack_error

    async def handle_with_broken_channel(message):
        if message.body == str(first).encode():
            message = SimpleNamespace(
                body=message.body, ack=broken_ack, nack=broken_ack, reject=broken_ack
            )
        await handle(message)

    monkeypatch.setattr(worker, "handle", handle_with_broken_channel)
    async with isolated_broker(endpoints["RABBITMQ_URL"]) as broker:
        original_iterator = broker.queues["transactional"].iterator

        async def broken_intake(messages):
            # Keep SMTP arrivals ordered so each reply gate belongs to a known delivery.
            first_message = await anext(messages)
            yield first_message
            await held_smtp.received.wait()
            yield await anext(messages)
            await both_sending.wait()
            if intake_fails:
                raise intake_error
            async for message in messages:
                yield message

        @asynccontextmanager
        async def observed_iterator(_queue):
            try:
                async with original_iterator() as messages:
                    yield broken_intake(messages)
            finally:
                intake_closed.set()

        monkeypatch.setattr(type(broker.queues["transactional"]), "iterator", observed_iterator)
        await broker.channel.set_qos(prefetch_count=2)
        for delivery_id in (first, second):
            await broker.publish(str(delivery_id), send_class="transactional")
        running = asyncio.create_task(worker.consume(broker, 0.05, asyncio.Event()))
        try:
            for expected in (1, 2):
                assert await asyncio.wait_for(held_smtp.arrivals.get(), 5) == expected
            both_sending.set()
            if intake_fails:
                await asyncio.wait_for(intake_closed.wait(), 5)
            first_reply.set()
            await asyncio.wait_for(ack_failed.wait(), 5)
            await asyncio.wait_for(intake_closed.wait(), 5)
            async with worker.ownership(second) as connection:
                assert connection is None, "Another handler's ack failure cancelled this SMTP send"
            second_reply.set()
            with pytest.raises(ExceptionGroup) as errors:
                await asyncio.wait_for(running, 5)
            expected_errors = {intake_error, ack_error} if intake_fails else {ack_error}
            assert set(errors.value.exceptions) == expected_errors
            assert len(held_smtp.messages) == 2
            async with db.transaction() as session:
                deliveries = list(await session.scalars(select(Delivery)))
                attempts = list(await session.scalars(select(DeliveryAttempt)))
                assert len(deliveries) == len(attempts) == 2
                assert all(d.status == "sent" and d.attempt_count == 1 for d in deliveries)
                assert all(a.outcome == "accepted" and not a.duplicate_risk for a in attempts)
        finally:
            first_reply.set()
            second_reply.set()
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize(
    "error",
    [
        ConnectionRefusedError(),
        ConnectionResetError(),
        TimeoutError(),
        DatabaseTimeoutError(),
        OSError(ENETUNREACH, "network unavailable"),
        gaierror(EAI_AGAIN, "temporary resolution failure"),
        DBAPIError(None, None, pg_errors.SerializationError()),
        DBAPIError(None, None, pg_errors.DeadlockDetectedError()),
        DBAPIError(None, None, pg_errors.TooManyConnectionsError()),
        DBAPIError(None, None, pg_errors.QueryCanceledError()),
        DBAPIError(None, None, pg_errors.CannotConnectNowError()),
        InterfaceError(None, None, Exception(), connection_invalidated=True),
    ],
)
def test_transient_database_failures_are_retryable(error):
    assert transient_recovery_error(error)


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("bug"),
        ValueError("bad config"),
        PermissionError(),
        FileNotFoundError(),
        gaierror(EAI_NONAME, "unknown host"),
        ProgrammingError(None, None, pg_errors.UndefinedTableError()),
        IntegrityError(None, None, pg_errors.UniqueViolationError()),
        DBAPIError(None, None, pg_errors.InvalidPasswordError()),
        DBAPIError(None, None, pg_errors.ProtocolViolationError()),
        InterfaceError(None, None, pg_errors.InterfaceError("operation already in progress")),
        OperationalError(None, None, Exception("unclassified error")),
    ],
)
def test_nontransient_failures_require_restart(error):
    assert not transient_recovery_error(error)
