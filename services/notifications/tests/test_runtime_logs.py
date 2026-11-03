"""Failure diagnostics keep identifiers/classes, never exception payloads."""

import logging
import subprocess
import sys
from unittest.mock import create_autospec
from uuid import uuid4

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.exc import IntegrityError

from notifications.delivery.worker import DeliveryWorker
from notifications.ingest.consumer import KafkaIngestConsumer

SECRET = "private-contact-sentinel@example.org"


def test_consumer_error_excludes_sql_parameters(caplog):
    consumer = object.__new__(KafkaIngestConsumer)
    consumer._failure = None
    error = IntegrityError(
        "INSERT INTO profiles(email) VALUES (?)", {"email": SECRET}, ValueError(SECRET)
    )
    with caplog.at_level(logging.ERROR):
        consumer._fail(error)
    assert consumer._failure is error
    assert "IntegrityError" in caplog.text
    assert SECRET not in caplog.text


async def test_delivery_failure_excludes_exception_chain(caplog, monkeypatch):
    worker = object.__new__(DeliveryWorker)
    delivery_id = uuid4()

    async def fail(_):
        try:
            raise ValueError(SECRET)
        except ValueError as error:
            raise IntegrityError("INSERT", {"email": SECRET}, error) from error

    message = create_autospec(AbstractIncomingMessage, instance=True)
    message.body = str(delivery_id).encode()
    monkeypatch.setattr(worker, "send_delivery", fail)
    with caplog.at_level(logging.ERROR):
        await worker.handle(message)
    message.nack.assert_awaited_once_with(requeue=True)
    assert str(delivery_id) in caplog.text
    assert "IntegrityError" in caplog.text
    assert SECRET not in caplog.text


def test_consumer_process_exits_nonzero_without_chained_payload():
    code = f"""
import sys
import notifications.__main__ as entry
from notifications.ingest.consumer import InvalidPayload
async def fail(args):
    try:
        raise ValueError({SECRET!r})
    except ValueError as cause:
        raise InvalidPayload(
            "Invalid payload at auth-test[2] offset 13; partition stopped"
        ) from cause
entry.consume = fail
sys.argv = ["notifications", "--group", "test"]
entry.main()
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 1
    assert "InvalidPayload" in result.stderr
    assert "auth-test[2] offset 13" in result.stderr
    assert SECRET not in result.stdout + result.stderr
    assert "Traceback" not in result.stderr
