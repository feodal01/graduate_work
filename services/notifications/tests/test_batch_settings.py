"""Batch sizes come from validated environment settings."""

from unittest.mock import AsyncMock, create_autospec

import pytest
from pydantic import ValidationError

from notifications.config import Settings
from notifications.db.session import Database

FIELDS = [
    "scheduler_batch_size",
    "transactional_batch_size",
    "service_batch_size",
    "bulk_batch_size",
    "recovery_batch_size",
    "consumer_batch_size",
    "campaign_batch_size",
]


@pytest.mark.parametrize("name", FIELDS)
def test_batch_size_environment(monkeypatch, name):
    monkeypatch.setenv("NOTIFICATIONS_" + name.upper(), "7")
    config = Settings(process="bootstrap", database_url="postgresql+asyncpg://localhost/db")
    assert getattr(config, name) == 7


@pytest.mark.parametrize("name", FIELDS)
@pytest.mark.parametrize("value", [0, -1, True, 1.5, 1.0])
def test_invalid_batch_sizes(name, value):
    with pytest.raises(ValidationError, match=name):
        Settings(
            process="bootstrap",
            database_url="postgresql+asyncpg://localhost/db",
            **{name: value},
        )


def test_consumer_reads_configured_batch_size(monkeypatch):
    from notifications.contracts import AuthEvent
    from notifications.ingest import consumer

    captured = {}

    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def subscribe(self, *args, **kwargs):
            pass

    monkeypatch.setenv("NOTIFICATIONS_CONSUMER_BATCH_SIZE", "7")
    monkeypatch.setattr(consumer, "AIOKafkaConsumer", Client)
    consumer.KafkaIngestConsumer(
        create_autospec(Database, instance=True),
        AsyncMock(),
        bootstrap_servers="localhost",
        group_id="test",
        topic_models={"auth": AuthEvent},
    )
    assert captured["max_poll_records"] == 7
