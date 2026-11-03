"""Real PG/ES/Kafka checks; select integration only with the test stack running."""

from collections.abc import Iterator
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import psycopg2
import pytest
import requests

from postgres_to_es.catalog_events import detector, store
from postgres_to_es.publication import migrate_publication_state
from postgres_to_es.settings import settings


def test_relay_batch_size_from_settings(
    catalog: str, publication_database: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from postgres_to_es.catalog_events import relay
    from postgres_to_es.settings import Settings

    for _ in range(3):
        put(str(uuid4()))
    assert detector.scan_catalog(NOW) == 3
    monkeypatch.setenv("CATALOG_RELAY_BATCH_SIZE", "2")
    # Обязательные поля BaseSettings поступают из окружения теста.
    configured = Settings(kafka_bootstrap_servers="unused")  # pyright: ignore[reportCallIssue]
    monkeypatch.setattr(relay, "settings", configured)

    class Producer:
        def __init__(self, config: dict[str, Any]) -> None:
            pass

        def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: Any) -> None:
            on_delivery(None, None)

        def flush(self, timeout: float) -> int:
            return 0

    monkeypatch.setattr(relay, "Producer", Producer)
    assert relay.publish_batch() == 2
    assert relay.publish_batch() == 1
    assert relay.publish_batch() == 0


pytestmark = pytest.mark.integration
NOW = datetime(2026, 1, 2, tzinfo=timezone.utc)


@pytest.fixture
def catalog(publication_database: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    migrate_publication_state()
    monkeypatch.setattr(settings, "catalog_overlap_seconds", 60)
    monkeypatch.setattr(settings, "catalog_batch_size", 2)
    name = f"publication-test-{uuid4().hex}"
    monkeypatch.setattr(settings, "elasticsearch_index", name)
    response = requests.put(
        f"{settings.elasticsearch_url}/{name}",
        json={
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "catalog_updated_at": {"type": "date_nanos"},
                }
            }
        },
        timeout=10,
    )
    response.raise_for_status()
    try:
        with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE notifications_source.detector_state SET baseline_at=%s, "
                "bootstrap_completed_at=%s, bootstrap_load_id=%s WHERE singleton",
                (NOW - timedelta(days=2), NOW, str(uuid4())),
            )
        yield name
    finally:
        requests.delete(f"{settings.elasticsearch_url}/{name}", timeout=10).raise_for_status()


def put(film_id: str, version: int = 1, at: datetime = NOW) -> None:
    response = requests.put(
        f"{settings.elasticsearch_url}/{settings.elasticsearch_index}/_doc/{film_id}",
        params={"refresh": "true"},
        json={
            "id": film_id,
            "film_version": version,
            "catalog_updated_at": at.isoformat(),
            "catalog_published_at": (NOW - timedelta(days=1)).isoformat(),
            "title": f"Film v{version}",
            "description": None,
            "creation_date": None,
            "imdb_rating": None,
            "genres": [],
            "actors": [],
        },
        timeout=10,
    )
    response.raise_for_status()


def state(dsl: dict[str, Any]) -> tuple[Any, ...]:
    with closing(psycopg2.connect(**dsl)) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT film_id::text,film_version FROM notifications_source.known_films "
            "ORDER BY film_id"
        )
        known = cur.fetchall()
        cur.execute("SELECT payload FROM notifications_source.outbox ORDER BY sequence")
        events = cur.fetchall()
        cur.execute("SELECT watermark_at,last_sort FROM notifications_source.detector_state")
        return known, events, cur.fetchone()


def test_versions_equal_timestamp_overlap_and_late_reconciliation(
    catalog: str,
    publication_database: dict[str, Any],
) -> None:
    ids = sorted(str(uuid4()) for _ in range(5))
    for film_id in ids:
        put(film_id)
    assert detector.scan_catalog(NOW) == 5
    assert detector.scan_catalog(NOW) == 0
    put(ids[0], 2)
    assert detector.scan_catalog(NOW) == 1
    put(ids[0], 1)
    assert detector.scan_catalog(NOW) == 0
    late = str(uuid4())
    put(late, at=NOW - timedelta(days=1))
    put(ids[1], 3, at=NOW - timedelta(hours=1))
    assert detector.scan_catalog(NOW + timedelta(seconds=1)) == 0
    assert detector.scan_catalog(NOW + timedelta(seconds=1), reconcile=True) == 2
    known, events, _ = state(publication_database)
    assert len(known) == 6
    assert [e[0]["event_type"] for e in events].count("film.published") == 6
    assert [e[0]["event_type"] for e in events].count("film.updated") == 2
    assert "actors" not in events[0][0]


@pytest.mark.parametrize("fault", ["timed_out", "shards", "http"])
def test_failed_page_preserves_progress_and_restart_replays(
    catalog: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    for _ in range(5):
        put(str(uuid4()))
    original = detector._request
    before: tuple[Any, ...] | None = None
    calls = 0

    def broken(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal calls, before
        result = original(method, path, **kwargs)
        if path == "_search":
            calls += 1
            if calls == 2:
                before = state(publication_database)
                if fault == "http":
                    raise requests.ConnectionError("Incomplete search connection")
                if fault == "timed_out":
                    result["timed_out"] = True
                else:
                    result["_shards"]["failed"] = 1
        return result

    monkeypatch.setattr(detector, "_request", broken)
    with pytest.raises((RuntimeError, requests.ConnectionError), match="Incomplete"):
        detector.scan_catalog(NOW)
    assert state(publication_database) == before
    monkeypatch.setattr(detector, "_request", original)
    # Caller now has a later clock, but unfinished upper bound must stay fixed.
    future = str(uuid4())
    put(future, at=NOW + timedelta(seconds=1))
    assert detector.scan_catalog(NOW + timedelta(seconds=2)) == 3
    assert detector.scan_catalog(NOW + timedelta(seconds=2)) == 1
    assert len(state(publication_database)[1]) == 6


def test_page_transaction_rolls_back(
    catalog: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(str(uuid4()))
    assert detector.scan_catalog(NOW) == 1
    previous = state(publication_database)
    put(str(uuid4()))
    original = store.save_progress

    def broken(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        raise RuntimeError("before commit")

    monkeypatch.setattr(store, "save_progress", broken)
    with pytest.raises(RuntimeError, match="before commit"):
        detector.scan_catalog(NOW)
    assert state(publication_database) == previous


def test_requires_bootstrap(catalog: str, publication_database: dict[str, Any]) -> None:
    with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
        cur.execute("UPDATE notifications_source.detector_state SET bootstrap_completed_at=NULL")
    with pytest.raises(RuntimeError, match="bootstrap"):
        detector.scan_catalog(NOW)


def test_live_kafka_ack_rollback_replays_identical_event(
    catalog: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import subprocess
    import sys
    import time
    from pathlib import Path

    from confluent_kafka import Consumer

    # NewTopic входит в публичный API admin, но не объявлен экспортом в типах библиотеки.
    from confluent_kafka.admin import (
        AdminClient,
        NewTopic,  # pyright: ignore[reportPrivateImportUsage]
    )

    from postgres_to_es.catalog_events import relay

    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(root / "tests/notifications/stack_endpoints.py")],
        check=True,
        capture_output=True,
        text=True,
    )
    broker = json.loads(result.stdout)["KAFKA_BOOTSTRAP_SERVERS"]
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", broker)
    topic = f"notifications-test-{uuid4().hex}"
    monkeypatch.setattr(settings, "kafka_movies_topic", topic)
    admin = AdminClient({"bootstrap.servers": broker})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(20)
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        film_id = str(uuid4())
        put(film_id)
        assert detector.scan_catalog(NOW) == 1
        with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
            cur.execute("""CREATE FUNCTION reject_mark() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'ack rollback'; END $$;
                CREATE TRIGGER reject_mark BEFORE UPDATE ON notifications_source.outbox
                FOR EACH ROW EXECUTE FUNCTION reject_mark()""")
        with pytest.raises(psycopg2.Error, match="ack rollback"):
            relay.publish_batch()
        with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
            cur.execute("SELECT published_at FROM notifications_source.outbox")
            assert cur.fetchone()[0] is None
            cur.execute("DROP TRIGGER reject_mark ON notifications_source.outbox")
        assert relay.publish_batch() == 1
        assert relay.publish_batch() == 0
        consumer.subscribe([topic])
        messages: list[Any] = []
        deadline = time.monotonic() + 15
        while len(messages) < 2 and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is not None:
                assert not message.error()
                messages.append(message)
        assert len(messages) == 2
        assert messages[0].key() == film_id.encode()
        assert messages[0].value() == messages[1].value()
        assert json.loads(messages[0].value()) == state(publication_database)[1][0][0]
        # Broker is reachable but topic auto-creation is disabled: no successful ack.
        put(str(uuid4()))
        assert detector.scan_catalog(NOW) == 1
        monkeypatch.setattr(settings, "kafka_movies_topic", f"notifications-test-{uuid4().hex}")
        monkeypatch.setattr(settings, "catalog_relay_delivery_timeout_seconds", 1)
        with pytest.raises(RuntimeError, match="delivery incomplete"):
            relay.publish_batch()
        with closing(psycopg2.connect(**publication_database)) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM notifications_source.outbox WHERE published_at IS NULL"
            )
            assert cur.fetchone()[0] == 1
    finally:
        consumer.close()
        admin.delete_topics([topic])[topic].result(20)


def test_detector_lock_excludes_second_process(catalog: str) -> None:
    put(str(uuid4()))
    with store.process_lock(store.DETECTOR_LOCK):
        with pytest.raises(RuntimeError, match="Another process"):
            detector.scan_catalog(NOW)
    assert detector.scan_catalog(NOW) == 1
    assert detector.scan_catalog(NOW) == 0


def test_lost_lock_connection_cannot_commit_page(
    catalog: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(str(uuid4()))
    before = state(publication_database)
    original = store.save_page

    def disconnected(conn: Any, *args: Any) -> int:
        with closing(psycopg2.connect(**publication_database)) as killer:
            killer.autocommit = True
            with killer.cursor() as cur:
                cur.execute("SELECT pg_terminate_backend(%s)", (conn.get_backend_pid(),))
        return original(conn, *args)

    monkeypatch.setattr(store, "save_page", disconnected)
    with pytest.raises(psycopg2.Error):
        detector.scan_catalog(NOW)
    assert state(publication_database) == before


def test_pit_cleanup_preserves_primary_failure(
    catalog: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    original = detector._request

    def fail(method: str, path: str, **kwargs: Any) -> Any:
        if path == "_search":
            raise RuntimeError("primary search failure")
        if method == "DELETE" and path == "_pit":
            raise RuntimeError("cleanup failure")
        return original(method, path, **kwargs)

    monkeypatch.setattr(detector, "_request", fail)
    with pytest.raises(RuntimeError, match="primary search failure"):
        detector.scan_catalog(NOW)
    assert "PIT cleanup failed" in caplog.text
