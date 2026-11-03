"""Bootstrap invariants against isolated, real PostgreSQL and Elasticsearch."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import psycopg2
import pytest
import requests

from postgres_to_es.catalog_events import bootstrap, detector, store
from postgres_to_es.settings import settings
from tests.test_catalog_detector import catalog, put, state  # noqa: F401

pytestmark = pytest.mark.integration


@pytest.fixture
def empty_gate(catalog: str, publication_database: dict[str, Any]) -> str:  # noqa: F811
    with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE notifications_source.detector_state SET "
            "bootstrap_load_id=NULL, baseline_at=NULL, bootstrap_completed_at=NULL"
        )
    return catalog


def test_frozen_versions_new_films_and_permanent_completion(
    empty_gate: str,
    publication_database: dict[str, Any],
) -> None:
    a, b, c = sorted(str(uuid4()) for _ in range(3))
    put(a)
    put(b)
    assert bootstrap.capture_catalog("initial-films-v1") == 2
    original = bootstrap.catalog_status("initial-films-v1")
    assert original["captured_at"] is not None
    assert original["released_at"] is not None
    # New content arrives while historical enqueue is still pending.
    now = datetime.now(timezone.utc)
    put(a, version=2, at=now)
    put(c, at=now)
    assert bootstrap.enqueue_catalog("initial-films-v1", 1) == 1
    with pytest.raises(RuntimeError, match="bootstrap"):
        detector.scan_catalog(now + timedelta(seconds=1))
    assert bootstrap.enqueue_catalog("initial-films-v1", 1) == 1
    assert store.readiness()["baseline_at"] == original["start_boundary"]
    assert detector.scan_catalog(now + timedelta(seconds=1), reconcile=True) == 2
    known, events, progress = state(publication_database)
    assert dict(known) == {a: 2, b: 1, c: 1}
    assert [(e[0]["film_id"], e[0]["film_version"], e[0]["event_type"]) for e in events[:2]] == [
        (a, 1, "film.updated"),
        (b, 1, "film.updated"),
    ]
    assert sorted((e[0]["film_id"], e[0]["event_type"]) for e in events[2:]) == sorted(
        [(a, "film.updated"), (c, "film.published")]
    )
    with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
        cur.execute("DELETE FROM notifications_source.outbox")
    assert bootstrap.capture_catalog("initial-films-v1") == 2
    assert bootstrap.enqueue_catalog("initial-films-v1", 10) == 0
    assert (
        bootstrap.catalog_status("initial-films-v1")["start_boundary"] == original["start_boundary"]
    )
    assert state(publication_database) == (known, [], progress)
    with pytest.raises(ValueError, match="load"):
        bootstrap.capture_catalog("different-load")


def test_interrupted_capture_stays_fenced_and_resumes_same_boundary(
    empty_gate: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = [str(uuid4()) for _ in range(3)]
    for film in ids:
        put(film)
    original = bootstrap._save_capture_page
    calls = 0

    def crash(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        original(*args, **kwargs)
        calls += 1
        if calls == 1:
            raise RuntimeError("capture crash")

    monkeypatch.setattr(bootstrap, "_save_capture_page", crash)
    with pytest.raises(RuntimeError, match="capture crash"):
        bootstrap.capture_catalog("resumable")
    status = bootstrap.catalog_status("resumable")
    assert status["captured_at"] is None
    assert status["captured_count"] == 2
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))
    monkeypatch.setattr(bootstrap, "_save_capture_page", original)
    assert bootstrap.capture_catalog("resumable") == 3
    assert bootstrap.catalog_status("resumable")["start_boundary"] == status["start_boundary"]
    put(str(uuid4()))  # Capture completion restores writes, before enqueue.


def test_enqueue_page_failure_rolls_back_known_events_and_markers(
    empty_gate: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for _ in range(3):
        put(str(uuid4()))
    bootstrap.capture_catalog("atomic")
    original = store.add_event
    calls = 0

    def crash(*args: Any, **kwargs: Any) -> bool:
        nonlocal calls
        added = original(*args, **kwargs)
        calls += 1
        if calls == 2:
            raise RuntimeError("enqueue crash")
        return added

    monkeypatch.setattr(store, "add_event", crash)
    with pytest.raises(RuntimeError, match="enqueue crash"):
        bootstrap.enqueue_catalog("atomic", 3)
    assert state(publication_database)[:2] == ([], [])
    assert bootstrap.catalog_status("atomic")["enqueued_count"] == 0
    assert store.readiness()["bootstrap_completed_at"] is None
    monkeypatch.setattr(store, "add_event", original)
    assert bootstrap.enqueue_catalog("atomic", 3) == 3
    assert bootstrap.catalog_status("atomic")["enqueued_count"] == 3
    assert store.readiness()["bootstrap_completed_at"] is not None


def test_empty_baseline_is_explicit_and_preserves_external_write_block(empty_gate: str) -> None:
    requests.put(
        f"{settings.elasticsearch_url}/{empty_gate}/_block/write", timeout=10
    ).raise_for_status()
    with pytest.raises(RuntimeError, match="external write fence"):
        bootstrap.capture_catalog("empty")
    assert bootstrap.catalog_status("empty")["fenced_at"] is None
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))
    # External owner permits establishing a verified dedicated fence before capture starts.
    requests.put(
        f"{settings.elasticsearch_url}/{empty_gate}/_settings",
        timeout=10,
        json={"index.blocks.write": False},
    ).raise_for_status()
    assert bootstrap.capture_catalog("empty") == 0
    assert store.readiness()["bootstrap_completed_at"] is None
    assert bootstrap.enqueue_catalog("empty", 1) == 0
    assert store.readiness()["bootstrap_completed_at"] is not None
    # The original external ownership is retained: bootstrap does not release its block.
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))


@pytest.mark.parametrize("stage", ["block", "release"])
def test_network_crash_after_block_or_release_is_resumable(
    empty_gate: str,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    put(str(uuid4()))
    original = bootstrap._request

    def crash(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = original(method, path, **kwargs)
        if method == "PUT" and path.endswith("/_block/write" if stage == "block" else "/_settings"):
            raise requests.ConnectionError("lost acknowledgement")
        return response

    monkeypatch.setattr(bootstrap, "_request", crash)
    with pytest.raises(requests.ConnectionError, match="lost acknowledgement"):
        bootstrap.capture_catalog("network-crash")
    initial = bootstrap.catalog_status("network-crash")
    assert initial["restore_writes"] is True
    assert initial["released_at"] is None
    drain_results = []

    def observe(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        result = original(method, path, **kwargs)
        if path.endswith("/_block/write"):
            drain_results.append(result)
        return result

    monkeypatch.setattr(bootstrap, "_request", observe)
    assert bootstrap.capture_catalog("network-crash") == 1
    if stage == "block":
        assert len(drain_results) == 1
        assert drain_results[0]["shards_acknowledged"] is True
        assert drain_results[0]["indices"] == [{"name": empty_gate, "blocked": True}]
    assert bootstrap.catalog_status("network-crash")["start_boundary"] == initial["start_boundary"]
    put(str(uuid4()))


def test_release_refuses_replaced_index(empty_gate: str, monkeypatch: pytest.MonkeyPatch) -> None:
    original = bootstrap._request

    def crash(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        if method == "PUT" and path.endswith("/_settings"):
            raise RuntimeError("before release")
        return original(method, path, **kwargs)

    monkeypatch.setattr(bootstrap, "_request", crash)
    with pytest.raises(RuntimeError, match="before release"):
        bootstrap.capture_catalog("replaced")
    monkeypatch.setattr(bootstrap, "_request", original)
    requests.delete(f"{settings.elasticsearch_url}/{empty_gate}", timeout=10).raise_for_status()
    requests.put(f"{settings.elasticsearch_url}/{empty_gate}", timeout=10).raise_for_status()
    requests.put(
        f"{settings.elasticsearch_url}/{empty_gate}/_block/write", timeout=10
    ).raise_for_status()
    with pytest.raises(ValueError, match="identity"):
        bootstrap.capture_catalog("replaced")
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))


def test_bootstrap_cli_status_and_enqueue(
    empty_gate: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json

    from postgres_to_es.catalog_events.__main__ import main

    monkeypatch.setattr("sys.argv", ["catalog-events", "bootstrap", "capture", "--load-id", "cli"])
    main()
    assert json.loads(capsys.readouterr().out)["captured_count"] == 0
    monkeypatch.setattr(
        "sys.argv", ["catalog-events", "bootstrap", "enqueue", "--load-id", "cli", "--limit", "2"]
    )
    main()
    assert json.loads(capsys.readouterr().out)["completed_at"] is not None
    monkeypatch.setattr("sys.argv", ["catalog-events", "bootstrap", "status", "--load-id", "cli"])
    main()
    assert json.loads(capsys.readouterr().out)["enqueued_count"] == 0


def test_legacy_baseline_uses_original_membership_and_versions(
    empty_gate: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from postgres_to_es.catalog_events.legacy import rebuild_legacy_catalog
    from postgres_to_es.pipelines.loader.client import bulk_index
    from postgres_to_es.publication import prepare_publication
    from postgres_to_es.schemas import MovieDocument

    a, b, c, unpublished = [str(uuid4()) for _ in range(4)]
    source, target = [f"publication-test-{uuid4().hex}" for _ in range(2)]
    legacy_id = uuid4()
    boundary = datetime(2026, 1, 1, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO content.film_work(id,title) VALUES (%s,'Old A'),(%s,'PG-only')",
            (a, unpublished),
        )
    try:
        for film_id in (a, b):
            requests.put(
                f"{settings.elasticsearch_url}/{source}/_doc/{film_id}",
                timeout=10,
                json=MovieDocument(id=film_id, title="Old A").to_elasticsearch(),
            ).raise_for_status()
        requests.put(
            f"{settings.elasticsearch_url}/{source}/_block/write", timeout=10
        ).raise_for_status()
        requests.post(
            f"{settings.elasticsearch_url}/{source}/_refresh", timeout=10
        ).raise_for_status()
        monkeypatch.setattr(settings, "catalog_legacy_batch_size", 1)
        rebuild_legacy_catalog(source, target, legacy_id, boundary)
        monkeypatch.setattr(settings, "elasticsearch_index", target)
        with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
            cur.execute("UPDATE content.film_work SET title='New A' WHERE id=%s", (a,))
        from uuid import UUID

        newer = prepare_publication(UUID(a), now)
        assert newer.film_version == 2
        bulk_index([newer], target)
        prepare_publication(UUID(unpublished), now)  # Durable PG preparation is not eligibility.
        put(c, at=now)
        with pytest.raises(ValueError, match="legacy load_id"):
            bootstrap.capture_catalog("wrong-baseline")
        assert bootstrap.capture_catalog(str(legacy_id)) == 2
        assert bootstrap.catalog_status(str(legacy_id))["start_boundary"] == boundary
        assert bootstrap.enqueue_catalog(str(legacy_id), 10) == 2
        assert detector.scan_catalog(now + timedelta(seconds=1), reconcile=True) == 2
        known, events, _ = state(publication_database)
        assert dict(known) == {a: 2, b: 1, c: 1}
        historical = [event[0] for event in events[:2]]
        assert {event["film_id"] for event in historical} == {a, b}
        assert {event["film_version"] for event in historical} == {1}
        assert {event["event_type"] for event in historical} == {"film.updated"}
        assert sorted((e[0]["film_id"], e[0]["event_type"]) for e in events[2:]) == sorted(
            [(a, "film.updated"), (c, "film.published")]
        )
        assert prepare_publication(UUID(a), now).film_version == 2
    finally:
        for name in (source, target):
            requests.delete(f"{settings.elasticsearch_url}/{name}", timeout=10).raise_for_status()


@pytest.mark.parametrize("fault", ["refresh", "search_timeout", "search_shards"])
def test_incomplete_capture_cannot_release_fence_or_enable_detector(
    empty_gate: str,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    put(str(uuid4()))
    original = bootstrap._request

    def incomplete(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        result = original(method, path, **kwargs)
        if fault == "refresh" and path.endswith("/_refresh"):
            result["_shards"]["failed"] = 1
        if path.endswith("/_search"):
            if fault == "search_timeout":
                result["timed_out"] = True
            if fault == "search_shards":
                result["_shards"]["failed"] = 1
        return result

    monkeypatch.setattr(bootstrap, "_request", incomplete)
    with pytest.raises(RuntimeError, match="Incomplete bootstrap"):
        bootstrap.capture_catalog("incomplete")
    assert bootstrap.catalog_status("incomplete")["captured_at"] is None
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))
    with pytest.raises(RuntimeError, match="capture"):
        bootstrap.enqueue_catalog("incomplete", 2)
    assert store.readiness()["bootstrap_completed_at"] is None
    monkeypatch.setattr(bootstrap, "_request", original)
    assert bootstrap.capture_catalog("incomplete") == 1


def test_partial_capture_refuses_lost_fence(
    empty_gate: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    put(str(uuid4()))
    original = bootstrap._save_capture_page

    def crash(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        raise RuntimeError("after page")

    monkeypatch.setattr(bootstrap, "_save_capture_page", crash)
    with pytest.raises(RuntimeError, match="after page"):
        bootstrap.capture_catalog("fence-lost")
    requests.put(
        f"{settings.elasticsearch_url}/{empty_gate}/_settings",
        timeout=10,
        json={"index.blocks.write": False},
    ).raise_for_status()
    monkeypatch.setattr(bootstrap, "_save_capture_page", original)
    with pytest.raises(RuntimeError, match="lost its write fence"):
        bootstrap.capture_catalog("fence-lost")


def test_bootstrap_excludes_other_detector_and_rejects_lost_pg_session(
    empty_gate: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(str(uuid4()))
    with store.process_lock(store.DETECTOR_LOCK):
        with pytest.raises(RuntimeError, match="Another process"):
            bootstrap.capture_catalog("session")
    bootstrap.capture_catalog("session")
    original = store.add_event

    def disconnect(cur: Any, *args: Any, **kwargs: Any) -> bool:
        result = original(cur, *args, **kwargs)
        with closing(psycopg2.connect(**publication_database)) as killer:
            killer.autocommit = True
            with killer.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_terminate_backend(%s)", (cur.connection.get_backend_pid(),)
                )
        return result

    monkeypatch.setattr(store, "add_event", disconnect)
    with pytest.raises(psycopg2.Error):
        bootstrap.enqueue_catalog("session", 1)
    assert state(publication_database)[:2] == ([], [])
    assert bootstrap.catalog_status("session")["enqueued_count"] == 0
    assert store.readiness()["bootstrap_completed_at"] is None


def test_live_update_during_enqueue_commits_frozen_then_new_version(
    empty_gate: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    a, c = [str(uuid4()) for _ in range(2)]
    put(a)
    bootstrap.capture_catalog("concurrent")
    inserted, resume = Event(), Event()
    original = store.add_event

    def pause(*args: Any, **kwargs: Any) -> bool:
        result = original(*args, **kwargs)
        inserted.set()
        assert resume.wait(10)
        return result

    monkeypatch.setattr(store, "add_event", pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        enqueue = pool.submit(bootstrap.enqueue_catalog, "concurrent", 10)
        try:
            assert inserted.wait(10)
            now = datetime.now(timezone.utc)
            put(a, version=2, at=now)
            put(c, at=now)
            # An independent session cannot see readiness or uncommitted historical rows.
            assert store.readiness()["bootstrap_completed_at"] is None
            assert state(publication_database)[:2] == ([], [])
        finally:
            resume.set()
        assert enqueue.result(10) == 1
    monkeypatch.setattr(store, "add_event", original)
    assert detector.scan_catalog(now + timedelta(seconds=1), reconcile=True) == 2
    known, events, _ = state(publication_database)
    assert dict(known) == {a: 2, c: 1}
    assert events[0][0]["film_version"] == 1
    assert events[0][0]["event_type"] == "film.updated"
    assert {(e[0]["film_id"], e[0]["film_version"], e[0]["event_type"]) for e in events[1:]} == {
        (a, 2, "film.updated"),
        (c, 1, "film.published"),
    }


def test_uncertain_fence_with_existing_snapshot_is_never_reopened(
    empty_gate: str,
    publication_database: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(str(uuid4()))
    original = bootstrap._save_capture_page

    def crash(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        raise RuntimeError("after page")

    monkeypatch.setattr(bootstrap, "_save_capture_page", crash)
    with pytest.raises(RuntimeError, match="after page"):
        bootstrap.capture_catalog("inconsistent-fence")
    with closing(psycopg2.connect(**publication_database)) as conn, conn, conn.cursor() as cur:
        cur.execute("UPDATE notifications_source.catalog_bootstrap_loads SET fenced_at=NULL")
    monkeypatch.setattr(bootstrap, "_save_capture_page", original)
    with pytest.raises(RuntimeError, match="snapshot already exists"):
        bootstrap.capture_catalog("inconsistent-fence")
    with pytest.raises(requests.HTTPError, match="403"):
        put(str(uuid4()))
