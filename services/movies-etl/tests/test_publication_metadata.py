"""Publication contracts; PG/ES tests use UUID databases in the test Compose stack."""

import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg2
import pytest
import requests

from postgres_to_es.schemas import MovieDocument


def test_publication_document_carries_durable_metadata() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    document = MovieDocument(
        id="film",
        title="Film",
        film_version=1,
        index_revision=1,
        catalog_published_at=now,
        catalog_updated_at=now,
    )
    payload = document.to_elasticsearch()
    assert payload["film_version"] == 1
    assert payload["catalog_published_at"] == "2026-01-01T00:00:00Z"
    assert payload["catalog_updated_at"] == payload["catalog_published_at"]
    assert "index_revision" not in payload


def test_bulk_rejects_unprepared_documents() -> None:
    import pytest

    from postgres_to_es.pipelines.loader.client import _build_bulk_payload

    with pytest.raises(ValueError, match="publication"):
        _build_bulk_payload([MovieDocument(id="film", title="Unprepared")])


def test_bulk_fences_older_and_duplicate_snapshots() -> None:
    import json

    from postgres_to_es.pipelines.loader.client import _build_bulk_payload

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    document = MovieDocument(
        id="film",
        title="Film",
        film_version=2,
        index_revision=5,
        catalog_published_at=now,
        catalog_updated_at=now,
    )
    action = json.loads(_build_bulk_payload([document]).splitlines()[0])
    assert action == {"index": {"_id": "film", "version": 5, "version_type": "external"}}


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def execute(dsl: dict[str, Any], query: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
    with closing(psycopg2.connect(**dsl)) as conn, conn, conn.cursor() as cursor:
        cursor.execute(query, parameters)
        return cursor.fetchall() if cursor.description else []


@pytest.fixture
def film(publication_database: dict[str, Any]) -> UUID:
    from postgres_to_es.publication import migrate_publication_state

    migrate_publication_state()
    film_id = uuid4()
    execute(
        publication_database,
        "INSERT INTO content.film_work(id,title) VALUES(%s,'Original')",
        (str(film_id),),
    )
    return film_id


@pytest.mark.integration
def test_retries_and_semantic_changes(publication_database: dict[str, Any], film: UUID) -> None:
    from postgres_to_es.publication import prepare_publication

    original = prepare_publication(film, NOW)
    assert original.film_version == original.index_revision == 1
    assert original.catalog_published_at == original.catalog_updated_at == NOW
    assert prepare_publication(film, NOW + timedelta(days=1)) == original
    changes = [
        ("title", "Changed"),
        ("description", "Description"),
        ("creation_date", "2020-02-03"),
        ("rating", 8.5),
    ]
    changed = original
    for version, (column, value) in enumerate(changes, 2):
        execute(
            publication_database,
            f"UPDATE content.film_work SET {column}=%s WHERE id=%s",
            (value, str(film)),
        )
        changed = prepare_publication(film, NOW + timedelta(days=version))
        assert changed.film_version == changed.index_revision == version
        assert changed.catalog_published_at == original.catalog_published_at
        assert changed.catalog_updated_at == NOW + timedelta(days=version)
    assert prepare_publication(film, NOW + timedelta(days=10)) == changed
    persisted = execute(
        publication_database,
        "SELECT payload, film_version FROM notifications_source.source_state WHERE film_id=%s",
        (str(film),),
    )[0]
    assert persisted[0]["title"] == "Changed"
    assert persisted[0]["creation_date"] == "2020-02-03"
    assert persisted[1] == 5


@pytest.mark.integration
def test_actor_only_change_advances_technical_revision(
    publication_database: dict[str, Any], film: UUID
) -> None:
    from postgres_to_es.publication import prepare_publication

    original = prepare_publication(film, NOW)
    person = str(uuid4())
    execute(publication_database, "INSERT INTO content.person VALUES(%s,'Actor',now())", (person,))
    execute(
        publication_database,
        "INSERT INTO content.person_film_work VALUES(%s,%s,%s,'actor')",
        (str(uuid4()), str(film), person),
    )
    changed = prepare_publication(film, NOW + timedelta(days=1))
    assert changed.film_version == original.film_version
    assert changed.catalog_updated_at == original.catalog_updated_at
    assert changed.index_revision == 2
    assert changed.actors_names == ["Actor"]
    assert prepare_publication(film, NOW + timedelta(days=2)) == changed


@pytest.mark.integration
def test_concurrent_first_preparations_share_one_revision(
    publication_database: dict[str, Any], film: UUID
) -> None:
    from postgres_to_es.publication import prepare_publication

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: prepare_publication(film, NOW), range(16)))
    assert {doc.film_version for doc in results} == {1}
    assert {doc.index_revision for doc in results} == {1}
    assert execute(
        publication_database, "SELECT count(*) FROM notifications_source.source_state"
    ) == [(1,)]


@pytest.mark.integration
def test_genre_relation_trigger_and_stable_order(
    publication_database: dict[str, Any], film: UUID
) -> None:
    from postgres_to_es.publication import prepare_publication

    # Execute actual migration SQL without importing the Admin/Django runtime.
    path = (
        Path(__file__).resolve().parents[2]
        / "admin/app/movies/migrations/0003_catalog_relation_changes.py"
    )
    tree = ast.parse(path.read_text())
    statement = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "FORWARD_SQL"
    )
    execute(publication_database, ast.literal_eval(statement.value))
    original = prepare_publication(film, NOW)
    ids = [str(uuid4()), str(uuid4())]
    before = execute(
        publication_database, "SELECT modified FROM content.film_work WHERE id=%s", (str(film),)
    )[0][0]
    for genre, name in zip(ids, ["Drama", "Comedy"], strict=True):
        execute(
            publication_database, "INSERT INTO content.genre VALUES(%s,%s,now())", (genre, name)
        )
        execute(
            publication_database,
            "INSERT INTO content.genre_film_work VALUES(%s,%s,%s)",
            (str(uuid4()), str(film), genre),
        )
    after = execute(
        publication_database, "SELECT modified FROM content.film_work WHERE id=%s", (str(film),)
    )[0][0]
    assert after > before
    added = prepare_publication(film, NOW + timedelta(days=1))
    assert added.film_version == 2
    execute(
        publication_database,
        "DELETE FROM content.genre_film_work WHERE film_work_id=%s",
        (str(film),),
    )
    for genre in reversed(ids):
        execute(
            publication_database,
            "INSERT INTO content.genre_film_work VALUES(%s,%s,%s)",
            (str(uuid4()), str(film), genre),
        )
    assert prepare_publication(film, NOW + timedelta(days=2)) == added
    execute(
        publication_database, "DELETE FROM content.genre_film_work WHERE genre_id=%s", (ids[0],)
    )
    removed = prepare_publication(film, NOW + timedelta(days=3))
    assert removed.film_version == 3
    assert len(removed.genres) == 1
    assert removed.catalog_published_at == original.catalog_published_at
    other = str(uuid4())
    execute(
        publication_database, "INSERT INTO content.film_work(id,title) VALUES(%s,'Other')", (other,)
    )
    before_move = dict(execute(publication_database, "SELECT id,modified FROM content.film_work"))
    execute(
        publication_database,
        "UPDATE content.genre_film_work SET film_work_id=%s WHERE genre_id=%s",
        (other, ids[1]),
    )
    after_move = dict(execute(publication_database, "SELECT id,modified FROM content.film_work"))
    assert after_move[str(film)] > before_move[str(film)]
    assert after_move[other] > before_move[other]
    assert prepare_publication(film, NOW + timedelta(days=4)).film_version == 4
    assert len(prepare_publication(UUID(other), NOW + timedelta(days=4)).genres) == 1
    execute(publication_database, "UPDATE content.genre_film_work SET genre_id=genre_id")
    assert (
        dict(execute(publication_database, "SELECT id,modified FROM content.film_work"))
        == after_move
    )


@pytest.mark.integration
def test_real_es_reindex_partial_failure_retry_and_reversed_bulk(
    publication_database: dict[str, Any], film: UUID
) -> None:
    from postgres_to_es.pipelines.loader.client import bulk_index
    from postgres_to_es.publication import prepare_publication
    from postgres_to_es.settings import settings

    indices = ["publication-test-" + uuid4().hex for _ in range(2)]
    original = prepare_publication(film, NOW)
    execute(
        publication_database,
        "UPDATE content.film_work SET title='Latest' WHERE id=%s",
        (str(film),),
    )
    latest = prepare_publication(film, NOW + timedelta(days=1))
    try:
        bulk_index([latest], indices[0])
        bulk_index([original], indices[0])
        bulk_index([latest], indices[0])
        bad = latest.model_copy(update={"id": str(uuid4()), "title": {"invalid": True}})
        with (
            pytest.warns(UserWarning, match="serializer warnings"),
            pytest.raises(RuntimeError, match="failed to index"),
        ):
            bulk_index([latest, bad], indices[0])
        bulk_index([prepare_publication(film, NOW + timedelta(days=2))], indices[0])
        bulk_index([prepare_publication(film, NOW + timedelta(days=3))], indices[1])
        sources = []
        for index in indices:
            requests.post(
                f"{settings.elasticsearch_url}/{index}/_refresh", timeout=5
            ).raise_for_status()
            response = requests.get(f"{settings.elasticsearch_url}/{index}/_search", timeout=5)
            response.raise_for_status()
            hits = response.json()["hits"]["hits"]
            assert len(hits) == 1
            sources.append(hits[0]["_source"])
        assert sources[0] == sources[1]
        assert sources[0]["title"] == "Latest"
        assert sources[0]["film_version"] == 2
        assert sources[0]["catalog_published_at"] == "2026-01-01T00:00:00Z"
    finally:
        for index in indices:
            requests.delete(f"{settings.elasticsearch_url}/{index}", timeout=5).raise_for_status()


@pytest.mark.parametrize("interruption", ["_capture_page", "bulk_index"])
@pytest.mark.integration
def test_legacy_copy_preserves_only_working_membership_and_resumes(
    publication_database: dict[str, Any],
    film: UUID,
    monkeypatch: pytest.MonkeyPatch,
    interruption: str,
) -> None:
    from postgres_to_es.catalog_events.legacy import rebuild_legacy_catalog
    from postgres_to_es.publication import prepare_publication
    from postgres_to_es.settings import settings

    source, target = ["publication-test-" + uuid4().hex for _ in range(2)]
    load_id = uuid4()
    old_payload = MovieDocument(id=str(film), title="Published old film").to_elasticsearch()
    unpublished = str(uuid4())
    execute(
        publication_database,
        "INSERT INTO content.film_work(id,title) VALUES(%s,'Never indexed')",
        (unpublished,),
    )
    try:
        # Old ETL used internal versions. Repeated indexing makes the source version > 1.
        for version in range(1, 4):
            response = requests.put(
                f"{settings.elasticsearch_url}/{source}/_doc/{film}", json=old_payload, timeout=5
            )
            response.raise_for_status()
            assert response.json()["_version"] == version
        # A working ES film may no longer have a PG row; retain its original membership too.
        zero_id = str(UUID(int=0))
        requests.put(
            f"{settings.elasticsearch_url}/{source}/_doc/{zero_id}",
            json=MovieDocument(id=zero_id, title="ES-only old film").to_elasticsearch(),
            timeout=5,
        ).raise_for_status()
        requests.put(
            f"{settings.elasticsearch_url}/{source}/_settings",
            json={"index.blocks.write": True},
            timeout=5,
        ).raise_for_status()
        requests.post(
            f"{settings.elasticsearch_url}/{source}/_refresh", timeout=5
        ).raise_for_status()
        import postgres_to_es.catalog_events.legacy as legacy

        monkeypatch.setattr(settings, "catalog_legacy_batch_size", 1)
        operation = getattr(legacy, interruption)

        def interrupted(*args: Any, **kwargs: Any) -> None:
            operation(*args, **kwargs)
            raise ConnectionError("simulated process failure after durable side effect")

        with monkeypatch.context() as patch:
            patch.setattr(legacy, interruption, interrupted)
            with pytest.raises(ConnectionError, match="simulated process failure"):
                rebuild_legacy_catalog(source, target, load_id, NOW)
        rebuild_legacy_catalog(source, target, load_id, NOW + timedelta(days=3))
        boundary = execute(
            publication_database,
            "SELECT started_at,captured_at,completed_at "
            "FROM notifications_source.legacy_catalog_loads WHERE load_id=%s",
            (str(load_id),),
        )[0]
        assert boundary[0] == NOW
        assert boundary[1] is not None and boundary[2] is not None
        membership = execute(
            publication_database,
            "SELECT film_id,payload FROM notifications_source.legacy_catalog_membership "
            "WHERE load_id=%s",
            (str(load_id),),
        )
        assert set(dict(membership)) == {str(film), zero_id}
        assert dict(membership)[str(film)]["title"] == "Published old film"
        assert {
            row[0]
            for row in execute(
                publication_database, "SELECT film_id FROM notifications_source.source_state"
            )
        } == {str(film), zero_id}
        assert (
            requests.get(f"{settings.elasticsearch_url}/{target}/_doc/{zero_id}", timeout=5).json()[
                "_source"
            ]["title"]
            == "ES-only old film"
        )
        result = requests.get(
            f"{settings.elasticsearch_url}/{target}/_doc/{film}", timeout=5
        ).json()
        assert result["_version"] == 1
        assert result["_source"]["catalog_published_at"] == "2026-01-01T00:00:00Z"
        assert result["_source"]["title"] == "Published old film"
        # Current PG content is only read later by normal ETL, generating a real update.
        current = prepare_publication(film, NOW + timedelta(days=4))
        assert current.title == "Original" and current.film_version == 2
        assert current.catalog_published_at == NOW
        assert requests.head(f"{settings.elasticsearch_url}/{source}", timeout=5).status_code == 200
    finally:
        for index in (source, target):
            response = requests.delete(f"{settings.elasticsearch_url}/{index}", timeout=5)
            assert response.status_code in (200, 404)


@pytest.mark.integration
def test_waiting_preparation_reads_source_after_lock(
    publication_database: dict[str, Any], film: UUID
) -> None:
    import time

    from postgres_to_es.publication import prepare_publication

    with closing(psycopg2.connect(**publication_database)) as holder, holder.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (str(film),))
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(prepare_publication, film, NOW)
            try:
                deadline = time.monotonic() + 5
                waiting = 0
                while time.monotonic() < deadline:
                    waiting = execute(
                        publication_database,
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event='advisory'",
                    )[0][0]
                    if waiting:
                        break
                    time.sleep(0.01)
                assert waiting == 1
                cursor.execute(
                    "UPDATE content.film_work SET title='Fresh under lock' WHERE id=%s",
                    (str(film),),
                )
            finally:
                holder.commit()
            prepared = pending.result(timeout=5)
    assert prepared.title == "Fresh under lock"
    assert prepared.film_version == 1


@pytest.mark.integration
def test_internal_legacy_version_conflict_is_not_acknowledged(
    publication_database: dict[str, Any],
    film: UUID,
) -> None:
    from postgres_to_es.pipelines.loader.client import bulk_index
    from postgres_to_es.publication import prepare_publication
    from postgres_to_es.settings import settings

    index = "publication-test-" + uuid4().hex
    try:
        for _ in range(3):
            requests.put(
                f"{settings.elasticsearch_url}/{index}/_doc/{film}",
                json={"id": str(film), "title": "Legacy"},
                timeout=5,
            ).raise_for_status()
        with pytest.raises(RuntimeError, match="legacy migration"):
            bulk_index([prepare_publication(film, NOW)], index)
    finally:
        requests.delete(f"{settings.elasticsearch_url}/{index}", timeout=5).raise_for_status()


@pytest.mark.integration
def test_worker_persists_fresh_snapshot_before_network(
    publication_database: dict[str, Any],
    film: UUID,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import postgres_to_es.pipelines.worker.pipeline as worker
    from postgres_to_es.schemas import MovieDocument
    from postgres_to_es.settings import settings
    from postgres_to_es.storage import ETLJobsTable, SQLiteFileDatabase

    monkeypatch.setattr(settings, "state_database_file_path", str(tmp_path / "jobs.sqlite"))
    jobs = ETLJobsTable(SQLiteFileDatabase(settings.state_database_file_path))
    jobs.enqueue(
        job_id="old-job",
        source_table="film_work",
        first_id=str(film),
        last_id=str(film),
        first_modified=NOW.isoformat(),
        last_modified=NOW.isoformat(),
        row_count=1,
        now=NOW.isoformat(),
    )
    monkeypatch.setattr(worker, "run_producer_pipeline", lambda: [])
    monkeypatch.setattr(worker, "extract_job_rows", lambda job: [])
    monkeypatch.setattr(
        worker,
        "transform_rows",
        lambda rows: [MovieDocument(id=str(film), title="Stale extracted title")],
    )

    def network_failure(documents: list[MovieDocument]) -> None:
        assert documents[0].title == "Original"
        assert documents[0].film_version == 1
        assert execute(
            publication_database, "SELECT count(*) FROM notifications_source.source_state"
        ) == [(1,)]
        assert execute(
            publication_database,
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND state='idle in transaction'",
        ) == [(0,)]
        raise ConnectionError("ES failed after PG commit")

    monkeypatch.setattr(worker, "load_documents", network_failure)
    with pytest.raises(ConnectionError, match="ES failed"):
        worker.run_worker_pipeline()
    first_payload = execute(
        publication_database, "SELECT payload FROM notifications_source.source_state"
    )[0][0]
    monkeypatch.setattr(worker, "load_documents", lambda documents: None)
    result = worker.run_worker_pipeline()
    assert result is not None and result.documents[0].model_dump(mode="json") == first_payload
    assert result.job["attempts"] == 2


@pytest.mark.integration
@pytest.mark.parametrize("timed_out,failed_shards", [(True, 0), (False, 1)])
def test_legacy_capture_refuses_incomplete_search(
    publication_database: dict[str, Any],
    film: UUID,
    monkeypatch: pytest.MonkeyPatch,
    timed_out: bool,
    failed_shards: int,
) -> None:
    import postgres_to_es.catalog_events.legacy as legacy

    monkeypatch.setattr(
        legacy,
        "_request",
        lambda *args, **kwargs: {
            "hits": {"hits": []},
            "timed_out": timed_out,
            "_shards": {"failed": failed_shards},
        },
    )
    with pytest.raises(RuntimeError, match="Incomplete legacy search"):
        legacy._capture("old-index", uuid4(), NOW, 1)


@pytest.mark.integration
def test_metadata_stays_utc_when_database_session_timezone_differs(
    publication_database: dict[str, Any],
    film: UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import postgres_to_es.publication as publication

    monkeypatch.setattr(
        publication,
        "get_postgres_dsl",
        lambda: {
            **publication_database,
            "options": "-c timezone=Pacific/Honolulu",
        },
    )
    publication.prepare_publication(film, NOW)
    execute(
        publication_database,
        "UPDATE content.film_work SET title='Updated' WHERE id=%s",
        (str(film),),
    )
    # Caller clock went backwards; registry must still advance updated_at in UTC.
    updated = publication.prepare_publication(film, NOW - timedelta(days=1))
    payload = updated.to_elasticsearch()
    assert payload["catalog_published_at"] == "2026-01-01T00:00:00Z"
    assert payload["catalog_updated_at"] == "2026-01-01T00:00:00.000001Z"


@pytest.mark.integration
def test_legacy_failed_refresh_keeps_completion_pending_until_retry(
    publication_database: dict[str, Any],
    film: UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import postgres_to_es.catalog_events.legacy as legacy
    from postgres_to_es.settings import settings

    source, target = ["publication-test-" + uuid4().hex for _ in range(2)]
    load_id = uuid4()
    try:
        requests.put(
            f"{settings.elasticsearch_url}/{source}/_doc/{film}",
            json=MovieDocument(id=str(film), title="Old film").to_elasticsearch(),
            timeout=5,
        ).raise_for_status()
        requests.put(
            f"{settings.elasticsearch_url}/{source}/_settings",
            json={"index.blocks.write": True},
            timeout=5,
        ).raise_for_status()
        requests.post(
            f"{settings.elasticsearch_url}/{source}/_refresh", timeout=5
        ).raise_for_status()
        real_request = legacy._request

        def failed_refresh(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
            result = real_request(method, path, **kwargs)
            if method == "POST" and path == f"{target}/_refresh":
                # Real bulk/refresh and PG writes run; inject the HTTP200 shard-failure body.
                return {
                    "_shards": {
                        "total": 2,
                        "successful": 1,
                        "failed": 1,
                        "failures": [{"reason": "simulated refresh shard failure"}],
                    }
                }
            return result

        with monkeypatch.context() as patch:
            patch.setattr(legacy, "_request", failed_refresh)
            with pytest.raises(RuntimeError, match="Incomplete legacy refresh"):
                legacy.rebuild_legacy_catalog(source, target, load_id, NOW)
        boundary = execute(
            publication_database,
            "SELECT started_at,captured_at,completed_at "
            "FROM notifications_source.legacy_catalog_loads WHERE load_id=%s",
            (str(load_id),),
        )[0]
        assert boundary[0] == NOW and boundary[1] is not None and boundary[2] is None
        legacy.rebuild_legacy_catalog(source, target, load_id, NOW + timedelta(days=1))
        resumed = execute(
            publication_database,
            "SELECT started_at,captured_at,completed_at "
            "FROM notifications_source.legacy_catalog_loads WHERE load_id=%s",
            (str(load_id),),
        )[0]
        assert resumed[:2] == boundary[:2] and resumed[2] is not None
        response = requests.get(f"{settings.elasticsearch_url}/{target}/_search", timeout=5)
        response.raise_for_status()
        assert [hit["_source"]["id"] for hit in response.json()["hits"]["hits"]] == [str(film)]
    finally:
        for index in (source, target):
            response = requests.delete(f"{settings.elasticsearch_url}/{index}", timeout=5)
            assert response.status_code in (200, 404)
