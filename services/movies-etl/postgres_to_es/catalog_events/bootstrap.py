"""Durable historical baseline, write fencing and batched non-announcement events."""

import logging
from contextlib import closing
from typing import Any

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import Json, RealDictCursor

from postgres_to_es.catalog_events import store
from postgres_to_es.catalog_events.contracts import FilmSnapshot
from postgres_to_es.catalog_events.legacy import _request
from postgres_to_es.db import get_postgres_dsl
from postgres_to_es.settings import settings

logger = logging.getLogger(__name__)


def _load(conn: connection, load_id: str) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM notifications_source.catalog_bootstrap_loads WHERE load_id=%s",
            (load_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Unknown bootstrap load: {load_id}")
        return dict(row)


def catalog_status(load_id: str) -> dict[str, Any]:
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn:
        result = _load(conn, load_id)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT count(*) AS captured_count, "
                "count(enqueued_at) AS enqueued_count "
                "FROM notifications_source.catalog_bootstrap_members WHERE load_id=%s",
                (load_id,),
            )
            result.update(dict(cur.fetchone() or {}))
        return result


def _initialize(conn: connection, load_id: str) -> dict[str, Any]:
    if not load_id.strip():
        raise ValueError("load_id must not be empty")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM notifications_source.catalog_bootstrap_loads")
        previous = cur.fetchone()
        if previous:
            if previous["load_id"] != load_id:
                raise ValueError("Historical baseline already belongs to another load_id")
            return dict(previous)
        cur.execute("SELECT bootstrap_completed_at FROM notifications_source.detector_state")
        readiness = cur.fetchone()
        if readiness is None:
            raise RuntimeError("Detector schema not initialized; run migrate")
        if readiness["bootstrap_completed_at"] is not None:
            raise ValueError("Historical bootstrap already completed under another load")
    indices = _request("GET", f"{settings.elasticsearch_index}/_settings")
    if len(indices) != 1:
        raise ValueError("Bootstrap requires exactly one working index")
    source = next(iter(indices))
    index = indices[source]["settings"]["index"]
    mapping = _request("GET", f"{source}/_mapping")
    legacy_id = mapping[source]["mappings"].get("_meta", {}).get("legacy_load_id")
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT clock_timestamp() AS boundary")
        clock = cur.fetchone()
        assert clock is not None
        boundary = clock["boundary"]
        if legacy_id:
            if str(legacy_id) != load_id:
                raise ValueError("Reuse the legacy load_id for the original historical baseline")
            cur.execute(
                "SELECT * FROM notifications_source.legacy_catalog_loads WHERE load_id=%s",
                (legacy_id,),
            )
            legacy = cur.fetchone()
            if not legacy or not legacy["completed_at"] or legacy["target_index"] != source:
                raise ValueError("Legacy migration must complete before bootstrap capture")
            boundary = legacy["started_at"]
        cur.execute(
            """INSERT INTO notifications_source.catalog_bootstrap_loads
            (load_id,source_index,source_uuid,start_boundary,restore_writes,legacy_load_id)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *""",
            (
                load_id,
                source,
                index["uuid"],
                boundary,
                index.get("blocks", {}).get("write") != "true",
                legacy_id,
            ),
        )
        row = cur.fetchone()
        assert row is not None
        return dict(row)


def _source_settings(load: dict[str, Any]) -> dict[str, Any]:
    source = load["source_index"]
    indices = _request("GET", f"{settings.elasticsearch_index}/_settings")
    if (
        set(indices) != {source}
        or indices[source]["settings"]["index"]["uuid"] != load["source_uuid"]
    ):
        raise ValueError("Working index identity changed during bootstrap")
    result: dict[str, Any] = indices[source]["settings"]["index"]
    return result


def _fence(conn: connection, load: dict[str, Any]) -> None:
    source = load["source_index"]
    blocked = _source_settings(load).get("blocks", {}).get("write") == "true"
    if load["fenced_at"] and not blocked:
        raise RuntimeError(
            "Interrupted capture lost its write fence; do not resume a changed source"
        )
    if not load["fenced_at"]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM notifications_source.catalog_bootstrap_members "
                "WHERE load_id=%s LIMIT 1",
                (load["load_id"],),
            )
            if cur.fetchone() is not None:
                raise RuntimeError(
                    "Unverified fence but snapshot already exists; cannot reopen writes"
                )
        if blocked:
            if not load["restore_writes"]:
                raise RuntimeError(
                    "Preexisting external write fence has no durable drain confirmation; "
                    "its owner must pause ETL and remove the block before retrying capture"
                )
            # Unknown initial acknowledgement: no snapshot exists yet. Re-arm ONLY our block
            # so add-block really drains shards rather than returning an already-blocked no-op.
            released = _request("PUT", f"{source}/_settings", json={"index.blocks.write": False})
            if not released.get("acknowledged"):
                raise RuntimeError("Unverified bootstrap fence release was not acknowledged")
        fence = _request("PUT", f"{source}/_block/write")
        if (
            not fence.get("acknowledged")
            or not fence.get("shards_acknowledged")
            or fence.get("indices") != [{"name": source, "blocked": True}]
        ):
            raise RuntimeError("Incomplete bootstrap write fence")
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE notifications_source.catalog_bootstrap_loads "
                "SET fenced_at=clock_timestamp() WHERE load_id=%s",
                (load["load_id"],),
            )
    refresh = _request("POST", f"{source}/_refresh")
    if refresh["_shards"]["failed"]:
        raise RuntimeError("Incomplete bootstrap refresh")


def _save_capture_page(conn: connection, load_id: str, hits: list[dict[str, Any]]) -> None:
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        for hit in hits:
            payload = hit["_source"]
            film = FilmSnapshot.from_source(payload)
            if str(film.film_id) != hit["_id"]:
                raise ValueError("ES document id differs from film_id")
            cur.execute(
                "SELECT payload FROM notifications_source.catalog_bootstrap_members "
                "WHERE load_id=%s AND film_id=%s",
                (load_id, str(film.film_id)),
            )
            existing = cur.fetchone()
            if existing and existing["payload"] != payload:
                raise ValueError("Frozen bootstrap source changed during capture")
            cur.execute(
                "INSERT INTO notifications_source.catalog_bootstrap_members "
                "(load_id,film_id,payload) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (load_id, str(film.film_id), Json(payload)),
            )


def _capture_es(conn: connection, load: dict[str, Any]) -> None:
    scroll_id = None
    try:
        page = _request(
            "POST",
            f"{load['source_index']}/_search",
            params={"scroll": "5m", "allow_partial_search_results": "false"},
            json={
                "size": settings.catalog_batch_size,
                "sort": ["_doc"],
                "query": {"match_all": {}},
            },
        )
        while True:
            scroll_id = page.get("_scroll_id", scroll_id)
            if page.get("timed_out") or page["_shards"]["failed"]:
                raise RuntimeError("Incomplete bootstrap search")
            if not page["hits"]["hits"]:
                return
            _save_capture_page(conn, load["load_id"], page["hits"]["hits"])
            page = _request("POST", "_search/scroll", json={"scroll": "5m", "scroll_id": scroll_id})
    finally:
        if scroll_id:
            try:
                _request("DELETE", "_search/scroll", json={"scroll_id": [scroll_id]})
            except Exception:
                logger.warning(
                    "Bootstrap scroll cleanup failed; context expires in 5m", exc_info=True
                )


def _capture_legacy(conn: connection, load: dict[str, Any]) -> None:
    after = None
    while True:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT film_id,payload FROM notifications_source.legacy_catalog_membership
                WHERE load_id=%s AND (%s::uuid IS NULL OR film_id>%s::uuid)
                ORDER BY film_id LIMIT %s""",
                (load["legacy_load_id"], after, after, settings.catalog_batch_size),
            )
            rows = cur.fetchall()
        if not rows:
            return
        _save_capture_page(
            conn,
            load["load_id"],
            [{"_id": str(row["film_id"]), "_source": row["payload"]} for row in rows],
        )
        after = str(rows[-1]["film_id"])


def capture_catalog(load_id: str) -> int:
    """Capture returns total baseline size; crashes retain the durable write fence."""
    with store.process_lock(store.DETECTOR_LOCK) as conn:
        load = _initialize(conn, load_id)
        if not load["captured_at"]:
            _fence(conn, load)
            if load["legacy_load_id"]:
                _capture_legacy(conn, load)
            else:
                _capture_es(conn, load)
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE notifications_source.catalog_bootstrap_loads "
                    "SET captured_at=clock_timestamp() WHERE load_id=%s",
                    (load_id,),
                )
            load = _load(conn, load_id)
        if not load["released_at"]:
            _source_settings(load)
            if load["restore_writes"]:
                result = _request(
                    "PUT", f"{load['source_index']}/_settings", json={"index.blocks.write": False}
                )
                if not result.get("acknowledged"):
                    raise RuntimeError("Bootstrap write-fence release was not acknowledged")
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE notifications_source.catalog_bootstrap_loads "
                    "SET released_at=clock_timestamp() WHERE load_id=%s",
                    (load_id,),
                )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM notifications_source.catalog_bootstrap_members "
                "WHERE load_id=%s",
                (load_id,),
            )
            count = cur.fetchone()
            assert count is not None
            return int(count[0])


def enqueue_catalog(load_id: str, limit: int) -> int:
    """One atomic batch; frozen snapshot, known version, outbox and marker commit together."""
    if limit < 1:
        raise ValueError("limit must be positive")
    with store.process_lock(store.DETECTOR_LOCK) as conn:
        load = _load(conn, load_id)
        if not load["released_at"]:
            raise RuntimeError("Complete bootstrap capture and fence release before enqueue")
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT film_id,payload FROM notifications_source.catalog_bootstrap_members "
                "WHERE load_id=%s AND enqueued_at IS NULL ORDER BY film_id LIMIT %s",
                (load_id, limit),
            )
            rows = cur.fetchall()
            count = 0
            for row in rows:
                count += store.add_event(
                    cur, FilmSnapshot.from_source(row["payload"]), load_id=load_id
                )
                cur.execute(
                    "UPDATE notifications_source.catalog_bootstrap_members "
                    "SET enqueued_at=clock_timestamp() WHERE load_id=%s AND film_id=%s",
                    (load_id, row["film_id"]),
                )
            cur.execute(
                "SELECT 1 FROM notifications_source.catalog_bootstrap_members "
                "WHERE load_id=%s AND enqueued_at IS NULL LIMIT 1",
                (load_id,),
            )
            if cur.fetchone() is None and not load["completed_at"]:
                cur.execute(
                    "UPDATE notifications_source.catalog_bootstrap_loads "
                    "SET completed_at=clock_timestamp() WHERE load_id=%s",
                    (load_id,),
                )
                cur.execute(
                    """UPDATE notifications_source.detector_state SET bootstrap_load_id=%s,
                    baseline_at=%s,bootstrap_completed_at=clock_timestamp()
                    WHERE singleton AND bootstrap_completed_at IS NULL""",
                    (load_id, load["start_boundary"]),
                )
            return count
