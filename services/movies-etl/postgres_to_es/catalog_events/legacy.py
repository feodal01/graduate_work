"""Frozen legacy working-index capture and resumable copy; no Kafka events or cutover."""

import argparse
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg2
import requests
from psycopg2.extras import Json, RealDictCursor

from postgres_to_es.db import get_postgres_dsl
from postgres_to_es.pipelines.loader.client import bulk_index
from postgres_to_es.pipelines.loader.mapping import MOVIES_INDEX_MAPPING
from postgres_to_es.publication import (
    _canonical,
    _lock_film,
    _persist_snapshot,
    migrate_publication_state,
)
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import settings


def _request(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = requests.request(
        method,
        f"{settings.elasticsearch_url}/{path}",
        timeout=settings.elasticsearch_timeout_seconds,
        **kwargs,
    )
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    return result


def _capture_page(load_id: UUID, hits: list[dict[str, Any]], started_at: datetime) -> None:
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn, conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            for hit in hits:
                document = MovieDocument.model_validate(hit["_source"])
                if document.id != hit["_id"]:
                    raise ValueError("Legacy film id differs from Elasticsearch _id")
                if document.film_version is not None:
                    raise ValueError(
                        "Source already has publication metadata; use normal reindexing"
                    )
                film_id = UUID(document.id)
                _lock_film(cursor, film_id)
                cursor.execute(
                    """SELECT payload FROM notifications_source.legacy_catalog_membership
                                  WHERE load_id=%s AND film_id=%s""",
                    (str(load_id), document.id),
                )
                previous = cursor.fetchone()
                if previous:
                    if _canonical(MovieDocument.model_validate(previous["payload"])) != _canonical(
                        document
                    ):
                        raise ValueError(
                            "Legacy source changed during capture; keep source write-blocked"
                        )
                    continue
                cursor.execute(
                    "SELECT 1 FROM notifications_source.source_state WHERE film_id=%s",
                    (document.id,),
                )
                if cursor.fetchone():
                    raise ValueError(
                        "Source already prepared outside this migration; drain ETL first"
                    )
                prepared = _persist_snapshot(cursor, document, started_at)
                cursor.execute(
                    """INSERT INTO notifications_source.legacy_catalog_membership
                                  (load_id,film_id,payload) VALUES(%s,%s,%s)""",
                    (str(load_id), document.id, Json(prepared.model_dump(mode="json"))),
                )


def _capture(source: str, load_id: UUID, started_at: datetime, batch_size: int) -> None:
    page = _request(
        "POST",
        f"{source}/_search",
        params={"scroll": "5m", "allow_partial_search_results": "false"},
        json={"size": batch_size, "sort": ["_doc"], "query": {"match_all": {}}},
    )
    scroll_id = page.get("_scroll_id")
    try:
        while True:
            if page.get("timed_out") or page["_shards"]["failed"]:
                raise RuntimeError("Incomplete legacy search; capture was not completed")
            if not page["hits"]["hits"]:
                break
            _capture_page(load_id, page["hits"]["hits"], started_at)
            page = _request("POST", "_search/scroll", json={"scroll": "5m", "scroll_id": scroll_id})
            scroll_id = page.get("_scroll_id", scroll_id)
    finally:
        if scroll_id:
            _request("DELETE", "_search/scroll", json={"scroll_id": [scroll_id]})
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn, conn, conn.cursor() as cursor:
        cursor.execute(
            "UPDATE notifications_source.legacy_catalog_loads "
            "SET captured_at=clock_timestamp() WHERE load_id=%s",
            (str(load_id),),
        )


def rebuild_legacy_catalog(
    source_index: str,
    target_index: str,
    load_id: UUID,
    now: datetime,
) -> None:
    """Retain the original snapshot and boundary; resume by the SAME load_id.

    Requires an operator-drained ETL and a write-blocked concrete source index.
    No transaction spans ES I/O. A session lock excludes concurrent migration runs.
    Never updates/deletes the source, switches aliases, or sends notifications.
    """
    if source_index == target_index:
        raise ValueError("Use distinct source/target indices")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Publication time must be timezone-aware")
    migrate_publication_state()
    with closing(psycopg2.connect(**get_postgres_dsl())) as lock_conn:
        lock_conn.autocommit = True
        with lock_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock("
                "hashtextextended('movies-legacy-capture', 0)) AS locked"
            )
            locked = cursor.fetchone()
            if not locked or not locked["locked"]:
                raise RuntimeError("Another legacy catalog migration is running")
            cursor.execute(
                "SELECT * FROM notifications_source.legacy_catalog_loads WHERE load_id=%s",
                (str(load_id),),
            )
            load = cursor.fetchone()
            if load:
                if (load["source_index"], load["target_index"]) != (source_index, target_index):
                    raise ValueError("load_id belongs to different source/target indices")
                if load["completed_at"]:
                    return
            source_settings = _request("GET", f"{source_index}/_settings")
            if set(source_settings) != {source_index}:
                raise ValueError("Source must be one concrete working index")
            if (
                source_settings[source_index]["settings"]["index"].get("blocks", {}).get("write")
                != "true"
            ):
                raise ValueError("Legacy source must be write-blocked after draining ETL")
            if not load:
                if (
                    requests.head(
                        f"{settings.elasticsearch_url}/{target_index}",
                        timeout=settings.elasticsearch_timeout_seconds,
                    ).status_code
                    != 404
                ):
                    raise ValueError("Target must be a new index")
                cursor.execute(
                    """INSERT INTO notifications_source.legacy_catalog_loads
                                  (load_id,source_index,target_index,started_at) VALUES(%s,%s,%s,%s)
                                  RETURNING *""",
                    (str(load_id), source_index, target_index, now.astimezone(timezone.utc)),
                )
                load = cursor.fetchone()
            target_exists = requests.head(
                f"{settings.elasticsearch_url}/{target_index}",
                timeout=settings.elasticsearch_timeout_seconds,
            )
            if target_exists.status_code == 404:
                mapping = deepcopy(MOVIES_INDEX_MAPPING)
                mapping["mappings"]["_meta"] = {"legacy_load_id": str(load_id)}
                _request("PUT", target_index, json=mapping)
            else:
                target_exists.raise_for_status()
                mapping = _request("GET", f"{target_index}/_mapping")
                if mapping.get(target_index, {}).get("mappings", {}).get("_meta", {}).get(
                    "legacy_load_id"
                ) != str(load_id):
                    raise ValueError("Target does not belong to this migration")
            assert load is not None
            if not load["captured_at"]:
                _capture(
                    source_index, load_id, load["started_at"], settings.catalog_legacy_batch_size
                )
            last_id: str | None = None
            while True:
                cursor.execute(
                    """SELECT film_id,payload FROM notifications_source.legacy_catalog_membership
                                  WHERE load_id=%s AND (%s::uuid IS NULL OR film_id>%s::uuid)
                                  ORDER BY film_id LIMIT %s""",
                    (str(load_id), last_id, last_id, settings.catalog_legacy_batch_size),
                )
                rows = cursor.fetchall()
                if not rows:
                    break
                bulk_index(
                    [MovieDocument.model_validate(row["payload"]) for row in rows], target_index
                )
                last_id = str(rows[-1]["film_id"])
            refresh = _request("POST", f"{target_index}/_refresh")
            if refresh["_shards"]["failed"]:
                raise RuntimeError("Incomplete legacy refresh; migration was not completed")
            cursor.execute(
                "UPDATE notifications_source.legacy_catalog_loads "
                "SET completed_at=clock_timestamp() WHERE load_id=%s",
                (str(load_id),),
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-index", required=True)
    parser.add_argument("--target-index", required=True)
    parser.add_argument("--load-id", required=True, type=UUID)
    args = parser.parse_args()
    rebuild_legacy_catalog(
        args.source_index,
        args.target_index,
        args.load_id,
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    main()
