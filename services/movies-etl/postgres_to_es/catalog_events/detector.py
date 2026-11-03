"""Read only the working ES index; replay durable windows with a fresh transient PIT."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from postgres_to_es.catalog_events import store
from postgres_to_es.catalog_events.contracts import FilmSnapshot
from postgres_to_es.catalog_events.legacy import _request
from postgres_to_es.settings import settings

logger = logging.getLogger(__name__)


def scan_catalog(until: datetime, reconcile: bool = False) -> int:
    if until.tzinfo is None or until.utcoffset() is None:
        raise ValueError("until must be timezone-aware")
    until = until.astimezone(timezone.utc)
    if settings.catalog_overlap_seconds is None:
        raise ValueError("CATALOG_OVERLAP_SECONDS is required")
    with store.process_lock(store.DETECTOR_LOCK) as conn:
        state = store.readiness()
        if not state["bootstrap_completed_at"]:
            raise RuntimeError("Historical bootstrap must complete before detector startup")
        if state["active_until"] is not None:
            # A new PIT cannot reuse the old PIT search_after. Replay entire fixed window.
            lower, until, reconcile = (
                state["active_from"],
                state["active_until"],
                state["active_reconcile"],
            )
        else:
            watermark = state["watermark_at"] or state["baseline_at"]
            if until < watermark:
                raise ValueError("until cannot precede the durable watermark")
            lower = (
                None
                if reconcile
                else watermark - timedelta(seconds=settings.catalog_overlap_seconds)
            )
        pit = _request("POST", f"{settings.elasticsearch_index}/_pit", params={"keep_alive": "5m"})
        pit_id = pit["id"]
        try:
            if pit.get("_shards", {}).get("failed", 0):
                raise RuntimeError("Incomplete PIT; progress unchanged")
            if state["active_until"] is None:
                store.begin_window(conn, lower, until, reconcile)
            count = 0
            after = None
            while True:
                bounds = {"lte": until.isoformat()}
                if lower is not None:
                    bounds["gte"] = lower.isoformat()
                body: dict[str, Any] = {
                    "size": settings.catalog_batch_size,
                    "pit": {"id": pit_id, "keep_alive": "5m"},
                    "sort": [
                        {
                            "catalog_updated_at": {
                                "order": "asc",
                                "format": "strict_date_optional_time_nanos",
                                "numeric_type": "date_nanos",
                            }
                        },
                        {"id": "asc"},
                    ],
                    "query": {"range": {"catalog_updated_at": bounds}},
                    "track_total_hits": False,
                }
                if after is not None:
                    body["search_after"] = after
                page = _request(
                    "POST", "_search", params={"allow_partial_search_results": "false"}, json=body
                )
                pit_id = page.get("pit_id", pit_id)
                if page.get("timed_out") or page["_shards"]["failed"]:
                    raise RuntimeError("Incomplete catalog search; progress unchanged")
                hits = page["hits"]["hits"]
                if not hits:
                    store.finish_window(conn, until, reconcile)
                    return count
                films = [FilmSnapshot.from_source(hit["_source"]) for hit in hits]
                if any(
                    str(film.film_id) != hit["_id"] for film, hit in zip(films, hits, strict=True)
                ):
                    raise ValueError("ES document id differs from film_id")
                after = hits[-1]["sort"]
                count += store.save_page(conn, films, after)
        finally:
            try:
                _request("DELETE", "_pit", json={"id": pit_id})
            except Exception:
                logger.exception("Catalog PIT cleanup failed; PIT expires automatically")
