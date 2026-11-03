import json
import logging
from typing import Any, cast

import requests

from postgres_to_es.pipelines.loader.mapping import MOVIES_INDEX_MAPPING
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import BackoffSettings, elasticsearch_backoff_settings, settings

logger = logging.getLogger(__name__)

ELASTICSEARCH_BACKOFF_SETTINGS = cast(BackoffSettings, elasticsearch_backoff_settings)


@ELASTICSEARCH_BACKOFF_SETTINGS.for_elasticsearch()
def ensure_index_exists(index: str) -> None:
    """Создать индекс с маппингом movies, если его ещё нет. Идемпотентно."""
    head = requests.head(
        f"{settings.elasticsearch_url}/{index}", timeout=settings.elasticsearch_timeout_seconds
    )
    if head.status_code == 200:
        logger.debug("Index %s already exists", index)
        return

    response = requests.put(
        f"{settings.elasticsearch_url}/{index}",
        json=MOVIES_INDEX_MAPPING,
        timeout=settings.elasticsearch_timeout_seconds,
    )
    if response.status_code == 400 and "resource_already_exists_exception" in response.text:
        # Гонка: другой воркер создал индекс между нашими HEAD и PUT — не ошибка.
        logger.debug("Index %s was created concurrently by another worker", index)
        return

    response.raise_for_status()
    logger.debug("Created index %s", index)


def _build_bulk_payload(documents: list[MovieDocument]) -> bytes:
    lines: list[str] = []
    for document in documents:
        if any(
            value is None
            for value in (
                document.index_revision,
                document.film_version,
                document.catalog_published_at,
                document.catalog_updated_at,
            )
        ):
            raise ValueError("Document requires persisted publication metadata before indexing")
        lines.append(
            json.dumps(
                {
                    "index": {
                        "_id": document.id,
                        "version": document.index_revision,
                        "version_type": "external",
                    }
                }
            )
        )
        lines.append(json.dumps(document.to_elasticsearch()))
    return ("\n".join(lines) + "\n").encode("utf-8")


@ELASTICSEARCH_BACKOFF_SETTINGS.for_elasticsearch()
def _post_bulk(payload: bytes, index: str) -> dict[str, Any]:
    response = requests.post(
        f"{settings.elasticsearch_url}/{index}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=settings.elasticsearch_timeout_seconds,
    )
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    return result


@ELASTICSEARCH_BACKOFF_SETTINGS.for_elasticsearch()
def _verify_version_conflict(document: MovieDocument, index: str) -> None:
    response = requests.get(
        f"{settings.elasticsearch_url}/{index}/_doc/{document.id}",
        timeout=settings.elasticsearch_timeout_seconds,
    )
    response.raise_for_status()
    existing = response.json()
    source = existing.get("_source", {})
    expected = document.to_elasticsearch()
    if (
        source.get("film_version", 0) < expected["film_version"]
        or source.get("catalog_published_at") != expected["catalog_published_at"]
        or not source.get("catalog_updated_at")
        or existing.get("_version", 0) < (document.index_revision or 0)
        or (existing["_version"] == document.index_revision and source != expected)
    ):
        raise RuntimeError("Index publication identity differs; perform legacy migration first")


def bulk_index(documents: list[MovieDocument], index: str) -> None:
    """Проиндексировать документы через _bulk.

    _bulk отвечает 200 даже если часть отдельных документов не
    проиндексировалась (transport-уровень и уровень документа разделены),
    поэтому raise_for_status() тут недостаточно — разбираем body сами.
    """
    if not documents:
        return

    ensure_index_exists(index)
    result = _post_bulk(_build_bulk_payload(documents), index)
    logger.debug(
        "Bulk indexed %s document(s) into %s (took=%sms)",
        len(documents),
        index,
        result.get("took"),
    )

    if result.get("errors"):
        by_id = {document.id: document for document in documents}
        for item in result.get("items", []):
            status = item.get("index", {})
            if (
                status.get("status") == 409
                and status.get("error", {}).get("type") == "version_conflict_engine_exception"
            ):
                _verify_version_conflict(by_id[status["_id"]], index)
        failed = [
            {"id": item["index"]["_id"], "error": item["index"]["error"]}
            for item in result.get("items", [])
            if item.get("index", {}).get("error") is not None
            and not (
                item["index"].get("status") == 409
                and item["index"]["error"].get("type") == "version_conflict_engine_exception"
            )
        ]
        # External revisions make equal/older snapshots safe no-ops.
        if not failed:
            return
        raise RuntimeError(
            f"{len(failed)} of {len(documents)} document(s) failed to index: {failed}"
        )
