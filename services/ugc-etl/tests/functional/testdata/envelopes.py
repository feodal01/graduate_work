"""Конверт Kafka, который ETL обязан читать. Дублируется специально: тесты не импортируют сервис."""

import json
from typing import Any, Optional
from uuid import UUID, uuid4

OCCURRED_AT = '2026-09-19T12:00:00+00:00'
RECEIVED_AT = '2026-09-19T12:00:01+00:00'
WATCH_PROGRESS_RATIO = 120 / 7200


def envelope(
    *,
    event_type: str,
    payload: dict[str, Any],
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        'event_id': str(event_id or uuid4()),
        'occurred_at': OCCURRED_AT,
        'received_at': RECEIVED_AT,
        'user_id': str(user_id or uuid4()),
        'request_id': 'ft-req',
        'event_type': event_type,
        'payload': payload,
    }
    if extra:
        body.update(extra)
    return body


def envelope_bytes(**kwargs: Any) -> bytes:
    return json.dumps(envelope(**kwargs), ensure_ascii=False).encode('utf-8')


def watch_progress(
    *,
    film_id: UUID,
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
    position_seconds: float = 120,
    duration_seconds: float = 7200,
    extra: Optional[dict[str, Any]] = None,
) -> bytes:
    return envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(film_id),
            'position_seconds': position_seconds,
            'duration_seconds': duration_seconds,
        },
        event_id=event_id,
        user_id=user_id,
        extra=extra,
    )


def watch_completed(
    *,
    film_id: UUID,
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
) -> bytes:
    return envelope_bytes(
        event_type='watch_completed',
        payload={'film_id': str(film_id)},
        event_id=event_id,
        user_id=user_id,
    )


def click_event(
    *,
    page: str,
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
    element_type: str = 'movie',
    element_id: str = 'play',
) -> bytes:
    return envelope_bytes(
        event_type='click',
        payload={'page': page, 'element_type': element_type, 'element_id': element_id},
        event_id=event_id,
        user_id=user_id,
    )


def page_view(
    *,
    page: str,
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
    duration_seconds: float = 15,
) -> bytes:
    return envelope_bytes(
        event_type='page_view',
        payload={'page': page, 'duration_seconds': duration_seconds},
        event_id=event_id,
        user_id=user_id,
    )


def quality_changed(
    *,
    film_id: UUID,
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
    from_quality: str = '480p',
    to_quality: str = '1080p',
) -> bytes:
    return envelope_bytes(
        event_type='quality_changed',
        payload={
            'film_id': str(film_id),
            'from_quality': from_quality,
            'to_quality': to_quality,
        },
        event_id=event_id,
        user_id=user_id,
    )


def search_filter_used(
    *,
    filters: dict[str, Any],
    event_id: Optional[UUID] = None,
    user_id: Optional[UUID] = None,
) -> bytes:
    return envelope_bytes(
        event_type='search_filter_used',
        payload={'filters': filters},
        event_id=event_id,
        user_id=user_id,
    )
