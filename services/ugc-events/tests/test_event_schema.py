from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from services.event_schema import (
    MAX_FILTER_VALUE_LENGTH,
    MAX_PAGE_LENGTH,
    EventBatch,
)

FILM_ID = '3d825f60-9fff-4dfe-b294-1a45fa1e115d'


def _event(
    event_type: str,
    payload: dict[str, Any],
    *,
    occurred_at: str = '2026-09-15T12:00:00Z',
) -> dict[str, Any]:
    return {
        'event_id': str(uuid4()),
        'occurred_at': occurred_at,
        'event_type': event_type,
        'payload': payload,
    }


@pytest.mark.parametrize(
    'event',
    (
        _event(
            'click',
            {
                'element_type': 'movie',
                'element_id': FILM_ID,
                'page': f'/film/{FILM_ID}',
            },
        ),
        _event('page_view', {'page': '/promo', 'duration_seconds': 12.5}),
        _event(
            'quality_changed',
            {
                'film_id': FILM_ID,
                'from_quality': '720p',
                'to_quality': '1080p',
            },
        ),
        _event('watch_completed', {'film_id': FILM_ID}),
        _event(
            'search_filter_used',
            {'filters': {'genre': 'action', 'actor': ['Ford']}},
        ),
        _event(
            'watch_progress',
            {
                'film_id': FILM_ID,
                'position_seconds': 90,
                'duration_seconds': 7200,
            },
        ),
    ),
)
def test_event_batch_accepts_known_types(event: dict[str, Any]) -> None:
    batch = EventBatch.model_validate({'events': [event]})

    assert len(batch.events) == 1
    assert batch.events[0].event_type == event['event_type']


def test_click_without_page_is_rejected() -> None:
    event = _event(
        'click',
        {'element_type': 'movie', 'element_id': FILM_ID},
    )

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})


def test_unknown_event_type_is_rejected() -> None:
    event = _event('unknown', {'page': '/'})

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})


def test_watch_progress_rejects_position_after_duration() -> None:
    event = _event(
        'watch_progress',
        {
            'film_id': FILM_ID,
            'position_seconds': 100,
            'duration_seconds': 10,
        },
    )

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})


def test_naive_occurred_at_is_rejected() -> None:
    event = _event(
        'watch_completed',
        {'film_id': FILM_ID},
        occurred_at='2026-09-15T12:00:00',
    )

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})


def test_empty_batch_is_rejected() -> None:
    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': []})


def test_click_page_longer_than_limit_is_rejected() -> None:
    event = _event(
        'click',
        {
            'element_type': 'movie',
            'element_id': FILM_ID,
            'page': 'p' * (MAX_PAGE_LENGTH + 1),
        },
    )

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})


def test_search_filter_value_longer_than_limit_is_rejected() -> None:
    event = _event(
        'search_filter_used',
        {'filters': {'genre': 'a' * (MAX_FILTER_VALUE_LENGTH + 1)}},
    )

    with pytest.raises(ValidationError):
        EventBatch.model_validate({'events': [event]})
