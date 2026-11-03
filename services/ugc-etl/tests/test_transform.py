import json
from typing import Optional
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from services.transform import MAX_PAGE_LENGTH, parse_envelope
from tests.conftest import FILM_ID, TEST_USER_ID

WATCH_PROGRESS_RATIO = 120 / 7200


def envelope_bytes(
    *,
    event_type: str,
    payload: dict[str, object],
    event_id: Optional[UUID] = None,
    occurred_at: str = '2026-09-16T12:00:00Z',
    received_at: str = '2026-09-16T12:00:01Z',
) -> bytes:
    body = {
        'event_id': str(event_id or uuid4()),
        'occurred_at': occurred_at,
        'received_at': received_at,
        'user_id': str(TEST_USER_ID),
        'request_id': 'req-1',
        'event_type': event_type,
        'payload': payload,
    }
    return json.dumps(body).encode('utf-8')


def test_watch_progress_row_has_film_id_and_completion_ratio() -> None:
    raw = envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(FILM_ID),
            'position_seconds': 120,
            'duration_seconds': 7200,
        },
    )

    row = parse_envelope(raw)

    assert row.film_id == FILM_ID
    assert row.event_type == 'watch_progress'
    assert row.completion_ratio == pytest.approx(WATCH_PROGRESS_RATIO)
    assert row.payload == raw.decode('utf-8')


def test_watch_completed_row_sets_completion_ratio_to_one() -> None:
    raw = envelope_bytes(
        event_type='watch_completed',
        payload={'film_id': str(FILM_ID)},
    )

    row = parse_envelope(raw)

    assert row.film_id == FILM_ID
    assert row.completion_ratio == 1.0


def test_click_row_has_page() -> None:
    raw = envelope_bytes(
        event_type='click',
        payload={
            'page': '/film/3d825f60-9fff-4dfe-b294-1a45fa1e115d',
            'element_type': 'movie',
            'element_id': str(FILM_ID),
        },
    )

    row = parse_envelope(raw)

    assert row.page == '/film/3d825f60-9fff-4dfe-b294-1a45fa1e115d'
    assert row.element_type == 'movie'


def test_quality_changed_row_has_from_quality() -> None:
    raw = envelope_bytes(
        event_type='quality_changed',
        payload={
            'film_id': str(FILM_ID),
            'from_quality': '720p',
            'to_quality': '1080p',
        },
    )

    row = parse_envelope(raw)

    assert row.film_id == FILM_ID
    assert row.from_quality == '720p'
    assert row.to_quality == '1080p'


def test_search_filter_used_keeps_filters_json_and_skips_page() -> None:
    raw = envelope_bytes(
        event_type='search_filter_used',
        payload={'filters': {'genre': 'drama', 'year': ['2024']}},
    )

    row = parse_envelope(raw)

    assert row.page is None
    assert row.film_id is None
    assert json.loads(row.filters) == {'genre': 'drama', 'year': ['2024']}


@pytest.mark.parametrize(
    'value',
    [
        b'{',
        envelope_bytes(
            event_type='watch_progress',
            payload={'position_seconds': 120, 'duration_seconds': 7200},
        ),
        envelope_bytes(
            event_type='watch_progress',
            payload={
                'film_id': str(FILM_ID),
                'position_seconds': 8000,
                'duration_seconds': 7200,
            },
        ),
        envelope_bytes(
            event_type='click',
            payload={'element_type': 'movie', 'element_id': str(FILM_ID)},
        ),
        envelope_bytes(
            event_type='click',
            payload={
                'element_type': 'movie',
                'element_id': str(FILM_ID),
                'page': 'p' * (MAX_PAGE_LENGTH + 1),
            },
        ),
    ],
)
def test_invalid_envelopes_raise_validation_error(value: bytes) -> None:
    with pytest.raises(ValidationError):
        parse_envelope(value)
