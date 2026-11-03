from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from aiokafka.record.default_records import DefaultRecordBatchBuilder

from core.config import settings
from services.event_broker import EventEnvelopeTooLargeError, PublishedEvent
from services.event_schema import (
    MAX_ELEMENT_ID_LENGTH,
    MAX_ELEMENT_TYPE_LENGTH,
    MAX_FILTER_FIELDS,
    MAX_FILTER_KEY_LENGTH,
    MAX_FILTER_VALUE_LENGTH,
    MAX_FILTER_VALUES,
    MAX_PAGE_LENGTH,
    MAX_QUALITY_LENGTH,
    MAX_REQUEST_ID_LENGTH,
    ClickEvent,
    ClickPayload,
    PageViewEvent,
    QualityChangedEvent,
    SearchFilterUsedEvent,
    UserEvent,
    WatchCompletedEvent,
    WatchProgressEvent,
)
from services.ingest import (
    build_published_event,
    ensure_records_fit_kafka,
    kafka_record_size,
    publish_user_events,
)
from tests.conftest import FILM_ID, RECEIVED_AT, TEST_USER_ID, MemoryEventBroker, click_event

_LONG_TS = datetime(
    9999,
    12,
    31,
    23,
    59,
    59,
    999999,
    tzinfo=timezone(timedelta(hours=14, minutes=45)),
)
_UTF8_MAX_CHAR = '\U00010000'


def _utf8_run(length: int, start: int = 0x10000) -> str:
    return ''.join(chr(start + offset) for offset in range(length))


def _max_events() -> list[UserEvent]:
    page = _UTF8_MAX_CHAR * MAX_PAGE_LENGTH
    click = ClickEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'click',
            'payload': {
                'element_type': _UTF8_MAX_CHAR * MAX_ELEMENT_TYPE_LENGTH,
                'element_id': _UTF8_MAX_CHAR * MAX_ELEMENT_ID_LENGTH,
                'page': page,
            },
        }
    )
    page_view = PageViewEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'page_view',
            'payload': {'page': page, 'duration_seconds': 1.7976931348623157e308},
        }
    )
    quality = QualityChangedEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'quality_changed',
            'payload': {
                'film_id': str(FILM_ID),
                'from_quality': _UTF8_MAX_CHAR * MAX_QUALITY_LENGTH,
                'to_quality': _UTF8_MAX_CHAR * MAX_QUALITY_LENGTH,
            },
        }
    )
    completed = WatchCompletedEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'watch_completed',
            'payload': {'film_id': str(FILM_ID)},
        }
    )
    filters = {
        _utf8_run(MAX_FILTER_KEY_LENGTH, start=0x10000 + index * MAX_FILTER_KEY_LENGTH): [
            _UTF8_MAX_CHAR * MAX_FILTER_VALUE_LENGTH
        ]
        * MAX_FILTER_VALUES
        for index in range(MAX_FILTER_FIELDS)
    }
    search = SearchFilterUsedEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'search_filter_used',
            'payload': {'filters': filters},
        }
    )
    progress = WatchProgressEvent.model_validate(
        {
            'event_id': str(uuid4()),
            'occurred_at': _LONG_TS.isoformat(),
            'event_type': 'watch_progress',
            'payload': {
                'film_id': str(FILM_ID),
                'position_seconds': 1.7976931348623157e308,
                'duration_seconds': 1.7976931348623157e308,
            },
        }
    )
    return [click, page_view, quality, completed, search, progress]


def _largest_fitting_payload(key: str) -> tuple[bytes, bytes]:
    key_bytes = key.encode('utf-8')
    max_size = settings.kafka_max_request_size
    low = 0
    high = max_size
    while low < high:
        mid = (low + high + 1) // 2
        candidate = b'x' * mid
        size = DefaultRecordBatchBuilder.estimate_size_in_bytes(key_bytes, candidate, [])
        if size <= max_size:
            low = mid
        else:
            high = mid - 1
    return b'x' * low, b'x' * (low + 1)


def test_kafka_record_size_matches_aiokafka_estimate() -> None:
    key = str(TEST_USER_ID)
    value = b'{"page":"/film"}'
    expected = DefaultRecordBatchBuilder.estimate_size_in_bytes(
        key.encode('utf-8'),
        value,
        [],
    )

    assert kafka_record_size(key, value) == expected


def test_ensure_records_fit_kafka_uses_same_boundary_as_aiokafka() -> None:
    key = str(TEST_USER_ID)
    fitting, oversized = _largest_fitting_payload(key)

    ensure_records_fit_kafka([PublishedEvent(key=key, value=fitting)])

    with pytest.raises(EventEnvelopeTooLargeError):
        ensure_records_fit_kafka(
            [
                PublishedEvent(key=key, value=fitting),
                PublishedEvent(key=key, value=oversized),
            ]
        )


async def test_publish_user_events_does_not_publish_oversized_batch() -> None:
    valid = ClickEvent.model_validate(click_event())
    oversized = ClickEvent.model_construct(
        event_id=uuid4(),
        occurred_at=RECEIVED_AT,
        event_type='click',
        payload=ClickPayload.model_construct(
            element_type='movie',
            element_id='x',
            page='p' * settings.kafka_max_request_size,
        ),
    )
    broker = MemoryEventBroker()

    with pytest.raises(EventEnvelopeTooLargeError):
        await publish_user_events(
            [valid, oversized],
            user_id=TEST_USER_ID,
            request_id=None,
            broker=broker,
            received_at=RECEIVED_AT,
        )

    assert broker.records == []


def test_max_schema_event_fits_kafka_limit() -> None:
    request_id = _UTF8_MAX_CHAR * MAX_REQUEST_ID_LENGTH

    for event in _max_events():
        record = build_published_event(
            event,
            user_id=TEST_USER_ID,
            request_id=request_id,
            received_at=_LONG_TS,
        )
        message_size = kafka_record_size(record.key, record.value)
        assert message_size <= settings.kafka_max_request_size, (
            event.event_type,
            message_size,
            settings.kafka_max_request_size,
        )
