import json
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from aiokafka.record.default_records import DefaultRecordBatchBuilder

from core.config import settings
from services.event_broker import (
    EventBroker,
    EventEnvelopeTooLargeError,
    PublishedEvent,
)
from services.event_schema import UserEvent

_KAFKA_RECORD_HEADERS: list[tuple[str, Optional[bytes]]] = []


def event_time_errors(events: list[UserEvent], received_at: datetime) -> list[dict[str, object]]:
    max_age = timedelta(seconds=settings.events_max_age_seconds)
    max_future_skew = timedelta(seconds=settings.events_max_future_skew_seconds)
    errors: list[dict[str, object]] = []
    for index, event in enumerate(events):
        delta = event.occurred_at - received_at
        if not -max_age <= delta <= max_future_skew:
            errors.append(
                {
                    'type': 'value_error',
                    'loc': ['body', 'events', index, 'occurred_at'],
                    'msg': 'occurred_at is outside the allowed window relative to received_at',
                    'input': event.occurred_at.isoformat(),
                    'ctx': {
                        'received_at': received_at.isoformat(),
                        'max_age_seconds': settings.events_max_age_seconds,
                        'max_future_skew_seconds': settings.events_max_future_skew_seconds,
                    },
                }
            )
    return errors


def build_published_event(
    event: UserEvent,
    *,
    user_id: UUID,
    request_id: Optional[str],
    received_at: datetime,
) -> PublishedEvent:
    envelope = {
        'event_id': str(event.event_id),
        'occurred_at': event.occurred_at.isoformat(),
        'received_at': received_at.isoformat(),
        'user_id': str(user_id),
        'request_id': request_id,
        'event_type': event.event_type,
        'payload': event.payload.model_dump(mode='json'),
    }
    return PublishedEvent(
        key=str(user_id),
        value=json.dumps(envelope, ensure_ascii=False).encode('utf-8'),
    )


def kafka_record_size(key: str, value: bytes) -> int:
    return int(
        DefaultRecordBatchBuilder.estimate_size_in_bytes(
            key.encode('utf-8'),
            value,
            _KAFKA_RECORD_HEADERS,
        )
    )


def ensure_records_fit_kafka(records: list[PublishedEvent]) -> None:
    max_request_size = settings.kafka_max_request_size
    for record in records:
        if kafka_record_size(record.key, record.value) > max_request_size:
            raise EventEnvelopeTooLargeError


async def publish_user_events(
    events: list[UserEvent],
    *,
    user_id: UUID,
    request_id: Optional[str],
    broker: EventBroker,
    received_at: datetime,
) -> int:
    records = [
        build_published_event(
            event,
            user_id=user_id,
            request_id=request_id,
            received_at=received_at,
        )
        for event in events
    ]
    ensure_records_fit_kafka(records)
    await broker.publish_batch(records)
    return len(records)
