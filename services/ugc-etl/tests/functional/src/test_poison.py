"""Сценарии яда: битое сообщение уходит в DLQ и не попадает в ClickHouse."""

import asyncio
import json
from typing import Optional
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client
from docker.models.containers import Container

from tests.functional.settings import test_settings
from tests.functional.testdata.envelopes import envelope_bytes, watch_progress
from tests.functional.utils.helpers import (
    consume_matching,
    describe_group_offsets,
    event_count,
    produce,
    wait_for_event,
    wait_until,
)

POSITION_AFTER_DURATION_FILM = uuid4()


def _event_id_from_json(raw: bytes) -> Optional[UUID]:
    try:
        payload = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        return None
    event_id = payload.get('event_id')
    if not event_id:
        return None
    return UUID(str(event_id))


@pytest.mark.parametrize(
    'poison',
    [
        pytest.param(b'{', id='truncated-json'),
        pytest.param(b'\xff\xfe not utf-8', id='invalid-utf8'),
        pytest.param(
            envelope_bytes(event_type='not_a_real_event', payload={'x': 1}),
            id='unknown-event-type',
        ),
        pytest.param(
            envelope_bytes(
                event_type='watch_progress',
                payload={'position_seconds': 1, 'duration_seconds': 10},
            ),
            id='watch-progress-without-film-id',
        ),
        pytest.param(
            envelope_bytes(
                event_type='watch_progress',
                payload={
                    'film_id': str(POSITION_AFTER_DURATION_FILM),
                    'position_seconds': 100,
                    'duration_seconds': 10,
                },
            ),
            id='position-after-duration',
        ),
    ],
)
async def test_poison_goes_to_dlq_and_is_not_inserted(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
    kafka_container: Container,
    poison: bytes,
) -> None:
    """Arrange: яд в ugc.events. Act: ETL обрабатывает батч.
    Assert: сообщение в DLQ, в ugc.events его нет, CURRENT-OFFSET вырос.
    """
    user_id = uuid4()
    before = await asyncio.to_thread(describe_group_offsets, kafka_container)
    current_before = before[0]['current'] if before else None

    await produce(kafka_producer, poison, key=user_id)
    await consume_matching(poison, topic=test_settings.kafka_dlq_topic)

    event_id = _event_id_from_json(poison)
    if event_id is not None:
        assert await event_count(clickhouse_client, 'ugc.events', event_id) == 0

    async def _offset_moved() -> bool:
        rows = await asyncio.to_thread(describe_group_offsets, kafka_container)
        if not rows:
            return False
        current = rows[0]['current']
        if current_before is None:
            return current is not None
        return current is not None and current > current_before

    await wait_until(_offset_moved, description='CURRENT-OFFSET group ugc-etl вырос после DLQ')


async def test_valid_event_after_poison_still_loads(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: яд, затем валидный progress. Assert: валидный event_id в ugc.events."""
    user_id = uuid4()
    event_id = uuid4()
    film_id = uuid4()

    await produce(kafka_producer, b'{', key=user_id)
    await produce(
        kafka_producer,
        watch_progress(film_id=film_id, event_id=event_id, user_id=user_id),
        key=user_id,
    )

    row = await wait_for_event(clickhouse_client, 'ugc.events', event_id)
    assert UUID(str(row['film_id'])) == film_id
