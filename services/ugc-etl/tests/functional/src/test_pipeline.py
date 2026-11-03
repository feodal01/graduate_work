"""Сценарии пайплайна: конверт из Kafka становится строкой ugc.events и витрины фильма."""

from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client

from tests.functional.testdata.envelopes import (
    WATCH_PROGRESS_RATIO,
    watch_completed,
    watch_progress,
)
from tests.functional.utils.helpers import ch_query, produce, wait_for_event


async def test_watch_progress_and_completed_land_in_events_and_film_watches(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: пара progress/completed. Act: produce в ugc.events.
    Assert: film_id колонкой, ratio 120/7200 и 1, uniq без FINAL.
    """
    user_id = uuid4()
    film_id = uuid4()
    progress_id = uuid4()
    completed_id = uuid4()

    await produce(
        kafka_producer,
        watch_progress(film_id=film_id, event_id=progress_id, user_id=user_id),
        key=user_id,
    )
    await produce(
        kafka_producer,
        watch_completed(film_id=film_id, event_id=completed_id, user_id=user_id),
        key=user_id,
    )

    events_progress = await wait_for_event(clickhouse_client, 'ugc.events', progress_id)
    events_completed = await wait_for_event(clickhouse_client, 'ugc.events', completed_id)
    watches_progress = await wait_for_event(clickhouse_client, 'ugc.film_watches', progress_id)
    watches_completed = await wait_for_event(clickhouse_client, 'ugc.film_watches', completed_id)

    assert UUID(str(events_progress['film_id'])) == film_id
    assert UUID(str(events_completed['film_id'])) == film_id
    assert events_progress['event_type'] == 'watch_progress'
    assert events_completed['event_type'] == 'watch_completed'
    assert events_progress['completion_ratio'] == pytest.approx(WATCH_PROGRESS_RATIO, rel=1e-5)
    assert events_completed['completion_ratio'] == pytest.approx(1.0)
    assert UUID(str(watches_progress['film_id'])) == film_id
    assert UUID(str(watches_completed['film_id'])) == film_id
    assert watches_progress['completion_ratio'] == pytest.approx(WATCH_PROGRESS_RATIO, rel=1e-5)
    assert watches_completed['completion_ratio'] == pytest.approx(1.0)

    named = await ch_query(
        clickhouse_client,
        'SELECT film_id, uniq(user_id) AS viewers '
        "FROM ugc.film_watches WHERE event_type = 'watch_progress' AND film_id = %(film_id)s "
        'GROUP BY film_id',
        {'film_id': str(film_id)},
    )
    assert len(named) == 1
    assert named[0]['viewers'] >= 1


async def test_unknown_envelope_field_does_not_block_partition(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: валидный progress с лишним полем. Act: produce. Assert: строка доехала."""
    user_id = uuid4()
    film_id = uuid4()
    event_id = uuid4()

    await produce(
        kafka_producer,
        watch_progress(
            film_id=film_id,
            event_id=event_id,
            user_id=user_id,
            extra={'future_field': 'keep-going'},
        ),
        key=user_id,
    )

    row = await wait_for_event(clickhouse_client, 'ugc.events', event_id)
    assert UUID(str(row['film_id'])) == film_id
    assert row['event_type'] == 'watch_progress'
