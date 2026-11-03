"""Сценарий дедупликации: повтор того же event_id после FINAL даёт одну строку."""

from uuid import uuid4

from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client

from tests.functional.testdata.envelopes import watch_progress
from tests.functional.utils.helpers import event_count, produce, wait_for_event, wait_until


async def test_repeated_event_id_collapses_with_final(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: два одинаковых watch_progress. Act: оба уходят в Kafka.
    Assert: count() FINAL = 1. count() без FINAL >= 1 (может быть 2 до merge).
    """
    user_id = uuid4()
    film_id = uuid4()
    event_id = uuid4()
    payload = watch_progress(film_id=film_id, event_id=event_id, user_id=user_id)

    await produce(kafka_producer, payload, key=user_id)
    await produce(kafka_producer, payload, key=user_id)
    await wait_for_event(clickhouse_client, 'ugc.film_watches', event_id)

    async def _final_is_one() -> bool:
        return await event_count(clickhouse_client, 'ugc.film_watches', event_id, final=True) == 1

    await wait_until(_final_is_one, description=f'FINAL count() == 1 для {event_id}')
    raw_count = await event_count(clickhouse_client, 'ugc.film_watches', event_id, final=False)
    assert raw_count >= 1
