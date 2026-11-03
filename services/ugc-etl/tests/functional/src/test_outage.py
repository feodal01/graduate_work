"""Сценарии сбоя хранилища и источника: health 503, офсет стоит, процесс жив, доезд без рестарта."""

import asyncio
import time
from uuid import uuid4

import aiohttp
from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client
from docker.models.containers import Container

from tests.functional.settings import test_settings
from tests.functional.testdata.envelopes import watch_progress
from tests.functional.utils.helpers import (
    container_started_at,
    container_status,
    describe_group_offsets,
    produce,
    wait_for_event,
    wait_for_health_status,
    wait_for_log_end,
    wait_until,
)
from tests.functional.utils.wait_for import probe_clickhouse, probe_etl, wait_for


async def test_clickhouse_down_returns_503(
    http_client: aiohttp.ClientSession,
    restored_clickhouse: Container,
) -> None:
    """Arrange: ClickHouse остановлен. Act: GET health. Assert: 503 clickhouse=unavailable."""
    await asyncio.to_thread(restored_clickhouse.stop, timeout=20)
    body = await wait_for_health_status(
        http_client,
        status_code=503,
        clickhouse='unavailable',
    )
    assert body['status'] == 'unavailable'
    assert body['clickhouse'] == 'unavailable'


async def test_clickhouse_outage_holds_offset_and_recovers_without_etl_restart(
    http_client: aiohttp.ClientSession,
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
    kafka_container: Container,
    etl_container: Container,
    restored_clickhouse: Container,
) -> None:
    """Arrange: пайплайн жив, затем ClickHouse down.
    Act: produce progress. Assert: CURRENT-OFFSET не растёт, пока склад лежит.
    После старта ClickHouse то же event_id появляется, StartedAt ETL не меняется.
    """
    user_id = uuid4()
    canary_id = uuid4()
    await produce(
        kafka_producer,
        watch_progress(film_id=uuid4(), event_id=canary_id, user_id=user_id),
        key=user_id,
    )
    await wait_for_event(clickhouse_client, 'ugc.events', canary_id)

    before = await asyncio.to_thread(describe_group_offsets, kafka_container)
    assert before, 'group ugc-etl ещё не закоммитила офсет'
    current_before = before[0]['current']
    end_before = before[0]['end']
    assert current_before is not None
    assert end_before is not None

    started_at = container_started_at(etl_container)
    await asyncio.to_thread(restored_clickhouse.stop, timeout=20)
    await wait_for_health_status(http_client, status_code=503, clickhouse='unavailable')

    held_id = uuid4()
    await produce(
        kafka_producer,
        watch_progress(film_id=uuid4(), event_id=held_id, user_id=user_id),
        key=user_id,
    )
    await wait_for_log_end(kafka_container, end_before + 1)

    hold_deadline = time.monotonic() + test_settings.offset_hold_seconds

    async def _offset_held() -> bool:
        during = await asyncio.to_thread(describe_group_offsets, kafka_container)
        if not during:
            return False
        current = during[0]['current']
        assert current == current_before
        return time.monotonic() >= hold_deadline

    await wait_until(_offset_held, description='CURRENT-OFFSET не вырос при недоступном ClickHouse')

    await asyncio.to_thread(restored_clickhouse.start)
    await wait_for('clickhouse', probe_clickhouse)
    await wait_for('etl', probe_etl)
    await wait_for_event(clickhouse_client, 'ugc.film_watches', held_id)

    assert container_started_at(etl_container) == started_at
    assert container_status(etl_container) == 'running'


async def test_kafka_down_returns_503_and_process_stays_running(
    http_client: aiohttp.ClientSession,
    etl_container: Container,
    restored_kafka: Container,
) -> None:
    """Arrange: брокеры остановлены. Act: GET health.
    Assert: 503 kafka=unavailable, контейнер ETL running, после старта health 200.
    """
    started_at = container_started_at(etl_container)
    await asyncio.to_thread(restored_kafka.stop, timeout=20)
    body = await wait_for_health_status(http_client, status_code=503, kafka='unavailable')

    assert body['status'] == 'unavailable'
    assert body['kafka'] == 'unavailable'
    assert container_status(etl_container) == 'running'
    assert container_started_at(etl_container) == started_at
