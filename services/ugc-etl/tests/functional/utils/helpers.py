import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any, Optional
from uuid import UUID

import aiohttp
import clickhouse_connect
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from clickhouse_connect.driver.client import Client
from docker.models.containers import Container

from tests.functional.settings import test_settings

Predicate = Callable[[], Awaitable[bool]]


async def wait_until(predicate: Predicate, *, description: str) -> None:
    deadline = time.monotonic() + test_settings.wait_timeout
    reason = 'проба ещё не выполнялась'
    while time.monotonic() < deadline:
        try:
            if await predicate():
                return
            reason = 'проба вернула False'
        except AssertionError:
            raise
        except Exception as exc:  # noqa: BLE001 - ждём готовности, ошибка = ещё не готово
            reason = repr(exc)
        await asyncio.sleep(test_settings.wait_interval)
    raise TimeoutError(
        f'{description} не выполнилось за {test_settings.wait_timeout}s, '
        f'последняя причина: {reason}'
    )


async def fetch_health(
    http_client: aiohttp.ClientSession,
    path: str = test_settings.etl_health_path,
) -> tuple[int, dict[str, Any]]:
    async with http_client.get(path) as response:
        return response.status, await response.json()


async def wait_for_health_status(
    http_client: aiohttp.ClientSession,
    *,
    status_code: int,
    kafka: Optional[str] = None,
    clickhouse: Optional[str] = None,
) -> dict[str, Any]:
    last: dict[str, Any] = {}

    async def _matches() -> bool:
        nonlocal last
        code, body = await fetch_health(http_client)
        last = body
        if code != status_code:
            return False
        if kafka is not None and body.get('kafka') != kafka:
            return False
        if clickhouse is not None and body.get('clickhouse') != clickhouse:
            return False
        return True

    await wait_until(
        _matches,
        description=f'health HTTP {status_code} kafka={kafka} clickhouse={clickhouse}',
    )
    return last


def connect_clickhouse() -> Client:
    return clickhouse_connect.get_client(
        host=test_settings.clickhouse_host,
        port=test_settings.clickhouse_port,
        username=test_settings.clickhouse_user,
        password=test_settings.clickhouse_password,
        database=test_settings.clickhouse_database,
        connect_timeout=5,
        send_receive_timeout=5,
    )


async def ch_query(
    client: Client,
    sql: str,
    parameters: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    result = await asyncio.to_thread(client.query, sql, parameters=parameters)
    return list(result.named_results())


async def event_rows(
    client: Client,
    table: str,
    event_id: UUID,
    *,
    final: bool = False,
) -> list[dict[str, Any]]:
    qualifier = ' FINAL' if final else ''
    return await ch_query(
        client,
        f'SELECT * FROM {table}{qualifier} WHERE event_id = %(event_id)s',
        {'event_id': str(event_id)},
    )


async def event_count(
    client: Client,
    table: str,
    event_id: UUID,
    *,
    final: bool = False,
) -> int:
    qualifier = ' FINAL' if final else ''
    rows = await ch_query(
        client,
        f'SELECT count() AS n FROM {table}{qualifier} WHERE event_id = %(event_id)s',
        {'event_id': str(event_id)},
    )
    return int(rows[0]['n'])


async def wait_for_event(
    client: Client,
    table: str,
    event_id: UUID,
) -> dict[str, Any]:
    async def _ready() -> bool:
        return await event_count(client, table, event_id) >= 1

    await wait_until(_ready, description=f'{table} содержит {event_id}')
    rows = await event_rows(client, table, event_id)
    return rows[0]


async def produce(producer: AIOKafkaProducer, value: bytes, *, key: UUID) -> None:
    await producer.send_and_wait(
        test_settings.kafka_topic,
        value=value,
        key=str(key).encode('utf-8'),
        partition=0,
    )


def parse_group_offsets(output: str, topic: str) -> list[dict[str, Optional[int]]]:
    rows: list[dict[str, Optional[int]]] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 5 or parts[0] == 'GROUP' or parts[1] != topic:
            continue
        current_raw = parts[3]
        end_raw = parts[4]
        rows.append(
            {
                'partition': int(parts[2]),
                'current': None if current_raw == '-' else int(current_raw),
                'end': None if end_raw == '-' else int(end_raw),
            }
        )
    return rows


def describe_group_offsets(kafka_container: Container) -> list[dict[str, Optional[int]]]:
    result = kafka_container.exec_run(
        [
            'kafka-consumer-groups.sh',
            '--bootstrap-server',
            '127.0.0.1:9092',
            '--describe',
            '--group',
            test_settings.kafka_group_id,
        ]
    )
    output = result.output.decode('utf-8', errors='replace')
    if result.exit_code != 0:
        raise RuntimeError(f'kafka-consumer-groups failed: {output}')
    return parse_group_offsets(output, test_settings.kafka_topic)


async def wait_for_log_end(kafka_container: Container, expected_end: int) -> None:
    async def _ready() -> bool:
        rows = await asyncio.to_thread(describe_group_offsets, kafka_container)
        return any(row['end'] is not None and row['end'] >= expected_end for row in rows)

    await wait_until(_ready, description=f'LOG-END-OFFSET >= {expected_end}')


async def consume_matching(
    value: bytes,
    *,
    topic: str,
) -> bytes:
    group_id = f'ft-dlq-{time.time_ns()}'
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=test_settings.bootstrap_servers(),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    await consumer.start()
    try:
        deadline = time.monotonic() + test_settings.wait_timeout
        reason = 'сообщений ещё не было'
        while time.monotonic() < deadline:
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            batches = await consumer.getmany(timeout_ms=min(remaining_ms, 1000))
            for messages in batches.values():
                for message in messages:
                    if message.value == value:
                        return message.value
            reason = 'совпадающего сообщения нет'
        raise TimeoutError(
            f'в {topic} нет ожидаемого сообщения за {test_settings.wait_timeout}s, {reason}'
        )
    finally:
        await consumer.stop()


def container_started_at(container: Container) -> str:
    container.reload()
    return str(container.attrs['State']['StartedAt'])


def container_status(container: Container) -> str:
    container.reload()
    return str(container.attrs['State']['Status'])
