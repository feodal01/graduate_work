"""Ожидание готовности Kafka, ClickHouse и ETL.

Запуск: python -m tests.functional.utils.wait_for [имя ...]
Без аргументов проверяются все известные сервисы.
"""

import asyncio
import sys
import time
from collections.abc import Awaitable, Callable

import aiohttp
import clickhouse_connect
from aiokafka import AIOKafkaProducer

from tests.functional.settings import test_settings


async def probe_kafka() -> bool:
    producer = AIOKafkaProducer(bootstrap_servers=test_settings.bootstrap_servers())
    await producer.start()
    try:
        updated = await asyncio.wait_for(producer.client.force_metadata_update(), timeout=2)
        if not updated:
            return False
        partitions = producer.client.cluster.partitions_for_topic(test_settings.kafka_topic)
    finally:
        await producer.stop()
    return partitions is not None and len(partitions) > 0


def _probe_clickhouse_sync() -> bool:
    client = clickhouse_connect.get_client(
        host=test_settings.clickhouse_host,
        port=test_settings.clickhouse_port,
        username=test_settings.clickhouse_user,
        password=test_settings.clickhouse_password,
        database=test_settings.clickhouse_database,
        connect_timeout=2,
        send_receive_timeout=2,
    )
    try:
        return client.command('SELECT 1') == 1
    finally:
        client.close()


async def probe_clickhouse() -> bool:
    return await asyncio.to_thread(_probe_clickhouse_sync)


async def probe_etl() -> bool:
    url = f'{test_settings.etl_url}{test_settings.etl_health_path}'
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            if response.status != 200:
                return False
            body = await response.json()
    return (
        body.get('status') == 'ok' and body.get('kafka') == 'ok' and body.get('clickhouse') == 'ok'
    )


PROBES: dict[str, Callable[[], Awaitable[bool]]] = {
    'kafka': probe_kafka,
    'clickhouse': probe_clickhouse,
    'etl': probe_etl,
}


async def wait_for(name: str, probe: Callable[[], Awaitable[bool]]) -> None:
    deadline = time.monotonic() + test_settings.wait_timeout
    reason = 'проба ещё не выполнялась'
    while time.monotonic() < deadline:
        try:
            if await probe():
                print(f'{name}: ready', flush=True)
                return
            reason = 'проба вернула False'
        except Exception as exc:  # noqa: BLE001 - любая ошибка означает "ещё не готов"
            reason = repr(exc)
        await asyncio.sleep(test_settings.wait_interval)
    raise TimeoutError(
        f'{name} не готов за {test_settings.wait_timeout}s, последняя причина: {reason}'
    )


async def main() -> None:
    names = sys.argv[1:] or list(PROBES)
    unknown = set(names) - set(PROBES)
    if unknown:
        raise SystemExit(f'неизвестные пробы: {sorted(unknown)}, доступные: {sorted(PROBES)}')
    for name in names:
        await wait_for(name, PROBES[name])


if __name__ == '__main__':
    asyncio.run(main())
