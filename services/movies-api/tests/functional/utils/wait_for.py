"""Ожидание готовности сервисов.

Запуск: python -m tests.functional.utils.wait_for [имя ...]
Без аргументов проверяются все известные сервисы.
"""

import asyncio
import sys
import time
from collections.abc import Awaitable, Callable

import aiohttp
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from tests.functional.settings import test_settings


async def probe_elasticsearch() -> bool:
    client = AsyncElasticsearch(hosts=test_settings.es_url)
    try:
        return bool(await client.ping())
    finally:
        await client.close()


async def probe_redis() -> bool:
    client = Redis(host=test_settings.redis_host, port=test_settings.redis_port)
    try:
        return bool(await client.ping())
    finally:
        await client.aclose()


async def probe_api() -> bool:
    url = f'{test_settings.api_url}{test_settings.api_health_path}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return response.status == 200


async def probe_auth() -> bool:
    async with aiohttp.ClientSession() as session:
        async with session.get(test_settings.auth_url) as response:
            return response.status == 200


PROBES: dict[str, Callable[[], Awaitable[bool]]] = {
    'elasticsearch': probe_elasticsearch,
    'redis': probe_redis,
    'auth': probe_auth,
    'api': probe_api,
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
