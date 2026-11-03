"""Сценарии health: процесс жив, зависимости видны, OpenAPI закрыт."""

import aiohttp
import pytest

from tests.functional.settings import test_settings
from tests.functional.utils.helpers import fetch_health


async def test_health_ok_exposes_kafka_clickhouse_and_rss(
    http_client: aiohttp.ClientSession,
) -> None:
    """Arrange: стек готов. Act: GET /api/v1/health/. Assert: 200 и контракт тела."""
    status, body = await fetch_health(http_client)

    assert status == 200
    assert body['status'] == 'ok'
    assert body['kafka'] == 'ok'
    assert body['clickhouse'] == 'ok'
    assert body['pipeline'] == 'ok'
    assert isinstance(body['rss_bytes'], int)
    assert body['rss_bytes'] > 0
    assert isinstance(body['rss_limit_bytes'], int)
    assert body['rss_limit_bytes'] > 0
    assert body['rss_bytes'] <= body['rss_limit_bytes']
    assert isinstance(body['batch_size'], int)
    assert body['batch_size'] >= 0


@pytest.mark.parametrize('path', ['/docs', '/redoc', '/openapi.json'])
async def test_openapi_is_not_published(
    http_client: aiohttp.ClientSession,
    path: str,
) -> None:
    """Arrange: стек готов. Act: GET документации. Assert: 404."""
    async with http_client.get(path) as response:
        status = response.status

    assert status == 404


@pytest.mark.skipif(not test_settings.gateway_url, reason='GATEWAY_URL не задан')
async def test_health_via_gateway() -> None:
    """Arrange: gateway доступен. Act: GET /ugc-etl/health. Assert: 200 ok."""
    url = f'{test_settings.gateway_url}{test_settings.gateway_health_path}'
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            status = response.status
            body = await response.json()

    assert status == 200
    assert body['status'] == 'ok'
    assert body['kafka'] == 'ok'
    assert body['clickhouse'] == 'ok'
