from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import aiohttp
import pytest
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from tests.functional.settings import test_settings
from tests.functional.testdata.es_mapping import MOVIES_INDEX

LoadData = Callable[[list[dict[str, Any]]], Awaitable[None]]


@pytest.fixture(scope='session')
async def es_client() -> AsyncIterator[AsyncElasticsearch]:
    client = AsyncElasticsearch(hosts=test_settings.es_url)
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis(host=test_settings.redis_host, port=test_settings.redis_port)
    yield client
    await client.aclose()


@pytest.fixture(scope='session')
async def http_client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession(base_url=test_settings.api_url) as session:
        yield session


@pytest.fixture(autouse=True)
async def clean_cache(redis_client: Redis) -> AsyncIterator[None]:
    """Каждый тест начинается с холодным кэшем, иначе он проверяет только кэш."""
    await redis_client.flushall()
    yield
    await redis_client.flushall()


@pytest.fixture
async def es_index(es_client: AsyncElasticsearch) -> AsyncIterator[str]:
    """Пустой индекс на время теста. Поиск по отсутствующему индексу отдаёт 500."""
    index = test_settings.elasticsearch_index
    await es_client.indices.delete(index=index, ignore_unavailable=True)
    await es_client.indices.create(
        index=index,
        settings=MOVIES_INDEX['settings'],
        mappings=MOVIES_INDEX['mappings'],
    )
    yield index
    await es_client.indices.delete(index=index, ignore_unavailable=True)


@pytest.fixture
async def es_write_data(
    es_client: AsyncElasticsearch,
    es_index: str,
) -> LoadData:
    async def load(documents: list[dict[str, Any]]) -> None:
        operations: list[dict[str, Any]] = []
        for document in documents:
            operations.append({'index': {'_index': es_index, '_id': document['id']}})
            operations.append(document)

        # refresh='wait_for' делает документы искомыми до отправки запроса в API
        response = await es_client.bulk(operations=operations, refresh='wait_for')
        if response['errors']:
            raise RuntimeError(f'не удалось загрузить фикстуры: {response["items"]}')

    return load
