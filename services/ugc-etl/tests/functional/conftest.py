import asyncio
from collections.abc import AsyncIterator, Iterator

import aiohttp
import pytest
from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client
from docker import DockerClient
from docker.models.containers import Container

from tests.functional.settings import test_settings
from tests.functional.utils.helpers import connect_clickhouse
from tests.functional.utils.wait_for import probe_clickhouse, probe_etl, probe_kafka, wait_for


@pytest.fixture(scope='session')
async def http_client() -> AsyncIterator[aiohttp.ClientSession]:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(base_url=test_settings.etl_url, timeout=timeout) as session:
        yield session


@pytest.fixture(scope='session')
def clickhouse_client() -> Iterator[Client]:
    client = connect_clickhouse()
    yield client
    client.close()


@pytest.fixture
async def kafka_producer() -> AsyncIterator[AIOKafkaProducer]:
    producer = AIOKafkaProducer(
        bootstrap_servers=test_settings.bootstrap_servers(),
        acks='all',
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture(scope='session')
def docker_client() -> DockerClient:
    import docker

    return docker.from_env()


@pytest.fixture(scope='session')
def kafka_container(docker_client: DockerClient) -> Container:
    return docker_client.containers.get(test_settings.kafka_container)


@pytest.fixture(scope='session')
def clickhouse_container(docker_client: DockerClient) -> Container:
    return docker_client.containers.get(test_settings.clickhouse_container)


@pytest.fixture(scope='session')
def etl_container(docker_client: DockerClient) -> Container:
    return docker_client.containers.get(test_settings.etl_container)


@pytest.fixture
async def restored_clickhouse(clickhouse_container: Container) -> AsyncIterator[Container]:
    try:
        yield clickhouse_container
    finally:
        clickhouse_container.reload()
        if clickhouse_container.status != 'running':
            await asyncio.to_thread(clickhouse_container.start)
        await wait_for('clickhouse', probe_clickhouse)
        await wait_for('etl', probe_etl)


@pytest.fixture
async def restored_kafka(kafka_container: Container) -> AsyncIterator[Container]:
    try:
        yield kafka_container
    finally:
        kafka_container.reload()
        if kafka_container.status != 'running':
            await asyncio.to_thread(kafka_container.start)
        await wait_for('kafka', probe_kafka)
        await wait_for('etl', probe_etl)
