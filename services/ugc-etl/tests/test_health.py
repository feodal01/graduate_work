import pytest
from fastapi.testclient import TestClient

from core.config import settings
from main import create_app
from tests.conftest import (
    MemoryConsumer,
    MemoryWarehouse,
    StoppedPipelineTask,
)


def test_health_ok_when_pings_pass_and_rss_below_limit(client: TestClient) -> None:
    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 200
    assert body['status'] == 'ok'
    assert body['kafka'] == 'ok'
    assert body['clickhouse'] == 'ok'
    assert body['pipeline'] == 'ok'
    assert body['rss_bytes'] > 0
    assert body['rss_limit_bytes'] == settings.etl_memory_limit_bytes
    assert body['batch_size'] == 0


def test_health_unavailable_when_kafka_ping_fails(
    client: TestClient,
    consumer: MemoryConsumer,
) -> None:
    consumer.reachable = False

    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 503
    assert body['status'] == 'unavailable'
    assert body['kafka'] == 'unavailable'
    assert body['clickhouse'] == 'ok'
    assert body['pipeline'] == 'ok'


def test_health_unavailable_when_clickhouse_ping_fails(
    client: TestClient,
    warehouse: MemoryWarehouse,
) -> None:
    warehouse.reachable = False

    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 503
    assert body['status'] == 'unavailable'
    assert body['kafka'] == 'ok'
    assert body['clickhouse'] == 'unavailable'
    assert body['pipeline'] == 'ok'


def test_health_unavailable_when_rss_exceeds_limit(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, 'etl_memory_limit_bytes', 1)

    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 503
    assert body['status'] == 'unavailable'
    assert body['pipeline'] == 'ok'
    assert body['rss_bytes'] > body['rss_limit_bytes']


def test_health_unavailable_when_pipeline_task_finished(client: TestClient) -> None:
    client.app.state.pipeline_task = StoppedPipelineTask()

    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 503
    assert body['status'] == 'unavailable'
    assert body['kafka'] == 'ok'
    assert body['clickhouse'] == 'ok'
    assert body['pipeline'] == 'unavailable'


def test_health_unavailable_when_pipeline_task_missing(
    consumer: MemoryConsumer,
    warehouse: MemoryWarehouse,
) -> None:
    client = TestClient(create_app(consumer=consumer, warehouse=warehouse))

    response = client.get('/api/v1/health/')

    body = response.json()
    assert response.status_code == 503
    assert body['status'] == 'unavailable'
    assert body['pipeline'] == 'unavailable'
