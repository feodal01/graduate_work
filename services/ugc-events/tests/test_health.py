from fastapi.testclient import TestClient

from tests.conftest import MemoryEventBroker


def test_health_ok_when_broker_reachable(client: TestClient) -> None:
    response = client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {'status': 'ok', 'kafka': 'ok', 'redis': 'ok'}


def test_health_unavailable_when_broker_down(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    broker.reachable = False

    response = client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json() == {'status': 'unavailable', 'kafka': 'unavailable', 'redis': 'ok'}
