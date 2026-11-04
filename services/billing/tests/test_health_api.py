from collections.abc import AsyncGenerator
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI, Request

from dependencies import get_database_health
from main import create_app


class BackendHealth:
    def __init__(self, available: bool) -> None:
        self._available = available

    async def ping(self) -> bool:
        """Возвращает заданную доступность тестовой БД."""
        return self._available


@pytest.mark.asyncio
async def test_health_dependency_reads_explicit_health_state() -> None:
    """Зависимость читает health check БД из состояния приложения."""
    database_health = BackendHealth(True)
    request = MagicMock(spec=Request)
    request.app.state.database_health = database_health

    assert await get_database_health(request) is database_health


@pytest.fixture
async def health_client() -> AsyncGenerator[
    tuple[httpx.AsyncClient, FastAPI],
    None,
]:
    """Создаёт HTTP-клиент и приложение для проверки readiness (BIL-API-15)."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'billing-test-request'},
    ) as client:
        yield client, app


@pytest.mark.asyncio
async def test_health_is_ok_when_postgres_is_available(
    health_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    """При доступной billing-db эндпоинт возвращает 200 и status=ok."""
    client, app = health_client
    app.dependency_overrides[get_database_health] = lambda: BackendHealth(True)

    response = await client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {
        'status': 'ok',
        'database': 'ok',
    }


@pytest.mark.asyncio
async def test_health_is_unavailable_when_postgres_is_down(
    health_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    """При недоступности billing-db эндпоинт возвращает 503 и status=unavailable."""
    client, app = health_client
    app.dependency_overrides[get_database_health] = lambda: BackendHealth(False)

    response = await client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json() == {
        'status': 'unavailable',
        'database': 'unavailable',
    }
