from collections.abc import AsyncGenerator
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI, Request

from dependencies import get_cache_health, get_database_health
from main import create_app


class BackendHealth:
    def __init__(self, available: bool) -> None:
        self._available = available

    async def ping(self) -> bool:
        """Возвращает заданную доступность тестового компонента."""
        return self._available


@pytest.mark.asyncio
async def test_health_dependencies_read_explicit_health_state() -> None:
    """Зависимости читают отдельные health check из состояния приложения."""
    database_health = BackendHealth(True)
    cache_health = BackendHealth(True)
    request = MagicMock(spec=Request)
    request.app.state.database_health = database_health
    request.app.state.cache_health = cache_health

    assert await get_database_health(request) is database_health
    assert await get_cache_health(request) is cache_health


@pytest.fixture
async def health_client() -> AsyncGenerator[
    tuple[httpx.AsyncClient, FastAPI],
    None,
]:
    """Создаёт HTTP-клиент и приложение для проверки readiness."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as client:
        yield client, app


@pytest.mark.asyncio
async def test_health_is_ok_when_postgres_and_redis_are_available(
    health_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    """Readiness 200 требует оба обязательных backend."""
    client, app = health_client
    app.dependency_overrides[get_database_health] = lambda: BackendHealth(True)
    app.dependency_overrides[get_cache_health] = lambda: BackendHealth(True)

    response = await client.get('/api/v1/health/')

    assert response.status_code == 200
    assert response.json() == {
        'status': 'ok',
        'database': 'ok',
        'cache': 'ok',
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('database_available', 'cache_available'),
    [(False, True), (True, False)],
)
async def test_health_is_unavailable_when_required_backend_is_down(
    health_client: tuple[httpx.AsyncClient, FastAPI],
    database_available: bool,
    cache_available: bool,
) -> None:
    """Падение DB или Redis даёт readiness 503."""
    client, app = health_client
    app.dependency_overrides[get_database_health] = lambda: BackendHealth(
        database_available
    )
    app.dependency_overrides[get_cache_health] = lambda: BackendHealth(cache_available)

    response = await client.get('/api/v1/health/')

    assert response.status_code == 503
    assert response.json()['status'] == 'unavailable'
