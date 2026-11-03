import httpx
import pytest

from main import create_app


@pytest.mark.asyncio
async def test_request_without_request_id_is_rejected() -> None:
    """Auth rejects a request that cannot be correlated with gateway logs."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
    ) as client:
        response = await client.get('/auth/openapi.json')

    assert response.status_code == 400
    assert response.json() == {'detail': 'X-Request-Id is required'}


@pytest.mark.asyncio
async def test_request_with_request_id_reaches_route() -> None:
    """Auth processes a request that has a request identifier."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-unit-request'},
    ) as client:
        response = await client.get('/auth/openapi.json')

    assert response.status_code == 200
