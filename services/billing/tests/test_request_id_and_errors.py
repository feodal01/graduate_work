import httpx
import pytest
from pydantic import BaseModel, Field

from api.errors import BillingDomainError, InvalidServiceCredentialsError
from db.sql.errors import DatabaseUnavailableError
from main import create_app


class SamplePayload(BaseModel):
    reason: str = Field(min_length=1)


@pytest.mark.asyncio
async def test_request_without_request_id_is_rejected() -> None:
    """Billing rejects requests without X-Request-Id (NFR-S-01, NFR-U-02)."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
    ) as client:
        response = await client.get('/billing/openapi.json')

    assert response.status_code == 400
    assert response.json() == {
        'error_code': 'missing_request_id',
        'detail': 'X-Request-Id is required',
    }


@pytest.mark.asyncio
async def test_openapi_and_docs_routes_are_exposed_under_billing_prefix() -> None:
    """Swagger and OpenAPI JSON are served under /billing/docs and openapi.json."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'billing-openapi-test'},
    ) as client:
        docs = await client.get('/billing/docs')
        schema = await client.get('/billing/openapi.json')

    assert docs.status_code == 200
    assert 'swagger-ui' in docs.text
    assert schema.status_code == 200
    assert '/api/v1/health/' in schema.json()['paths']


@pytest.mark.asyncio
async def test_uniform_error_format_for_domain_db_and_validation_errors() -> None:
    """JSON API errors follow {"error_code": ..., "detail": ...} format (NFR-U-02)."""
    app = create_app(app_lifespan=None)

    @app.get('/test/domain-error')
    async def raise_domain() -> None:
        raise BillingDomainError(
            'Active subscription already exists',
            error_code='subscription_already_active',
            status_code=409,
        )

    @app.get('/test/service-auth-error')
    async def raise_service_auth() -> None:
        raise InvalidServiceCredentialsError()

    @app.get('/test/db-error')
    async def raise_db() -> None:
        raise DatabaseUnavailableError('billing-db connection failed')

    @app.post('/test/validation')
    async def validate_body(payload: SamplePayload) -> dict[str, str]:
        return {'reason': payload.reason}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'billing-error-test'},
    ) as client:
        domain_resp = await client.get('/test/domain-error')
        auth_resp = await client.get('/test/service-auth-error')
        db_resp = await client.get('/test/db-error')
        val_resp = await client.post('/test/validation', json={'reason': ''})

    assert domain_resp.status_code == 409
    assert domain_resp.json() == {
        'error_code': 'subscription_already_active',
        'detail': 'Active subscription already exists',
    }
    assert auth_resp.status_code == 401
    assert auth_resp.json()['error_code'] == 'invalid_service_credentials'
    assert db_resp.status_code == 503
    assert db_resp.json() == {
        'error_code': 'database_unavailable',
        'detail': 'billing-db connection failed',
    }
    assert val_resp.status_code == 422
    val_body = val_resp.json()
    assert val_body['error_code'] == 'validation_error'
    assert isinstance(val_body['errors'], list)
