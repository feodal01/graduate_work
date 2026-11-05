import httpx
import pytest
from pydantic import BaseModel, Field

from api.errors import BillingDomainError, InvalidServiceCredentialsError
from core.observability import get_current_request_id
from db.sql.errors import DatabaseUnavailableError
from main import create_app


class SamplePayload(BaseModel):
    reason: str = Field(min_length=1)


@pytest.mark.asyncio
async def test_api_request_without_or_blank_request_id_is_rejected() -> None:
    """API routes reject missing or whitespace X-Request-Id (NFR-S-01, NFR-U-02)."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
    ) as client:
        missing_resp = await client.get('/billing/openapi.json')
        blank_resp = await client.get(
            '/billing/openapi.json',
            headers={'X-Request-Id': '    '},
        )

    for response in (missing_resp, blank_resp):
        assert response.status_code == 400
        assert response.json() == {
            'error_code': 'missing_request_id',
            'detail': 'X-Request-Id is required',
        }


@pytest.mark.asyncio
async def test_browser_ui_route_generates_request_id_and_echoes_header() -> None:
    """Browser HTML routes under /billing/* auto-generate X-Request-Id if absent."""
    app = create_app(app_lifespan=None)
    seen_context_ids: list[str] = []

    @app.get('/billing/probe-req-id')
    async def probe_ui() -> dict[str, str]:
        seen_context_ids.append(get_current_request_id())
        return {'request_id': get_current_request_id()}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
    ) as client:
        gen_resp = await client.get('/billing/probe-req-id')
        pass_resp = await client.get(
            '/billing/probe-req-id',
            headers={'X-Request-Id': 'incoming-req-42'},
        )

    assert gen_resp.status_code == 200
    generated_id = gen_resp.headers['X-Request-Id']
    assert generated_id.startswith('ui-')
    assert seen_context_ids[0] == generated_id

    assert pass_resp.status_code == 200
    assert pass_resp.headers['X-Request-Id'] == 'incoming-req-42'
    assert seen_context_ids[1] == 'incoming-req-42'


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
