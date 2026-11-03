import httpx
import pytest

from main import create_app


@pytest.mark.asyncio
async def test_service_specific_openapi_routes_keep_api_paths_unchanged() -> None:
    """Swagger uses service paths without changing public API paths."""
    app = create_app(app_lifespan=None)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as client:
        docs = await client.get('/auth/docs')
        schema = await client.get('/auth/openapi.json')

    assert docs.status_code == 200
    assert 'swagger-ui' in docs.text
    assert schema.status_code == 200
    assert any(path.startswith('/api/v1/auth') for path in schema.json()['paths'])
    assert app.root_path == ''


def test_all_auth_business_operations_document_rate_limit_response() -> None:
    """Every Token Bucket-protected operation publishes the HTTP 429 contract."""
    schema = create_app(app_lifespan=None).openapi()
    expected_operations = {
        ('/api/v1/auth/signup', 'post'),
        ('/api/v1/auth/login', 'post'),
        ('/api/v1/auth/refresh', 'post'),
        ('/api/v1/auth/profile', 'get'),
        ('/api/v1/auth/profile', 'patch'),
        ('/api/v1/auth/login-history', 'get'),
        ('/api/v1/auth/logout', 'post'),
        ('/api/v1/auth/roles', 'get'),
        ('/api/v1/auth/roles', 'post'),
        ('/api/v1/auth/roles/{role_id}', 'patch'),
        ('/api/v1/auth/roles/{role_id}', 'delete'),
        ('/api/v1/auth/users/{user_id}/roles', 'post'),
        ('/api/v1/auth/users/{user_id}/roles/{role_id}', 'delete'),
        ('/api/v1/auth/permissions/check', 'post'),
        ('/api/v1/internal/notification-contacts', 'post'),
        ('/api/v1/internal/introspect', 'post'),
    }

    actual_operations = {
        (path, method)
        for path, path_item in schema['paths'].items()
        for method, operation in path_item.items()
        if '429' in operation.get('responses', {})
    }

    assert actual_operations == expected_operations


def test_social_provider_routes_publish_the_complete_http_contract() -> None:
    """Swagger must expose one provider-neutral OAuth route contract."""
    schema = create_app(app_lifespan=None).openapi()
    login = schema['paths']['/api/v1/auth/social/{provider_name}/login']['get']
    callback = schema['paths']['/api/v1/auth/social/{provider_name}/callback']['get']

    assert set(login['responses']) == {'307', '404', '422', '503'}
    assert {'200', '400', '404', '422', '502', '503'} <= set(callback['responses'])
    assert len(login['parameters']) == 2
    provider_parameter = next(
        item for item in login['parameters'] if item['name'] == 'provider_name'
    )
    timezone_parameter = next(
        item for item in login['parameters'] if item['name'] == 'tz'
    )
    assert timezone_parameter['in'] == 'query'
    assert timezone_parameter['required'] is True
    assert provider_parameter['name'] == 'provider_name'
    assert provider_parameter['in'] == 'path'
    assert provider_parameter['required'] is True
    assert provider_parameter['description'] == 'Registered social provider name'
    success_schema = callback['responses']['200']['content'][
        'application/json; charset=utf-8'
    ]['schema']
    assert success_schema == {'$ref': '#/components/schemas/TokenPairResponse'}
