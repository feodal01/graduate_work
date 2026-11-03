import httpx
import pytest

from db.cache.errors import CacheUnavailableError, OAuthStateStoreUnavailableError
from db.sql.errors import DatabaseUnavailableError
from main import create_app
from repositories.errors import UnknownPermissionError, UserNotFoundError
from services.errors import (
    InvalidOAuthCodeError,
    InvalidOAuthStateError,
    OAuthStateGenerationError,
    SocialAuthenticationRejectedError,
    SocialProviderContractError,
    SocialProviderUnavailableError,
    YandexOAuthNotConfiguredError,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('error', 'status_code', 'detail'),
    [
        (DatabaseUnavailableError(), 503, 'Database is unavailable'),
        (CacheUnavailableError(), 503, 'Cache is unavailable'),
        (
            OAuthStateStoreUnavailableError(),
            503,
            'OAuth state store is unavailable',
        ),
        (
            YandexOAuthNotConfiguredError(),
            503,
            'Yandex OAuth is not configured',
        ),
        (
            OAuthStateGenerationError(),
            503,
            'OAuth state could not be created',
        ),
        (
            SocialAuthenticationRejectedError(),
            400,
            'Yandex authentication was cancelled',
        ),
        (
            InvalidOAuthStateError(),
            400,
            'OAuth state is invalid or expired',
        ),
        (
            InvalidOAuthCodeError(),
            400,
            'OAuth code is invalid or expired',
        ),
        (
            SocialProviderUnavailableError(),
            503,
            'Yandex OAuth is unavailable',
        ),
        (
            SocialProviderContractError(),
            502,
            'Yandex OAuth returned an invalid response',
        ),
        (UserNotFoundError(), 404, 'User not found'),
        (
            UnknownPermissionError('Unknown permission: films:unknown'),
            422,
            'Unknown permission: films:unknown',
        ),
    ],
)
async def test_domain_errors_have_stable_http_responses(
    error: Exception,
    status_code: int,
    detail: str,
) -> None:
    """Доменные ошибки должны возвращать предусмотренные HTTP-ответы."""
    app = create_app(app_lifespan=None)

    @app.get('/test/domain-error')
    async def raise_domain_error() -> None:
        raise error

    transport = httpx.ASGITransport(
        app=app,
        raise_app_exceptions=False,
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as client:
        response = await client.get('/test/domain-error')

    assert response.status_code == status_code
    assert response.json() == {'detail': detail}
