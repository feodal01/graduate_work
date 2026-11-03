from collections.abc import Awaitable, Callable
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from starlette.types import ExceptionHandler

from db.cache.errors import CacheUnavailableError, OAuthStateStoreUnavailableError
from db.sql.errors import DatabaseUnavailableError
from repositories.errors import (
    LoginAlreadyTakenError,
    RoleAlreadyAssignedError,
    RoleAlreadyExistsError,
    RoleAssignmentNotFoundError,
    RoleNotFoundError,
    UnknownPermissionError,
    UserNotFoundError,
)
from services.errors import (
    AccessTokenExpiredError,
    AccessTokenRevokedError,
    InvalidAccessTokenError,
    InvalidCredentialsError,
    InvalidOAuthCallbackError,
    InvalidOAuthCodeError,
    InvalidOAuthStateError,
    InvalidRefreshTokenError,
    InvalidServiceCredentialsError,
    OAuthStateGenerationError,
    PermissionDeniedError,
    RateLimitExceededError,
    SocialAuthenticationRejectedError,
    SocialProviderContractError,
    SocialProviderNotFoundError,
    SocialProviderUnavailableError,
    YandexOAuthNotConfiguredError,
)
from validation.credentials import CredentialValidationError


def _error_response(
    status_code: int,
    detail: str,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={'detail': detail},
        headers=headers,
    )


def _register_handler[ErrorT: Exception](
    app: FastAPI,
    error_type: type[ErrorT],
    handler: Callable[[Request, ErrorT], Awaitable[JSONResponse]],
) -> None:
    """Bridge Starlette's untyped exception parameter with a typed handler."""
    app.add_exception_handler(error_type, cast(ExceptionHandler, handler))


async def login_taken_handler(
    _request: Request,
    _exc: LoginAlreadyTakenError,
) -> JSONResponse:
    return _error_response(status.HTTP_409_CONFLICT, 'Login already taken')


async def credential_validation_handler(
    _request: Request,
    exc: CredentialValidationError,
) -> JSONResponse:
    return _error_response(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc))


async def invalid_credentials_handler(
    _request: Request,
    exc: InvalidCredentialsError,
) -> JSONResponse:
    return _error_response(status.HTTP_401_UNAUTHORIZED, str(exc))


async def invalid_refresh_handler(
    _request: Request,
    exc: InvalidRefreshTokenError,
) -> JSONResponse:
    return _error_response(status.HTTP_401_UNAUTHORIZED, str(exc))


async def invalid_access_handler(
    _request: Request,
    exc: InvalidAccessTokenError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_401_UNAUTHORIZED,
        str(exc),
        headers={'WWW-Authenticate': 'Bearer'},
    )


async def expired_access_handler(
    _request: Request,
    exc: AccessTokenExpiredError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_401_UNAUTHORIZED,
        str(exc),
        headers={'WWW-Authenticate': 'Bearer'},
    )


async def revoked_access_handler(
    _request: Request,
    exc: AccessTokenRevokedError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_401_UNAUTHORIZED,
        str(exc),
        headers={'WWW-Authenticate': 'Bearer'},
    )


async def database_unavailable_handler(
    _request: Request,
    exc: DatabaseUnavailableError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        str(exc) or 'Database is unavailable',
    )


async def cache_unavailable_handler(
    _request: Request,
    exc: CacheUnavailableError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        str(exc) or 'Cache is unavailable',
    )


async def oauth_state_store_unavailable_handler(
    _request: Request,
    exc: OAuthStateStoreUnavailableError,
) -> JSONResponse:
    return _error_response(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc))


async def yandex_not_configured_handler(
    _request: Request,
    exc: YandexOAuthNotConfiguredError,
) -> JSONResponse:
    return _error_response(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc))


async def social_provider_not_found_handler(
    _request: Request,
    exc: SocialProviderNotFoundError,
) -> JSONResponse:
    return _error_response(status.HTTP_404_NOT_FOUND, str(exc))


async def oauth_state_generation_handler(
    _request: Request,
    exc: OAuthStateGenerationError,
) -> JSONResponse:
    return _error_response(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc))


async def social_authentication_rejected_handler(
    _request: Request,
    exc: SocialAuthenticationRejectedError,
) -> JSONResponse:
    return _error_response(status.HTTP_400_BAD_REQUEST, str(exc))


async def invalid_oauth_callback_handler(
    _request: Request,
    exc: InvalidOAuthCallbackError,
) -> JSONResponse:
    return _error_response(status.HTTP_400_BAD_REQUEST, str(exc))


async def invalid_oauth_state_handler(
    _request: Request,
    exc: InvalidOAuthStateError,
) -> JSONResponse:
    return _error_response(status.HTTP_400_BAD_REQUEST, str(exc))


async def invalid_oauth_code_handler(
    _request: Request,
    exc: InvalidOAuthCodeError,
) -> JSONResponse:
    return _error_response(status.HTTP_400_BAD_REQUEST, str(exc))


async def social_provider_unavailable_handler(
    _request: Request,
    exc: SocialProviderUnavailableError,
) -> JSONResponse:
    return _error_response(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc))


async def social_provider_contract_handler(
    _request: Request,
    exc: SocialProviderContractError,
) -> JSONResponse:
    return _error_response(status.HTTP_502_BAD_GATEWAY, str(exc))


async def forbidden_handler(
    _request: Request,
    exc: PermissionDeniedError,
) -> JSONResponse:
    return _error_response(status.HTTP_403_FORBIDDEN, str(exc))


async def invalid_service_credentials_handler(
    _request: Request,
    exc: InvalidServiceCredentialsError,
) -> JSONResponse:
    return _error_response(status.HTTP_401_UNAUTHORIZED, str(exc))


async def rate_limit_exceeded_handler(
    _request: Request,
    exc: RateLimitExceededError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_429_TOO_MANY_REQUESTS,
        'Too many requests',
        headers={'Retry-After': str(exc.retry_after_seconds)},
    )


async def role_exists_handler(
    _request: Request,
    _exc: RoleAlreadyExistsError,
) -> JSONResponse:
    return _error_response(status.HTTP_409_CONFLICT, 'Role already exists')


async def role_not_found_handler(
    _request: Request,
    _exc: RoleNotFoundError,
) -> JSONResponse:
    return _error_response(status.HTTP_404_NOT_FOUND, 'Role not found')


async def user_not_found_handler(
    _request: Request,
    _exc: UserNotFoundError,
) -> JSONResponse:
    return _error_response(status.HTTP_404_NOT_FOUND, 'User not found')


async def role_assigned_handler(
    _request: Request,
    _exc: RoleAlreadyAssignedError,
) -> JSONResponse:
    return _error_response(status.HTTP_409_CONFLICT, 'Role already assigned')


async def assignment_not_found_handler(
    _request: Request,
    _exc: RoleAssignmentNotFoundError,
) -> JSONResponse:
    return _error_response(
        status.HTTP_404_NOT_FOUND,
        'Role assignment not found',
    )


async def unknown_permission_handler(
    _request: Request,
    exc: UnknownPermissionError,
) -> JSONResponse:
    return _error_response(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc))


def register_exception_handlers(app: FastAPI) -> None:
    _register_handler(app, LoginAlreadyTakenError, login_taken_handler)
    _register_handler(
        app,
        CredentialValidationError,
        credential_validation_handler,
    )
    _register_handler(app, InvalidCredentialsError, invalid_credentials_handler)
    _register_handler(app, InvalidRefreshTokenError, invalid_refresh_handler)
    _register_handler(app, InvalidAccessTokenError, invalid_access_handler)
    _register_handler(app, AccessTokenExpiredError, expired_access_handler)
    _register_handler(app, AccessTokenRevokedError, revoked_access_handler)
    _register_handler(
        app,
        DatabaseUnavailableError,
        database_unavailable_handler,
    )
    _register_handler(app, CacheUnavailableError, cache_unavailable_handler)
    _register_handler(
        app,
        OAuthStateStoreUnavailableError,
        oauth_state_store_unavailable_handler,
    )
    _register_handler(
        app,
        YandexOAuthNotConfiguredError,
        yandex_not_configured_handler,
    )
    _register_handler(
        app,
        SocialProviderNotFoundError,
        social_provider_not_found_handler,
    )
    _register_handler(
        app,
        OAuthStateGenerationError,
        oauth_state_generation_handler,
    )
    _register_handler(
        app,
        SocialAuthenticationRejectedError,
        social_authentication_rejected_handler,
    )
    _register_handler(
        app,
        InvalidOAuthCallbackError,
        invalid_oauth_callback_handler,
    )
    _register_handler(app, InvalidOAuthStateError, invalid_oauth_state_handler)
    _register_handler(app, InvalidOAuthCodeError, invalid_oauth_code_handler)
    _register_handler(
        app,
        SocialProviderUnavailableError,
        social_provider_unavailable_handler,
    )
    _register_handler(
        app,
        SocialProviderContractError,
        social_provider_contract_handler,
    )
    _register_handler(app, PermissionDeniedError, forbidden_handler)
    _register_handler(
        app,
        InvalidServiceCredentialsError,
        invalid_service_credentials_handler,
    )
    _register_handler(
        app,
        RateLimitExceededError,
        rate_limit_exceeded_handler,
    )
    _register_handler(app, RoleAlreadyExistsError, role_exists_handler)
    _register_handler(app, RoleNotFoundError, role_not_found_handler)
    _register_handler(app, UserNotFoundError, user_not_found_handler)
    _register_handler(app, RoleAlreadyAssignedError, role_assigned_handler)
    _register_handler(
        app,
        RoleAssignmentNotFoundError,
        assignment_not_found_handler,
    )
    _register_handler(app, UnknownPermissionError, unknown_permission_handler)
