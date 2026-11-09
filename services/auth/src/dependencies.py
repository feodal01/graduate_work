from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, cast

from fastapi import Depends, Header, Path, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.cache.authorization import AuthorizationCache
from db.cache.oauth_state import OAuthStateStore
from db.cache.session import TokenSessionStore
from db.health import HealthCheck
from db.sql.postgres import get_session
from repositories.login_history import SQLAlchemyLoginHistoryRepository
from repositories.roles import SQLAlchemyRoleRepository
from repositories.social_accounts import (
    SQLAlchemySocialAccountRepository,
)
from repositories.users import SQLAlchemyUserRepository
from security.passwords import PasswordHasher
from security.service_credentials import ServiceCredentials
from security.tokens import TokenService
from services.authentication import AuthenticationService
from services.authorization import AuthorizationService
from services.errors import (
    InvalidAccessTokenError,
    InvalidServiceCredentialsError,
    SocialProviderNotFoundError,
    YandexOAuthNotConfiguredError,
)
from services.film_access import create_film_access_policy
from services.rate_limit import RateLimiter
from services.social_authentication import (
    SocialAuthenticationService,
    SocialProviderClient,
)
from services.types import ClientInfo, CurrentUser
from validation.credentials import PasswordPolicy

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True, slots=True)
class ResolvedSocialProvider:
    name: str
    client: SocialProviderClient
    state_ttl_seconds: int


SocialProviderResolver = Callable[[Request], ResolvedSocialProvider]
SocialProviderName = Annotated[
    str,
    Path(description='Registered social provider name'),
]


async def get_cache_health(request: Request) -> HealthCheck:
    return cast(HealthCheck, request.app.state.cache_health)


async def get_database_health(request: Request) -> HealthCheck:
    return cast(HealthCheck, request.app.state.database_health)


async def get_token_session_store(request: Request) -> TokenSessionStore:
    return cast(TokenSessionStore, request.app.state.token_sessions)


async def get_authorization_cache(request: Request) -> AuthorizationCache:
    return cast(AuthorizationCache, request.app.state.authorization_cache)


async def get_rate_limiter(request: Request) -> RateLimiter:
    return cast(RateLimiter, request.app.state.rate_limiter)


async def get_token_service(request: Request) -> TokenService:
    return cast(TokenService, request.app.state.token_service)


async def get_password_hasher(request: Request) -> PasswordHasher:
    return cast(PasswordHasher, request.app.state.password_hasher)


async def get_password_policy(request: Request) -> PasswordPolicy:
    return cast(PasswordPolicy, request.app.state.password_policy)


async def get_service_credentials(request: Request) -> ServiceCredentials:
    return cast(ServiceCredentials, request.app.state.service_credentials)


async def get_oauth_state_store(request: Request) -> OAuthStateStore:
    return cast(OAuthStateStore, request.app.state.oauth_state_store)


def _resolve_yandex_provider(request: Request) -> ResolvedSocialProvider:
    client = getattr(request.app.state, 'yandex_oauth_client', None)
    if not settings.yandex_oauth.enabled or client is None:
        raise YandexOAuthNotConfiguredError
    return ResolvedSocialProvider(
        name='yandex',
        client=cast(SocialProviderClient, client),
        state_ttl_seconds=settings.yandex_oauth.state_ttl_seconds,
    )


SOCIAL_PROVIDER_RESOLVERS: dict[str, SocialProviderResolver] = {
    'yandex': _resolve_yandex_provider,
}


async def get_social_provider(
    provider_name: SocialProviderName,
    request: Request,
) -> ResolvedSocialProvider:
    try:
        resolver = SOCIAL_PROVIDER_RESOLVERS[provider_name]
    except KeyError:
        raise SocialProviderNotFoundError(provider_name) from None
    return resolver(request)


async def get_authentication_service(
    db_session: Annotated[AsyncSession, Depends(get_session)],
    token_sessions: Annotated[
        TokenSessionStore,
        Depends(get_token_session_store),
    ],
    token_service: Annotated[TokenService, Depends(get_token_service)],
    password_hasher: Annotated[PasswordHasher, Depends(get_password_hasher)],
    password_policy: Annotated[PasswordPolicy, Depends(get_password_policy)],
) -> AuthenticationService:
    return AuthenticationService(
        users=SQLAlchemyUserRepository(db_session),
        login_history=SQLAlchemyLoginHistoryRepository(db_session),
        token_sessions=token_sessions,
        password_hasher=password_hasher,
        password_policy=password_policy,
        token_service=token_service,
    )


async def get_social_authentication_service(
    provider: Annotated[
        ResolvedSocialProvider,
        Depends(get_social_provider),
    ],
    db_session: Annotated[AsyncSession, Depends(get_session)],
    authentication: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    state_store: Annotated[
        OAuthStateStore,
        Depends(get_oauth_state_store),
    ],
) -> SocialAuthenticationService:
    return SocialAuthenticationService(
        provider_name=provider.name,
        state_store=state_store,
        provider_client=provider.client,
        accounts=SQLAlchemySocialAccountRepository(db_session),
        authentication=authentication,
        state_ttl_seconds=provider.state_ttl_seconds,
    )


async def get_authorization_service(
    db_session: Annotated[AsyncSession, Depends(get_session)],
    authorization_cache: Annotated[
        AuthorizationCache,
        Depends(get_authorization_cache),
    ],
) -> AuthorizationService:
    return AuthorizationService(
        users=SQLAlchemyUserRepository(db_session),
        roles=SQLAlchemyRoleRepository(db_session),
        authorization_cache=authorization_cache,
        film_policy=create_film_access_policy(),
    )


async def get_client_info(request: Request) -> ClientInfo:
    return ClientInfo(
        ip_address=request.client.host if request.client is not None else None,
        user_agent=request.headers.get('user-agent'),
    )


async def get_current_user(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer_scheme),
    ],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    client: Annotated[ClientInfo, Depends(get_client_info)],
) -> CurrentUser:
    if credentials is None:
        raise InvalidAccessTokenError
    return await service.authenticate_access(
        credentials.credentials,
        client=client,
    )


async def get_optional_current_user(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer_scheme),
    ],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    client: Annotated[ClientInfo, Depends(get_client_info)],
) -> CurrentUser | None:
    if credentials is None:
        return None
    return await service.authenticate_access(
        credentials.credentials,
        client=client,
    )


async def require_internal_service(
    credentials: Annotated[
        ServiceCredentials,
        Depends(get_service_credentials),
    ],
    service_key: Annotated[
        str | None,
        Header(alias='X-Service-Key'),
    ] = None,
    service_api_key: Annotated[
        str | None,
        Header(alias='X-Service-Api-Key'),
    ] = None,
) -> str:
    provided_key = service_api_key if service_api_key is not None else service_key
    if not credentials.verify(provided_key):
        raise InvalidServiceCredentialsError
    # Ключ общий для всех внутренних сервисов, поэтому вызывающего различить
    # нельзя. Отдельные квоты задаёт назначение эндпоинта (`service_rate_limit`).
    return 'internal-service'
