from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from api.rate_limit import enforce_rate_limit, policy
from core.config import settings
from dependencies import (
    get_authentication_service,
    get_client_info,
    get_rate_limiter,
    require_internal_service,
)
from schemas.token_introspection import (
    TokenIntrospectionRequest,
    TokenIntrospectionResponse,
)
from services.authentication import AuthenticationService
from services.errors import (
    AccessTokenExpiredError,
    AccessTokenRevokedError,
    InvalidAccessTokenError,
)
from services.rate_limit import RateLimiter
from services.types import ClientInfo


async def enforce_introspection_rate_limit(
    request: Request,
    service_identity: Annotated[str, Depends(require_internal_service)],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> None:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('service', settings.rate_limit),
        identity=f'{service_identity}:token-introspection',
    )


router = APIRouter(
    tags=['internal-token-introspection'],
    dependencies=[Depends(enforce_introspection_rate_limit)],
)


@router.post(
    '/introspect',
    response_model_exclude_none=True,
    responses={
        401: {'description': 'Invalid service credentials'},
        429: {'description': 'Too many requests'},
        503: {'description': 'Session state unavailable'},
    },
)
async def introspect_token(
    payload: TokenIntrospectionRequest,
    response: Response,
    service: Annotated[AuthenticationService, Depends(get_authentication_service)],
    client: Annotated[ClientInfo, Depends(get_client_info)],
) -> TokenIntrospectionResponse:
    response.headers['Cache-Control'] = 'no-store'
    try:
        user = await service.authenticate_access(
            payload.token.get_secret_value(), client=client
        )
    except (AccessTokenExpiredError, AccessTokenRevokedError, InvalidAccessTokenError):
        return TokenIntrospectionResponse(active=False)
    return TokenIntrospectionResponse(active=True, sub=user.user_id)
