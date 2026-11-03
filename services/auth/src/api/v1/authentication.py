from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status

from api.rate_limit import enforce_rate_limit, policy
from core.config import settings
from dependencies import (
    get_authentication_service,
    get_client_info,
    get_current_user,
    get_rate_limiter,
)
from schemas.authentication import (
    LoginHistoryResponse,
    LoginRequest,
    LogoutRequest,
    ProfileUpdateRequest,
    RefreshRequest,
    SignupRequest,
    TokenPairResponse,
    UserResponse,
)
from services.authentication import AuthenticationService
from services.profile_events import CONTACT_FIELDS
from services.rate_limit import RateLimiter
from services.types import ClientInfo, CurrentUser

router = APIRouter(tags=['authentication'])


@router.post(
    '/signup',
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        409: {'description': 'Login already taken'},
        429: {'description': 'Too many requests'},
    },
)
async def signup(
    request: Request,
    payload: SignupRequest,
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> UserResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('signup', settings.rate_limit),
        identity=payload.login,
    )
    user = await service.signup(
        login=payload.login,
        password=payload.password,
        first_name=payload.first_name,
        last_name=payload.last_name,
        email=payload.email,
        locale=payload.locale,
        tz=payload.tz,
    )
    return UserResponse.model_validate(user)


@router.post(
    '/login',
    response_model=TokenPairResponse,
    responses={
        401: {'description': 'Invalid credentials'},
        429: {'description': 'Too many requests'},
    },
)
async def login(
    request: Request,
    payload: LoginRequest,
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    client: Annotated[ClientInfo, Depends(get_client_info)],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> TokenPairResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('login', settings.rate_limit),
        identity=payload.login,
    )
    pair = await service.login(
        login=payload.login,
        password=payload.password,
        client=client,
    )
    return TokenPairResponse(
        access_token=pair.access.value,
        refresh_token=pair.refresh.value,
    )


@router.post(
    '/refresh',
    response_model=TokenPairResponse,
    responses={
        401: {'description': 'Invalid refresh token'},
        429: {'description': 'Too many requests'},
    },
)
async def refresh(
    request: Request,
    payload: RefreshRequest,
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> TokenPairResponse:
    claims = service.verify_refresh_token(payload.refresh_token)
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('refresh', settings.rate_limit),
        identity=str(claims.user_id),
    )
    pair = await service.rotate_refresh(claims)
    return TokenPairResponse(
        access_token=pair.access.value,
        refresh_token=pair.refresh.value,
    )


@router.patch(
    '/profile',
    response_model=UserResponse,
    responses={
        401: {'description': 'Token expired, invalid, or revoked'},
        409: {'description': 'Login already taken'},
        429: {'description': 'Too many requests'},
    },
)
async def update_profile(
    request: Request,
    payload: ProfileUpdateRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> UserResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('user', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    user = await service.update_profile(
        current_user,
        login=payload.new_login,
        password=payload.new_password,
        contacts=payload.model_dump(include=set(CONTACT_FIELDS), exclude_unset=True),
    )
    return UserResponse.model_validate(user)


@router.get(
    '/profile',
    response_model=UserResponse,
    responses={
        401: {'description': 'Token expired, invalid, or revoked'},
        429: {'description': 'Too many requests'},
    },
)
async def get_profile(
    request: Request,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> UserResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('user', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    user = await service.get_profile(current_user)
    return UserResponse.model_validate(user)


@router.get(
    '/login-history',
    response_model=list[LoginHistoryResponse],
    responses={
        401: {'description': 'Token expired, invalid, or revoked'},
        429: {'description': 'Too many requests'},
    },
)
async def login_history(
    request: Request,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> list[LoginHistoryResponse]:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('user', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    items = await service.get_login_history(current_user)
    return [LoginHistoryResponse.model_validate(item) for item in items]


@router.post(
    '/logout',
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        401: {'description': 'Token expired, invalid, or revoked'},
        429: {'description': 'Too many requests'},
    },
)
async def logout(
    request: Request,
    payload: LogoutRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthenticationService,
        Depends(get_authentication_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> Response:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('user', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    await service.logout(current_user, payload.refresh_token)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete('/profile', status_code=status.HTTP_204_NO_CONTENT)
async def delete_profile(
    request: Request,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[AuthenticationService, Depends(get_authentication_service)],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> Response:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('user', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    await service.delete_profile(current_user)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
