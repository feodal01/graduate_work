from collections.abc import Callable, Coroutine
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, status

from api.rate_limit import enforce_rate_limit, policy
from core.config import settings
from dependencies import (
    get_authorization_service,
    get_current_user,
    get_optional_current_user,
    get_rate_limiter,
    require_internal_service,
)
from schemas.authorization import (
    PermissionCheckRequest,
    PermissionCheckResponse,
    RoleAssignmentRequest,
    RoleCreateRequest,
    RoleResponse,
    RoleUpdateRequest,
)
from services.authorization import AuthorizationService
from services.rate_limit import RateLimiter
from services.types import CurrentUser

router = APIRouter(tags=['authorization'])


def service_rate_limit(
    purpose: str,
    *,
    policy_name: str = 'service',
) -> Callable[..., Coroutine[Any, Any, None]]:
    """Сервисная квота отдельно для каждого назначения внутреннего API.

    Все внутренние сервисы предъявляют один ключ, поэтому раньше у них была
    одна общая квота: поток проверок прав от movies-api (на каждую карточку
    фильма) выбирал её, и доставка доступа из Billing получала 429. Теперь
    проверка прав, доставка доступа, контакты и поиск пользователей считаются
    в разных корзинах.
    """

    async def enforce(
        request: Request,
        service_identity: Annotated[str, Depends(require_internal_service)],
        limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
    ) -> None:
        await enforce_rate_limit(
            request=request,
            limiter=limiter,
            policy=policy(policy_name, settings.rate_limit),
            identity=f'{service_identity}:{purpose}',
        )

    return enforce


@router.get(
    '/roles',
    response_model=list[RoleResponse],
    responses={429: {'description': 'Too many requests'}},
)
async def list_roles(
    request: Request,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> list[RoleResponse]:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    roles = await service.list_roles(current_user)
    return [RoleResponse.from_entity(role) for role in roles]


@router.post(
    '/roles',
    response_model=RoleResponse,
    status_code=status.HTTP_201_CREATED,
    responses={429: {'description': 'Too many requests'}},
)
async def create_role(
    request: Request,
    payload: RoleCreateRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> RoleResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    role = await service.create_role(
        current_user,
        name=payload.name,
        permission_codes=payload.permissions,
    )
    return RoleResponse.from_entity(role)


@router.patch(
    '/roles/{role_id}',
    response_model=RoleResponse,
    responses={429: {'description': 'Too many requests'}},
)
async def update_role(
    request: Request,
    role_id: UUID,
    payload: RoleUpdateRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> RoleResponse:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    role = await service.update_role(
        current_user,
        role_id,
        name=payload.name,
        permission_codes=payload.permissions,
    )
    return RoleResponse.from_entity(role)


@router.delete(
    '/roles/{role_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    responses={429: {'description': 'Too many requests'}},
)
async def delete_role(
    request: Request,
    role_id: UUID,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> Response:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    await service.delete_role(current_user, role_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    '/users/{user_id}/roles',
    status_code=status.HTTP_204_NO_CONTENT,
    responses={429: {'description': 'Too many requests'}},
)
async def assign_role(
    request: Request,
    user_id: UUID,
    payload: RoleAssignmentRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> Response:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    await service.assign_role(current_user, user_id, payload.role_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    '/users/{user_id}/roles/{role_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    responses={429: {'description': 'Too many requests'}},
)
async def revoke_role(
    request: Request,
    user_id: UUID,
    role_id: UUID,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> Response:
    await enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=policy('admin', settings.rate_limit),
        identity=str(current_user.user_id),
    )
    await service.revoke_role(current_user, user_id, role_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    '/permissions/check',
    response_model=PermissionCheckResponse,
    responses={429: {'description': 'Too many requests'}},
)
async def check_permission(
    payload: PermissionCheckRequest,
    _rate_limit: Annotated[
        None,
        Depends(service_rate_limit('permission-check', policy_name='permission_check')),
    ],
    current_user: Annotated[
        CurrentUser | None,
        Depends(get_optional_current_user),
    ],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
) -> PermissionCheckResponse:
    allowed = await service.can_read_film(
        current_user,
        payload.attributes,
    )
    return PermissionCheckResponse(allowed=allowed)
