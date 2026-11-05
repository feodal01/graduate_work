from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from api.v1.authorization import enforce_service_rate_limit
from dependencies import get_authorization_service
from schemas.subscription_access import (
    SubscriptionAccessPutRequest,
    SubscriptionAccessPutResponse,
)
from services.authorization import AuthorizationService

router = APIRouter(tags=['internal-subscription-access'])


@router.put(
    '/users/{user_id}/subscription-access',
    response_model=SubscriptionAccessPutResponse,
    responses={
        401: {'description': 'Invalid service credentials'},
        404: {'description': 'User not found'},
        429: {'description': 'Too many requests'},
        503: {'description': 'Database or cache is unavailable'},
    },
)
async def put_subscription_access(
    user_id: UUID,
    payload: SubscriptionAccessPutRequest,
    _rate_limit: Annotated[None, Depends(enforce_service_rate_limit)],
    service: Annotated[
        AuthorizationService,
        Depends(get_authorization_service),
    ],
) -> SubscriptionAccessPutResponse:
    applied, current_version = await service.put_subscription_access(
        user_id,
        version=payload.version,
        expires_at=payload.expires_at,
    )
    return SubscriptionAccessPutResponse(
        applied=applied,
        current_version=current_version,
    )
