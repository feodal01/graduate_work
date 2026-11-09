from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.v1.authorization import service_rate_limit
from db.sql.postgres import get_session
from repositories.user_lookup import lookup_users
from schemas.user_lookup import UsersLookupRequest, UsersLookupResponse

router = APIRouter(tags=['internal-user-lookup'])


@router.post(
    '/users/lookup',
    response_model=UsersLookupResponse,
    responses={
        401: {'description': 'Invalid service credentials'},
        429: {'description': 'Too many requests'},
        503: {'description': 'Database is unavailable'},
    },
)
async def post_users_lookup(
    payload: UsersLookupRequest,
    _rate_limit: Annotated[None, Depends(service_rate_limit('user-lookup'))],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> UsersLookupResponse:
    items = await lookup_users(
        session,
        login=payload.login,
        user_ids=payload.user_ids,
    )
    return UsersLookupResponse(items=items)
