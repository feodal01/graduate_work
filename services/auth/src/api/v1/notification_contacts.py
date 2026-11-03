from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.v1.authorization import enforce_service_rate_limit
from db.sql.postgres import get_session
from repositories.notification_contacts import read_notification_contacts
from schemas.notification_contacts import (
    NotificationContactsRequest,
    NotificationContactsResponse,
    UnavailableNotificationContact,
)

router = APIRouter(tags=['internal-notification-contacts'])


@router.post(
    '/notification-contacts',
    response_model=NotificationContactsResponse,
    responses={
        401: {'description': 'Invalid service credentials'},
        429: {'description': 'Too many requests'},
    },
)
async def notification_contacts(
    payload: NotificationContactsRequest,
    _rate_limit: Annotated[None, Depends(enforce_service_rate_limit)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> NotificationContactsResponse:
    contacts = await read_notification_contacts(session, payload.user_ids)
    return NotificationContactsResponse(
        items=[
            contacts[user_id]
            if user_id in contacts
            else UnavailableNotificationContact(user_id=user_id)
            for user_id in payload.user_ids
        ]
    )
