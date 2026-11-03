from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from core.config import settings


class NotificationContactsRequest(BaseModel):
    user_ids: list[UUID] = Field(min_length=1)

    @model_validator(mode='after')
    def validate_batch(self) -> Self:
        if len(self.user_ids) > settings.auth_notification_contacts_batch_size:
            raise ValueError('Notification contacts batch exceeds configured maximum')
        if len(set(self.user_ids)) != len(self.user_ids):
            raise ValueError('Duplicate user IDs are not allowed')
        return self


class ActiveNotificationContact(BaseModel):
    user_id: UUID
    status: Literal['active'] = 'active'
    email: str | None
    first_name: str | None
    last_name: str | None
    locale: str | None


class UnavailableNotificationContact(BaseModel):
    user_id: UUID
    status: Literal['unavailable'] = 'unavailable'


NotificationContact = Annotated[
    ActiveNotificationContact | UnavailableNotificationContact,
    Field(discriminator='status'),
]


class NotificationContactsResponse(BaseModel):
    items: list[NotificationContact]
