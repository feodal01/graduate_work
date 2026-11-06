from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator

from core.config import settings


class UsersLookupRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    login: str | None = None
    user_ids: list[UUID] | None = None

    @model_validator(mode='after')
    def validate_query(self) -> Self:
        normalized_login = self.login.strip() if self.login is not None else None
        if self.login is not None and not normalized_login:
            raise ValueError('login must not be empty')
        self.login = normalized_login

        has_login = self.login is not None
        has_user_ids = self.user_ids is not None
        if has_login == has_user_ids:
            raise ValueError('Specify either login or user_ids, not both or neither')
        if self.user_ids is not None:
            if len(self.user_ids) == 0:
                raise ValueError('user_ids must not be empty')
            if len(self.user_ids) > settings.auth_user_lookup_batch_size:
                raise ValueError('user_ids batch exceeds configured maximum')
        return self


class UserLookupItem(BaseModel):
    user_id: UUID
    login: str
    tz: str


class UsersLookupResponse(BaseModel):
    items: list[UserLookupItem]
