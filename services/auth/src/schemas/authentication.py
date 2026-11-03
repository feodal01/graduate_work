import re
from datetime import datetime
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)

from validation.credentials import Login, Password
from validation.timezones import Timezone, validate_timezone


class ContactProfile(BaseModel):
    first_name: str | None = Field(default=None, max_length=50)
    last_name: str | None = Field(default=None, max_length=50)
    email: EmailStr | None = Field(default=None, max_length=255)
    locale: str | None = Field(default=None, max_length=64)

    @field_validator('locale')
    @classmethod
    def valid_locale(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(
            r'[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*', value
        ):
            raise ValueError('locale must be a language tag such as ru-RU')
        return value


class SignupRequest(ContactProfile):
    tz: Timezone
    login: Login
    password: Password


class LoginRequest(BaseModel):
    login: Login
    password: Password


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


class LogoutRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


class ProfileUpdateRequest(ContactProfile):
    tz: str | None = Field(default=None, max_length=64)
    new_login: Login | None = None
    new_password: Password | None = None

    @field_validator('tz')
    @classmethod
    def valid_timezone(cls, value: str | None) -> str | None:
        if value is None:
            raise ValueError('tz cannot be null')
        return validate_timezone(value)

    @model_validator(mode='after')
    def require_change(self) -> 'ProfileUpdateRequest':
        for name in ('new_login', 'new_password'):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f'{name} cannot be null')
        if not self.model_fields_set:
            raise ValueError('At least one profile field must be provided')
        return self


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    login: str
    first_name: str | None
    last_name: str | None
    email: str | None = None
    locale: str | None = None
    tz: str | None = None
    user_version: int = 1
    created_at: datetime


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = 'bearer'


class LoginHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    logged_at: datetime
    ip_address: str | None
    user_agent: str | None
