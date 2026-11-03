from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
ProviderIdentifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]


class YandexTokenResponse(BaseModel):
    model_config = ConfigDict(extra='ignore', strict=True)

    token_type: Literal['bearer']
    access_token: NonEmptyText
    expires_in: int = Field(gt=0)
    refresh_token: str | None = None
    scope: str | None = None


class YandexUserInfoResponse(BaseModel):
    model_config = ConfigDict(extra='ignore', strict=True)

    id: ProviderIdentifier
    login: ProviderIdentifier
    client_id: NonEmptyText
    default_email: str | None = Field(default=None, max_length=255)
    first_name: str | None = Field(default=None, max_length=255)
    last_name: str | None = Field(default=None, max_length=255)
