from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class TokenIntrospectionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', hide_input_in_errors=True)

    token: SecretStr = Field(min_length=1)


class TokenIntrospectionResponse(BaseModel):
    active: bool
    sub: UUID | None = None
