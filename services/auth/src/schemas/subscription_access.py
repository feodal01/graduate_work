from datetime import UTC, datetime
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class SubscriptionAccessPutRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    version: Annotated[int, Field(strict=True, ge=1)]
    expires_at: AwareDatetime | None = None

    @field_validator('expires_at')
    @classmethod
    def normalize_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value.astimezone(UTC)


class SubscriptionAccessPutResponse(BaseModel):
    applied: bool
    current_version: int
