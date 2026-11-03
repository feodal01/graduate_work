"""Ask Auth for current access validity; never cache or bypass its decision."""

from typing import Self
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, StrictBool, ValidationError, model_validator


class TokenStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    active: StrictBool
    sub: UUID | None = None

    @model_validator(mode="after")
    def subject_matches_status(self) -> Self:
        if self.active != (self.sub is not None):
            raise ValueError("Only an active token must include its subject")
        return self


class AuthIntrospectionUnavailable(Exception):
    """Auth could not establish token validity; contains no credentials or body."""


class AuthIntrospectionClient:
    def __init__(self, http: httpx.AsyncClient, url: str, api_key: str) -> None:
        self.http = http
        self.url = url
        self.api_key = api_key

    async def check(self, token: str, request_id: str) -> UUID | None:
        try:
            response = await self.http.post(
                self.url,
                headers={"X-Service-Key": self.api_key, "X-Request-Id": request_id},
                json={"token": token},
            )
        except httpx.HTTPError:
            raise AuthIntrospectionUnavailable("transport_failure") from None
        if response.status_code != 200:
            raise AuthIntrospectionUnavailable("http_failure")
        try:
            status = TokenStatus.model_validate_json(response.content)
        except ValidationError:
            raise AuthIntrospectionUnavailable("invalid_response") from None
        return status.sub
