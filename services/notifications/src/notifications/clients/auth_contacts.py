"""Fetch ephemeral contact data; integration failures never imply missing email."""

from typing import Annotated, Literal
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class ActiveContact(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    user_id: UUID
    status: Literal["active"]
    email: str | None
    first_name: str | None
    last_name: str | None
    locale: str | None


class UnavailableContact(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    user_id: UUID
    status: Literal["unavailable"]


ContactResult = Annotated[ActiveContact | UnavailableContact, Field(discriminator="status")]


class ContactResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    items: list[ContactResult]


class ContactsError(Exception):
    """Only a safe diagnostic code, never a response body or contact address."""

    def __init__(self, code: str) -> None:
        self.code = (
            code
            if code
            in {
                "invalid_request",
                "transport_failure",
                "authorization_failure",
                "http_failure",
                "invalid_response",
            }
            else "invalid_response"
        )
        super().__init__(self.code)


class AuthContactsClient:
    def __init__(
        self,
        url: str,
        api_key: str,
        timeout: float,
        batch_size: int,
        *,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.url, self.api_key = url, api_key
        self.timeout, self.batch_size = timeout, batch_size
        self.http = http

    async def fetch(self, user_ids: list[UUID]) -> dict[UUID, ContactResult]:
        results: dict[UUID, ContactResult] = {}
        if len(set(user_ids)) != len(user_ids):
            raise ContactsError("invalid_request")
        if self.http is not None:
            return await self._fetch(self.http, user_ids)
        async with httpx.AsyncClient() as http:
            results = await self._fetch(http, user_ids)
        return results

    async def _fetch(
        self, http: httpx.AsyncClient, user_ids: list[UUID]
    ) -> dict[UUID, ContactResult]:
        results: dict[UUID, ContactResult] = {}
        for start in range(0, len(user_ids), self.batch_size):
            batch = user_ids[start : start + self.batch_size]
            try:
                response = await http.post(
                    self.url,
                    headers={"X-Service-Key": self.api_key, "X-Request-Id": str(uuid4())},
                    json={"user_ids": [str(uid) for uid in batch]},
                    timeout=self.timeout,
                )
            except httpx.HTTPError:
                raise ContactsError("transport_failure") from None
            if response.status_code != 200:
                code = (
                    "authorization_failure"
                    if response.status_code in (401, 403)
                    else "http_failure"
                )
                raise ContactsError(code)
            try:
                parsed = ContactResponse.model_validate(response.json())
            except (ValueError, ValidationError):
                raise ContactsError("invalid_response") from None
            ids = [item.user_id for item in parsed.items]
            if len(ids) != len(set(ids)) or set(ids) != set(batch):
                raise ContactsError("invalid_response")
            results.update({item.user_id: item for item in parsed.items})
        return results
