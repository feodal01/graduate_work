from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ClientInfo:
    ip_address: str | None
    user_agent: str | None


@dataclass(frozen=True, slots=True)
class CurrentUser:
    user_id: UUID
    token_id: UUID
    session_epoch: UUID
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class SocialIdentity:
    provider: str
    provider_user_id: str
    provider_login: str
    email: str | None
    first_name: str | None
    last_name: str | None
