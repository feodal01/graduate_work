from dataclasses import dataclass
from uuid import UUID

USER_EVENT_TYPES = frozenset(
    {'user.registered', 'user.profile_updated', 'user.deleted'}
)


@dataclass(frozen=True, slots=True)
class UserProfileSnapshot:
    user_id: UUID
    user_version: int
    tz: str | None
