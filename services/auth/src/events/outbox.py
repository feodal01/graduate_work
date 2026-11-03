from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from events.contracts import USER_EVENT_TYPES, UserProfileSnapshot
from models import NotificationLoadMarker, NotificationOutbox


async def append_user_event(
    session: AsyncSession,
    event_type: str,
    profile: UserProfileSnapshot,
    load_id: str | None = None,
) -> UUID:
    """Persist an event snapshot in the caller's transaction without committing."""
    if event_type not in USER_EVENT_TYPES:
        raise ValueError(f'Unsupported Auth event type: {event_type}')
    if profile.user_version < 1:
        raise ValueError('user_version must be at least 1')
    event_id = uuid4()
    payload: dict[str, str | int | None] = {
        'event_id': str(event_id),
        'event_type': event_type,
        'user_id': str(profile.user_id),
        'user_version': profile.user_version,
    }
    if event_type != 'user.deleted':
        payload['tz'] = profile.tz
    if load_id is not None:
        session.add(NotificationLoadMarker(load_id=load_id, user_id=profile.user_id))
    session.add(
        NotificationOutbox(
            event_id=event_id,
            user_id=profile.user_id,
            user_version=profile.user_version,
            event_type=event_type,
            payload=payload,
            load_id=load_id,
        )
    )
    await session.flush()
    return event_id
