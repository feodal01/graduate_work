"""Profile snapshots in the same transaction as the corresponding Auth mutation."""

from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from events.contracts import UserProfileSnapshot
from events.outbox import append_user_event
from models import LoginHistory, User
from repositories.errors import UserNotFoundError
from validation.timezones import validate_timezone

CONTACT_FIELDS = frozenset({'email', 'first_name', 'last_name', 'locale', 'tz'})


def snapshot(user: User) -> UserProfileSnapshot:
    return UserProfileSnapshot(
        user_id=user.id,
        user_version=user.user_version,
        tz=user.tz,
    )


async def create_profile_event(session: AsyncSession, user: User) -> UUID:
    """Flush a new profile and its event; the owning repository commits both."""
    await session.flush()
    return await append_user_event(session, 'user.registered', snapshot(user))


async def lock_profile(session: AsyncSession, user_id: UUID) -> User:
    """Refresh under a row lock, including objects already in the identity map."""
    user = await session.scalar(
        select(User)
        .where(User.id == user_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if user is None:
        raise UserNotFoundError
    return user


async def apply_contact_changes(
    session: AsyncSession,
    user: User,
    changes: dict[str, str | None],
) -> bool:
    """Apply fields to an already locked user without ending its transaction."""
    if changes.keys() - CONTACT_FIELDS:
        raise ValueError('Unsupported contact profile field')
    if 'tz' in changes:
        validate_timezone(changes['tz'])
    timezone_changed = 'tz' in changes and changes['tz'] != user.tz
    changed = any(getattr(user, name) != value for name, value in changes.items())
    if not changed:
        return False
    for name, value in changes.items():
        setattr(user, name, value)
    user.user_version += 1
    if timezone_changed:
        await append_user_event(session, 'user.profile_updated', snapshot(user))
    return True


async def update_contact_profile(
    session: AsyncSession,
    user_id: UUID,
    changes: dict[str, str | None],
) -> bool:
    """Own one commit/rollback boundary, including any preceding autobegun read."""
    try:
        user = await lock_profile(session, user_id)
        changed = await apply_contact_changes(session, user, changes)
        await session.commit()
        return changed
    except BaseException:
        await session.rollback()
        raise


async def delete_profile(session: AsyncSession, user_id: UUID) -> None:
    """Delete Auth data and commit a retained event with the final user version."""
    try:
        user = await lock_profile(session, user_id)
        user.user_version += 1
        await append_user_event(session, 'user.deleted', snapshot(user))
        await session.execute(
            delete(LoginHistory).where(LoginHistory.user_id == user_id)
        )
        # ORM removes user_roles and social_accounts before their owning user.
        await session.delete(user)
        await session.commit()
    except BaseException:
        await session.rollback()
        raise
