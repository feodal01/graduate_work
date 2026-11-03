from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from events.contracts import UserProfileSnapshot
from events.outbox import append_user_event


def _profile(version: int = 1) -> UserProfileSnapshot:
    return UserProfileSnapshot(uuid4(), version, None)


async def test_unknown_event_type_is_rejected_before_database_access() -> None:
    """Unknown event types must never enter the Kafka delivery queue."""
    async with AsyncSession() as session:
        with pytest.raises(ValueError, match='Unsupported Auth event type'):
            await append_user_event(session, 'user.unknown', _profile())
        assert not session.new
        assert not session.in_transaction()


async def test_nonpositive_version_is_rejected_before_database_access() -> None:
    """Version zero must not enter the monotonic consumer contract."""
    async with AsyncSession() as session:
        with pytest.raises(ValueError, match='user_version must be at least 1'):
            await append_user_event(session, 'user.registered', _profile(0))
        assert not session.new
        assert not session.in_transaction()


def test_profile_snapshot_cannot_be_changed_after_capture() -> None:
    """A caller cannot alter the captured version before event persistence."""
    profile = _profile()
    with pytest.raises(FrozenInstanceError):
        profile.user_version = 9  # type: ignore[misc]
