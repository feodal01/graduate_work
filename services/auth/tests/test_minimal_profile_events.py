from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models import User
from services.profile_events import apply_contact_changes, snapshot


@pytest.mark.asyncio
async def test_contact_change_advances_version_without_notification() -> None:
    """A contact edit advances profile version without publishing private data."""
    user = User(
        id=uuid4(), login='Alice', user_version=4, email='old@example.org', tz='UTC'
    )
    session = AsyncMock(spec=AsyncSession)
    changed = await apply_contact_changes(session, user, {'email': 'new@example.org'})
    assert changed
    assert user.email == 'new@example.org'
    assert user.user_version == 5
    session.add.assert_not_called()
    session.flush.assert_not_awaited()


def test_notification_snapshot_contains_no_contacts() -> None:
    """Snapshots expose only delivery timezone and versioned identity."""
    from dataclasses import asdict

    user = User(
        id=uuid4(), login='Alice', user_version=4, email='private@example.org', tz='UTC'
    )
    assert asdict(snapshot(user)) == {
        'user_id': user.id,
        'user_version': 4,
        'tz': 'UTC',
    }
