import asyncio
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from core.config import PostgresSettings
from models import User

if TYPE_CHECKING:
    from events.contracts import UserProfileSnapshot

pytestmark = pytest.mark.integration


@pytest.fixture
async def notification_database() -> AsyncIterator[AsyncEngine]:
    """Migrate an isolated database, preserving a user from the previous schema."""
    settings = PostgresSettings()
    if settings.user != 'notifications_test' or settings.host != '127.0.0.1':
        raise RuntimeError(
            'Set PostgreSQL endpoints from tests/notifications/conftest.py'
        )
    database = f'notifications_test_{uuid4().hex}'
    admin = await asyncpg.connect(
        user=settings.user,
        password=settings.password.get_secret_value(),
        host=settings.host,
        port=settings.port,
        database='notifications_test_admin',
    )
    await admin.execute(f'CREATE DATABASE "{database}"')
    engine = create_async_engine(settings.model_copy(update={'db': database}).dsn)
    service_root = Path(__file__).parents[1]
    environment = dict(os.environ, POSTGRES_DB=database)

    async def migrate(revision: str) -> None:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            '-m',
            'alembic',
            '-c',
            str(service_root / 'alembic.ini'),
            'upgrade',
            revision,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, (stdout + stderr).decode()

    try:
        await migrate('20260830_0004')
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    'INSERT INTO users (id, login, password, first_name, last_name) '
                    "VALUES ('00000000-0000-0000-0000-000000000001', "
                    "'legacy@example.org', 'existing-hash', 'Old', 'Profile')"
                )
            )
        await migrate('head')
        yield engine
    finally:
        await engine.dispose()
        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        await admin.close()


def _profile(user_id: UUID | None = None) -> 'UserProfileSnapshot':
    from events.contracts import UserProfileSnapshot

    return UserProfileSnapshot(
        user_id=user_id or uuid4(),
        user_version=2,
        tz='Europe/Moscow',
    )


async def test_upgrade_preserves_legacy_profile_without_emitting_events(
    notification_database: AsyncEngine,
) -> None:
    """Upgrade must not infer email from login or emit registration events."""
    from models import NotificationLoadMarker, NotificationOutbox

    async with AsyncSession(notification_database) as session:
        user = await session.get(User, UUID(int=1))
        assert user is not None
        assert user.user_version == 1
        assert user.email is None
        assert user.locale is None
        assert user.tz is None
        assert (user.first_name, user.last_name) == ('Old', 'Profile')
        assert user.login == 'legacy@example.org'
        assert user.password_hash == 'existing-hash'
        assert (
            await session.scalar(select(func.count()).select_from(NotificationOutbox))
            == 0
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(NotificationLoadMarker)
            )
            == 0
        )


@pytest.mark.parametrize('event_type', ['user.registered', 'user.profile_updated'])
async def test_append_stores_minimal_snapshot_and_rollback_removes_event(
    notification_database: AsyncEngine,
    event_type: str,
) -> None:
    """Append must preserve its snapshot and participate in the caller transaction."""
    from events.outbox import append_user_event
    from models import NotificationOutbox

    profile = _profile(UUID(int=1))
    async with AsyncSession(notification_database) as session:
        event_id = await append_user_event(session, event_type, profile)
        row = await session.get(NotificationOutbox, event_id)
        assert row is not None
        assert row.payload == {
            'event_id': str(event_id),
            'event_type': event_type,
            'user_id': str(profile.user_id),
            'user_version': 2,
            'tz': 'Europe/Moscow',
        }
        assert row.published_at is None
        async with AsyncSession(notification_database) as observer:
            assert await observer.get(NotificationOutbox, event_id) is None
        await session.rollback()
    async with AsyncSession(notification_database) as observer:
        assert await observer.get(NotificationOutbox, event_id) is None


async def test_regular_events_repeat_and_load_marker_survives_cleanup(
    notification_database: AsyncEngine,
) -> None:
    """A cleaned outbox must still reject the same bootstrap load/user pair."""
    from events.outbox import append_user_event
    from models import NotificationLoadMarker, NotificationOutbox

    profile = _profile()
    async with AsyncSession(notification_database) as session:
        first = await append_user_event(session, 'user.registered', profile)
        second = await append_user_event(session, 'user.profile_updated', profile)
        assert first != second
        await session.commit()
        loaded = await append_user_event(
            session, 'user.profile_updated', profile, load_id='initial-users-v1'
        )
        await session.commit()
        with pytest.raises(IntegrityError):
            await append_user_event(
                session, 'user.profile_updated', profile, load_id='initial-users-v1'
            )
        await session.rollback()
        await session.execute(
            delete(NotificationOutbox).where(NotificationOutbox.event_id == loaded)
        )
        await session.commit()
        assert (
            await session.get(
                NotificationLoadMarker, ('initial-users-v1', profile.user_id)
            )
            is not None
        )
        with pytest.raises(IntegrityError):
            await append_user_event(
                session, 'user.profile_updated', profile, load_id='initial-users-v1'
            )
        await session.rollback()
        assert (
            await session.scalar(select(func.count()).select_from(NotificationOutbox))
            == 2
        )


async def test_deleted_payload_survives_user_deletion_and_cannot_change(
    notification_database: AsyncEngine,
) -> None:
    """Deletion must keep events and prevent changing the stored delivery snapshot."""
    from events.outbox import append_user_event
    from models import NotificationOutbox

    async with AsyncSession(notification_database) as session:
        event_id = await append_user_event(
            session, 'user.deleted', _profile(UUID(int=1))
        )
        await session.execute(delete(User).where(User.id == UUID(int=1)))
        await session.commit()
        row = await session.get(NotificationOutbox, event_id)
        assert row is not None
        assert row.payload == {
            'event_id': str(event_id),
            'event_type': 'user.deleted',
            'user_id': str(UUID(int=1)),
            'user_version': 2,
        }
        with pytest.raises(DBAPIError, match='immutable'):
            await session.execute(
                update(NotificationOutbox)
                .where(NotificationOutbox.event_id == event_id)
                .values(payload={'changed': True})
            )
        await session.rollback()
        await session.execute(
            update(NotificationOutbox)
            .where(NotificationOutbox.event_id == event_id)
            .values(published_at=func.now())
        )
        await session.commit()
        await session.refresh(row)
        assert row.published_at is not None


async def test_bootstrap_marker_is_rolled_back_with_event(
    notification_database: AsyncEngine,
) -> None:
    """A rolled-back bootstrap attempt must be retryable with the same load id."""
    from events.outbox import append_user_event
    from models import NotificationLoadMarker, NotificationOutbox

    profile = _profile()
    async with AsyncSession(notification_database) as session:
        event_id = await append_user_event(
            session, 'user.profile_updated', profile, load_id='initial-users-v1'
        )
        await session.rollback()
        assert await session.get(NotificationOutbox, event_id) is None
        assert (
            await session.get(
                NotificationLoadMarker, ('initial-users-v1', profile.user_id)
            )
            is None
        )
        await append_user_event(
            session, 'user.profile_updated', profile, load_id='initial-users-v1'
        )
        await session.commit()


async def test_database_rejects_duplicate_load_pair_without_application_helper(
    notification_database: AsyncEngine,
) -> None:
    """The partial outbox index must enforce bootstrap uniqueness for bulk inserts."""
    from events.outbox import append_user_event
    from models import NotificationOutbox

    profile = _profile()
    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        event_id = await append_user_event(
            session, 'user.profile_updated', profile, load_id='initial-users-v1'
        )
        await session.commit()
        row = await session.get(NotificationOutbox, event_id)
        assert row is not None
        duplicate_id = uuid4()
        session.add(
            NotificationOutbox(
                event_id=duplicate_id,
                user_id=profile.user_id,
                user_version=profile.user_version,
                event_type='user.profile_updated',
                payload={**row.payload, 'event_id': str(duplicate_id)},
                load_id='initial-users-v1',
            )
        )
        with pytest.raises(IntegrityError, match='uq_notification_outbox_load_user'):
            await session.flush()
        await session.rollback()
