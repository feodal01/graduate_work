import asyncio
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from bootstrap_notifications import bootstrap_users
from models import NotificationLoadMarker, NotificationOutbox, User
from tests import test_notification_migration

notification_database = test_notification_migration.notification_database
pytestmark = pytest.mark.integration


async def test_bulk_snapshot_versions_repeat_and_cleanup(
    notification_database: AsyncEngine,
) -> None:
    """Import complete nullable profiles once, even after published outbox cleanup."""
    async with AsyncSession(notification_database) as session:
        session.add(User(login='current', user_version=7, email='current@example.org'))
        await session.commit()
    assert await bootstrap_users('initial-users-v1', engine=notification_database) == 2
    async with AsyncSession(notification_database) as session:
        rows = (await session.scalars(select(NotificationOutbox))).all()
        users = {str(u.id): u for u in (await session.scalars(select(User))).all()}
        for row in rows:
            source = users[str(row.user_id)]
            assert row.user_version == source.user_version
            assert row.event_type == 'user.profile_updated'
            assert row.load_id == 'initial-users-v1'
            assert row.published_at is None
            assert row.payload == dict(
                event_id=str(row.event_id),
                event_type='user.profile_updated',
                user_id=str(source.id),
                user_version=source.user_version,
                tz=source.tz,
            )
        await session.execute(
            update(NotificationOutbox).values(published_at=func.now())
        )
        await session.execute(delete(NotificationOutbox))
        await session.commit()
    assert await bootstrap_users('initial-users-v1', engine=notification_database) == 0
    async with AsyncSession(notification_database) as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(NotificationLoadMarker)
            )
            == 2
        )
        assert (
            await session.scalar(select(func.count()).select_from(NotificationOutbox))
            == 0
        )


async def test_concurrent_bootstraps_claim_users_once(
    notification_database: AsyncEngine,
) -> None:
    """Concurrent executions share permanent load markers and produce no duplicates."""
    counts = await asyncio.gather(
        *[
            bootstrap_users('initial-users-v1', engine=notification_database)
            for _ in range(3)
        ]
    )
    assert sorted(counts) == [0, 0, 1]


async def test_bootstrap_failure_rolls_back_all_markers(
    notification_database: AsyncEngine,
) -> None:
    """An insert failure rolls back the whole import and its permanent markers."""
    async with AsyncSession(notification_database) as session:
        session.add_all([User(login='second'), User(login='third')])
        await session.commit()
    async with notification_database.begin() as connection:
        await connection.execute(text('CREATE SEQUENCE bootstrap_attempt'))
        await connection.execute(
            text("""
            CREATE FUNCTION reject_bootstrap() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF nextval('bootstrap_attempt') = 2 THEN
                    RAISE EXCEPTION 'injected bootstrap failure';
                END IF;
                RETURN NEW;
            END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER reject_bootstrap BEFORE INSERT ON notification_outbox
            FOR EACH ROW EXECUTE FUNCTION reject_bootstrap()
        """)
        )
    with pytest.raises(DBAPIError, match='injected bootstrap failure'):
        await bootstrap_users('initial-users-v1', engine=notification_database)
    async with notification_database.begin() as connection:
        for table in (NotificationOutbox, NotificationLoadMarker):
            assert await connection.scalar(select(func.count()).select_from(table)) == 0
        await connection.execute(
            text('DROP TRIGGER reject_bootstrap ON notification_outbox')
        )
    assert await bootstrap_users('initial-users-v1', engine=notification_database) == 3


async def test_bootstrap_cli_runs_once(notification_database: AsyncEngine) -> None:
    """The standalone module uses its configured database and exits successfully."""
    root = Path(__file__).parents[1]
    environment = dict(
        os.environ,
        POSTGRES_DB=str(notification_database.url.database),
        PYTHONPATH=str(root / 'src'),
    )
    for expected in (1, 0):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            '-m',
            'bootstrap_notifications',
            '--load-id',
            'cli-load-v1',
            cwd=root,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        assert str(expected) in stdout.decode()
