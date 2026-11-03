"""Atomic profile operations against an isolated, migrated PostgreSQL database."""

import asyncio
from dataclasses import replace
from uuid import UUID

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from db.sql.errors import DatabaseUnavailableError
from models import NotificationOutbox, User
from repositories.social_accounts import SQLAlchemySocialAccountRepository
from repositories.users import SQLAlchemyUserRepository
from services.types import SocialIdentity
from tests import test_notification_migration

notification_database = test_notification_migration.notification_database

pytestmark = pytest.mark.integration


async def register(
    session: AsyncSession,
    *,
    email: str | None = None,
    locale: str | None = None,
    tz: str = 'Europe/Moscow',
) -> User:
    """Create a local user through the real repository transaction boundary."""
    return await SQLAlchemyUserRepository(session).create(
        login='local-user',
        password_hash='hash',
        first_name='First',
        last_name='Last',
        email=email,
        locale=locale,
        tz=tz,
    )


async def events(session: AsyncSession, user_id: UUID) -> list[NotificationOutbox]:
    """Read immutable snapshots in version order."""
    return list(
        (
            await session.scalars(
                select(NotificationOutbox)
                .where(NotificationOutbox.user_id == user_id)
                .order_by(NotificationOutbox.user_version)
            )
        ).all()
    )


async def test_registration_and_each_contact_change(
    notification_database: AsyncEngine,
) -> None:
    """Contact edits advance versions; only timezone edits emit minimal events."""
    from services.profile_events import update_contact_profile

    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        user = await register(
            session, email='first@example.org', locale='en-US', tz='UTC'
        )
        rows = await events(session, user.id)
        assert rows[0].payload == dict(
            event_id=str(rows[0].event_id),
            event_type='user.registered',
            user_id=str(user.id),
            user_version=1,
            tz='UTC',
        )
        for field, value in dict(
            email='changed@example.org',
            first_name='Changed',
            last_name=None,
            locale='ru-RU',
        ).items():
            assert await update_contact_profile(session, user.id, {field: value})
            assert len(await events(session, user.id)) == 1
        assert user.user_version == 5
        assert await update_contact_profile(
            session, user.id, {'tz': 'Europe/Moscow', 'email': 'mixed@example.org'}
        )
        rows = await events(session, user.id)
        assert [r.user_version for r in rows] == [1, 6]
        assert rows[-1].payload == dict(
            event_id=str(rows[-1].event_id),
            event_type='user.profile_updated',
            user_id=str(user.id),
            user_version=6,
            tz='Europe/Moscow',
        )
        assert not await update_contact_profile(
            session, user.id, {'tz': 'Europe/Moscow'}
        )
        await SQLAlchemyUserRepository(session).update(
            user, login='renamed', password_hash='new-hash'
        )
        assert user.user_version == 6
        assert len(await events(session, user.id)) == 2


async def test_outbox_failure_rolls_back_registration(
    notification_database: AsyncEngine,
) -> None:
    """A database rejection of outbox insertion must roll back the new user."""
    async with AsyncSession(notification_database) as session:
        await session.execute(
            text(
                'ALTER TABLE notification_outbox ADD CONSTRAINT fail_event '
                "CHECK (event_type = 'impossible')"
            )
        )
        await session.commit()
        with pytest.raises(DatabaseUnavailableError):
            await register(session)
    async with AsyncSession(notification_database) as observer:
        exists = (
            await observer.scalar(select(User.id).where(User.login == 'local-user'))
            is not None
        )
        count = await observer.scalar(
            select(func.count()).select_from(NotificationOutbox)
        )
        assert (exists, count) == (False, 0)


async def test_concurrent_updates_refresh_stale_identity_map(
    notification_database: AsyncEngine,
) -> None:
    """Row locks allocate distinct versions and preserve both concurrent changes."""
    from services.profile_events import update_contact_profile

    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        user_id = (await register(session)).id
    async with (
        AsyncSession(notification_database, expire_on_commit=False) as first,
        AsyncSession(notification_database, expire_on_commit=False) as second,
    ):
        await first.get(User, user_id)
        stale = await second.get(User, user_id)
        await asyncio.gather(
            update_contact_profile(first, user_id, {'email': 'concurrent@example.org'}),
            update_contact_profile(second, user_id, {'locale': 'ru-RU'}),
        )
        assert stale is not None
    async with AsyncSession(notification_database) as observer:
        rows = await events(observer, user_id)
        assert [r.user_version for r in rows] == [1]
        current = await observer.get(User, user_id)
        assert current is not None and current.user_version == 3
        assert current.email == 'concurrent@example.org'
        assert current.locale == 'ru-RU'


async def test_delete_keeps_event_and_removes_dependencies(
    notification_database: AsyncEngine,
) -> None:
    """Deletion removes Auth dependencies while retaining its final outbox event."""
    from models import LoginHistory, Role, SocialAccount
    from services.profile_events import delete_profile

    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        user = await register(session)
        user_id = user.id
        user.roles.append(Role(name='member'))
        user.social_accounts.append(
            SocialAccount(provider='yandex', provider_user_id='delete-me')
        )
        session.add(LoginHistory(user_id=user_id))
        await session.commit()
        await delete_profile(session, user_id)
    async with AsyncSession(notification_database) as observer:
        assert await observer.get(User, user_id) is None
        rows = await events(observer, user_id)
        assert [r.user_version for r in rows] == [1, 2]
        assert rows[-1].payload == dict(
            event_id=str(rows[-1].event_id),
            event_type='user.deleted',
            user_id=str(user_id),
            user_version=2,
        )
        assert (
            await observer.scalar(
                select(func.count())
                .select_from(LoginHistory)
                .where(LoginHistory.user_id == user_id)
            )
            == 0
        )


async def test_oauth_registration_names_and_contact_email(
    notification_database: AsyncEngine,
) -> None:
    """OAuth emits registration/name updates but preserves explicitly edited email."""
    from services.profile_events import update_contact_profile

    identity = SocialIdentity(
        provider='yandex',
        provider_user_id='123',
        provider_login='provider-user',
        email='provider@example.org',
        first_name='Social',
        last_name='User',
    )
    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        repository = SQLAlchemySocialAccountRepository(session)
        user = await repository.get_or_create_user(identity, tz='Europe/Moscow')
        assert user.email == identity.email
        await update_contact_profile(
            session, user.id, {'email': 'personal@example.org'}
        )
        await repository.get_or_create_user(
            replace(identity, first_name='Changed', email='provider-new@example.org'),
            tz='Europe/Moscow',
        )
        await repository.get_or_create_user(
            replace(identity, first_name='Changed', email='provider-new@example.org'),
            tz='Europe/Moscow',
        )
        rows = await events(session, user.id)
        assert [r.event_type for r in rows] == ['user.registered']
        assert user.email == 'personal@example.org'
        assert user.first_name == 'Changed'
        assert user.user_version == 3
        assert user.tz == 'Europe/Moscow'
        assert set(rows[0].payload) == {
            'event_id',
            'event_type',
            'user_id',
            'user_version',
            'tz',
        }


@pytest.mark.parametrize('operation', ['update', 'delete'])
async def test_outbox_failure_rolls_back_existing_profile(
    notification_database: AsyncEngine, operation: str
) -> None:
    """Failed outbox insertion rolls back profile/version, credentials and deletion."""
    from services.profile_events import delete_profile

    async with AsyncSession(notification_database, expire_on_commit=False) as session:
        user = await register(session)
        user_id = user.id
        original_name = user.first_name
        await session.execute(
            text(
                'ALTER TABLE notification_outbox ADD CONSTRAINT fail_event '
                "CHECK (event_type = 'user.registered')"
            )
        )
        await session.commit()
        with pytest.raises(
            IntegrityError if operation == 'delete' else DatabaseUnavailableError
        ):
            if operation == 'update':
                await SQLAlchemyUserRepository(session).update(
                    user,
                    login='new-login',
                    password_hash='changed',
                    contacts={'first_name': 'After', 'tz': 'Asia/Tokyo'},
                )
            else:
                await delete_profile(session, user_id)
    async with AsyncSession(notification_database) as observer:
        restored = await observer.get(User, user_id)
        assert restored is not None
        assert restored.user_version == 1 and restored.first_name == original_name
        assert restored.login == 'local-user' and restored.password_hash == 'hash'
        assert len(await events(observer, user_id)) == 1


async def test_oauth_outbox_failure_rolls_back_user_and_social_account(
    notification_database: AsyncEngine,
) -> None:
    """OAuth identity must not survive when its registration event cannot persist."""
    from models import SocialAccount

    identity = SocialIdentity(
        provider='yandex',
        provider_user_id='123',
        provider_login='social',
        email=None,
        first_name=None,
        last_name=None,
    )
    async with AsyncSession(notification_database) as session:
        await session.execute(
            text(
                'ALTER TABLE notification_outbox ADD CONSTRAINT fail_event '
                "CHECK (event_type = 'impossible')"
            )
        )
        await session.commit()
        with pytest.raises(DatabaseUnavailableError):
            await SQLAlchemySocialAccountRepository(session).get_or_create_user(
                identity, tz='Europe/Moscow'
            )
    async with AsyncSession(notification_database) as observer:
        assert (
            await observer.scalar(select(User.id).where(User.login == 'social')) is None
        )
        assert (
            await observer.scalar(select(func.count()).select_from(SocialAccount)) == 0
        )
        assert (
            await observer.scalar(select(func.count()).select_from(NotificationOutbox))
            == 0
        )


@pytest.mark.parametrize('reject_outbox', [False, True])
async def test_delete_revokes_real_redis_sessions(
    notification_database: AsyncEngine,
    reject_outbox: bool,
) -> None:
    """A real committed deletion invalidates both existing access and refresh tokens."""
    from uuid import uuid4

    from redis.asyncio import Redis

    from core.config import RedisSettings
    from db.cache.session import RedisTokenSessionStore
    from repositories.login_history import SQLAlchemyLoginHistoryRepository
    from security.passwords import PasswordHasher
    from security.tokens import TokenService
    from services.authentication import AuthenticationService
    from services.errors import AccessTokenRevokedError, InvalidRefreshTokenError
    from services.types import ClientInfo
    from validation.credentials import PasswordPolicy

    settings = RedisSettings()
    if settings.host != '127.0.0.1' or settings.port == 6379:
        raise RuntimeError('Use the dedicated notifications test Redis endpoint')
    redis = Redis(host=settings.host, port=settings.port)
    namespace = f'profile-test-{uuid4().hex}'
    store = RedisTokenSessionStore(redis, namespace=namespace)
    try:
        async with AsyncSession(
            notification_database, expire_on_commit=False
        ) as session:
            service = AuthenticationService(
                users=SQLAlchemyUserRepository(session),
                login_history=SQLAlchemyLoginHistoryRepository(session),
                token_sessions=store,
                password_hasher=PasswordHasher(),
                password_policy=PasswordPolicy(min_length=8, min_score=3),
                token_service=TokenService(
                    secret_key='profile-test-secret-long-enough-for-hs256',
                    algorithm='HS256',
                    access_ttl_seconds=900,
                    refresh_ttl_seconds=3600,
                ),
            )
            user = await service.signup(
                login='DeleteReal',
                password='correct horse',
                first_name=None,
                last_name=None,
                tz='Europe/Moscow',
            )
            user_id = user.id
            client = ClientInfo(None, None)
            first = await service.login(
                login=user.login, password='correct horse', client=client
            )
            second = await service.login(
                login=user.login, password='correct horse', client=client
            )
            current = await service.authenticate_access(
                first.access.value, client=client
            )
            if reject_outbox:
                await session.execute(
                    text(
                        'ALTER TABLE notification_outbox ADD CONSTRAINT fail_event '
                        "CHECK (event_type = 'user.registered')"
                    )
                )
                await session.commit()
                with pytest.raises(DatabaseUnavailableError):
                    await service.delete_profile(current)
            else:
                await service.delete_profile(current)
            for pair in (first, second):
                with pytest.raises(AccessTokenRevokedError):
                    await service.authenticate_access(pair.access.value, client=client)
                with pytest.raises(InvalidRefreshTokenError):
                    await service.rotate_refresh(
                        service.verify_refresh_token(pair.refresh.value)
                    )
        async with AsyncSession(notification_database) as observer:
            assert (await observer.get(User, user_id) is not None) == reject_outbox
            rows = await events(observer, user_id)
            assert rows[-1].event_type == (
                'user.registered' if reject_outbox else 'user.deleted'
            )
            assert len(rows) == (1 if reject_outbox else 2)
    finally:
        keys = [key async for key in redis.scan_iter(match=f'*{namespace}*')]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()


async def test_unexpected_event_failure_leaves_session_reusable(
    notification_database: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An application failure cannot leave a pending user for a later commit."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        'services.profile_events.append_user_event',
        AsyncMock(side_effect=RuntimeError('event failure')),
    )
    async with AsyncSession(notification_database) as session:
        with pytest.raises(RuntimeError, match='event failure'):
            await register(session)
        await session.commit()
        assert (
            await session.scalar(select(User.id).where(User.login == 'local-user'))
            is None
        )
        assert (
            await session.scalar(select(func.count()).select_from(NotificationOutbox))
            == 0
        )
