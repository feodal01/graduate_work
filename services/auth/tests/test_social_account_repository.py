import asyncio
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import PostgresSettings
from models import Base, SocialAccount, User
from repositories.social_accounts import SQLAlchemySocialAccountRepository
from repositories.users import SQLAlchemyUserRepository
from services.types import SocialIdentity


@pytest.fixture
async def session_factory() -> AsyncGenerator[
    async_sessionmaker[AsyncSession],
    None,
]:
    """Create isolated PostgreSQL sessions for social-account tests."""
    settings = PostgresSettings()
    schema = f'test_{uuid4().hex}'
    admin_engine = create_async_engine(settings.dsn)
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        settings.dsn,
        connect_args={'server_settings': {'search_path': schema}},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin_engine.dispose()


def _identity(
    *,
    provider_user_id: str = '123',
    provider_login: str = 'alice',
    email: str | None = 'alice@yandex.ru',
    first_name: str | None = 'Alice',
    last_name: str | None = 'Viewer',
) -> SocialIdentity:
    return SocialIdentity(
        provider='yandex',
        provider_user_id=provider_user_id,
        provider_login=provider_login,
        email=email,
        first_name=first_name,
        last_name=last_name,
    )


@pytest.mark.asyncio
async def test_first_and_repeated_social_login_reuse_one_user(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Repeated provider identity must not create duplicate users."""
    async with session_factory() as session:
        repository = SQLAlchemySocialAccountRepository(session)

        first = await repository.get_or_create_user(_identity(), tz='Europe/Moscow')
        repeated = await repository.get_or_create_user(_identity(), tz='Asia/Tokyo')

        user_count = await session.scalar(select(func.count()).select_from(User))
        account_count = await session.scalar(
            select(func.count()).select_from(SocialAccount)
        )
        assert first.id == repeated.id
        assert user_count == 1
        assert account_count == 1


@pytest.mark.asyncio
async def test_social_login_does_not_link_an_existing_user_by_email(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A matching email must not attach to a local password account."""
    async with session_factory() as session:
        users = SQLAlchemyUserRepository(session)
        existing = await users.create(
            login='viewer@yandex.ru',
            password_hash='local-password-hash',
            first_name='Local',
            last_name='Viewer',
            tz='Europe/Moscow',
        )
        existing_id = existing.id
        repository = SQLAlchemySocialAccountRepository(session)

        social = await repository.get_or_create_user(
            _identity(
                provider_user_id='456',
                provider_login='viewer',
                email='viewer@yandex.ru',
            ),
            tz='Europe/Moscow',
        )

        assert social.id != existing_id
        assert social.login == 'yandex_456'
        assert social.password_hash is None
        assert social.is_superuser is False
        assert social.roles == []


@pytest.mark.asyncio
async def test_repeated_social_login_updates_provider_data_not_local_login(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Provider profile changes must preserve the allocated local login."""
    async with session_factory() as session:
        repository = SQLAlchemySocialAccountRepository(session)
        first = await repository.get_or_create_user(_identity(), tz='Europe/Moscow')

        repeated = await repository.get_or_create_user(
            _identity(
                email='new-address@yandex.ru',
                first_name='Updated',
                last_name='Name',
            ),
            tz='Europe/Moscow',
        )
        account = await session.scalar(select(SocialAccount))

        assert repeated.tz == 'Europe/Moscow'
        assert repeated.id == first.id
        assert repeated.login == 'alice@yandex.ru'
        assert repeated.first_name == 'Updated'
        assert repeated.last_name == 'Name'
        assert account is not None
        assert account.email == 'new-address@yandex.ru'


@pytest.mark.asyncio
async def test_concurrent_callbacks_reuse_the_created_provider_account(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Concurrent callbacks must converge on one provider identity."""
    async with (
        session_factory() as first_session,
        session_factory() as second_session,
    ):
        first_repository = SQLAlchemySocialAccountRepository(first_session)
        second_repository = SQLAlchemySocialAccountRepository(second_session)

        first, second = await asyncio.gather(
            first_repository.get_or_create_user(_identity(), tz='Europe/Moscow'),
            second_repository.get_or_create_user(_identity(), tz='Europe/Moscow'),
        )

    async with session_factory() as verification_session:
        user_count = await verification_session.scalar(
            select(func.count()).select_from(User)
        )
        account_count = await verification_session.scalar(
            select(func.count()).select_from(SocialAccount)
        )

    assert first.id == second.id
    assert user_count == 1
    assert account_count == 1
