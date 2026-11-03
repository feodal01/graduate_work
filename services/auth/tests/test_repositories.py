from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import PostgresSettings
from models import Base, LoginHistory
from repositories.errors import LoginAlreadyTakenError
from repositories.login_history import SQLAlchemyLoginHistoryRepository
from repositories.users import SQLAlchemyUserRepository


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    """Создаёт изолированную схему PostgreSQL для одного теста."""
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
        for remainder in range(16):
            await connection.execute(
                text(
                    f'CREATE TABLE login_history_p{remainder:02d} '
                    'PARTITION OF login_history '
                    'FOR VALUES WITH '
                    f'(MODULUS 16, REMAINDER {remainder})'
                )
            )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as database_session:
        yield database_session
    await engine.dispose()
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin_engine.dispose()


@pytest.mark.asyncio
async def test_user_repository_persists_hash_and_optional_names(
    session: AsyncSession,
) -> None:
    """Репозиторий не должен подменять hash plaintext-паролем."""
    repository = SQLAlchemyUserRepository(session)

    created = await repository.create(
        login='Alice',
        password_hash='stored-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    loaded = await repository.get_by_id(created.id)
    assert loaded is not None
    assert loaded.login == 'Alice'
    assert loaded.password_hash == 'stored-hash'
    assert loaded.first_name is None
    assert loaded.last_name is None


@pytest.mark.asyncio
async def test_user_repository_persists_missing_password(
    session: AsyncSession,
) -> None:
    """A social-only user must round-trip with a NULL password."""
    repository = SQLAlchemyUserRepository(session)

    created = await repository.create(
        login='yandex_123',
        password_hash=None,
        first_name='Alice',
        last_name=None,
        tz='Europe/Moscow',
    )

    loaded = await repository.get_by_id(created.id)
    assert loaded is not None
    assert loaded.password_hash is None


@pytest.mark.asyncio
async def test_user_lookup_is_case_sensitive(session: AsyncSession) -> None:
    """Незаявленное lower-casing логина должно обнаруживаться."""
    repository = SQLAlchemyUserRepository(session)
    await repository.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    assert await repository.get_by_login('Alice') is not None
    assert await repository.get_by_login('alice') is None


@pytest.mark.asyncio
async def test_user_id_lookup_by_login_does_not_load_user(
    session: AsyncSession,
) -> None:
    """Login workflow сначала должен получить только стабильный UUID."""
    repository = SQLAlchemyUserRepository(session)
    user = await repository.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    assert await repository.get_id_by_login('Alice') == user.id
    assert await repository.get_id_by_login('Missing') is None


@pytest.mark.asyncio
async def test_duplicate_login_is_translated_and_rolled_back(
    session: AsyncSession,
) -> None:
    """Raw IntegrityError не должен протечь из repository boundary."""
    repository = SQLAlchemyUserRepository(session)
    await repository.create(
        login='Alice',
        password_hash='first-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    with pytest.raises(LoginAlreadyTakenError):
        await repository.create(
            login='Alice',
            password_hash='second-hash',
            first_name=None,
            last_name=None,
            tz='Europe/Moscow',
        )

    loaded = await repository.get_by_login('Alice')
    assert loaded is not None
    assert loaded.password_hash == 'first-hash'


@pytest.mark.asyncio
async def test_user_update_changes_only_provided_fields(
    session: AsyncSession,
) -> None:
    """PATCH не должен очищать login или password при None."""
    repository = SQLAlchemyUserRepository(session)
    user = await repository.create(
        login='Alice',
        password_hash='first-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    updated = await repository.update(
        user,
        login=None,
        password_hash='second-hash',
    )

    assert updated.login == 'Alice'
    assert updated.password_hash == 'second-hash'


@pytest.mark.asyncio
async def test_login_history_is_scoped_and_newest_first(
    session: AsyncSession,
) -> None:
    """История другого user и неверная сортировка должны ломать тест."""
    users = SQLAlchemyUserRepository(session)
    histories = SQLAlchemyLoginHistoryRepository(session)
    first_user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    second_user = await users.create(
        login='Bob',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    old_time = datetime(2026, 8, 14, tzinfo=UTC)
    new_time = old_time + timedelta(days=1)
    session.add_all(
        [
            LoginHistory(
                user_id=first_user.id,
                logged_at=old_time,
                ip_address='127.0.0.1',
                user_agent='old',
            ),
            LoginHistory(
                user_id=second_user.id,
                logged_at=new_time,
                ip_address='127.0.0.2',
                user_agent='other-user',
            ),
            LoginHistory(
                user_id=first_user.id,
                logged_at=new_time,
                ip_address='127.0.0.3',
                user_agent='new',
            ),
        ]
    )
    await session.commit()

    result = await histories.list_for_user(first_user.id)

    assert [item.user_agent for item in result] == ['new', 'old']


@pytest.mark.asyncio
async def test_login_history_record_persists_client_context(
    session: AsyncSession,
) -> None:
    """Login должен сохранять IP и User-Agent, а не пустую запись."""
    users = SQLAlchemyUserRepository(session)
    histories = SQLAlchemyLoginHistoryRepository(session)
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    created = await histories.record(
        user_id=user.id,
        ip_address='203.0.113.10',
        user_agent='test-agent',
    )

    result = await histories.list_for_user(user.id)
    assert result == [created]
    assert created.ip_address == '203.0.113.10'
    assert created.user_agent == 'test-agent'
