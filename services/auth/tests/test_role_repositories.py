from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import PostgresSettings
from models import Base, Permission, User
from repositories.errors import (
    RoleAlreadyAssignedError,
    RoleAlreadyExistsError,
    RoleAssignmentNotFoundError,
    RoleNotFoundError,
    UserNotFoundError,
)
from repositories.roles import SQLAlchemyRoleRepository
from repositories.users import SQLAlchemyUserRepository


@pytest.fixture
async def session() -> AsyncGenerator[AsyncSession, None]:
    """Создаёт изолированную схему PostgreSQL со справочником прав."""
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
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as database_session:
        database_session.add_all(
            [
                Permission(code='roles:manage'),
                Permission(code='user_roles:manage'),
                Permission(code='films.early_access:read'),
            ]
        )
        await database_session.commit()
        yield database_session
    await engine.dispose()
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin_engine.dispose()


async def create_user(
    session: AsyncSession,
    *,
    login: str = 'Alice',
) -> User:
    """Создаёт пользователя в тестовой схеме PostgreSQL."""
    return await SQLAlchemyUserRepository(session).create(
        login=login,
        password_hash='stored-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )


@pytest.mark.asyncio
async def test_role_repository_crud_preserves_permission_codes(
    session: AsyncSession,
) -> None:
    """CRUD ролей сохраняет и возвращает коды разрешений."""
    repository = SQLAlchemyRoleRepository(session)

    role = await repository.create(
        name='subscriber',
        permission_codes={'films.early_access:read'},
    )
    assert role.name == 'subscriber'
    assert {item.code for item in role.permissions} == {'films.early_access:read'}

    updated = await repository.update(
        role.id,
        name='premium',
        permission_codes={'films.early_access:read', 'roles:manage'},
    )
    assert updated.name == 'premium'
    assert {item.code for item in updated.permissions} == {
        'films.early_access:read',
        'roles:manage',
    }
    assert [item.id for item in await repository.list()] == [role.id]

    await repository.delete(role.id)
    assert await repository.list() == []


@pytest.mark.asyncio
async def test_role_repository_translates_duplicate_and_missing_roles(
    session: AsyncSession,
) -> None:
    """Репозиторий переводит дубликаты и отсутствие ролей в ошибки."""
    repository = SQLAlchemyRoleRepository(session)
    role = await repository.create(name='admin', permission_codes=set())
    role_id = role.id

    with pytest.raises(RoleAlreadyExistsError):
        await repository.create(name='admin', permission_codes=set())
    with pytest.raises(RoleNotFoundError):
        await repository.update(
            uuid4(),
            name='missing',
            permission_codes=None,
        )
    await repository.delete(role_id)
    with pytest.raises(RoleNotFoundError):
        await repository.delete(role_id)


@pytest.mark.asyncio
async def test_role_assignment_changes_effective_user_permissions(
    session: AsyncSession,
) -> None:
    """Назначение и отзыв роли изменяют эффективные права пользователя."""
    repository = SQLAlchemyRoleRepository(session)
    user = await create_user(session)
    role = await repository.create(
        name='subscriber',
        permission_codes={'films.early_access:read'},
    )
    user_id = user.id
    role_id = role.id

    await repository.assign(user_id, role_id)
    assert await repository.get_permissions_for_user(user_id) == {
        'films.early_access:read'
    }
    with pytest.raises(RoleAlreadyAssignedError):
        await repository.assign(user_id, role_id)

    await repository.revoke(user_id, role_id)
    assert await repository.get_permissions_for_user(user_id) == set()
    with pytest.raises(RoleAssignmentNotFoundError):
        await repository.revoke(user_id, role_id)


@pytest.mark.asyncio
async def test_role_assignment_reports_missing_user_and_role(
    session: AsyncSession,
) -> None:
    """Назначение сообщает об отсутствующем пользователе или роли."""
    repository = SQLAlchemyRoleRepository(session)
    user = await create_user(session)
    role = await repository.create(name='subscriber', permission_codes=set())

    with pytest.raises(UserNotFoundError):
        await repository.assign(uuid4(), role.id)
    with pytest.raises(RoleNotFoundError):
        await repository.assign(user.id, uuid4())
