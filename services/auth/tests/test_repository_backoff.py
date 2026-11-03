from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from db.sql.errors import DatabaseUnavailableError
from models import LoginHistory, User
from repositories.login_history import SQLAlchemyLoginHistoryRepository
from repositories.users import SQLAlchemyUserRepository


def _connection_error() -> OperationalError:
    return OperationalError(
        'SELECT users',
        {},
        ConnectionRefusedError('temporarily unavailable'),
    )


@pytest.mark.asyncio
async def test_user_lookup_rolls_back_and_retries_connection_error() -> None:
    """Чтение пользователя повторяется после временного сбоя соединения."""
    expected = MagicMock(spec=User)
    query_result = MagicMock()
    query_result.scalar_one_or_none.return_value = expected
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(
        side_effect=[
            _connection_error(),
            query_result,
        ]
    )
    repository = SQLAlchemyUserRepository(session)

    with patch('asyncio.sleep', new_callable=AsyncMock):
        result = await repository.get_by_login('Alice')

    assert result is expected
    assert session.execute.await_count == 2
    session.rollback.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_user_get_by_id_rolls_back_and_retries_connection_error() -> None:
    """Поиск по ID повторяется после временного сбоя соединения."""
    expected = MagicMock(spec=User)
    session = AsyncMock(spec=AsyncSession)
    session.get = AsyncMock(
        side_effect=[
            _connection_error(),
            expected,
        ]
    )
    repository = SQLAlchemyUserRepository(session)

    with patch('asyncio.sleep', new_callable=AsyncMock):
        result = await repository.get_by_id(uuid4())

    assert result is expected
    assert session.get.await_count == 2
    session.rollback.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_login_history_read_rolls_back_and_retries_connection_error() -> None:
    """Чтение истории входов повторяется после временного сбоя."""
    expected = [MagicMock(spec=LoginHistory)]
    query_result = MagicMock()
    query_result.scalars.return_value.all.return_value = expected
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock(
        side_effect=[
            _connection_error(),
            query_result,
        ]
    )
    repository = SQLAlchemyLoginHistoryRepository(session)

    with patch('asyncio.sleep', new_callable=AsyncMock):
        result = await repository.list_for_user(uuid4())

    assert result == expected
    assert session.execute.await_count == 2
    session.rollback.assert_awaited_once_with()


@pytest.mark.asyncio
@patch('repositories.users.create_profile_event', new_callable=AsyncMock)
async def test_user_create_does_not_retry_uncertain_commit(
    create_event: AsyncMock,
) -> None:
    """Неопределённый результат COMMIT не приводит к повторной записи."""
    session = AsyncMock(spec=AsyncSession)
    session.commit = AsyncMock(side_effect=_connection_error())
    repository = SQLAlchemyUserRepository(session)

    with patch('asyncio.sleep', new_callable=AsyncMock) as sleep:
        with pytest.raises(DatabaseUnavailableError):
            await repository.create(
                login='Alice',
                password_hash='hash',
                first_name=None,
                last_name=None,
                tz='Europe/Moscow',
            )

    create_event.assert_awaited_once()
    assert session.commit.await_count == 1
    session.rollback.assert_awaited_once_with()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
@patch('repositories.users.create_profile_event', new_callable=AsyncMock)
async def test_user_create_translates_direct_connection_error(
    create_event: AsyncMock,
) -> None:
    """Прямой обрыв соединения переводится в доменную ошибку БД."""
    session = AsyncMock(spec=AsyncSession)
    session.commit = AsyncMock(
        side_effect=ConnectionRefusedError('database unavailable')
    )
    repository = SQLAlchemyUserRepository(session)

    with pytest.raises(DatabaseUnavailableError):
        await repository.create(
            login='Alice',
            password_hash='hash',
            first_name=None,
            last_name=None,
            tz='Europe/Moscow',
        )

    create_event.assert_awaited_once()
    assert session.commit.await_count == 1
    session.rollback.assert_awaited_once_with()
