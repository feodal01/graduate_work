from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.health import HealthCheck
from db.sql.base import AbstractDatabase
from db.sql.postgres import PostgresDatabase, get_session
from db.sql.postgres_health import PostgresHealthCheck
from models import Base, User

DSN = 'postgresql+asyncpg://app:secret@localhost:5432/auth'


def test_postgres_health_check_implements_health_check_contract() -> None:
    """PostgresHealthCheck реализует общий контракт проверки состояния."""
    assert issubclass(PostgresHealthCheck, HealthCheck)


@pytest.fixture
def engine() -> MagicMock:
    """Создаёт тестовый объект SQLAlchemy engine."""
    return MagicMock()


@pytest.fixture
def sessionmaker() -> MagicMock:
    """Создаёт тестовую фабрику SQLAlchemy-сессий."""
    return MagicMock()


@pytest.fixture
def database(engine: MagicMock, sessionmaker: MagicMock) -> PostgresDatabase:
    """Создаёт PostgresDatabase с тестовыми engine и sessionmaker."""
    with (
        patch('db.sql.postgres.create_async_engine', return_value=engine),
        patch('db.sql.postgres.async_sessionmaker', return_value=sessionmaker),
    ):
        return PostgresDatabase(DSN)


@pytest.fixture
def health_check(engine: MagicMock) -> PostgresHealthCheck:
    """Создаёт проверку PostgreSQL с тестовым engine."""
    return PostgresHealthCheck(engine)


class TestAbstractDatabase:
    def test_cannot_be_instantiated(self) -> None:
        """Абстрактную базу данных нельзя создать напрямую."""
        assert AbstractDatabase.__abstractmethods__ == frozenset({'close', 'session'})
        with pytest.raises(TypeError):
            type.__call__(AbstractDatabase)

    def test_postgres_is_abstract_database(
        self,
        database: PostgresDatabase,
    ) -> None:
        """PostgresDatabase реализует контракт абстрактной базы."""
        assert isinstance(database, AbstractDatabase)


class TestModelRegistration:
    def test_user_table_is_registered_on_metadata(self) -> None:
        """Таблица пользователей зарегистрирована в metadata."""
        assert User.__tablename__ in Base.metadata.tables


class TestPostgresDatabase:
    def test_does_not_implement_health_check(self, database: PostgresDatabase) -> None:
        """PostgresDatabase не смешивает соединение и health check."""
        assert not hasattr(database, 'ping')

    def test_creates_engine_and_sessionmaker(
        self, engine: MagicMock, sessionmaker: MagicMock
    ) -> None:
        """PostgresDatabase создаёт engine и фабрику сессий."""
        with (
            patch(
                'db.sql.postgres.create_async_engine', return_value=engine
            ) as create_engine,
            patch(
                'db.sql.postgres.async_sessionmaker', return_value=sessionmaker
            ) as make_sessions,
        ):
            PostgresDatabase(
                DSN,
                connect_timeout_seconds=5.0,
                pool_timeout_seconds=10.0,
                command_timeout_seconds=15.0,
                statement_timeout_ms=15_000,
            )

        create_engine.assert_called_once_with(
            DSN,
            echo=False,
            future=True,
            pool_timeout=10.0,
            connect_args={
                'timeout': 5.0,
                'command_timeout': 15.0,
                'server_settings': {'statement_timeout': '15000'},
            },
        )
        make_sessions.assert_called_once_with(
            engine, class_=AsyncSession, expire_on_commit=False
        )

    @pytest.mark.asyncio
    async def test_session_yields_session_from_sessionmaker(
        self, database: PostgresDatabase, sessionmaker: MagicMock
    ) -> None:
        """Контекст базы возвращает сессию созданной фабрики."""
        expected = object()
        session_cm = AsyncMock()
        session_cm.__aenter__.return_value = expected
        session_cm.__aexit__.return_value = False
        sessionmaker.return_value = session_cm

        async with database.session() as session:
            yielded = session

        assert yielded is expected
        session_cm.__aenter__.assert_awaited_once()
        session_cm.__aexit__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_disposes_engine(
        self, database: PostgresDatabase, engine: MagicMock
    ) -> None:
        """Закрытие базы освобождает SQLAlchemy engine."""
        engine.dispose = AsyncMock()

        await database.close()

        engine.dispose.assert_awaited_once_with()


class TestPostgresHealthCheck:
    @pytest.mark.asyncio
    async def test_ping_returns_true_when_query_succeeds(
        self, health_check: PostgresHealthCheck, engine: MagicMock
    ) -> None:
        """Успешный SELECT возвращает доступность PostgreSQL."""
        conn = AsyncMock()
        engine.connect.return_value.__aenter__.return_value = conn
        engine.connect.return_value.__aexit__.return_value = False

        result = await health_check.ping()

        assert result is True
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ping_returns_false_on_sqlalchemy_error(
        self, health_check: PostgresHealthCheck, engine: MagicMock
    ) -> None:
        """Ошибка SQLAlchemy возвращает недоступность PostgreSQL."""
        engine.connect.side_effect = SQLAlchemyError('unreachable')

        result = await health_check.ping()

        assert result is False

    @pytest.mark.asyncio
    async def test_ping_returns_false_on_connection_error(
        self, health_check: PostgresHealthCheck, engine: MagicMock
    ) -> None:
        """Ошибка соединения возвращает недоступность PostgreSQL."""
        engine.connect.side_effect = ConnectionRefusedError('unreachable')

        result = await health_check.ping()

        assert result is False


class TestGetSession:
    @pytest.mark.asyncio
    async def test_yields_session_from_app_state_database(self) -> None:
        """Зависимость выдаёт сессию базы из состояния приложения."""
        expected = cast(AsyncSession, MagicMock(spec=AsyncSession))

        class FakeDatabase(AbstractDatabase):
            def session(self) -> AbstractAsyncContextManager[AsyncSession]:
                @asynccontextmanager
                async def _session() -> AsyncGenerator[AsyncSession, None]:
                    yield expected

                return _session()

            async def close(self) -> None:
                raise NotImplementedError

        request = MagicMock()
        request.app.state.database = FakeDatabase()

        sessions = [session async for session in get_session(request)]

        assert sessions == [expected]
