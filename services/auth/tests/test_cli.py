import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

import cli


@pytest.mark.asyncio
async def test_create_superuser_builds_service_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI собирает сервис и закрывает PostgreSQL и Redis после работы."""
    session = object()
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=False)
    database = MagicMock()
    database.session.return_value = session_context
    database.close = AsyncMock()
    redis = MagicMock()
    redis.aclose = AsyncMock()
    user_id = uuid4()
    superuser_service = MagicMock()
    superuser_service.create_or_promote = AsyncMock(
        return_value=SimpleNamespace(id=user_id)
    )
    repository = object()
    authorization_cache = object()
    password_hasher = object()
    password_policy = object()
    postgres_settings = SimpleNamespace(
        dsn='postgres-dsn',
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=10.0,
        command_timeout_seconds=15.0,
        statement_timeout_ms=15_000,
    )
    database_type = MagicMock(return_value=database)

    monkeypatch.setattr(
        cli,
        'PostgresSettings',
        MagicMock(return_value=postgres_settings),
    )
    monkeypatch.setattr(
        cli,
        'PostgresDatabase',
        database_type,
    )
    monkeypatch.setattr(
        cli,
        'RedisSettings',
        MagicMock(
            return_value=SimpleNamespace(
                host='redis',
                port=6379,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            )
        ),
    )
    redis_type = MagicMock(return_value=redis)
    monkeypatch.setattr(cli, 'Redis', redis_type)
    monkeypatch.setattr(
        cli,
        'PasswordPolicySettings',
        MagicMock(return_value=SimpleNamespace(min_length=12, min_score=3)),
    )
    monkeypatch.setattr(
        cli,
        'AuthorizationCacheSettings',
        MagicMock(return_value=SimpleNamespace(ttl_seconds=300)),
    )
    repository_type = MagicMock(return_value=repository)
    cache_type = MagicMock(return_value=authorization_cache)
    hasher_type = MagicMock(return_value=password_hasher)
    policy_type = MagicMock(return_value=password_policy)
    service_type = MagicMock(return_value=superuser_service)
    monkeypatch.setattr(cli, 'SQLAlchemyUserRepository', repository_type)
    monkeypatch.setattr(cli, 'RedisAuthorizationCache', cache_type)
    monkeypatch.setattr(cli, 'PasswordHasher', hasher_type)
    monkeypatch.setattr(cli, 'PasswordPolicy', policy_type)
    monkeypatch.setattr(cli, 'SuperuserService', service_type)

    result = await cli._create_superuser('Root', 'correct horse', 'Europe/Moscow')

    assert result == str(user_id)
    database_type.assert_called_once_with(
        'postgres-dsn',
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=10.0,
        command_timeout_seconds=15.0,
        statement_timeout_ms=15_000,
    )
    repository_type.assert_called_once_with(session)
    cache_type.assert_called_once_with(redis, ttl_seconds=300)
    policy_type.assert_called_once_with(min_length=12, min_score=3)
    service_type.assert_called_once_with(
        users=repository,
        authorization_cache=authorization_cache,
        password_hasher=password_hasher,
        password_policy=password_policy,
    )
    superuser_service.create_or_promote.assert_awaited_once_with(
        login='Root',
        password='correct horse',
        tz='Europe/Moscow',
    )
    database.close.assert_awaited_once_with()
    redis.aclose.assert_awaited_once_with()


def test_main_creates_superuser_after_password_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI создаёт суперпользователя после совпадения двух паролей."""
    create_superuser = AsyncMock(return_value='user-id')
    monkeypatch.setattr(cli, '_create_superuser', create_superuser)
    monkeypatch.setattr(
        cli,
        'getpass',
        MagicMock(side_effect=['correct horse', 'correct horse']),
    )
    monkeypatch.setattr(
        sys, 'argv', ['cli.py', '--login', 'Root', '--tz', 'Europe/Moscow']
    )

    cli.main()

    create_superuser.assert_awaited_once_with('Root', 'correct horse', 'Europe/Moscow')
    assert capsys.readouterr().out == 'Superuser ready: user-id\n'


def test_main_rejects_different_passwords(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI завершает работу до записи, если пароли не совпадают."""
    create_superuser = AsyncMock()
    monkeypatch.setattr(cli, '_create_superuser', create_superuser)
    monkeypatch.setattr(
        cli,
        'getpass',
        MagicMock(side_effect=['correct horse', 'different horse']),
    )
    monkeypatch.setattr(
        sys, 'argv', ['cli.py', '--login', 'Root', '--tz', 'Europe/Moscow']
    )

    with pytest.raises(SystemExit, match='2'):
        cli.main()

    create_superuser.assert_not_awaited()
    assert 'passwords do not match' in capsys.readouterr().err


@pytest.mark.parametrize('arguments', [[], ['--tz', ''], ['--tz', 'Unknown/Zone']])
def test_main_requires_valid_timezone_before_prompt(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    """Invalid or missing timezone stops CLI before credentials or database use."""
    prompt = MagicMock()
    create = AsyncMock()
    monkeypatch.setattr(cli, 'getpass', prompt)
    monkeypatch.setattr(cli, '_create_superuser', create)
    monkeypatch.setattr(sys, 'argv', ['cli.py', '--login', 'Root', *arguments])
    with pytest.raises(SystemExit, match='2'):
        cli.main()
    prompt.assert_not_called()
    create.assert_not_awaited()
