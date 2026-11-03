from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest


def _migration() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / 'src/db/migrations/versions/'
        / '20260830_0004_partition_login_history.py'
    )
    assert path.is_file(), 'Login history partition migration is missing'
    spec = spec_from_file_location('login_history_partition_migration', path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Could not load the login history migration')
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _executed_sql(migration: ModuleType) -> list[str]:
    return [str(call.args[0]) for call in migration.op.execute.call_args_list]


def test_upgrade_builds_sixteen_hash_partitions_and_copies_all_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ошибка числа секций, remainder или списка колонок должна быть видна."""
    migration = _migration()
    monkeypatch.setattr(migration, 'op', MagicMock())

    migration.upgrade()

    statements = _executed_sql(migration)
    assert 'PARTITION BY HASH (user_id)' in statements[1]
    partition_statements = [
        statement
        for statement in statements
        if 'PARTITION OF login_history_partitioned' in statement
    ]
    assert len(partition_statements) == 16
    assert {
        int(statement.rsplit('REMAINDER ', 1)[1].split(')', 1)[0])
        for statement in partition_statements
    } == set(range(16))
    assert all('MODULUS 16' in statement for statement in partition_statements)
    copy_statement = next(
        statement
        for statement in statements
        if statement.lstrip().startswith('INSERT INTO')
    )
    assert '(id, user_id, logged_at, ip_address, user_agent)' in copy_statement
    assert 'SELECT id, user_id, logged_at, ip_address, user_agent' in copy_statement
    row_count_check = next(
        statement
        for statement in statements
        if 'row count mismatch during partitioning' in statement
    )
    assert 'SELECT count(*) FROM login_history' in row_count_check
    assert 'SELECT count(*) FROM login_history_partitioned' in row_count_check
    assert statements.index(row_count_check) < len(statements) - 3
    assert statements[-3:] == [
        'ALTER TABLE login_history RENAME TO login_history_unpartitioned',
        'ALTER TABLE login_history_partitioned RENAME TO login_history',
        'DROP TABLE login_history_unpartitioned',
    ]
    migration.op.get_bind.assert_not_called()


def test_downgrade_checks_duplicate_ids_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Повторяющийся id нельзя переносить в обычный первичный ключ."""
    migration = _migration()
    monkeypatch.setattr(migration, 'op', MagicMock())

    migration.downgrade()

    statements = _executed_sql(migration)
    duplicate_check = statements[1]
    assert 'duplicate id values' in duplicate_check
    assert 'GROUP BY id HAVING count(*) > 1' in duplicate_check
    assert statements[2].startswith('CREATE TABLE login_history_unpartitioned')
    migration.op.get_bind.assert_not_called()


def test_downgrade_restores_plain_table_and_original_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Downgrade должен вернуть исходный ключ, индексы и все строки."""
    migration = _migration()
    monkeypatch.setattr(migration, 'op', MagicMock())

    migration.downgrade()

    statements = _executed_sql(migration)
    create_table = next(
        statement
        for statement in statements
        if statement.lstrip().startswith('CREATE TABLE')
    )
    assert 'PARTITION BY' not in create_table
    assert 'PRIMARY KEY (id)' in create_table
    copy_statement = next(
        statement
        for statement in statements
        if statement.lstrip().startswith('INSERT INTO')
    )
    assert '(id, user_id, logged_at, ip_address, user_agent)' in copy_statement
    assert 'SELECT id, user_id, logged_at, ip_address, user_agent' in copy_statement
    row_count_check = next(
        statement
        for statement in statements
        if 'row count mismatch during downgrade' in statement
    )
    assert 'SELECT count(*) FROM login_history' in row_count_check
    assert 'SELECT count(*) FROM login_history_unpartitioned' in row_count_check
    assert statements.index(row_count_check) < len(statements) - 7
    assert statements[-7:] == [
        'ALTER TABLE login_history RENAME TO login_history_partitioned',
        'ALTER TABLE login_history_unpartitioned RENAME TO login_history',
        'DROP TABLE login_history_partitioned',
        'ALTER INDEX ix_login_history_unpartitioned_user_id '
        'RENAME TO ix_login_history_user_id',
        'ALTER INDEX ix_login_history_unpartitioned_logged_at '
        'RENAME TO ix_login_history_logged_at',
        'ALTER TABLE login_history RENAME CONSTRAINT '
        'pk_login_history_unpartitioned TO login_history_pkey',
        'ALTER TABLE login_history RENAME CONSTRAINT '
        'fk_login_history_unpartitioned_user_id TO '
        'login_history_user_id_fkey',
    ]
    migration.op.get_bind.assert_not_called()
