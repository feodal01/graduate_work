from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest


def _migration() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / 'src/db/migrations/versions/20260829_0003_social_accounts.py'
    )
    spec = spec_from_file_location('social_accounts_migration', path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Could not load the social accounts migration')
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_downgrade_stops_before_data_loss_for_social_only_users(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nullable password must block every destructive downgrade step."""
    migration = _migration()
    monkeypatch.setattr(migration, 'op', MagicMock())
    migration.op.get_bind.return_value.execute.return_value.scalar_one.return_value = 1

    with pytest.raises(
        RuntimeError,
        match='social-only users exist',
    ):
        migration.downgrade()

    migration.op.drop_index.assert_not_called()
    migration.op.drop_table.assert_not_called()
    migration.op.alter_column.assert_not_called()


def test_downgrade_restores_required_password_when_all_users_have_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A safe database may remove the social schema in dependency order."""
    migration = _migration()
    monkeypatch.setattr(migration, 'op', MagicMock())
    migration.op.get_bind.return_value.execute.return_value.scalar_one.return_value = 0

    migration.downgrade()

    assert [item[0] for item in migration.op.method_calls] == [
        'get_bind',
        'drop_index',
        'drop_table',
        'alter_column',
    ]
    migration.op.drop_index.assert_called_once_with(
        'ix_social_accounts_user_id',
        table_name='social_accounts',
    )
    migration.op.drop_table.assert_called_once_with('social_accounts')
    alter_call = migration.op.alter_column.call_args
    assert alter_call.args == ('users', 'password')
    assert isinstance(alter_call.kwargs['existing_type'], migration.sa.String)
    assert alter_call.kwargs['existing_type'].length == 255
    assert alter_call.kwargs['nullable'] is False
