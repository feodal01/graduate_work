from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import Index, Table, UniqueConstraint

from models import (
    Base,
    LoginHistory,
    Permission,
    Role,
    User,
    role_permissions,
    user_roles,
)


def test_auth_tables_are_registered() -> None:
    """Удаление любой auth-таблицы должно обнаруживаться через metadata."""
    assert {
        'users',
        'login_history',
        'roles',
        'permissions',
        'role_permissions',
        'user_roles',
    } <= set(Base.metadata.tables)


def test_rbac_join_rows_are_deleted_with_role() -> None:
    """Удалённая роль не должна оставлять назначения и permissions."""
    for table in (role_permissions, user_roles):
        role_foreign_key = next(
            foreign_key
            for foreign_key in table.c.role_id.foreign_keys
            if foreign_key.target_fullname == 'roles.id'
        )
        assert role_foreign_key.ondelete == 'CASCADE'


def test_role_accepts_stable_permission_codes() -> None:
    """Роль хранит стабильные строковые коды разрешений."""
    permission = Permission(code='films.early_access:read')
    role = Role(name='subscriber', permissions=[permission])

    assert role.name == 'subscriber'
    assert [item.code for item in role.permissions] == ['films.early_access:read']


def test_user_maps_hash_to_existing_password_column() -> None:
    """ORM принимает только hash, сохраняя совместимое имя SQL-колонки."""
    assert User.password_hash.property.columns[0].name == 'password'
    assert not hasattr(User, 'password')


def test_social_account_table_and_constraints_are_registered() -> None:
    """Social login must keep one provider identity per local user."""
    assert 'social_accounts' in Base.metadata.tables
    table = Base.metadata.tables['social_accounts']

    foreign_key = next(iter(table.c.user_id.foreign_keys))
    assert foreign_key.target_fullname == 'users.id'
    assert foreign_key.ondelete == 'CASCADE'
    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ('provider', 'provider_user_id') in unique_columns
    assert ('user_id', 'provider') in unique_columns


def test_social_user_accepts_missing_password() -> None:
    """A provider-only account must not need a fake local password."""
    user = User(login='user@yandex.ru', password_hash=None)

    assert user.password_hash is None
    assert User.__table__.c.password.nullable is True


def test_user_can_be_created_with_optional_names() -> None:
    """Signup по диаграмме не должен требовать имена пользователя."""
    user = User(
        id=uuid4(),
        login='Alice',
        password_hash='stored-hash',
        first_name=None,
        last_name=None,
    )

    assert user.login == 'Alice'
    assert user.password_hash == 'stored-hash'
    assert user.first_name is None
    assert user.last_name is None


def test_login_history_references_user_without_delete_cascade() -> None:
    """История привязана к user, но удаление пользователя не проектируется."""
    foreign_keys = LoginHistory.__table__.c.user_id.foreign_keys

    assert len(foreign_keys) == 1
    foreign_key = next(iter(foreign_keys))
    assert foreign_key.target_fullname == 'users.id'
    assert foreign_key.ondelete is None


def test_login_history_uses_hash_partitioning_by_user() -> None:
    """Изменение ключа или числа PK-колонок должно ломать схему секций."""
    table = LoginHistory.__table__
    assert isinstance(table, Table)

    assert tuple(column.name for column in table.primary_key.columns) == (
        'user_id',
        'id',
    )
    assert table.dialect_options['postgresql']['partition_by'] == ('HASH (user_id)')


def test_login_history_index_matches_user_history_query() -> None:
    """Индекс должен фильтровать user и выдавать новые входы первыми."""
    table = LoginHistory.__table__
    assert isinstance(table, Table)
    indexes = {
        str(index.name): index for index in table.indexes if isinstance(index, Index)
    }

    assert set(indexes) == {'ix_login_history_user_id_logged_at'}
    expressions = tuple(
        str(expression)
        for expression in indexes['ix_login_history_user_id_logged_at'].expressions
    )
    assert expressions == (
        'login_history.user_id',
        'login_history.logged_at DESC',
    )


def test_model_timestamps_are_timezone_aware() -> None:
    """Наивный UTC timestamp нельзя вернуть как локальное время."""
    user_default = User.__table__.c.created_at.default
    history_default = LoginHistory.__table__.c.logged_at.default

    assert user_default is not None
    assert history_default is not None
    user_created_at = user_default.arg(None)
    history_logged_at = history_default.arg(None)
    assert isinstance(user_created_at, datetime)
    assert isinstance(history_logged_at, datetime)
    assert user_created_at.tzinfo is UTC
    assert history_logged_at.tzinfo is UTC
