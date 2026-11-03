"""Add roles, permissions, assignments, and superuser support."""

# alembic.op members are injected at runtime via ModuleClsProxy.
# pylint: disable=no-member
from collections.abc import Sequence
from uuid import UUID

import sqlalchemy as sa
from alembic import op

revision: str = '20260815_0002'
down_revision: str | None = '20260815_0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ADMIN_ROLE_ID = UUID('00000000-0000-0000-0000-000000000001')
SUBSCRIBER_ROLE_ID = UUID('00000000-0000-0000-0000-000000000002')


def upgrade() -> None:
    op.add_column(
        'users',
        sa.Column(
            'is_superuser',
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    permissions = op.create_table(
        'permissions',
        sa.Column('code', sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint('code'),
    )
    roles = op.create_table(
        'roles',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    role_permissions = op.create_table(
        'role_permissions',
        sa.Column('role_id', sa.Uuid(), nullable=False),
        sa.Column('permission_code', sa.String(length=100), nullable=False),
        sa.ForeignKeyConstraint(
            ['permission_code'],
            ['permissions.code'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('role_id', 'permission_code'),
    )
    op.create_table(
        'user_roles',
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('role_id', sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('user_id', 'role_id'),
    )

    op.bulk_insert(
        permissions,
        [
            {'code': 'roles:manage'},
            {'code': 'user_roles:manage'},
            {'code': 'films.early_access:read'},
        ],
    )
    op.bulk_insert(
        roles,
        [
            {'id': ADMIN_ROLE_ID, 'name': 'admin'},
            {'id': SUBSCRIBER_ROLE_ID, 'name': 'subscriber'},
        ],
    )
    op.bulk_insert(
        role_permissions,
        [
            {'role_id': ADMIN_ROLE_ID, 'permission_code': 'roles:manage'},
            {
                'role_id': ADMIN_ROLE_ID,
                'permission_code': 'user_roles:manage',
            },
            {
                'role_id': SUBSCRIBER_ROLE_ID,
                'permission_code': 'films.early_access:read',
            },
        ],
    )


def downgrade() -> None:
    op.drop_table('user_roles')
    op.drop_table('role_permissions')
    op.drop_table('roles')
    op.drop_table('permissions')
    op.drop_column('users', 'is_superuser')
