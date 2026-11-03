"""Create user authentication tables."""

# alembic.op members are injected at runtime via ModuleClsProxy.
# pylint: disable=no-member
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20260815_0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('login', sa.String(length=255), nullable=False),
        sa.Column('password', sa.String(length=255), nullable=False),
        sa.Column('first_name', sa.String(length=50), nullable=True),
        sa.Column('last_name', sa.String(length=50), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('login'),
    )
    op.create_table(
        'login_history',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column(
            'logged_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column('ip_address', sa.String(length=45), nullable=True),
        sa.Column('user_agent', sa.String(length=512), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_login_history_logged_at',
        'login_history',
        ['logged_at'],
        unique=False,
    )
    op.create_index(
        'ix_login_history_user_id',
        'login_history',
        ['user_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_login_history_user_id', table_name='login_history')
    op.drop_index('ix_login_history_logged_at', table_name='login_history')
    op.drop_table('login_history')
    op.drop_table('users')
