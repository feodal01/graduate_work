"""Add social accounts and allow users without a local password."""

# alembic.op members are injected at runtime via ModuleClsProxy.
# pylint: disable=no-member
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20260829_0003'
down_revision: str | None = '20260815_0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        'users',
        'password',
        existing_type=sa.String(length=255),
        nullable=True,
    )
    op.create_table(
        'social_accounts',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False),
        sa.Column('provider_user_id', sa.String(length=255), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['user_id'],
            ['users.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'provider',
            'provider_user_id',
            name='uq_social_accounts_provider_identity',
        ),
        sa.UniqueConstraint(
            'user_id',
            'provider',
            name='uq_social_accounts_user_provider',
        ),
    )
    op.create_index(
        'ix_social_accounts_user_id',
        'social_accounts',
        ['user_id'],
        unique=False,
    )


def downgrade() -> None:
    connection = op.get_bind()
    missing_passwords = connection.execute(
        sa.text('SELECT count(*) FROM users WHERE password IS NULL')
    ).scalar_one()
    if missing_passwords:
        raise RuntimeError(
            'Cannot restore a required password while social-only users exist'
        )

    op.drop_index('ix_social_accounts_user_id', table_name='social_accounts')
    op.drop_table('social_accounts')
    op.alter_column(
        'users',
        'password',
        existing_type=sa.String(length=255),
        nullable=False,
    )
