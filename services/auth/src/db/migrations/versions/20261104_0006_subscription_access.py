"""Add user_roles.expires_at and user_subscription_access_state for Billing (T-14)."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20261104_0006'
down_revision: str | None = '20261010_0005'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        'user_roles',
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        'user_subscription_access_state',
        sa.Column(
            'user_id',
            sa.Uuid(),
            sa.ForeignKey('users.id', ondelete='CASCADE'),
            primary_key=True,
            nullable=False,
        ),
        sa.Column('access_version', sa.BigInteger(), nullable=False),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            'access_version >= 1',
            name='ck_user_subscription_access_state_version',
        ),
    )


def downgrade() -> None:
    op.drop_table('user_subscription_access_state')
    op.drop_column('user_roles', 'expires_at')
