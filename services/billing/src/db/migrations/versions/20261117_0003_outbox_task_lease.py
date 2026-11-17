"""Lease outbox tasks instead of holding a row lock during delivery.

The outbox handler used to lock the task row, call Auth or Notifications over
the network and only then commit. Every handler kept a database connection in
an open transaction for the whole network wait, so slow recipients could
exhaust the pool. A handler now claims the task in a short transaction by
writing a lease, delivers without a connection, and records the outcome in a
second short transaction that checks the lease owner.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20261117_0003'
down_revision: str | None = '20261109_0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('outbox_tasks', sa.Column('lease_token', sa.Uuid(), nullable=True))
    op.add_column(
        'outbox_tasks',
        sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        'ck_outbox_tasks_lease',
        'outbox_tasks',
        '(lease_token IS NULL) = (lease_until IS NULL)',
    )


def downgrade() -> None:
    op.drop_constraint('ck_outbox_tasks_lease', 'outbox_tasks', type_='check')
    op.drop_column('outbox_tasks', 'lease_until')
    op.drop_column('outbox_tasks', 'lease_token')
