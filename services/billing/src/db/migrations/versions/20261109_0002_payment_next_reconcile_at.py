"""Separate background reconcile backoff from the user check throttle.

Background backoff used to push `payments.last_checked_at` into the future.
The user-facing 5-second throttle reads the same column, so after provider
errors `/continue` skipped the YooKassa check for up to ~5 minutes and could
send the user to an already cancelled payment page. Backoff now lives in its
own column.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20261109_0002'
down_revision: str | None = '20261103_0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        'payments',
        sa.Column('next_reconcile_at', sa.DateTime(timezone=True), nullable=True),
    )
    # Перенести отложенную сверку: будущие значения были backoff-ом.
    op.execute(
        'UPDATE payments SET next_reconcile_at = last_checked_at, '
        'last_checked_at = now() WHERE last_checked_at > now()'
    )


def downgrade() -> None:
    op.execute(
        'UPDATE payments SET last_checked_at = next_reconcile_at '
        'WHERE next_reconcile_at > now()'
    )
    op.drop_column('payments', 'next_reconcile_at')
