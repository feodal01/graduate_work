"""Derive the immutable dispatch class from the delivery kind."""

import sqlalchemy as sa
from alembic import op

revision = "003_delivery_send_class"
down_revision = "002_receipt_event_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "deliveries",
        sa.Column(
            "send_class",
            sa.Text(),
            sa.Computed(
                "CASE kind WHEN 'welcome' THEN 'transactional' "
                "WHEN 'likes' THEN 'service' WHEN 'new_films' THEN 'bulk' END",
                persisted=True,
            ),
            nullable=False,
        ),
    )
    op.create_index("ix_deliveries_send_class", "deliveries", ["send_class", "delivery_id"])


def downgrade() -> None:
    op.drop_index("ix_deliveries_send_class", table_name="deliveries")
    op.drop_column("deliveries", "send_class")
