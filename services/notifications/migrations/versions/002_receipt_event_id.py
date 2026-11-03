"""Persist immutable source event IDs for bootstrap manifest verification."""

import sqlalchemy as sa
from alembic import op

revision = "002_receipt_event_id"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM bootstrap_receipts)")):
        raise RuntimeError(
            "Cannot migrate existing bootstrap receipts: original event_id is unavailable. "
            "Recover original source event IDs before migrating; fabricated IDs are invalid."
        )
    op.add_column("bootstrap_receipts", sa.Column("event_id", sa.Uuid(), nullable=False))
    op.create_index("ix_bootstrap_receipts_entity", "bootstrap_receipts", ["entity_id"])


def downgrade() -> None:
    op.drop_index("ix_bootstrap_receipts_entity", table_name="bootstrap_receipts")
    op.drop_column("bootstrap_receipts", "event_id")
