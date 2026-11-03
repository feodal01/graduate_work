"""Preserve unknown historical authors without rewriting batch identities."""

import sqlalchemy as sa
from alembic import op

revision = "005_campaign_author"
down_revision = "004_campaigns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("campaigns", sa.Column("created_by_user_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("campaigns", "created_by_user_id")
