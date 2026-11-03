"""Allow erasing campaign source content while preserving result history."""

import sqlalchemy as sa
from alembic import op

revision = "008_campaign_content_retention"
down_revision = "007_contact_privacy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("subject", "text"):
        op.alter_column("campaign_recipients", name, existing_type=sa.Text(), nullable=True)
    op.create_check_constraint(
        "content_pair", "campaign_recipients", "(subject IS NULL) = (text IS NULL)"
    )


def downgrade() -> None:
    op.execute("LOCK TABLE campaign_recipients IN ACCESS EXCLUSIVE MODE")
    redacted = op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM campaign_recipients WHERE subject IS NULL)")
    )
    if redacted:
        raise RuntimeError(
            "Cannot downgrade 008_campaign_content_retention: erased campaign content "
            "cannot be reconstructed. Restore a pre-008 backup to roll back."
        )
    op.drop_constraint(
        op.f("ck_campaign_recipients_content_pair"), "campaign_recipients", type_="check"
    )
    for name in ("subject", "text"):
        op.alter_column("campaign_recipients", name, existing_type=sa.Text(), nullable=False)
