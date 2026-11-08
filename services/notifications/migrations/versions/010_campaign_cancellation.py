"""Record sender-cancelled campaigns and the cancelled_by_sender delivery outcome."""

import sqlalchemy as sa
from alembic import op

revision = "010_campaign_cancellation"
down_revision = "009_film_digest_selection"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cancelled_campaigns",
        sa.Column("campaign_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "cancelled_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.drop_constraint(op.f("ck_deliveries_terminal_reason"), "deliveries", type_="check")
    op.create_check_constraint(
        "terminal_reason",
        "deliveries",
        "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
        "'contact_unavailable','invalid_timezone','smtp_permanent','attempts_exhausted',"
        "'smtp_uncertain','cancelled_by_sender')",
    )


def downgrade() -> None:
    op.execute("LOCK TABLE deliveries, cancelled_campaigns IN ACCESS EXCLUSIVE MODE")
    excluded = op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM deliveries "
            "WHERE terminal_reason = 'cancelled_by_sender') "
            "OR EXISTS (SELECT 1 FROM cancelled_campaigns)"
        )
    )
    if excluded:
        raise RuntimeError(
            "Cannot downgrade 010_campaign_cancellation: cancelled campaigns would be "
            "delivered again. Restore a pre-010 backup to roll back."
        )
    op.drop_constraint(op.f("ck_deliveries_terminal_reason"), "deliveries", type_="check")
    op.create_check_constraint(
        "terminal_reason",
        "deliveries",
        "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
        "'contact_unavailable','invalid_timezone','smtp_permanent','attempts_exhausted',"
        "'smtp_uncertain')",
    )
    op.drop_table("cancelled_campaigns")
