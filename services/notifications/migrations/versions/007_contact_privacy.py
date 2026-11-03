"""Remove local contacts and support terminal delivery retention timestamps."""

import sqlalchemy as sa
from alembic import op

revision = "007_contact_privacy"
down_revision = "006_configurable_batch_size"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("email", "first_name", "last_name", "locale"):
        op.drop_column("profiles", name)
    op.add_column("deliveries", sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(
        "UPDATE deliveries SET terminal_at = COALESCE(sent_at, now()) "
        "WHERE status IN ('sent','cancelled','skipped','failed')"
    )
    op.drop_constraint(op.f("ck_deliveries_snapshot_complete"), "deliveries", type_="check")
    op.create_check_constraint(
        "snapshot_complete",
        "deliveries",
        "(prepared_at IS NULL AND payload IS NULL AND message_id IS NULL) OR "
        "(prepared_at IS NOT NULL AND payload IS NOT NULL AND message_id IS NOT NULL) OR "
        "(status IN ('sent','cancelled','skipped','failed') AND payload IS NULL "
        "AND prepared_at IS NOT NULL AND message_id IS NOT NULL)",
    )
    op.drop_constraint(op.f("ck_deliveries_terminal_reason"), "deliveries", type_="check")
    op.create_check_constraint(
        "terminal_reason",
        "deliveries",
        "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
        "'contact_unavailable','invalid_timezone','smtp_permanent','attempts_exhausted','smtp_uncertain')",
    )
    op.drop_constraint(
        op.f("ck_campaign_recipients_skip_reason"), "campaign_recipients", type_="check"
    )
    op.create_check_constraint(
        "skip_reason",
        "campaign_recipients",
        "skip_reason IN ('profile_deleted','subscription_disabled','no_email','invalid_timezone')",
    )


def downgrade() -> None:
    # Keep preflight and DDL in one transaction; no concurrent writer may introduce
    # a new incompatible row between the check and reinstating the old checks.
    op.execute("LOCK TABLE deliveries, campaign_recipients, profiles IN ACCESS EXCLUSIVE MODE")
    incompatible = op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM deliveries WHERE "
            "terminal_reason IN ('contact_unavailable','invalid_timezone') OR "
            "(payload IS NULL AND (prepared_at IS NOT NULL OR message_id IS NOT NULL))) "
            "OR EXISTS (SELECT 1 FROM campaign_recipients "
            "WHERE skip_reason = 'invalid_timezone')"
        )
    )
    if incompatible:
        raise RuntimeError(
            "Cannot downgrade 007_contact_privacy: retained delivery history or redacted "
            "snapshots cannot be represented by schema 006. Restore a pre-007 backup "
            "to roll back; no contact data or message payloads can be reconstructed."
        )
    # Adding nullable columns restores only the old shape, never erased contacts.
    op.drop_constraint(op.f("ck_deliveries_snapshot_complete"), "deliveries", type_="check")
    op.create_check_constraint(
        "snapshot_complete",
        "deliveries",
        "(prepared_at IS NULL AND payload IS NULL AND message_id IS NULL) OR "
        "(prepared_at IS NOT NULL AND payload IS NOT NULL AND message_id IS NOT NULL)",
    )
    op.drop_constraint(
        op.f("ck_campaign_recipients_skip_reason"), "campaign_recipients", type_="check"
    )
    op.create_check_constraint(
        "skip_reason",
        "campaign_recipients",
        "skip_reason IN ('profile_deleted','subscription_disabled','no_email')",
    )
    op.drop_constraint(op.f("ck_deliveries_terminal_reason"), "deliveries", type_="check")
    op.create_check_constraint(
        "terminal_reason",
        "deliveries",
        "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
        "'smtp_permanent','attempts_exhausted','smtp_uncertain')",
    )
    op.drop_column("deliveries", "terminal_at")
    for name in ("email", "first_name", "last_name", "locale"):
        op.add_column("profiles", sa.Column(name, sa.Text(), nullable=True))
