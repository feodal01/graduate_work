"""Persist campaign intake and recipient outcomes without changing existing work."""

import sqlalchemy as sa
from alembic import op

revision = "004_campaigns"
down_revision = "003_delivery_send_class"
branch_labels = None
depends_on = None


def _send_class(*, external: bool) -> None:
    expression = (
        "CASE kind WHEN 'welcome' THEN 'transactional' "
        "WHEN 'likes' THEN 'service' WHEN 'new_films' THEN 'bulk' "
    )
    if external:
        expression += "WHEN 'external' THEN external_send_class "
    op.add_column(
        "deliveries",
        sa.Column(
            "send_class", sa.Text(), sa.Computed(expression + "END", persisted=True), nullable=False
        ),
    )
    op.create_index("ix_deliveries_send_class", "deliveries", ["send_class", "delivery_id"])


def upgrade() -> None:
    for name in ("external_send_class", "required_subscription", "terminal_reason"):
        op.add_column("deliveries", sa.Column(name, sa.Text(), nullable=True))
    op.drop_index("ix_deliveries_send_class", table_name="deliveries")
    op.drop_column("deliveries", "send_class")
    op.drop_constraint(op.f("ck_deliveries_kind"), "deliveries", type_="check")
    op.create_check_constraint(
        "kind", "deliveries", "kind IN ('welcome','likes','new_films','external')"
    )
    op.create_check_constraint(
        "external_policy",
        "deliveries",
        "(kind = 'external' AND external_send_class IS NOT NULL "
        "AND external_send_class IN ('transactional','service','bulk') AND run_id IS NULL) "
        "OR (kind <> 'external' AND external_send_class IS NULL AND required_subscription IS NULL)",
    )
    op.create_check_constraint(
        "required_subscription",
        "deliveries",
        "required_subscription IN ('likes','new_films','marketing')",
    )
    op.create_check_constraint(
        "terminal_reason",
        "deliveries",
        "terminal_reason IN ('profile_deleted','subscription_disabled','no_email',"
        "'smtp_permanent','attempts_exhausted','smtp_uncertain')",
    )
    _send_class(external=True)

    op.create_table(
        "campaigns",
        sa.Column("campaign_id", sa.Uuid(), primary_key=True),
        sa.Column("intake_status", sa.Text(), nullable=False, server_default="open"),
        sa.Column("expected_batches", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("intake_status IN ('open','closed')", name="intake_status"),
        sa.CheckConstraint("expected_batches > 0", name="expected_batches_positive"),
        sa.CheckConstraint(
            "(intake_status = 'open' AND closed_at IS NULL AND expected_batches IS NULL) OR "
            "(intake_status = 'closed' AND closed_at IS NOT NULL AND expected_batches IS NOT NULL)",
            name="closure_complete",
        ),
    )
    op.create_table(
        "campaign_batches",
        sa.Column(
            "campaign_id", sa.Uuid(), sa.ForeignKey("campaigns.campaign_id"), primary_key=True
        ),
        sa.Column("batch_id", sa.Uuid(), primary_key=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("item_count", sa.Integer(), nullable=False),
        sa.Column(
            "accepted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="hash_format"),
        sa.CheckConstraint("item_count BETWEEN 1 AND 100", name="item_count_range"),
    )
    op.create_table(
        "campaign_recipients",
        sa.Column("campaign_id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("profiles.user_id"), primary_key=True),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column(
            "delivery_id",
            sa.Uuid(),
            sa.ForeignKey("deliveries.delivery_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("template_id", sa.String(64), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("source_available_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("effective_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("skip_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["campaign_id", "batch_id"],
            ["campaign_batches.campaign_id", "campaign_batches.batch_id"],
        ),
        sa.CheckConstraint("channel = 'email'", name="channel"),
        sa.CheckConstraint(
            "skip_reason IN ('profile_deleted','subscription_disabled','no_email')",
            name="skip_reason",
        ),
        sa.CheckConstraint("(delivery_id IS NULL) = (skip_reason IS NOT NULL)", name="outcome"),
    )
    op.create_index(
        "ix_campaign_recipients_batch", "campaign_recipients", ["campaign_id", "batch_id"]
    )


def downgrade() -> None:
    # Refuse to silently destroy accepted external work when reverting the schema.
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM deliveries WHERE kind = 'external') THEN "
        "RAISE EXCEPTION 'Cannot downgrade campaigns while external deliveries exist'; "
        "END IF; END $$"
    )
    op.drop_table("campaign_recipients")
    op.drop_table("campaign_batches")
    op.drop_table("campaigns")
    op.drop_index("ix_deliveries_send_class", table_name="deliveries")
    op.drop_column("deliveries", "send_class")
    for name in ("kind", "external_policy", "required_subscription", "terminal_reason"):
        op.drop_constraint(op.f("ck_deliveries_" + name), "deliveries", type_="check")
    op.create_check_constraint("kind", "deliveries", "kind IN ('welcome','likes','new_films')")
    for name in ("external_send_class", "required_subscription", "terminal_reason"):
        op.drop_column("deliveries", name)
    _send_class(external=False)
