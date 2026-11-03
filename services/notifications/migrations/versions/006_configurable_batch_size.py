"""Keep batch positivity in storage; the configured API limit belongs to intake."""

from alembic import op

revision = "006_configurable_batch_size"
down_revision = "005_campaign_author"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        op.f("ck_campaign_batches_item_count_range"), "campaign_batches", type_="check"
    )
    op.create_check_constraint("item_count_range", "campaign_batches", "item_count >= 1")


def downgrade() -> None:
    # PostgreSQL refuses this downgrade if larger historical batches exist.
    # The migration transaction then restores the previous constraint and revision.
    op.drop_constraint(
        op.f("ck_campaign_batches_item_count_range"), "campaign_batches", type_="check"
    )
    op.create_check_constraint(
        "item_count_range", "campaign_batches", "item_count BETWEEN 1 AND 100"
    )
