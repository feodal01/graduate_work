"""Freeze the selected subset while consuming all eligible film announcements."""

import sqlalchemy as sa
from alembic import op

revision = "009_film_digest_selection"
down_revision = "008_campaign_content_retention"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("schedule_runs", sa.Column("films_per_digest", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "films_per_digest",
        "schedule_runs",
        "films_per_digest IS NULL OR (kind = 'new_films' AND films_per_digest >= 1)",
    )
    # Preserve membership of issues frozen before this migration.
    op.add_column(
        "film_announcements",
        sa.Column("included_in_digest", sa.Boolean(), nullable=False, server_default=sa.true()),
    )


def downgrade() -> None:
    op.execute("LOCK TABLE film_announcements IN ACCESS EXCLUSIVE MODE")
    excluded = op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM film_announcements WHERE NOT included_in_digest)")
    )
    if excluded:
        raise RuntimeError(
            "Cannot downgrade 009_film_digest_selection: excluded announcements would "
            "enter existing issues. Restore a pre-009 backup to roll back."
        )
    op.drop_column("film_announcements", "included_in_digest")
    op.drop_constraint(op.f("ck_schedule_runs_films_per_digest"), "schedule_runs", type_="check")
    op.drop_column("schedule_runs", "films_per_digest")
