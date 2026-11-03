"""Add nullable delivery contacts, user versions and durable notification events."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '20261010_0005'
down_revision: str | None = '20260830_0004'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('users', sa.Column('email', sa.String(255), nullable=True))
    op.add_column('users', sa.Column('locale', sa.String(64), nullable=True))
    op.add_column('users', sa.Column('tz', sa.String(64), nullable=True))
    op.add_column(
        'users',
        sa.Column('user_version', sa.BigInteger(), server_default='1', nullable=False),
    )
    op.create_check_constraint('ck_users_user_version', 'users', 'user_version >= 1')
    op.create_table(
        'notification_outbox',
        sa.Column('event_id', sa.Uuid(), primary_key=True),
        sa.Column('user_id', sa.Uuid(), nullable=False),
        sa.Column('user_version', sa.BigInteger(), nullable=False),
        sa.Column('event_type', sa.String(32), nullable=False),
        sa.Column('payload', postgresql.JSONB(), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('load_id', sa.String(255), nullable=True),
        sa.CheckConstraint(
            'user_version >= 1', name='ck_notification_outbox_user_version'
        ),
        sa.CheckConstraint(
            "event_type IN ('user.registered', 'user.profile_updated', 'user.deleted')",
            name='ck_notification_outbox_event_type',
        ),
    )
    op.create_index(
        'uq_notification_outbox_load_user',
        'notification_outbox',
        ['load_id', 'user_id'],
        unique=True,
        postgresql_where=sa.text('load_id IS NOT NULL'),
    )
    op.create_index(
        'ix_notification_outbox_unpublished',
        'notification_outbox',
        ['created_at', 'event_id'],
        postgresql_where=sa.text('published_at IS NULL'),
    )
    op.create_table(
        'notification_load_markers',
        sa.Column('load_id', sa.String(255), primary_key=True),
        sa.Column('user_id', sa.Uuid(), primary_key=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.execute(
        sa.text("""
        CREATE FUNCTION protect_notification_event() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.event_id, NEW.user_id, NEW.user_version, NEW.event_type,
                NEW.payload, NEW.created_at, NEW.load_id)
                IS DISTINCT FROM
               (OLD.event_id, OLD.user_id, OLD.user_version, OLD.event_type,
                OLD.payload, OLD.created_at, OLD.load_id)
            THEN
                RAISE EXCEPTION 'Notification event snapshot is immutable';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    )
    op.execute(
        sa.text("""
        CREATE TRIGGER notification_event_immutable
        BEFORE UPDATE ON notification_outbox
        FOR EACH ROW EXECUTE FUNCTION protect_notification_event()
    """)
    )


def downgrade() -> None:
    op.drop_table('notification_load_markers')
    op.drop_table('notification_outbox')
    op.execute(sa.text('DROP FUNCTION protect_notification_event()'))
    op.drop_constraint('ck_users_user_version', 'users', type_='check')
    op.drop_column('users', 'user_version')
    op.drop_column('users', 'tz')
    op.drop_column('users', 'locale')
    op.drop_column('users', 'email')
