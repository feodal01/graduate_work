"""Partition login history by user identifier."""

# alembic.op members are injected at runtime via ModuleClsProxy.
# pylint: disable=no-member
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '20260830_0004'
down_revision: str | None = '20260829_0003'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PARTITION_COUNT = 16


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def upgrade() -> None:
    _execute('LOCK TABLE login_history IN ACCESS EXCLUSIVE MODE')
    _execute(
        'CREATE TABLE login_history_partitioned ('
        'user_id UUID NOT NULL, '
        'id UUID NOT NULL, '
        'logged_at TIMESTAMPTZ NOT NULL DEFAULT now(), '
        'ip_address VARCHAR(45), '
        'user_agent VARCHAR(512), '
        'CONSTRAINT pk_login_history_partitioned '
        'PRIMARY KEY (user_id, id), '
        'CONSTRAINT fk_login_history_partitioned_user_id '
        'FOREIGN KEY (user_id) REFERENCES users(id)'
        ') PARTITION BY HASH (user_id)'
    )
    for remainder in range(PARTITION_COUNT):
        _execute(
            f'CREATE TABLE login_history_p{remainder:02d} '
            'PARTITION OF login_history_partitioned '
            f'FOR VALUES WITH (MODULUS {PARTITION_COUNT}, '
            f'REMAINDER {remainder})'
        )
    _execute(
        'CREATE INDEX ix_login_history_user_id_logged_at '
        'ON login_history_partitioned (user_id, logged_at DESC)'
    )
    _execute(
        'INSERT INTO login_history_partitioned '
        '(id, user_id, logged_at, ip_address, user_agent) '
        'SELECT id, user_id, logged_at, ip_address, user_agent '
        'FROM login_history'
    )

    _execute(
        'DO $$ BEGIN '
        'IF (SELECT count(*) FROM login_history) <> '
        '(SELECT count(*) FROM login_history_partitioned) THEN '
        "RAISE EXCEPTION 'Login history row count mismatch during "
        "partitioning'; "
        'END IF; END $$'
    )

    _execute('ALTER TABLE login_history RENAME TO login_history_unpartitioned')
    _execute('ALTER TABLE login_history_partitioned RENAME TO login_history')
    _execute('DROP TABLE login_history_unpartitioned')


def downgrade() -> None:
    _execute('LOCK TABLE login_history IN ACCESS EXCLUSIVE MODE')
    _execute(
        'DO $$ BEGIN '
        'IF EXISTS ('
        'SELECT 1 FROM login_history '
        'GROUP BY id HAVING count(*) > 1'
        ') THEN '
        "RAISE EXCEPTION 'Cannot downgrade login history with duplicate id "
        "values'; "
        'END IF; END $$'
    )

    _execute(
        'CREATE TABLE login_history_unpartitioned ('
        'id UUID NOT NULL, '
        'user_id UUID NOT NULL, '
        'logged_at TIMESTAMPTZ NOT NULL DEFAULT now(), '
        'ip_address VARCHAR(45), '
        'user_agent VARCHAR(512), '
        'CONSTRAINT pk_login_history_unpartitioned PRIMARY KEY (id), '
        'CONSTRAINT fk_login_history_unpartitioned_user_id '
        'FOREIGN KEY (user_id) REFERENCES users(id)'
        ')'
    )
    _execute(
        'CREATE INDEX ix_login_history_unpartitioned_user_id '
        'ON login_history_unpartitioned (user_id)'
    )
    _execute(
        'CREATE INDEX ix_login_history_unpartitioned_logged_at '
        'ON login_history_unpartitioned (logged_at)'
    )
    _execute(
        'INSERT INTO login_history_unpartitioned '
        '(id, user_id, logged_at, ip_address, user_agent) '
        'SELECT id, user_id, logged_at, ip_address, user_agent '
        'FROM login_history'
    )

    _execute(
        'DO $$ BEGIN '
        'IF (SELECT count(*) FROM login_history) <> '
        '(SELECT count(*) FROM login_history_unpartitioned) THEN '
        "RAISE EXCEPTION 'Login history row count mismatch during downgrade'; "
        'END IF; END $$'
    )

    _execute('ALTER TABLE login_history RENAME TO login_history_partitioned')
    _execute('ALTER TABLE login_history_unpartitioned RENAME TO login_history')
    _execute('DROP TABLE login_history_partitioned')
    _execute(
        'ALTER INDEX ix_login_history_unpartitioned_user_id '
        'RENAME TO ix_login_history_user_id'
    )
    _execute(
        'ALTER INDEX ix_login_history_unpartitioned_logged_at '
        'RENAME TO ix_login_history_logged_at'
    )
    _execute(
        'ALTER TABLE login_history RENAME CONSTRAINT '
        'pk_login_history_unpartitioned TO login_history_pkey'
    )
    _execute(
        'ALTER TABLE login_history RENAME CONSTRAINT '
        'fk_login_history_unpartitioned_user_id TO '
        'login_history_user_id_fkey'
    )
