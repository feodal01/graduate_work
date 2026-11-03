"""One-off import of existing Auth profiles through the durable notification outbox."""

import argparse
import asyncio

from sqlalchemy import String, bindparam, func, literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from core.config import PostgresSettings
from models import NotificationLoadMarker, NotificationOutbox, User

# A marker is claimed before its event is inserted, in the same SQL statement and
# transaction. ON CONFLICT handles concurrent runs and survives outbox cleanup.
_load_id = bindparam('load_id', type_=String(255))
_claimed = (
    insert(NotificationLoadMarker)
    .from_select(
        [NotificationLoadMarker.load_id, NotificationLoadMarker.user_id],
        select(_load_id, User.id).order_by(User.id),
    )
    .on_conflict_do_nothing(
        index_elements=[NotificationLoadMarker.load_id, NotificationLoadMarker.user_id]
    )
    .returning(NotificationLoadMarker.user_id)
    .cte('claimed')
)
_snapshots = (
    select(
        func.gen_random_uuid().label('event_id'),
        User.id.label('user_id'),
        User.user_version,
        User.tz,
    )
    .join(_claimed, _claimed.c.user_id == User.id)
    .cte('snapshots')
    .prefix_with('MATERIALIZED', dialect='postgresql')
)
_inserted = (
    insert(NotificationOutbox)
    .from_select(
        [
            NotificationOutbox.event_id,
            NotificationOutbox.user_id,
            NotificationOutbox.user_version,
            NotificationOutbox.event_type,
            NotificationOutbox.payload,
            NotificationOutbox.load_id,
        ],
        select(
            _snapshots.c.event_id,
            _snapshots.c.user_id,
            _snapshots.c.user_version,
            literal('user.profile_updated'),
            func.jsonb_build_object(
                'event_id',
                _snapshots.c.event_id,
                'event_type',
                'user.profile_updated',
                'user_id',
                _snapshots.c.user_id,
                'user_version',
                _snapshots.c.user_version,
                'tz',
                _snapshots.c.tz,
            ),
            _load_id,
        ),
    )
    .returning(NotificationOutbox.event_id)
    .cte('inserted')
)
_IMPORT_USERS = select(func.count()).select_from(_inserted)


async def bootstrap_users(load_id: str, *, engine: AsyncEngine | None = None) -> int:
    """Insert every extant user's current profile once for a permanent load ID."""
    if not load_id.strip() or len(load_id) > 255:
        raise ValueError('load_id must be nonempty and at most 255 characters')
    owned_engine = engine is None
    if engine is None:
        engine = create_async_engine(PostgresSettings().dsn)
    try:
        async with engine.begin() as connection:
            result = await connection.execute(_IMPORT_USERS, {'load_id': load_id})
            return result.scalar_one()
    finally:
        if owned_engine:
            await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--load-id', required=True)
    arguments = parser.parse_args()
    print(f'Imported {asyncio.run(bootstrap_users(arguments.load_id))} users')


if __name__ == '__main__':
    main()
