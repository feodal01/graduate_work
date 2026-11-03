from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.sql.errors import DatabaseUnavailableError
from models import User
from schemas.notification_contacts import ActiveNotificationContact


async def read_notification_contacts(
    session: AsyncSession,
    user_ids: list[UUID],
) -> dict[UUID, ActiveNotificationContact]:
    try:
        rows = await session.execute(
            select(
                User.id, User.email, User.first_name, User.last_name, User.locale
            ).where(User.id.in_(user_ids))
        )
    except (SQLAlchemyError, OSError) as exc:
        raise DatabaseUnavailableError from exc
    return {
        row.id: ActiveNotificationContact(
            user_id=row.id,
            email=row.email,
            first_name=row.first_name,
            last_name=row.last_name,
            locale=row.locale,
        )
        for row in rows
    }
