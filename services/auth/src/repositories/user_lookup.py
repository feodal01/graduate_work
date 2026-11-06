from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.sql.errors import DatabaseUnavailableError
from models import User
from schemas.user_lookup import UserLookupItem


async def lookup_users(
    session: AsyncSession,
    *,
    login: str | None,
    user_ids: list[UUID] | None,
) -> list[UserLookupItem]:
    if login is not None:
        try:
            rows = await session.execute(
                select(User.id, User.login, User.tz).where(User.login == login)
            )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc
        return [
            UserLookupItem(user_id=row.id, login=row.login, tz=row.tz) for row in rows
        ]

    if not user_ids:
        return []

    unique_ids = list(dict.fromkeys(user_ids))
    try:
        rows = await session.execute(
            select(User.id, User.login, User.tz).where(User.id.in_(unique_ids))
        )
    except (SQLAlchemyError, OSError) as exc:
        raise DatabaseUnavailableError from exc
    found_by_id = {
        row.id: UserLookupItem(user_id=row.id, login=row.login, tz=row.tz)
        for row in rows
    }
    return [found_by_id[uid] for uid in unique_ids if uid in found_by_id]
