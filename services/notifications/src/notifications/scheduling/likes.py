"""Freeze eligible review counts without changing the sent baseline."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import exists, insert, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.db.models import (
    DeliveryItem,
    Profile,
    ReviewCounter,
    ScheduleItem,
    ScheduleRecipient,
)


async def snapshot_likes(session: AsyncSession, run_id: UUID, min_delta: int) -> None:
    """Caller owns transaction and the likes scheduler advisory lock."""
    candidates = (
        select(
            literal(run_id),
            ReviewCounter.review_id,
            ReviewCounter.user_id,
            ReviewCounter.likes_count,
        )
        .join(Profile, Profile.user_id == ReviewCounter.user_id)
        .where(
            Profile.deleted_at.is_(None),
            text("profiles.tz IN (SELECT name FROM pg_timezone_names)"),
            Profile.likes.is_(True),
            ReviewCounter.likes_count - ReviewCounter.last_sent_likes_count >= min_delta,
            ~exists().where(
                ScheduleItem.review_id == ReviewCounter.review_id, ScheduleItem.active.is_(True)
            ),
            ~exists().where(
                DeliveryItem.review_id == ReviewCounter.review_id, DeliveryItem.active.is_(True)
            ),
        )
    )
    await session.execute(
        insert(ScheduleItem).from_select(
            ["run_id", "review_id", "user_id", "likes_count"], candidates
        )
    )
    await session.execute(
        insert(ScheduleRecipient).from_select(
            ["run_id", "user_id"],
            select(ScheduleItem.run_id, ScheduleItem.user_id)
            .where(ScheduleItem.run_id == run_id)
            .distinct(),
        )
    )


async def schedule_likes(period_start: datetime) -> UUID | None:
    """Schedule using the explicit NOTIFICATIONS_ scheduler settings."""
    from notifications.scheduling.scheduler import get_scheduler

    return await get_scheduler().schedule_likes(period_start)
