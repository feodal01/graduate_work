"""Assign each publication to one issue and freeze its subscribed audience."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import exists, insert, literal, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.db.models import FilmAnnouncement, FilmCard, Profile, ScheduleRecipient


async def snapshot_films(
    session: AsyncSession, run_id: UUID, period_start: datetime, limit: int
) -> None:
    """Caller owns transaction and the films scheduler advisory lock.

    Late publications enter the next issue; a period never changes on replay.
    Overflow and issues with no subscribers still consume their publications.
    """
    eligible = (
        FilmAnnouncement.assigned_run_id.is_(None),
        FilmAnnouncement.detected_at <= period_start,
    )
    selected = (
        select(FilmAnnouncement.film_id)
        .join(FilmCard, FilmCard.film_id == FilmAnnouncement.film_id)
        .where(*eligible)
        .order_by(FilmCard.catalog_published_at.desc(), FilmAnnouncement.film_id)
        .limit(limit)
        .cte("selected_films")
    )
    await session.execute(
        update(FilmAnnouncement)
        .where(*eligible)
        .values(
            assigned_run_id=run_id,
            included_in_digest=FilmAnnouncement.film_id.in_(select(selected.c.film_id)),
        )
    )
    await session.execute(
        insert(ScheduleRecipient).from_select(
            ["run_id", "user_id"],
            select(literal(run_id), Profile.user_id).where(
                Profile.deleted_at.is_(None),
                text("profiles.tz IN (SELECT name FROM pg_timezone_names)"),
                Profile.new_films.is_(True),
                exists().where(
                    FilmAnnouncement.assigned_run_id == run_id,
                    FilmAnnouncement.included_in_digest.is_(True),
                ),
            ),
        )
    )


async def schedule_films(period_start: datetime) -> UUID | None:
    """Schedule using the explicit NOTIFICATIONS_ scheduler settings."""
    from notifications.scheduling.scheduler import get_scheduler

    return await get_scheduler().schedule_films(period_start)
