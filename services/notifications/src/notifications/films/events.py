"""Project Movies events inside the caller-owned ingestion transaction."""

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import FilmEvent
from notifications.db.models import FilmAnnouncement, FilmCard


async def apply_film(session: AsyncSession, event: FilmEvent) -> None:
    """Keep the newest card and independently remember its first publication.

    Movies bootstrap emits film.updated, so historical cards create no marker.
    The caller commits card, marker, processed event and Kafka cursor together.
    """
    values = {
        "film_id": event.film_id,
        "film_version": event.film_version,
        "catalog_published_at": event.catalog_published_at,
        "catalog_updated_at": event.catalog_updated_at,
        "title": event.title,
        "description": event.description,
        "creation_date": event.creation_date,
        "imdb_rating": event.imdb_rating,
        "genres": [genre.model_dump(mode="json") for genre in event.genres],
    }
    card = insert(FilmCard).values(**values)
    await session.execute(
        card.on_conflict_do_update(
            index_elements=[FilmCard.film_id],
            set_={name: card.excluded[name] for name in values if name != "film_id"},
            where=FilmCard.film_version < event.film_version,
        )
    )
    # Publication is independent of whether this event advanced the card version.
    if event.event_type == "film.published":
        await session.execute(
            insert(FilmAnnouncement)
            .values(film_id=event.film_id)
            .on_conflict_do_nothing(index_elements=[FilmAnnouncement.film_id])
        )
