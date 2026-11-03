import asyncio
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from notifications.contracts import FilmEvent, SourcePosition
from notifications.db.models import (
    FilmAnnouncement,
    FilmCard,
    KafkaOffset,
    ProcessedEvent,
    ScheduleRun,
)
from notifications.db.session import Database
from notifications.ingest.transactions import AtomicIngest


def film_event(film_id, version=1, kind="film.updated", **changes):
    values = dict(
        event_id=uuid4(),
        event_type=kind,
        film_id=film_id,
        film_version=version,
        catalog_published_at=datetime(2020, 1, 1, tzinfo=UTC),
        catalog_updated_at=datetime(2020, 1, version, tzinfo=UTC),
        title=f"Film v{version}",
        description=None,
        creation_date=None,
        imdb_rating=None,
        genres=[],
    )
    values.update(changes)
    return FilmEvent.model_validate(values)


def film_handler():
    from notifications.films.events import apply_film

    return apply_film


@pytest.fixture
async def db(database_url):
    database = Database(database_url)
    try:
        yield database
    finally:
        await database.close()


async def test_late_publication_announces_once_without_downgrading_card(db):
    apply_film = film_handler()
    film_id = uuid4()
    published = film_event(film_id, 1, "film.published")
    async with db.transaction() as session:
        await apply_film(session, film_event(film_id, 2))
        await apply_film(session, published)
        await apply_film(session, published)
        await apply_film(session, film_event(film_id, 1, "film.published"))
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        announcements_count = await session.scalar(
            select(func.count()).select_from(FilmAnnouncement)
        )
        assert (card.film_version, announcements_count) == (2, 1)
        assert card.title == "Film v2"
        assert card.catalog_updated_at == datetime(2020, 1, 2, tzinfo=UTC)
        announcement = await session.get(FilmAnnouncement, film_id)
        assert announcement.detected_at.tzinfo is not None
        assert announcement.assigned_run_id is None


async def test_unknown_updated_and_historical_bootstrap_create_only_cards(db):
    apply_film = film_handler()
    film_id, bootstrap_film_id, genre_id = uuid4(), uuid4(), uuid4()
    async with db.transaction() as session:
        await apply_film(session, film_event(film_id, genres=[dict(id=genre_id, name="Drama")]))
        # Movies bootstrap emits film.updated, retaining the source version/times.
        await apply_film(session, film_event(bootstrap_film_id, 7))
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.description, card.creation_date, card.imdb_rating) == (None, None, None)
        assert card.genres == [{"id": str(genre_id), "name": "Drama"}]
        bootstrap_card = await session.get(FilmCard, bootstrap_film_id)
        assert bootstrap_card.film_version == 7
        assert bootstrap_card.catalog_published_at == datetime(2020, 1, 1, tzinfo=UTC)
        assert await session.scalar(select(func.count()).select_from(FilmAnnouncement)) == 0


async def test_newer_card_replaces_all_projected_fields_and_accepts_nulls(db):
    apply_film = film_handler()
    film_id, genre_id = uuid4(), uuid4()
    async with db.transaction() as session:
        await apply_film(
            session,
            film_event(
                film_id,
                description="Description",
                creation_date=date(1999, 2, 3),
                imdb_rating=8.5,
                genres=[dict(id=genre_id, name="Drama")],
            ),
        )
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.description, card.creation_date, card.imdb_rating) == (
            "Description",
            date(1999, 2, 3),
            8.5,
        )
        await apply_film(session, film_event(film_id, 2, title="Renamed"))
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.film_version, card.title, card.genres) == (2, "Renamed", [])
        assert (card.description, card.creation_date, card.imdb_rating) == (None, None, None)
        assert card.catalog_published_at == datetime(2020, 1, 1, tzinfo=UTC)
        assert card.catalog_updated_at == datetime(2020, 1, 2, tzinfo=UTC)


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize(
    "kind, expected_announcements", [("film.updated", 0), ("film.published", 1)]
)
async def test_equal_and_older_versions_preserve_card(db, version, kind, expected_announcements):
    apply_film = film_handler()
    film_id = uuid4()
    async with db.transaction() as session:
        await apply_film(session, film_event(film_id, 2, description="Current"))
        await apply_film(
            session, film_event(film_id, version, kind, title="Conflicting stale title")
        )
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.film_version, card.title, card.description) == (2, "Film v2", "Current")
        assert (
            await session.scalar(select(func.count()).select_from(FilmAnnouncement))
            == expected_announcements
        )


async def test_republication_preserves_detection_and_assigned_run(db):
    apply_film = film_handler()
    film_id, run_id = uuid4(), uuid4()
    async with db.transaction() as session:
        await apply_film(session, film_event(film_id, 1, "film.published"))
        session.add(ScheduleRun(run_id=run_id, kind="new_films", period_start=datetime.now(UTC)))
        await session.flush()
        announcement = await session.get(FilmAnnouncement, film_id)
        detected_at = announcement.detected_at
        announcement.assigned_run_id = run_id
    async with db.transaction() as session:
        await apply_film(session, film_event(film_id, 2, "film.published"))
    async with db.transaction() as session:
        announcement = await session.get(FilmAnnouncement, film_id)
        assert (announcement.detected_at, announcement.assigned_run_id) == (detected_at, run_id)
        assert (await session.get(FilmCard, film_id)).film_version == 2
        assert await session.scalar(select(func.count()).select_from(FilmAnnouncement)) == 1


async def test_concurrent_publications_keep_one_marker_and_newest_card(db):
    apply_film = film_handler()
    film_id = uuid4()

    async def apply(version, kind):
        async with db.transaction() as session:
            await apply_film(session, film_event(film_id, version, kind))

    await asyncio.gather(
        apply(2, "film.updated"),
        apply(1, "film.published"),
        apply(3, "film.published"),
        apply(3, "film.published"),
    )
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.film_version, card.title) == (3, "Film v3")
        assert await session.scalar(select(func.count()).select_from(FilmAnnouncement)) == 1


@pytest.mark.parametrize("existing", [False, True])
async def test_failure_between_card_and_announcement_rolls_back_ingest_then_retries(db, existing):
    apply_film = film_handler()
    film_id = uuid4()
    async with db.transaction() as session:
        if existing:
            await apply_film(session, film_event(film_id))
        await session.execute(
            text("""
            CREATE FUNCTION fail_announcement() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'announcement insertion failed'; END $$
        """)
        )
        await session.execute(
            text("""
            CREATE TRIGGER reject_announcement BEFORE INSERT ON film_announcements
            FOR EACH ROW EXECUTE FUNCTION fail_announcement()
        """)
        )

    async def dispatch(session, event):
        assert isinstance(event, FilmEvent)
        await apply_film(session, event)

    ingest = AtomicIngest(db, dispatch)
    assert await ingest.claim("movies.films.v1", 0, earliest=0, latest=1) == 0
    event = film_event(film_id, 2, "film.published")
    position = SourcePosition(topic="movies.films.v1", partition=0, next_offset=1)
    with pytest.raises(DBAPIError, match="announcement insertion failed"):
        await ingest.handle_record(event, position)
    async with db.transaction() as session:
        card = await session.get(FilmCard, film_id)
        assert (card.film_version if card else None) == (1 if existing else None)
        assert await session.scalar(select(func.count()).select_from(FilmAnnouncement)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0
        assert (await session.get(KafkaOffset, ("movies.films.v1", 0))).next_offset == 0
        await session.execute(text("DROP TRIGGER reject_announcement ON film_announcements"))
        await session.execute(text("DROP FUNCTION fail_announcement()"))
    await ingest.handle_record(event, position)
    async with db.transaction() as session:
        assert (await session.get(FilmCard, film_id)).film_version == 2
        assert await session.scalar(select(func.count()).select_from(FilmAnnouncement)) == 1
        assert await session.get(ProcessedEvent, event.event_id) is not None
        assert (await session.get(KafkaOffset, ("movies.films.v1", 0))).next_offset == 1


async def test_caller_failure_rolls_back_card_and_announcement(db):
    apply_film = film_handler()
    with pytest.raises(RuntimeError, match="caller failed"):
        async with db.transaction() as session:
            await apply_film(session, film_event(uuid4(), 1, "film.published"))
            raise RuntimeError("caller failed")
    async with db.transaction() as session:
        for model in (FilmCard, FilmAnnouncement):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
