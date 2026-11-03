"""Bound new-film snapshots and real card reads, including old oversized jobs."""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, event, select, update
from sqlalchemy.exc import DBAPIError
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup
from test_schedulers import PERIOD, film, scheduler, seed, trigger
from test_schedulers import db as db

from notifications.config import TimingSettings
from notifications.db.models import (
    Delivery,
    DeliveryItem,
    FilmAnnouncement,
    FilmCard,
    ScheduleRun,
)


async def catalog(db, count, *, delivery_id=None):
    ids = [UUID(int=i + 1) for i in range(count)]
    async with db.transaction() as session:
        session.add_all(
            FilmCard(
                film_id=film_id,
                film_version=1,
                catalog_published_at=PERIOD - timedelta(hours=count - i // 2),
                catalog_updated_at=PERIOD,
                title=f"Film {i + 1}",
                genres=[],
            )
            for i, film_id in enumerate(ids)
        )
        await session.flush()
        if delivery_id is None:
            # Detection order intentionally disagrees with catalog publication order.
            session.add_all(
                FilmAnnouncement(film_id=film_id, detected_at=PERIOD - timedelta(seconds=i))
                for i, film_id in enumerate(ids)
            )
        else:
            session.add_all(
                DeliveryItem(delivery_id=delivery_id, film_id=film_id) for film_id in ids
            )
    return ids


def test_digest_limit_defaults_and_environment(monkeypatch):
    monkeypatch.delenv("NOTIFICATIONS_FILMS_PER_DIGEST", raising=False)
    assert TimingSettings().films_per_digest == 20
    monkeypatch.setenv("NOTIFICATIONS_FILMS_PER_DIGEST", "3")
    assert TimingSettings().films_per_digest == 3


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "invalid"])
def test_digest_limit_requires_positive_integer(value):
    with pytest.raises(ValidationError):
        TimingSettings(films_per_digest=value)


@pytest.mark.parametrize("limit", [None, 3])
async def test_freshest_selection_is_frozen_and_overflow_never_enters_next_issue(db, limit):
    await seed(db, users=2)
    ids = await catalog(db, 40)
    late = await film(db, detected_at=PERIOD + timedelta(seconds=1))
    timing = TimingSettings() if limit is None else TimingSettings(films_per_digest=limit)
    service = scheduler(db, timing=timing)
    first, duplicate = await asyncio.gather(
        service.schedule_films(PERIOD), service.schedule_films(PERIOD)
    )
    assert first == duplicate
    # Equal publication dates are ordered by film_id, independently of detection order.
    expected = set(ids[20:]) if limit is None else {ids[38], ids[39], ids[36]}
    async with db.transaction() as session:
        await session.execute(
            update(FilmCard).where(FilmCard.film_id == ids[0]).values(catalog_published_at=PERIOD)
        )
    # A restarted scheduler with a different limit must preserve the frozen membership.
    resumed = scheduler(db)
    assert await resumed.expand_run(first, 1) == 1
    assert await resumed.expand_run(first, 10) == 1
    async with db.transaction() as session:
        deliveries = list(await session.scalars(select(Delivery).where(Delivery.run_id == first)))
        assert len(deliveries) == 2
        assert (await session.get(ScheduleRun, first)).films_per_digest == (limit or 20)
        for delivery in deliveries:
            selected = set(
                await session.scalars(
                    select(DeliveryItem.film_id).where(
                        DeliveryItem.delivery_id == delivery.delivery_id
                    )
                )
            )
            assert selected == expected
        announcements = list(
            await session.scalars(select(FilmAnnouncement).where(FilmAnnouncement.film_id.in_(ids)))
        )
        assert all(row.assigned_run_id == first for row in announcements)
        assert {row.film_id for row in announcements if row.included_in_digest} == expected
    next_run = await resumed.schedule_films(PERIOD + timedelta(days=7))
    assert await resumed.expand_run(next_run, 10) == 2
    async with db.transaction() as session:
        selected = list(
            await session.scalars(
                select(DeliveryItem.film_id).join(Delivery).where(Delivery.run_id == next_run)
            )
        )
        assert selected == [late, late]


async def test_failed_snapshot_does_not_consume_overflow(db):
    await seed(db)
    await catalog(db, 40)
    await trigger(db, "schedule_recipients", "RAISE EXCEPTION 'snapshot failed'")
    with pytest.raises(DBAPIError):
        await scheduler(db).schedule_films(PERIOD)
    async with db.transaction() as session:
        assert await session.scalar(select(ScheduleRun)) is None
        rows = list(await session.scalars(select(FilmAnnouncement)))
        assert len(rows) == 40
        assert all(row.assigned_run_id is None for row in rows)


@pytest.mark.parametrize("count,limit", [(20, 20), (1000, 20), (1000, 3)])
async def test_prepare_reads_cards_once_and_bounds_legacy_digest(setup, count, limit):
    db, worker, did, _, _ = setup
    worker.send_class = "bulk"
    worker.settings.films_per_digest = limit
    async with db.transaction() as session:
        await session.execute(
            update(Delivery).where(Delivery.delivery_id == did).values(kind="new_films")
        )
        await session.execute(delete(DeliveryItem).where(DeliveryItem.delivery_id == did))
    ids = await catalog(db, count, delivery_id=did)
    statements = []

    def record_query(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "film_cards" in statement:
            statements.append(statement)

    event.listen(db.engine.sync_engine, "before_cursor_execute", record_query)
    try:
        snapshot = await worker.prepare_delivery(did)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", record_query)
    assert len(statements) == 1
    expected = [str(ids[i]) for start in range(count - 2, -1, -2) for i in (start, start + 1)][
        :limit
    ]
    assert [card["film_id"] for card in snapshot.payload["films"]] == expected
    assert snapshot.payload["html"].count("<article>") == limit


async def test_no_audience_consumes_selected_films_and_overflow(db):
    ids = await catalog(db, 40)
    service = scheduler(db)
    run = await service.schedule_films(PERIOD)
    assert await service.expand_run(run, 10) == 0
    await seed(db)
    next_run = await service.schedule_films(PERIOD + timedelta(days=7))
    assert await service.expand_run(next_run, 10) == 0
    async with db.transaction() as session:
        rows = list(await session.scalars(select(FilmAnnouncement)))
        assert len(rows) == len(ids)
        assert all(row.assigned_run_id == run for row in rows)
        assert sum(row.included_in_digest for row in rows) == 20
        assert await session.scalar(select(Delivery)) is None


@pytest.mark.parametrize("new_limit", [3, 50])
async def test_worker_config_change_preserves_frozen_issue_membership(setup, new_limit):
    db, worker, _, uid, _ = setup
    worker.send_class = "bulk"
    ids = await catalog(db, 40)
    run = await scheduler(db).schedule_films(PERIOD)
    assert await scheduler(db).expand_run(run, 10) == 1
    async with db.transaction() as session:
        did = await session.scalar(
            select(Delivery.delivery_id).where(Delivery.run_id == run, Delivery.user_id == uid)
        )
    worker.settings.films_per_digest = new_limit
    snapshot = await worker.prepare_delivery(did)
    assert {card["film_id"] for card in snapshot.payload["films"]} == {
        str(film_id) for film_id in ids[20:]
    }
