"""Сценарии витрин: какие типы попадают в film_watches / page_events, а search_filter — нет."""

from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaProducer
from clickhouse_connect.driver.client import Client

from tests.functional.testdata.envelopes import (
    click_event,
    page_view,
    quality_changed,
    search_filter_used,
    watch_completed,
    watch_progress,
)
from tests.functional.utils.helpers import ch_query, event_count, produce, wait_for_event


async def test_abandoned_film_has_started_without_completed(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: фильм A progress+completed, фильм B только progress.
    Assert: abandoned B >= 1, abandoned A = 0.
    """
    user_id = uuid4()
    finished_film = uuid4()
    abandoned_film = uuid4()

    await produce(
        kafka_producer,
        watch_progress(film_id=finished_film, event_id=uuid4(), user_id=user_id),
        key=user_id,
    )
    await produce(
        kafka_producer,
        watch_completed(film_id=finished_film, event_id=uuid4(), user_id=user_id),
        key=user_id,
    )
    abandoned_progress_id = uuid4()
    await produce(
        kafka_producer,
        watch_progress(film_id=abandoned_film, event_id=abandoned_progress_id, user_id=user_id),
        key=user_id,
    )
    await wait_for_event(clickhouse_client, 'ugc.film_watches', abandoned_progress_id)

    rows = await ch_query(
        clickhouse_client,
        'SELECT film_id, '
        "uniqIf(user_id, event_type = 'watch_progress') AS started, "
        "uniqIf(user_id, event_type = 'watch_completed') AS completed, "
        'started - completed AS abandoned '
        'FROM ugc.film_watches '
        'WHERE film_id = %(finished)s OR film_id = %(abandoned)s '
        'GROUP BY film_id',
        {'finished': str(finished_film), 'abandoned': str(abandoned_film)},
    )
    by_film = {UUID(str(row['film_id'])): row for row in rows}

    assert by_film[finished_film]['started'] >= 1
    assert by_film[finished_film]['abandoned'] == 0
    assert by_film[abandoned_film]['started'] >= 1
    assert by_film[abandoned_film]['abandoned'] >= 1


async def test_click_and_page_view_land_in_page_events(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: click и page_view одной страницы. Assert: витрина page_events, без JSONExtract."""
    user_id = uuid4()
    page = f'/film/{uuid4()}'
    click_id = uuid4()
    view_id = uuid4()

    await produce(
        kafka_producer,
        click_event(page=page, event_id=click_id, user_id=user_id, element_type='button'),
        key=user_id,
    )
    await produce(
        kafka_producer,
        page_view(page=page, event_id=view_id, user_id=user_id, duration_seconds=42),
        key=user_id,
    )

    click_row = await wait_for_event(clickhouse_client, 'ugc.page_events', click_id)
    view_row = await wait_for_event(clickhouse_client, 'ugc.page_events', view_id)

    assert click_row['page'] == page
    assert click_row['element_type'] == 'button'
    assert click_row['event_type'] == 'click'
    assert view_row['page'] == page
    assert view_row['duration_seconds'] == pytest.approx(42)
    assert view_row['event_type'] == 'page_view'
    assert await event_count(clickhouse_client, 'ugc.film_watches', click_id) == 0
    assert await event_count(clickhouse_client, 'ugc.film_watches', view_id) == 0


async def test_quality_changed_lands_in_film_watches(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: quality_changed. Assert: from_quality/to_quality в film_watches."""
    user_id = uuid4()
    film_id = uuid4()
    event_id = uuid4()

    await produce(
        kafka_producer,
        quality_changed(
            film_id=film_id,
            event_id=event_id,
            user_id=user_id,
            from_quality='720p',
            to_quality='1080p',
        ),
        key=user_id,
    )

    row = await wait_for_event(clickhouse_client, 'ugc.film_watches', event_id)
    assert UUID(str(row['film_id'])) == film_id
    assert row['from_quality'] == '720p'
    assert row['to_quality'] == '1080p'
    assert await event_count(clickhouse_client, 'ugc.page_events', event_id) == 0


async def test_search_filter_used_stays_in_events_filters(
    kafka_producer: AIOKafkaProducer,
    clickhouse_client: Client,
) -> None:
    """Arrange: search_filter_used. Assert: filters в ugc.events, витрин нет."""
    user_id = uuid4()
    event_id = uuid4()
    filters = {'genre': 'comedy', 'year': ['2024', '2025']}

    await produce(
        kafka_producer,
        search_filter_used(filters=filters, event_id=event_id, user_id=user_id),
        key=user_id,
    )

    row = await wait_for_event(clickhouse_client, 'ugc.events', event_id)
    assert row['event_type'] == 'search_filter_used'
    assert 'comedy' in row['filters']
    assert await event_count(clickhouse_client, 'ugc.film_watches', event_id) == 0
    assert await event_count(clickhouse_client, 'ugc.page_events', event_id) == 0
