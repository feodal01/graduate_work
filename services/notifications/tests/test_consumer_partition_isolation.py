"""A poison movie record cannot hold up Auth, UGC or another movie partition."""

import asyncio
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition
from sqlalchemy import select
from test_consumer_transactions import kafka_topic as kafka_topic
from test_consumer_transactions import wait_for_paused, wait_until
from test_film_projection import film_event
from test_profiles import auth_event, profile_handler

from notifications.contracts import AuthEvent, FilmEvent, LikesEvent
from notifications.db.models import (
    Delivery,
    FilmCard,
    KafkaOffset,
    ProcessedEvent,
    Profile,
    ReviewCounter,
)
from notifications.db.session import Database
from notifications.films.events import apply_film
from notifications.ingest.consumer import KafkaIngestConsumer
from notifications.likes.events import apply_likes


@pytest.fixture
async def kafka_sources(kafka_topic):
    server, auth = kafka_topic
    likes, films = auth + "_likes", auth + "_films"
    admin = AIOKafkaAdminClient(bootstrap_servers=server)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(likes, num_partitions=1, replication_factor=1),
                NewTopic(films, num_partitions=2, replication_factor=1),
            ]
        )
        yield server, auth, likes, films
    finally:
        await admin.delete_topics([likes, films])
        await admin.close()


@pytest.mark.parametrize("handoff", ["restart", "rebalance"])
async def test_invalid_movie_partition_preserves_independent_streams(
    database_url, kafka_sources, caplog, handoff
):
    server, auth_topic, likes_topic, films_topic = kafka_sources
    db = Database(database_url)
    auth = profile_handler(database_url)

    async def dispatch(session, event):
        if isinstance(event, AuthEvent):
            await auth(session, event)
        elif isinstance(event, LikesEvent):
            await apply_likes(session, event)
        else:
            await apply_film(session, event)

    async def cursors():
        async with db.transaction() as session:
            return {
                TopicPartition(row.topic, row.partition): row.next_offset
                for row in await session.scalars(select(KafkaOffset))
            }

    producer = AIOKafkaProducer(bootstrap_servers=server)
    await producer.start()
    tasks = []
    group = uuid4().hex
    blocked = TopicPartition(films_topic, 0)
    secret = "private-title-sentinel@example.org"
    blocked_film, healthy_film = film_event(uuid4()), film_event(uuid4())
    deleted_user, new_user = uuid4(), uuid4()
    registered = auth_event(new_user, kind="user.registered")
    deleted = auth_event(deleted_user, 2, "user.deleted")
    likes = LikesEvent(
        event_id=uuid4(),
        event_type="review.likes_updated",
        review_id=uuid4().hex[:24],
        user_id=new_user,
        likes_count=4,
        likes_version=1,
    )

    def consumer():
        return KafkaIngestConsumer(
            db,
            dispatch,
            bootstrap_servers=server,
            group_id=group,
            topic_models={auth_topic: AuthEvent, likes_topic: LikesEvent, films_topic: FilmEvent},
        )

    try:
        async with db.transaction() as session:
            await auth(session, auth_event(deleted_user))
        await producer.send_and_wait(
            films_topic, value=(f'{{"title":"{secret}"}}').encode(), partition=0
        )
        await producer.send_and_wait(
            films_topic, value=blocked_film.model_dump_json().encode(), partition=0
        )
        first = consumer()
        task = asyncio.create_task(first.run())
        tasks.append(task)
        await wait_for_paused(first, task, blocked)
        assert first.client.paused() == {blocked}
        assert (await cursors())[blocked] == 0

        for topic, partition, event in (
            (auth_topic, 0, registered),
            (auth_topic, 0, deleted),
            (likes_topic, 0, likes),
            (films_topic, 1, healthy_film),
        ):
            await producer.send_and_wait(
                topic, value=event.model_dump_json().encode(), partition=partition
            )
        expected = {
            TopicPartition(auth_topic, 0): 2,
            TopicPartition(likes_topic, 0): 1,
            blocked: 0,
            TopicPartition(films_topic, 1): 1,
        }

        async def applied():
            return await cursors() == expected

        await wait_until(applied)
        async with db.transaction() as session:
            assert (await session.get(Profile, deleted_user)).deleted_at is not None
            welcome = (await session.scalars(select(Delivery))).one()
            assert welcome.user_id == new_user and welcome.kind == "welcome"
            assert (await session.get(ReviewCounter, likes.review_id)).likes_count == 4
            assert await session.get(FilmCard, healthy_film.film_id) is not None
            assert await session.get(FilmCard, blocked_film.film_id) is None
        assert not task.done()

        previous_generation = first.assignment_generation
        if handoff == "restart":
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        second = consumer()
        second_task = asyncio.create_task(second.run())
        tasks.append(second_task)
        if handoff == "rebalance":

            async def rebalanced():
                return first.assignment_generation > previous_generation

            await wait_until(rebalanced)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await wait_for_paused(second, second_task, blocked)
        assert await cursors() == expected
        assert await second.client.position(blocked) == 0
        assert await second.client.committed(blocked) is None

        another = auth_event(uuid4(), kind="user.registered")
        newer_likes = likes.model_copy(
            update={"event_id": uuid4(), "likes_version": 2, "likes_count": 5}
        )
        another_film = film_event(uuid4())
        for topic, partition, event in (
            (auth_topic, 0, another),
            (likes_topic, 0, newer_likes),
            (films_topic, 1, another_film),
        ):
            await producer.send_and_wait(
                topic, value=event.model_dump_json().encode(), partition=partition
            )
        expected.update(
            {
                TopicPartition(auth_topic, 0): 3,
                TopicPartition(likes_topic, 0): 2,
                TopicPartition(films_topic, 1): 2,
            }
        )
        await wait_until(applied)
        async with db.transaction() as session:
            assert set(await session.scalars(select(Delivery.user_id))) == {
                new_user,
                another.user_id,
            }
            assert (await session.get(ReviewCounter, likes.review_id)).likes_count == 5
            assert await session.get(FilmCard, another_film.film_id) is not None
            assert await session.get(FilmCard, blocked_film.film_id) is None
            assert set(await session.scalars(select(ProcessedEvent.event_id))) == {
                registered.event_id,
                deleted.event_id,
                likes.event_id,
                healthy_film.event_id,
                another.event_id,
                newer_likes.event_id,
                another_film.event_id,
            }
        assert not second_task.done()
        assert f"{films_topic}[0] offset 0" in caplog.text
        assert "manual repair required" in caplog.text
        assert secret not in caplog.text
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()
        await db.close()
