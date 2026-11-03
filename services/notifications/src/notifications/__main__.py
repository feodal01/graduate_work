"""Independent Kafka projection process; PostgreSQL owns committed positions."""

import argparse
import asyncio
import logging
import signal
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession

from notifications.config import Settings
from notifications.contracts import AuthEvent, FilmEvent, LikesEvent
from notifications.db.session import Database
from notifications.films.events import apply_film
from notifications.ingest.consumer import InvalidPayload, KafkaIngestConsumer
from notifications.likes.events import apply_likes
from notifications.profiles.events import AuthProfileHandler


async def consume(args: argparse.Namespace) -> None:
    settings = Settings()  # type: ignore[call-arg]
    if settings.process != "consumer" or settings.kafka_bootstrap_servers is None:
        raise ValueError("Consumer entrypoint requires process=consumer")
    database = Database(settings.database_url)
    auth = AuthProfileHandler(settings)

    async def dispatch(session: AsyncSession, event: AuthEvent | LikesEvent | FilmEvent) -> None:
        if isinstance(event, AuthEvent):
            await auth(session, event)
        elif isinstance(event, LikesEvent):
            await apply_likes(session, event)
        else:
            await apply_film(session, event)

    consumer = KafkaIngestConsumer(
        database,
        dispatch,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=args.group,
        batch_settings=settings,
        topic_models={
            args.auth_topic: AuthEvent,
            args.likes_topic: LikesEvent,
            args.films_topic: FilmEvent,
        },
    )
    task = asyncio.create_task(consumer.run())
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, task.cancel)
    try:
        with suppress(asyncio.CancelledError):
            await task
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", required=True)
    parser.add_argument("--auth-topic", default="auth.users.v1")
    parser.add_argument("--likes-topic", default="ugc.reviews.v1")
    parser.add_argument("--films-topic", default="movies.films.v1")
    args = parser.parse_args()
    if len({args.auth_topic, args.likes_topic, args.films_topic}) != 3:
        parser.error("Source topics must be distinct")
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(consume(args))
    except Exception as error:
        # Never emit chained validation payloads or SQL bind parameters.
        detail = str(error) if isinstance(error, InvalidPayload) else type(error).__name__
        logging.getLogger(__name__).error("Consumer stopped: %s; %s", type(error).__name__, detail)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
