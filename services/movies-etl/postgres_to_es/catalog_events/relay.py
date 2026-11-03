"""Independent Kafka relay; acknowledgements precede the published_at transaction."""

import json
from functools import partial
from typing import Any

from confluent_kafka import Producer
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from postgres_to_es.catalog_events.store import process_lock
from postgres_to_es.settings import settings


def _mark_published(conn: connection, event_ids: list[str]) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE notifications_source.outbox SET published_at=clock_timestamp()
            WHERE event_id=ANY(%s::uuid[]) AND published_at IS NULL""",
            (event_ids,),
        )


def publish_batch() -> int:
    if not settings.kafka_bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required for relay")
    # One relay per database avoids overtaking an unacked older version across processes.
    with process_lock("movies-catalog-relay") as conn:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """SELECT event_id,film_id,payload FROM notifications_source.outbox
                    WHERE published_at IS NULL ORDER BY sequence LIMIT %s""",
                    (settings.catalog_relay_batch_size,),
                )
                rows = cur.fetchall()
        if not rows:
            return 0
        producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": int(settings.catalog_relay_delivery_timeout_seconds * 1000),
            }
        )
        acknowledged: list[str] = []
        errors: list[str] = []

        def delivered(event_id: str, error: Any, _message: Any) -> None:
            if error is None:
                acknowledged.append(event_id)
            else:
                errors.append(str(error))

        for row in rows:
            event_id = str(row["event_id"])
            producer.produce(
                settings.kafka_movies_topic,
                key=str(row["film_id"]).encode(),
                value=json.dumps(
                    row["payload"],
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode(),
                on_delivery=partial(delivered, event_id),
            )
        pending = producer.flush(settings.catalog_relay_delivery_timeout_seconds + 1)
        # Uncertain messages remain pending; ack/commit crash deliberately permits replay.
        if acknowledged:
            _mark_published(conn, acknowledged)
        if pending or errors:
            raise RuntimeError(f"Kafka delivery incomplete: pending={pending}; errors={errors}")
        return len(acknowledged)
