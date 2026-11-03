"""Real-process class isolation, limiter failure and legacy queue cutover.

Only UUID databases, Rabbit topology and one UUID Redis account hash are mutated.
The shared test Redis is never stopped, flushed or globally reconfigured.
"""

import os
import signal
import socket
import time
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pika
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from conftest import compose
from test_end_to_end import (
    SEND_CLASSES,
    ContactsAuth,
    Processes,
    cleanup_topology,
    contacts_env,
    daytime_tz,
    http,
    limiter_env,
    limiter_key,
    remove_limiter,
    start_publisher,
    start_worker,
    wait_for,
)


def stop(processes):
    for process in processes:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
    for process in processes:
        process.wait(timeout=10)


def test_delivery_classes(notification_stack, tmp_path):
    e = notification_stack
    namespace = "notifications-e2e-" + uuid4().hex
    recipient_tz = daytime_tz()
    name = "notifications_test_" + uuid4().hex
    pg_base = f"postgresql://{e['POSTGRES_USER']}:{e['POSTGRES_PASSWORD']}@127.0.0.1:{e['POSTGRES_PORT']}/"
    processes = Processes(tmp_path)
    env = {
        "NOTIFICATIONS_DATABASE_URL": (pg_base + name).replace(
            "postgresql://", "postgresql+asyncpg://"
        ),
        "NOTIFICATIONS_PROCESS": "bootstrap",
        "NOTIFICATIONS_RABBITMQ_URL": e["RABBITMQ_URL"],
        **limiter_env(e, namespace),
        "NOTIFICATIONS_LIMITER_TOTAL_RATE": "10",
        "NOTIFICATIONS_LIMITER_TOTAL_CAPACITY": "5",
        "NOTIFICATIONS_LIMITER_TRANSACTIONAL_RATE": "5",
        "NOTIFICATIONS_LIMITER_TRANSACTIONAL_CAPACITY": "2",
        "NOTIFICATIONS_LIMITER_SERVICE_RATE": "4",
        "NOTIFICATIONS_LIMITER_SERVICE_CAPACITY": "2",
        "NOTIFICATIONS_LIMITER_BULK_RATE": "0.5",
        "NOTIFICATIONS_LIMITER_BULK_CAPACITY": "1",
    }
    with ExitStack() as resources:
        admin = resources.enter_context(
            psycopg.connect(pg_base + "notifications_test_admin", autocommit=True)
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        resources.callback(
            admin.execute, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
        )
        resources.callback(cleanup_topology, e, namespace)
        resources.callback(remove_limiter, namespace)
        contacts = ContactsAuth(processes, e, resources, admin, pg_base)
        env.update(contacts_env(contacts.url))
        resources.callback(processes.close)
        # A bound, non-listening UUID-test port gives a genuine connection refusal
        # without changing the shared Redis or SMTP service used by other tests.
        unavailable = resources.enter_context(socket.socket())
        unavailable.bind(("127.0.0.1", 0))
        unavailable_port = str(unavailable.getsockname()[1])

        def query(statement, params=()):
            with psycopg.connect(pg_base + name) as database:
                return database.execute(statement, params).fetchall()

        def emails():
            messages = http(e["SMTP_API_URL"] + "/api/v1/messages")["messages"]
            return [
                message
                for message in messages
                if any(namespace in recipient["Address"] for recipient in message["To"])
            ]

        def attempts():
            return query("SELECT count(*) FROM delivery_attempts")[0][0]

        def seed(kind, *, published=False, due=None, frozen=False):
            did, uid = uuid4(), uuid4()
            at = due or datetime.now(UTC)
            payload = {
                "to": f"{did}-{namespace}@example.org",
                "from": "cinema@example.org",
                "subject": "Frozen legacy snapshot",
                "html": "<p>Preserved legacy body</p>",
                "first_name": None,
                "last_name": None,
                "locale": "ru",
                "tz": recipient_tz,
                "reviews": [],
                "films": [],
            }
            with psycopg.connect(pg_base + name) as database:
                database.execute(
                    "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
                    "VALUES (%s,1,%s,true,true,false)",
                    (uid, recipient_tz),
                )
                database.execute(
                    "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,status,"
                    "next_attempt_at,payload,message_id,prepared_at) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        did,
                        str(did),
                        uid,
                        kind,
                        "retry" if due else "prepared" if frozen else "pending",
                        at,
                        Jsonb(payload) if frozen else None,
                        f"<{did}@legacy.test>" if frozen else None,
                        datetime.now(UTC) if frozen else None,
                    ),
                )
                database.execute(
                    "INSERT INTO outbox(outbox_id,delivery_id,available_at,published_at) "
                    "VALUES (%s,%s,%s,%s)",
                    (
                        uuid4(),
                        did,
                        at,
                        datetime.now(UTC) - timedelta(minutes=10) if published else None,
                    ),
                )
            contacts.seed(uid, payload["to"], recipient_tz)
            return did

        # These rows and the old handoff really exist before migration 003.
        processes.run("notifications", env, "alembic", "upgrade", "002_receipt_event_id")
        legacy = [
            seed(kind, published=True, frozen=True) for kind in ("welcome", "likes", "new_films")
        ]
        future_due = datetime.now(UTC) + timedelta(seconds=18)
        future = seed("welcome", due=future_due, frozen=True)
        snapshots = query(
            "SELECT delivery_id,payload,message_id,next_attempt_at FROM deliveries "
            "ORDER BY delivery_id"
        )
        rabbit = pika.BlockingConnection(pika.URLParameters(e["RABBITMQ_URL"]))
        resources.callback(rabbit.close)
        channel = rabbit.channel()
        legacy_queue = namespace + ".legacy"
        channel.queue_declare(queue=legacy_queue, durable=True)
        resources.callback(channel.queue_delete, queue=legacy_queue)
        for did in legacy:
            channel.basic_publish(exchange="", routing_key=legacy_queue, body=str(did).encode())
        processes.run("notifications", env, "alembic", "upgrade", "head")
        assert (
            query(
                "SELECT delivery_id,payload,message_id,next_attempt_at FROM deliveries "
                "ORDER BY delivery_id"
            )
            == snapshots
        )
        assert query("SELECT DISTINCT send_class FROM deliveries ORDER BY send_class") == [
            ("bulk",),
            ("service",),
            ("transactional",),
        ]
        for _ in range(12):
            seed("new_films")
        start_publisher(processes, env, namespace)

        workers = [
            start_worker(
                processes,
                env,
                e,
                namespace,
                cls,
                NOTIFICATIONS_LIMITER_REDIS_URL=f"redis://127.0.0.1:{unavailable_port}/0",
            )
            for cls in SEND_CLASSES
        ]
        # A real Redis connection failure creates retry outbox rows, no SMTP attempts.
        wait_for(
            lambda: (
                query(
                    "SELECT 1 FROM deliveries WHERE payload IS NOT NULL "
                    "AND next_attempt_at > clock_timestamp() AND lease_token IS NULL"
                )
                and query("SELECT count(*) FROM outbox")[0][0] > 16
            )
        )
        assert all(worker.poll() is None for worker in workers)
        assert attempts() == 0 and emails() == []
        stop(workers)
        before = query("SELECT count(*) FROM outbox")[0][0]
        workers = [start_worker(processes, env, e, namespace, cls) for cls in SEND_CLASSES]
        # Redis is now reachable but its UUID account hash is absent.
        wait_for(lambda: query("SELECT count(*) FROM outbox")[0][0] > before)
        assert attempts() == 0 and emails() == []

        def initialize_ready_workers():
            # All consumers are running while the account is still absent.
            wait_for(
                lambda: all(
                    channel.queue_declare(
                        queue=namespace + ".email." + cls, passive=True
                    ).method.consumer_count
                    > 0
                    for cls in SEND_CLASSES
                )
            )
            previous = query("SELECT attempt_id FROM delivery_attempts")
            processes.run(
                "notifications",
                env,
                "python",
                "-m",
                "notifications.delivery.rate_limit",
                "initialize",
                "--cooldown-seconds",
                "2",
            )
            ready_at = datetime.fromtimestamp(
                float(
                    compose(
                        "exec",
                        "-T",
                        "redis",
                        "redis-cli",
                        "HGET",
                        limiter_key(namespace),
                        "ready_at",
                    )
                ),
                UTC,
            )
            return ready_at, [row[0] for row in previous]

        def assert_no_early_attempts(ready_at, previous):
            # Persisted timestamps remain valid even if a slow runner resumes
            # after the entire cooldown; no process-startup timing assumption.
            assert query(
                "SELECT count(*) FROM delivery_attempts "
                "WHERE NOT (attempt_id = ANY(%s::uuid[])) AND started_at < %s",
                (previous, ready_at),
            ) == [(0,)]
            assert query(
                "SELECT count(*) FROM deliveries WHERE sent_at < %s AND delivery_id IN "
                "(SELECT delivery_id FROM delivery_attempts "
                "WHERE NOT (attempt_id = ANY(%s::uuid[])))",
                (ready_at, previous),
            ) == [(0,)]

        ready_at, previous = initialize_ready_workers()
        wait_for(
            lambda: (
                query(
                    "SELECT count(*) FROM deliveries WHERE delivery_id=ANY(%s) AND status='sent'",
                    (legacy[:2],),
                )[0][0]
                == 2
            )
        )
        assert_no_early_attempts(ready_at, previous)
        # Welcome and service progress while the bulk group still has a backlog.
        assert (
            query("SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status<>'sent'")[0][
                0
            ]
            > 0
        )
        # Bulk ordering may change across limiter deferrals. Check its legacy
        # handoff separately, after observing urgent/service progress above.
        wait_for(
            lambda: query(
                "SELECT 1 FROM deliveries WHERE delivery_id=%s AND status='sent'",
                (legacy[2],),
            )
        )
        assert len(emails()) >= 3
        assert channel.queue_declare(queue=legacy_queue, passive=True).method.message_count == 3
        for did, payload, message_id, _ in snapshots:
            assert query(
                "SELECT payload,message_id FROM deliveries WHERE delivery_id=%s", (did,)
            ) == [(payload, message_id)]
        if datetime.now(UTC) < future_due:
            assert query(
                "SELECT attempt_count FROM deliveries WHERE delivery_id=%s", (future,)
            ) == [(0,)]
        wait_for(
            lambda: query(
                "SELECT sent_at FROM deliveries WHERE delivery_id=%s AND status='sent'", (future,)
            )
        )
        assert query(
            "SELECT sent_at >= %s FROM deliveries WHERE delivery_id=%s", (future_due, future)
        ) == [(True,)]
        assert query(
            "SELECT bool_and(published_at >= %s) FROM outbox WHERE delivery_id=%s",
            (future_due, future),
        ) == [(True,)]

        # A real refused SMTP connection retries in service, with its frozen payload.
        stop([workers[1]])
        bad_service = start_worker(
            processes, env, e, namespace, "service", NOTIFICATIONS_SMTP_PORT=unavailable_port
        )
        retry = seed("likes")
        wait_for(
            lambda: query(
                "SELECT 1 FROM delivery_attempts WHERE delivery_id=%s AND outcome='retry'", (retry,)
            )
        )
        stop([bad_service])
        retry_snapshot = query(
            "SELECT payload,message_id FROM deliveries WHERE delivery_id=%s", (retry,)
        )
        workers[1] = start_worker(processes, env, e, namespace, "service")
        wait_for(
            lambda: query(
                "SELECT 1 FROM deliveries WHERE delivery_id=%s AND status='sent'", (retry,)
            )
        )
        assert query(
            "SELECT send_class,attempt_count FROM deliveries WHERE delivery_id=%s", (retry,)
        ) == [("service", 2)]
        assert (
            query("SELECT payload,message_id FROM deliveries WHERE delivery_id=%s", (retry,))
            == retry_snapshot
        )

        # State loss after previous sends must not mint a fresh full bucket.
        stop(workers)
        remove_limiter(namespace)
        seed("welcome")
        before_attempts, before_emails = attempts(), len(emails())
        before = query("SELECT count(*) FROM outbox")[0][0]
        workers = [start_worker(processes, env, e, namespace, cls) for cls in SEND_CLASSES]
        wait_for(lambda: query("SELECT count(*) FROM outbox")[0][0] > before)
        time.sleep(0.8)
        assert attempts() == before_attempts and len(emails()) == before_emails
        ready_at, previous = initialize_ready_workers()
        wait_for(lambda: attempts() > before_attempts and len(emails()) > before_emails)
        assert_no_early_attempts(ready_at, previous)
        assert all(worker.poll() is None for worker in workers)
        print(
            "three_classes_progress=true; legacy_queue_preserved=3; redis_failure_attempts=0; "
            f"UUID={namespace}"
        )
