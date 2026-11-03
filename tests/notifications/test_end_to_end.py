"""Real OS processes, HTTP, PostgreSQL, Mongo, Kafka, RabbitMQ and Mailpit.

All mutable resources have a per-run UUID. No dependency mocks or missing-service skips.
"""

import base64
import hmac
import json
import os
import socket
import subprocess
import time
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

import pika
import psycopg
from bson import ObjectId
from psycopg import sql
from pymongo import MongoClient

from conftest import compose

ROOT = Path(__file__).resolve().parents[2]
SECRET = "isolated-notifications-e2e-secret-at-least-32"


def http(url, data=None, method=None, token=None):
    headers = {"Content-Type": "application/json", "X-Request-Id": "notifications-e2e"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(
        url,
        data=None if data is None else json.dumps(data).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read()
            return json.loads(body) if body else None
    except HTTPError as error:
        raise AssertionError(f"HTTP {error.code}: {error.read().decode()}") from error


def wait_for(check, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.2)
    raise AssertionError(f"Condition did not become true: {check}")


def token(user_id):
    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")

    head = encode(b'{"alg":"HS256","typ":"JWT"}')
    body = encode(
        json.dumps(
            {
                "sub": str(user_id),
                "jti": str(uuid4()),
                "type": "access",
                "iat": int(time.time()),
                "exp": int(time.time()) + 1800,
            }
        ).encode()
    )
    message = head + b"." + body
    return (message + b"." + encode(hmac.digest(SECRET.encode(), message, "sha256"))).decode()


class Processes:
    def __init__(self, directory):
        self.directory = directory
        self.children = []

    def command(self, service, *args):
        return ["uv", "run", "--directory", str(ROOT / "services" / service), *args]

    def environment(self, service, env):
        result = {**os.environ, **env}
        # Optional isolated environments keep concurrent service checks independent.
        key = "SERVICE_TEST_ENV_" + service.upper().replace("-", "_")
        if selected := os.environ.get(key):
            result["UV_PROJECT_ENVIRONMENT"] = selected
        return result

    def run(self, service, env, *args):
        result = subprocess.run(
            self.command(service, *args),
            env=self.environment(service, env),
            text=True,
            capture_output=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout

    def start(self, service, env, *args):
        log = self.directory / f"process-{len(self.children)}-{service}.log"
        stream = log.open("w", encoding="utf-8")
        process = subprocess.Popen(
            self.command(service, *args),
            env=self.environment(service, env),
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.children.append((process, stream, log))
        return process

    def api(self, service, env, app, path):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        args = ["uvicorn", app, "--host", "127.0.0.1", "--port", str(port)]
        if service == "notifications":
            args.append("--factory")
        process = self.start(service, env, *args)
        url = f"http://127.0.0.1:{port}"

        def ready():
            assert process.poll() is None, self.children[-1][2].read_text(encoding="utf-8")
            try:
                return http(url + path) is not None
            except (URLError, ConnectionError):
                return False

        wait_for(ready)
        return url

    def close(self):
        import signal

        for process, _, _ in reversed(self.children):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process, stream, _ in reversed(self.children):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            stream.close()


SEND_CLASSES = ("transactional", "service", "bulk")


def daytime_tz():
    """Use an actual recipient zone safely inside 10:00–20:00 at execution time."""
    now = datetime.now(UTC)
    for name in (
        "Pacific/Honolulu",
        "America/Los_Angeles",
        "America/New_York",
        "America/Sao_Paulo",
        "Europe/London",
        "Europe/Moscow",
        "Asia/Dubai",
        "Asia/Kolkata",
        "Asia/Bangkok",
        "Asia/Tokyo",
        "Australia/Sydney",
        "Pacific/Auckland",
    ):
        if 12 <= now.astimezone(ZoneInfo(name)).hour < 17:
            return name
    raise AssertionError("No daytime IANA zone found")


def limiter_env(endpoints, account):
    return {
        "NOTIFICATIONS_LIMITER_REDIS_URL": f"redis://{endpoints['REDIS_HOST']}:{endpoints['REDIS_PORT']}/0",
        "NOTIFICATIONS_LIMITER_ACCOUNT_ID": account,
        "NOTIFICATIONS_LIMITER_TOTAL_RATE": "30",
        "NOTIFICATIONS_LIMITER_TOTAL_CAPACITY": "30",
        "NOTIFICATIONS_LIMITER_TRANSACTIONAL_RATE": "10",
        "NOTIFICATIONS_LIMITER_TRANSACTIONAL_CAPACITY": "10",
        "NOTIFICATIONS_LIMITER_SERVICE_RATE": "10",
        "NOTIFICATIONS_LIMITER_SERVICE_CAPACITY": "10",
        "NOTIFICATIONS_LIMITER_BULK_RATE": "10",
        "NOTIFICATIONS_LIMITER_BULK_CAPACITY": "10",
        "NOTIFICATIONS_LIMITER_UNAVAILABLE_RETRY_SECONDS": "0.5",
        "NOTIFICATIONS_LIMITER_REDIS_TIMEOUT_SECONDS": "0.2",
    }


def limiter_key(account):
    assert account.startswith("notifications-e2e-")
    return "notifications:smtp:{" + account + "}:limiter"


def remove_limiter(account):
    compose("exec", "-T", "redis", "redis-cli", "DEL", limiter_key(account))


def cleanup_topology(endpoints, namespace):
    assert namespace.startswith("notifications-e2e-")
    rabbit = pika.BlockingConnection(pika.URLParameters(endpoints["RABBITMQ_URL"]))
    try:
        channel = rabbit.channel()
        for name in [*("email." + cls for cls in SEND_CLASSES), "notifications.delivery.dead"]:
            channel.queue_delete(queue=namespace + "." + name)
        for name in ("notifications.email", "notifications.delivery.dlx"):
            channel.exchange_delete(exchange=namespace + "." + name)
    finally:
        rabbit.close()


def start_publisher(processes, env, namespace):
    return processes.start(
        "notifications",
        {
            **env,
            "NOTIFICATIONS_PROCESS": "publisher",
            "NOTIFICATIONS_TRANSACTIONAL_BATCH_SIZE": "5",
            "NOTIFICATIONS_SERVICE_BATCH_SIZE": "5",
            "NOTIFICATIONS_BULK_BATCH_SIZE": "5",
        },
        "python",
        "-m",
        "notifications.delivery.publisher",
        "--namespace",
        namespace,
        "--poll-seconds",
        "0.1",
    )


def contacts_env(auth_url):
    return {
        "NOTIFICATIONS_AUTH_CONTACTS_URL": auth_url + "/api/v1/internal/notification-contacts",
        "NOTIFICATIONS_AUTH_CONTACTS_API_KEY": SECRET,
        "NOTIFICATIONS_AUTH_CONTACTS_TIMEOUT_SECONDS": "1",
        "NOTIFICATIONS_AUTH_CONTACTS_RETRY_SECONDS": "0.5",
        "NOTIFICATIONS_AUTH_CONTACTS_BATCH_SIZE": "100",
    }


def introspection_env(auth_url):
    return {
        "NOTIFICATIONS_AUTH_INTROSPECTION_URL": auth_url + "/api/v1/internal/introspect",
        "NOTIFICATIONS_AUTH_INTROSPECTION_API_KEY": SECRET,
        "NOTIFICATIONS_AUTH_INTROSPECTION_TIMEOUT_SECONDS": "1",
    }


class ContactsAuth:
    """Real isolated Auth API/DB for delivery-only process tests."""

    def __init__(self, processes, endpoints, resources, admin, pg_base):
        name = "notifications_test_" + uuid4().hex
        self.database_url = pg_base + name
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        resources.callback(
            admin.execute, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
        )
        env = {
            **endpoints,
            "POSTGRES_DB": name,
            "PYTHONPATH": str(ROOT / "services/auth/src"),
            "JWT_SECRET": SECRET,
            "JWT_ALGORITHM": "HS256",
            "SERVICE_API_KEY": SECRET,
            "RATE_LIMIT_ENABLED": "false",
            "TRACING_ENABLED": "false",
            "LOGSTASH_HOST": "",
            "SENTRY_DSN": "",
            "YANDEX_OAUTH_ENABLED": "false",
        }
        processes.run("auth", env, "alembic", "upgrade", "head")
        self.url = processes.api("auth", env, "main:app", "/api/v1/health/")
        self.process = processes.children[-1][0]

    def seed(self, user_id, email, tz, *, deleted=False):
        if deleted:
            return
        with psycopg.connect(self.database_url) as db:
            db.execute(
                "INSERT INTO users(id,login,email,first_name,tz) VALUES (%s,%s,%s,'Анна',%s)",
                (user_id, str(user_id), email, tz),
            )

    def update(self, user_id, email):
        with psycopg.connect(self.database_url) as db:
            db.execute(
                "UPDATE users SET email=%s,first_name='Changed' WHERE id=%s",
                (email, user_id),
            )


def start_worker(processes, env, endpoints, namespace, send_class, **overrides):
    return processes.start(
        "notifications",
        {
            **env,
            "NOTIFICATIONS_PROCESS": "worker",
            "NOTIFICATIONS_SMTP_HOST": endpoints["SMTP_HOST"],
            "NOTIFICATIONS_SMTP_PORT": endpoints["SMTP_PORT"],
            "NOTIFICATIONS_SMTP_FROM": "cinema@example.org",
            "NOTIFICATIONS_SMTP_STARTTLS": "false",
            "NOTIFICATIONS_FALLBACK_LOCALE": "ru",
            "NOTIFICATIONS_FALLBACK_TZ": "UTC",
            **overrides,
        },
        "python",
        "-m",
        "notifications.delivery.worker",
        "--namespace",
        namespace,
        "--send-class",
        send_class,
        "--max-concurrency",
        "1",
        "--poll-seconds",
        "0.1",
        "--retry-seconds",
        "1",
        "--max-attempts",
        "3",
        "--lease-seconds",
        "3",
        "--smtp-timeout",
        "1",
    )


def test_end_to_end(notification_stack, tmp_path):
    e = notification_stack
    run = uuid4().hex
    recipient_tz = daytime_tz()
    topics = ["notifications-e2e-" + kind + "-" + run for kind in ("auth", "ugc", "movies")]
    namespace = "notifications-e2e-" + run
    load_id = "likes-" + run
    processes = Processes(tmp_path)
    pg_base = f"postgresql://{e['POSTGRES_USER']}:{e['POSTGRES_PASSWORD']}@127.0.0.1:{e['POSTGRES_PORT']}/"
    names = ["notifications_test_" + uuid4().hex for _ in range(3)]
    auth_db, notification_db, movies_db = names
    mongo = MongoClient(e["MONGO_URI"], uuidRepresentation="standard")
    index = "notifications-e2e-" + run
    with ExitStack() as resources:
        admin = resources.enter_context(
            psycopg.connect(pg_base + "notifications_test_admin", autocommit=True)
        )
        for name in names:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            resources.callback(
                admin.execute, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
        resources.callback(mongo.close)
        for topic in topics:
            compose(
                "exec",
                "-T",
                "kafka",
                "kafka-topics.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            )
            resources.callback(
                compose,
                "exec",
                "-T",
                "kafka",
                "kafka-topics.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--delete",
                "--topic",
                topic,
            )
        auth_env = {
            **e,
            "POSTGRES_DB": auth_db,
            "PYTHONPATH": str(ROOT / "services/auth/src"),
            "JWT_SECRET": SECRET,
            "JWT_ALGORITHM": "HS256",
            "RATE_LIMIT_ENABLED": "false",
            "TRACING_ENABLED": "false",
            "LOGSTASH_HOST": "",
            "SENTRY_DSN": "",
            "AUTH_RELAY_TOPIC": topics[0],
            "PASSWORD_MIN_SCORE": "0",
            "SERVICE_API_KEY": SECRET,
            "YANDEX_OAUTH_ENABLED": "false",
        }
        nenv = {
            "NOTIFICATIONS_DATABASE_URL": (pg_base + notification_db).replace(
                "postgresql://", "postgresql+asyncpg://"
            ),
            "NOTIFICATIONS_PROCESS": "bootstrap",
        }
        uenv = {
            **e,
            "MONGO_DB": e["MONGO_DB"],
            "PYTHONPATH": str(ROOT / "services/ugc-api/src"),
            "AUTH_JWT_SECRET": SECRET,
            "KAFKA_LIKES_TOPIC": topics[1],
            "LOGSTASH_HOST": "",
            "SENTRY_DSN": "",
        }
        menv = {
            **e,
            "POSTGRES_DB": movies_db,
            "SQL_HOST": "127.0.0.1",
            "SQL_PORT": e["POSTGRES_PORT"],
            "ELASTICSEARCH_INDEX": index,
            "KAFKA_MOVIES_TOPIC": topics[2],
            "CATALOG_SCAN_INTERVAL_SECONDS": "1",
            "CATALOG_OVERLAP_SECONDS": "60",
            "CATALOG_RECONCILE_INTERVAL_SECONDS": "2",
        }
        processes.run("auth", auth_env, "alembic", "upgrade", "head")
        processes.run("notifications", nenv, "alembic", "upgrade", "head")
        # Historical source fixtures predate event capture: ordinary events are not erased.
        old_user = uuid4()
        old_email = f"old-{run}@example.org"
        with psycopg.connect(pg_base + auth_db) as db:
            db.execute(
                "INSERT INTO users(id,login,email,tz) VALUES (%s,%s,%s,%s)",
                (old_user, "old-" + run, old_email, recipient_tz),
            )
        old_review = ObjectId()
        now = datetime.now(UTC)
        mongo[e["MONGO_DB"]].reviews.insert_one(
            {
                "_id": old_review,
                "user_id": old_user,
                "film_id": uuid4(),
                "text": "Historical review",
                "likes": 10,
                "likes_version": 10,
                "dislikes": 0,
                "created_at": now,
                "updated_at": now,
                "score": None,
            }
        )
        http(
            e["ELASTICSEARCH_URL"] + "/" + index,
            {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "catalog_updated_at": {"type": "date_nanos"},
                    }
                }
            },
            "PUT",
        )
        resources.callback(http, e["ELASTICSEARCH_URL"] + "/" + index, None, "DELETE")
        old_film = str(uuid4())

        def film(film_id, version, title):
            at = datetime.now(UTC).isoformat()
            http(
                f"{e['ELASTICSEARCH_URL']}/{index}/_doc/{film_id}?refresh=true",
                {
                    "id": film_id,
                    "title": title,
                    "description": None,
                    "genres": [],
                    "creation_date": None,
                    "imdb_rating": None,
                    "film_version": version,
                    "catalog_published_at": now.isoformat(),
                    "catalog_updated_at": at,
                },
                "PUT",
            )

        film(old_film, 1, "Historical film")
        # Publication migration is independent of content reads for already indexed Movies.
        with psycopg.connect(pg_base + movies_db) as db:
            db.execute(
                "CREATE SCHEMA content; CREATE TABLE content.film_work "
                "(id uuid PRIMARY KEY, modified timestamptz)"
            )
        processes.run(
            "movies-etl", menv, "python", "-m", "postgres_to_es.catalog_events", "migrate"
        )
        resources.callback(processes.close)
        consumer_env = {
            **nenv,
            "NOTIFICATIONS_PROCESS": "consumer",
            "NOTIFICATIONS_KAFKA_BOOTSTRAP_SERVERS": e["KAFKA_BOOTSTRAP_SERVERS"],
            "NOTIFICATIONS_INITIAL_LIKES": "true",
            "NOTIFICATIONS_INITIAL_NEW_FILMS": "true",
            "NOTIFICATIONS_INITIAL_MARKETING": "false",
        }
        processes.start(
            "notifications",
            consumer_env,
            "python",
            "-m",
            "notifications",
            "--group",
            run,
            "--auth-topic",
            topics[0],
            "--likes-topic",
            topics[1],
            "--films-topic",
            topics[2],
        )
        auth_url = processes.api("auth", auth_env, "main:app", "/api/v1/health/")
        ugc_url = processes.api("ugc-api", uenv, "main:app", "/api/v1/health/")
        preferences_url = processes.api(
            "notifications",
            {
                **nenv,
                "NOTIFICATIONS_PROCESS": "api",
                "NOTIFICATIONS_JWT_SECRET": SECRET,
                "NOTIFICATIONS_JWT_ALGORITHM": "HS256",
                **introspection_env(auth_url),
            },
            "notifications.main:create_app",
            "/openapi.json",
        )
        processes.start("auth", auth_env, "python", "-m", "relay")
        processes.start("ugc-api", uenv, "python", "-m", "relay")
        processes.start(
            "movies-etl", menv, "python", "-m", "postgres_to_es.catalog_events", "relay"
        )
        rabbit_env = {
            **nenv,
            **contacts_env(auth_url),
            "NOTIFICATIONS_RABBITMQ_URL": e["RABBITMQ_URL"],
            **limiter_env(e, namespace),
        }
        resources.callback(cleanup_topology, e, namespace)
        resources.callback(remove_limiter, namespace)
        resources.callback(processes.close)
        processes.run(
            "notifications",
            rabbit_env,
            "python",
            "-m",
            "notifications.delivery.rate_limit",
            "initialize",
            "--cooldown-seconds",
            "0.1",
        )
        start_publisher(processes, rabbit_env, namespace)
        for send_class in SEND_CLASSES:
            start_worker(processes, rabbit_env, e, namespace, send_class)

        def query(statement, params=()):
            with psycopg.connect(pg_base + notification_db) as db:
                return db.execute(statement, params).fetchall()

        def emails():
            messages = http(e["SMTP_API_URL"] + "/api/v1/messages")["messages"]
            return [
                message
                for message in messages
                if any(run in recipient["Address"] for recipient in message["To"])
            ]

        processes.run(
            "auth", auth_env, "python", "-m", "bootstrap_notifications", "--load-id", "users-" + run
        )
        processes.run(
            "ugc-api",
            uenv,
            "python",
            "-m",
            "bootstrap_notifications",
            "capture",
            "--load-id",
            load_id,
        )
        processes.run(
            "movies-etl",
            menv,
            "python",
            "-m",
            "postgres_to_es.catalog_events",
            "bootstrap",
            "capture",
            "--load-id",
            run,
        )
        # New data arrives after durable capture, while historical enqueue is unfinished.
        login = "new-" + run
        password = "E2eSecurePassword987!"
        user = http(
            auth_url + "/api/v1/auth/signup",
            {
                "login": login,
                "password": password,
                "email": f"new-{run}@example.org",
                "locale": "ru",
                "tz": recipient_tz,
            },
        )
        access = http(auth_url + "/api/v1/auth/login", {"login": login, "password": password})[
            "access_token"
        ]
        for _ in range(2):
            http(
                f"{ugc_url}/api/v1/ugc/reviews/{old_review}/vote",
                {"value": "like"},
                "PUT",
                token(uuid4()),
            )
        new_film = str(uuid4())
        film(new_film, 1, "New during migration")
        processes.run(
            "ugc-api",
            uenv,
            "python",
            "-m",
            "bootstrap_notifications",
            "enqueue",
            "--load-id",
            load_id,
        )
        manifest = processes.run(
            "ugc-api",
            uenv,
            "python",
            "-m",
            "bootstrap_notifications",
            "status",
            "--load-id",
            load_id,
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(manifest, encoding="utf-8")
        processes.run(
            "notifications",
            nenv,
            "python",
            "-m",
            "notifications.ingest.bootstrap",
            "register",
            "--manifest",
            str(manifest_path),
        )
        processes.run(
            "movies-etl",
            menv,
            "python",
            "-m",
            "postgres_to_es.catalog_events",
            "bootstrap",
            "enqueue",
            "--load-id",
            run,
        )
        wait_for(lambda: query("SELECT 1 FROM bootstrap_receipts WHERE applied_at IS NOT NULL"))
        processes.run(
            "notifications",
            nenv,
            "python",
            "-m",
            "notifications.ingest.bootstrap",
            "finalize",
            "--load-id",
            load_id,
        )
        wait_for(lambda: query("SELECT 1 FROM film_cards WHERE film_id=%s", (old_film,)))
        assert query("SELECT count(*) FROM film_announcements WHERE film_id=%s", (old_film,)) == [
            (0,)
        ]
        assert query("SELECT count(*) FROM deliveries WHERE user_id=%s", (old_user,)) == [(0,)]
        old_data_emails = len(
            [
                message
                for message in emails()
                if any(r["Address"] == old_email for r in message["To"])
            ]
        )
        assert old_data_emails == 0
        assert query(
            "SELECT last_sent_likes_count, likes_count FROM review_counters WHERE review_id=%s",
            (str(old_review),),
        ) == [(10, 12)]
        wait_for(
            lambda: any(
                any(r["Address"] == f"new-{run}@example.org" for r in m["To"]) for m in emails()
            )
        )
        # Contact-only changes stay in Auth; later preparation reads them over HTTP.
        with psycopg.connect(pg_base + auth_db) as db:
            events_before = db.execute("SELECT count(*) FROM notification_outbox").fetchone()[0]
        http(
            auth_url + "/api/v1/auth/profile",
            {"email": f"changed-{run}@example.org"},
            "PATCH",
            access,
        )
        with psycopg.connect(pg_base + auth_db) as db:
            assert (
                db.execute("SELECT count(*) FROM notification_outbox").fetchone()[0]
                == events_before
            )
        assert not query(
            "SELECT 1 FROM information_schema.columns WHERE table_name='profiles' "
            "AND column_name IN ('email','first_name','last_name','locale')"
        )
        review = http(
            f"{ugc_url}/api/v1/ugc/films/{new_film}/reviews",
            {"text": "Live review"},
            "POST",
            access,
        )
        for _ in range(2):
            http(
                f"{ugc_url}/api/v1/ugc/reviews/{review['id']}/vote",
                {"value": "like"},
                "PUT",
                token(uuid4()),
            )
        processes.start(
            "movies-etl", menv, "python", "-m", "postgres_to_es.catalog_events", "detect"
        )
        scheduler_env = {
            **nenv,
            "NOTIFICATIONS_PROCESS": "scheduler",
            "NOTIFICATIONS_LIKES_MIN_DELTA": "2",
            "NOTIFICATIONS_LIKES_INTERVAL_SECONDS": "1",
            "NOTIFICATIONS_FILMS_INTERVAL_SECONDS": "1",
            "NOTIFICATIONS_LIKES_BOOTSTRAP_LOAD_ID": load_id,
        }
        processes.start(
            "notifications",
            scheduler_env,
            "python",
            "-m",
            "notifications.scheduling.scheduler",
            "--poll-seconds",
            "0.1",
        )
        wait_for(
            lambda: query(
                "SELECT 1 FROM deliveries WHERE user_id=%s AND kind='likes' AND status='sent'",
                (user["id"],),
            )
        )
        wait_for(
            lambda: query(
                "SELECT 1 FROM deliveries WHERE user_id=%s AND kind='new_films' AND status='sent'",
                (user["id"],),
            )
        )
        new_data_emails = len(emails())
        assert new_data_emails > 0
        assert any(
            any(r["Address"] == f"changed-{run}@example.org" for r in m["To"]) for m in emails()
        )
        film(new_film, 2, "Updated title")
        wait_for(
            lambda: query(
                "SELECT 1 FROM film_cards WHERE film_id=%s AND film_version=2", (new_film,)
            )
        )
        assert query("SELECT count(*) FROM film_announcements WHERE film_id=%s", (new_film,)) == [
            (1,)
        ]
        prefs = preferences_url + "/api/v1/notifications/preferences/me"
        http(prefs, {"likes": False, "new_films": False, "marketing": False}, "PATCH", access)

        def changed_emails():
            return [
                m
                for m in emails()
                if any(r["Address"] == f"changed-{run}@example.org" for r in m["To"])
            ]

        before = len(changed_emails())
        for _ in range(2):
            http(
                f"{ugc_url}/api/v1/ugc/reviews/{review['id']}/vote",
                {"value": "like"},
                "PUT",
                token(uuid4()),
            )
        film(str(uuid4()), 1, "After preferences off")
        wait_for(
            lambda: query(
                "SELECT 1 FROM review_counters WHERE review_id=%s AND likes_count=4",
                (review["id"],),
            )
        )
        time.sleep(3)
        assert len(changed_emails()) == before
        http(auth_url + "/api/v1/auth/profile", method="DELETE", token=access)
        wait_for(
            lambda: query(
                "SELECT 1 FROM profiles WHERE user_id=%s AND deleted_at IS NOT NULL "
                "AND NOT likes AND NOT new_films AND NOT marketing",
                (user["id"],),
            )
        )
        with psycopg.connect(pg_base + notification_db) as db:
            db.execute((ROOT / "infra/notifications/metrics.sql").read_text(encoding="utf-8"))
        # Assertions inspect actual SMTP messages and source/projection databases.
        print(f"old_data_emails={old_data_emails}; new_data_emails={new_data_emails}; UUID={run}")
        processes.close()
