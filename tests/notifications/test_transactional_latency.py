"""Fixed healthy-stack acceptance profile; conservative source→SMTP upper bounds."""

import json
import time
from contextlib import ExitStack
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from conftest import compose
from test_campaigns_end_to_end import AUTHOR_ID, CAMPAIGN_KEY, response
from test_end_to_end import (
    ROOT,
    SECRET,
    SEND_CLASSES,
    Processes,
    cleanup_topology,
    contacts_env,
    daytime_tz,
    http,
    limiter_env,
    remove_limiter,
    start_publisher,
    wait_for,
)


def assert_bounds(started, observed, limit=180):
    """Every expected recipient must have a finite, nonnegative bound ≤ limit."""
    assert started.keys() <= observed.keys(), (
        f"Missing recipients: {started.keys() - observed.keys()}"
    )
    bounds = {recipient: observed[recipient] - at for recipient, at in started.items()}
    assert bounds and all(0 <= bound <= limit for bound in bounds.values()), bounds
    return bounds


def test_bound_checker_negative_controls():
    with pytest.raises(AssertionError, match="Missing recipients"):
        assert_bounds({"missing": 0}, {})
    with pytest.raises(AssertionError):
        assert_bounds({"late": 0}, {"late": 180.001})
    assert assert_bounds({"boundary": 0}, {"boundary": 180}) == {"boundary": 180}


# Register only inside isolated child processes. Product personal_offer policy is untouched.
WRAPPER = """
import runpy
import notifications.template_registry as registry
from notifications.campaigns import schemas
registry._TEMPLATES = {**registry._TEMPLATES, "latency_transactional": registry.TemplateSpec(
    "latency_transactional", "personal_offer.html", "email", "transactional", None, True)}
schemas._TEMPLATE_INPUT_SCHEMAS["latency_transactional"] = schemas.CampaignItem
from notifications.campaigns.app import create_campaign_app
if __name__ == "__main__":
    runpy.run_module("notifications.delivery.worker", run_name="__main__")
"""


def test_transactional_latency(notification_stack, tmp_path):
    e = notification_stack
    run = uuid4().hex
    namespace = "notifications-e2e-" + run
    topics = [namespace + suffix for suffix in ("-auth", "-ugc", "-films")]
    names = ["notifications_test_" + uuid4().hex for _ in range(2)]
    auth_db, notifications_db = names
    pg_base = f"postgresql://{e['POSTGRES_USER']}:{e['POSTGRES_PASSWORD']}@127.0.0.1:{e['POSTGRES_PORT']}/"
    processes = Processes(tmp_path)
    (tmp_path / "latency_process.py").write_text(WRAPPER, encoding="utf-8")
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
    env = {
        "NOTIFICATIONS_DATABASE_URL": (pg_base + notifications_db).replace(
            "postgresql://", "postgresql+asyncpg://"
        ),
        "NOTIFICATIONS_PROCESS": "bootstrap",
        "NOTIFICATIONS_RABBITMQ_URL": e["RABBITMQ_URL"],
        "PYTHONPATH": str(tmp_path),
        "NOTIFICATIONS_FALLBACK_LOCALE": "ru",
        "NOTIFICATIONS_FALLBACK_TZ": "UTC",
        "NOTIFICATIONS_INITIAL_LIKES": "false",
        "NOTIFICATIONS_INITIAL_NEW_FILMS": "false",
        "NOTIFICATIONS_INITIAL_MARKETING": "false",
        **limiter_env(e, namespace),
        "NOTIFICATIONS_LIMITER_TOTAL_RATE": "20",
        "NOTIFICATIONS_LIMITER_TOTAL_CAPACITY": "20",
        "NOTIFICATIONS_LIMITER_SERVICE_RATE": "5",
        "NOTIFICATIONS_LIMITER_SERVICE_CAPACITY": "5",
        "NOTIFICATIONS_LIMITER_BULK_RATE": "5",
        "NOTIFICATIONS_LIMITER_BULK_CAPACITY": "5",
    }
    with ExitStack() as resources:
        admin = resources.enter_context(
            psycopg.connect(pg_base + "notifications_test_admin", autocommit=True)
        )
        for name in names:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            resources.callback(
                admin.execute, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
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
        resources.callback(cleanup_topology, e, namespace)
        resources.callback(remove_limiter, namespace)
        resources.callback(processes.close)
        processes.run("auth", auth_env, "alembic", "upgrade", "head")
        processes.run("notifications", env, "alembic", "upgrade", "head")
        auth = processes.api("auth", auth_env, "main:app", "/api/v1/health/")
        env.update(contacts_env(auth))
        api = processes.api(
            "notifications",
            {
                **env,
                "NOTIFICATIONS_PROCESS": "campaign-api",
                "NOTIFICATIONS_CAMPAIGN_API_KEY": CAMPAIGN_KEY,
            },
            "latency_process:create_campaign_app",
            "/openapi.json",
        )
        processes.start(
            "notifications",
            {
                **env,
                "NOTIFICATIONS_PROCESS": "consumer",
                "NOTIFICATIONS_KAFKA_BOOTSTRAP_SERVERS": e["KAFKA_BOOTSTRAP_SERVERS"],
            },
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
        processes.start("auth", auth_env, "python", "-m", "relay")
        processes.run(
            "notifications",
            env,
            "python",
            "-m",
            "notifications.delivery.rate_limit",
            "initialize",
            "--cooldown-seconds",
            "0.1",
        )

        def query(statement, params=()):
            with psycopg.connect(pg_base + notifications_db) as db:
                return db.execute(statement, params).fetchall()

        def batch(users, template):
            url = api + "/api/v1/notifications/campaigns/" + str(uuid4())
            body = {
                "batch_id": str(uuid4()),
                "created_by_user_id": AUTHOR_ID,
                "items": [
                    {
                        "user_id": str(user),
                        "template_id": template,
                        "type": "email",
                        "subject": "Latency acceptance",
                        "text": "Fixed test profile",
                    }
                    for user in users
                ],
            }
            code, result = response(url + "/batches", body)
            assert code == 202, result
            assert response(url + "/close", {"expected_batches": 1})[0] == 200

        bulk_users = [uuid4() for _ in range(300)]
        external = uuid4()
        with psycopg.connect(pg_base + notifications_db) as db:
            with db.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
                    "VALUES (%s,1,%s,false,false,true)",
                    [(user, daytime_tz()) for user in [*bulk_users, external]],
                )
        with psycopg.connect(pg_base + auth_db) as db:
            with db.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO users(id,login,email,tz) VALUES (%s,%s,%s,%s)",
                    [
                        (user, str(user), f"{user}-{run}@example.org", daytime_tz())
                        for user in [*bulk_users, external]
                    ],
                )
        for offset in range(0, 300, 100):
            batch(bulk_users[offset : offset + 100], "personal_offer")
        start_publisher(processes, env, namespace)
        for send_class in SEND_CLASSES:
            processes.start(
                "notifications",
                {
                    **env,
                    "NOTIFICATIONS_PROCESS": "worker",
                    "NOTIFICATIONS_SMTP_HOST": e["SMTP_HOST"],
                    "NOTIFICATIONS_SMTP_PORT": e["SMTP_PORT"],
                    "NOTIFICATIONS_SMTP_FROM": "cinema@example.org",
                    "NOTIFICATIONS_SMTP_STARTTLS": "false",
                },
                "python",
                "-m",
                "latency_process",
                "--namespace",
                namespace,
                "--send-class",
                send_class,
                "--max-concurrency",
                "2",
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
        wait_for(
            lambda: query("SELECT 1 FROM deliveries WHERE send_class='bulk' AND status='sent'")
        )
        initial_bulk_sent = query(
            "SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status='sent'"
        )[0][0]
        started, observed, mail_seen = {}, {}, set()

        def observe():
            # Pagination avoids Mailpit's default latest-50 visibility window.
            offset = 0
            while True:
                page = http(e["SMTP_API_URL"] + f"/api/v1/messages?start={offset}&limit=100")
                for message in page["messages"]:
                    mail_seen.update(r["Address"] for r in message["To"] if run in r["Address"])
                offset += len(page["messages"])
                if offset >= page["total"] or not page["messages"]:
                    break
            sent = query("SELECT payload->>'to' FROM deliveries WHERE status='sent'")
            now = time.monotonic()
            for (email,) in sent:
                if email in started and email in mail_seen:
                    observed.setdefault(email, now)
            return started.keys() <= observed.keys()

        overlap = []
        registration_at = []
        for index in range(12):
            due = time.monotonic() + 1
            email = f"welcome-{index}-{run}@example.org"
            overlap.append(
                query("SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status!='sent'")[
                    0
                ][0]
            )
            started[email] = time.monotonic()
            registration_at.append(started[email])
            http(
                auth + "/api/v1/auth/signup",
                {
                    "login": f"latency-{index}-{run}",
                    "password": "E2eSecurePassword987!",
                    "email": email,
                    "locale": "ru",
                    "tz": daytime_tz(),
                },
            )
            observe()
            time.sleep(max(0, due - time.monotonic()))
        external_email = f"{external}-{run}@example.org"
        overlap.append(
            query("SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status!='sent'")[0][
                0
            ]
        )
        started[external_email] = time.monotonic()
        batch([external], "latency_transactional")
        wait_for(observe, timeout=180)
        bounds = assert_bounds(started, observed)
        assert all(count > 0 for count in overlap), overlap
        during_sent = query(
            "SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status='sent'"
        )[0][0]
        assert during_sent > initial_bulk_sent
        wait_for(
            lambda: (
                query("SELECT count(*) FROM deliveries WHERE send_class='bulk' AND status='sent'")
                == [(300,)]
            ),
            timeout=180,
        )
        observe()
        assert len(mail_seen & {f"{user}-{run}@example.org" for user in bulk_users}) == 300
        report = {
            "UUID": run,
            "bulk_sent": 300,
            "welcome_sent": 12,
            "external_transactional_sent": 1,
            "workers_per_class": 1,
            "concurrency": 2,
            "rates_and_capacities": {"total": 20, "transactional": 10, "service": 5, "bulk": 5},
            "registration_intervals_seconds": [
                b - a for a, b in zip(registration_at, registration_at[1:])
            ],
            "bulk_pending_during_registration": overlap,
            "bulk_sent_before_urgent": initial_bulk_sent,
            "bulk_sent_after_urgent": during_sent,
            "conservative_upper_bounds_seconds": bounds,
            "max_upper_bound_seconds": max(bounds.values()),
        }
        (tmp_path / "latency-results.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(json.dumps(report, indent=2))
