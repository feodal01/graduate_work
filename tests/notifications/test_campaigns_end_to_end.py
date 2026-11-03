"""Campaign HTTP, publisher and worker processes with real PG/Rabbit/Redis/Mailpit.

Only UUID-owned databases, Rabbit topology and Redis account state are removed.
Profiles and Auth contacts are seeded separately; contact lookup uses real Auth HTTP.
"""

import json
import os
import signal
import socket
import socketserver
import threading
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg
from psycopg import sql

from test_delivery_classes import stop
from test_end_to_end import (
    SECRET,
    ContactsAuth,
    Processes,
    cleanup_topology,
    contacts_env,
    daytime_tz,
    http,
    introspection_env,
    limiter_env,
    remove_limiter,
    start_publisher,
    start_worker,
    wait_for,
)

CAMPAIGN_KEY = "campaign-e2e-test-only-key"
AUTHOR_ID = str(uuid4())


def response(url, data=None, *, method=None, key=CAMPAIGN_KEY):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["X-API-Key"] = key
    request = Request(
        url,
        data=None if data is None else json.dumps(data).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as result:
            return result.status, json.load(result)
    except HTTPError as error:
        return error.code, json.load(error)


class UnconfirmedSMTP(socketserver.StreamRequestHandler):
    """Read an entire SMTP DATA payload, then disconnect without confirming it."""

    def handle(self):
        self.connection.settimeout(5)
        self.wfile.write(b"220 fault.example ESMTP\r\n")
        while line := self.rfile.readline():
            command = line.split()[0].upper()
            if command in {b"EHLO", b"HELO", b"MAIL", b"RCPT"}:
                self.wfile.write(b"250 fault.example\r\n")
            elif command == b"DATA":
                self.wfile.write(b"354 End data with <CR><LF>.<CR><LF>\r\n")
                chunks = []
                while line := self.rfile.readline():
                    if line == b".\r\n":
                        self.server.messages.append(b"".join(chunks))
                        return
                    chunks.append(line)
            else:
                raise AssertionError(f"Unexpected SMTP command: {command!r}")


def test_campaigns_end_to_end(notification_stack, tmp_path):
    endpoints = notification_stack
    run = uuid4().hex
    namespace = "notifications-e2e-" + run
    database_name = "notifications_test_" + uuid4().hex
    pg_base = (
        f"postgresql://{endpoints['POSTGRES_USER']}:{endpoints['POSTGRES_PASSWORD']}"
        f"@127.0.0.1:{endpoints['POSTGRES_PORT']}/"
    )
    processes = Processes(tmp_path)
    env = {
        "NOTIFICATIONS_DATABASE_URL": (pg_base + database_name).replace(
            "postgresql://", "postgresql+asyncpg://"
        ),
        "NOTIFICATIONS_PROCESS": "bootstrap",
        "NOTIFICATIONS_RABBITMQ_URL": endpoints["RABBITMQ_URL"],
        **limiter_env(endpoints, namespace),
    }
    with ExitStack() as resources:
        admin = resources.enter_context(
            psycopg.connect(pg_base + "notifications_test_admin", autocommit=True)
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        resources.callback(
            admin.execute,
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database_name)),
        )
        resources.callback(cleanup_topology, endpoints, namespace)
        resources.callback(remove_limiter, namespace)
        contacts = ContactsAuth(processes, endpoints, resources, admin, pg_base)
        env.update(contacts_env(contacts.url))
        resources.callback(processes.close)
        processes.run("notifications", env, "alembic", "upgrade", "head")
        api = processes.api(
            "notifications",
            {
                **env,
                "NOTIFICATIONS_PROCESS": "campaign-api",
                "NOTIFICATIONS_CAMPAIGN_API_KEY": CAMPAIGN_KEY,
            },
            "notifications.campaigns.app:create_campaign_app",
            "/openapi.json",
        )
        public_api = processes.api(
            "notifications",
            {
                **env,
                "NOTIFICATIONS_PROCESS": "api",
                "NOTIFICATIONS_JWT_SECRET": SECRET,
                "NOTIFICATIONS_JWT_ALGORITHM": "HS256",
                **introspection_env(contacts.url),
            },
            "notifications.main:create_app",
            "/openapi.json",
        )
        path = "/api/v1/notifications/campaigns/"
        campaign_id = uuid4()
        campaign = api + path + str(campaign_id)
        assert response(public_api + path + str(campaign_id))[0] == 404
        assert response(public_api + path + str(campaign_id) + "/batches", {})[0] == 404

        def query(statement, params=()):
            with psycopg.connect(pg_base + database_name) as db:
                return db.execute(statement, params).fetchall()

        def profile(*, marketing=True, deleted=False, email=True, missing_timezone=False):
            user_id = uuid4()
            timezone = None if missing_timezone else daytime_tz()
            query(
                "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,"
                "marketing,deleted_at) VALUES (%s,1,%s,false,false,%s,%s) "
                "RETURNING user_id",
                (
                    user_id,
                    timezone,
                    marketing and not deleted,
                    datetime.now(UTC) if deleted else None,
                ),
            )
            contacts.seed(
                user_id,
                f"{user_id}-{run}@example.org" if email else None,
                timezone,
                deleted=deleted,
            )
            return user_id

        literal_subject = "  Подборка {{ 7 * 7 }} <sale>  "
        literal_text = '<script>alert("x")</script>\n{{ 7 * 7 }} & предложение'

        def item(user_id, *, due=None):
            return {
                "user_id": str(user_id),
                "template_id": "personal_offer",
                "type": "email",
                "subject": literal_subject,
                "text": literal_text,
                "available_at": None if due is None else due.isoformat(),
            }

        def status(url=campaign):
            code, body = response(url)
            assert code == 200, body
            assert body["created_by_user_id"] == AUTHOR_ID
            assert body["total"] == sum(
                body[key] for key in ("sent", "failed", "skipped", "pending")
            )
            assert sum(body["skipped_reasons"].values()) == body["skipped"]
            assert sum(body["failed_reasons"].values()) == body["failed"]
            return body

        def emails():
            messages = http(endpoints["SMTP_API_URL"] + "/api/v1/messages")["messages"]
            return [m for m in messages if any(run in r["Address"] for r in m["To"])]

        enabled = profile()
        disabled = profile(marketing=False)
        deleted = profile(deleted=True)
        no_email = profile(email=False)
        no_timezone = profile(missing_timezone=True)
        future = profile()
        due = datetime.now(UTC) + timedelta(seconds=12)
        batch = {
            "batch_id": str(uuid4()),
            "created_by_user_id": AUTHOR_ID,
            "items": [
                item(enabled),
                item(disabled),
                item(deleted),
                item(no_email),
                item(no_timezone),
                item(future, due=due),
            ],
        }
        for key in (None, "wrong-test-key"):
            assert response(campaign + "/batches", batch, key=key)[0] == 401
        accepted = response(campaign + "/batches", batch)
        assert accepted == (
            202,
            {
                "campaign_id": str(campaign_id),
                "batch_id": batch["batch_id"],
                "status": "accepted",
                "item_count": 6,
            },
        )
        initial = status()
        assert (initial["total"], initial["pending"], initial["skipped"]) == (6, 3, 3)
        assert initial["skipped_reasons"] == {
            "subscription_disabled": 1,
            "profile_deleted": 1,
            "invalid_timezone": 1,
        }
        assert initial["status"] == "in_progress" and initial["intake_status"] == "open"
        missing = uuid4()
        second = {
            "batch_id": str(uuid4()),
            "created_by_user_id": AUTHOR_ID,
            "items": [item(missing)],
        }
        code, error = response(campaign + "/batches", second)
        assert code == 409 and error["error_code"] == "profile_not_ready"
        assert error["user_ids"] == [str(missing)]
        assert status()["accepted_batches"] == 1
        query(
            "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
            "VALUES (%s,1,%s,false,false,true) RETURNING user_id",
            (missing, daytime_tz()),
        )
        contacts.seed(missing, f"{missing}-{run}@example.org", daytime_tz())
        assert response(campaign + "/batches", second)[0] == 202
        code, error = response(campaign + "/close", {"expected_batches": 1})
        assert code == 409 and error["error_code"] == "batch_count_mismatch"
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
        start_publisher(processes, env, namespace)
        worker = start_worker(processes, env, endpoints, namespace, "bulk")
        wait_for(lambda: status()["sent"] >= 2)
        assert status()["status"] == "in_progress"  # Open intake is never a final result.
        closed = response(campaign + "/close", {"expected_batches": 2})
        assert closed == (200, {"campaign_id": str(campaign_id), "intake_status": "closed"})
        assert response(campaign + "/close", {"expected_batches": 2}) == closed
        query("UPDATE profiles SET marketing=false WHERE user_id=%s RETURNING user_id", (enabled,))
        for key in (None, "wrong-test-key"):
            assert response(campaign + "/batches", batch, key=key)[0] == 401
            assert response(campaign + "/close", {"expected_batches": 2}, key=key)[0] == 401
            assert response(campaign, key=key)[0] == 401
        assert response(campaign + "/batches", batch) == accepted
        changed = {
            **batch,
            "items": [{**batch["items"][0], "text": "Changed"}, *batch["items"][1:]],
        }
        code, error = response(campaign + "/batches", changed)
        assert code == 409 and error["error_code"] == "batch_content_conflict"
        code, error = response(
            campaign + "/batches",
            {"batch_id": str(uuid4()), "created_by_user_id": AUTHOR_ID, "items": [item(uuid4())]},
        )
        assert code == 409 and error["error_code"] == "campaign_closed"
        wait_for(lambda: status()["pending"] == 0)
        final = status()
        assert {key: final[key] for key in ("total", "sent", "failed", "skipped", "pending")} == {
            "total": 7,
            "sent": 3,
            "failed": 0,
            "skipped": 4,
            "pending": 0,
        }
        assert final["status"] == "partially_completed" and final["accepted_batches"] == 2
        assert query(
            "SELECT r.source_available_at, bool_and(a.started_at >= r.source_available_at) "
            "FROM campaign_recipients r JOIN delivery_attempts a USING(delivery_id) "
            "WHERE r.campaign_id=%s AND r.user_id=%s GROUP BY r.source_available_at",
            (campaign_id, future),
        ) == [(due, True)]
        assert query(
            "SELECT bool_and(o.published_at >= r.source_available_at) FROM campaign_recipients r "
            "JOIN outbox o USING(delivery_id) WHERE r.campaign_id=%s AND r.user_id=%s",
            (campaign_id, future),
        ) == [(True,)]
        wait_for(lambda: len(emails()) == 3)
        for message in emails():
            detail = http(endpoints["SMTP_API_URL"] + "/api/v1/message/" + message["ID"])
            assert detail["Subject"].strip() == literal_subject.strip()
            assert "{{ 7 * 7 }}" in detail["HTML"]
            assert "&lt;script&gt;" in detail["HTML"] and "<script>" not in detail["HTML"]
            assert "&amp; предложение" in detail["HTML"]
            assert "&lt;/script&gt;<br>{{ 7 * 7 }}" in detail["HTML"]
        assert query("SELECT count(*) FROM deliveries") == [(4,)]
        assert query("SELECT count(*) FROM deliveries WHERE user_id=%s", (no_timezone,)) == [(0,)]
        assert final["skipped_reasons"]["no_email"] == 1
        assert final["skipped_reasons"]["invalid_timezone"] == 1
        assert query(
            "SELECT DISTINCT payload->>'subject',payload->>'text' FROM deliveries "
            "WHERE payload IS NOT NULL"
        ) == [(literal_subject, literal_text)]

        # A genuine refused SMTP connection freezes the snapshot, then real Mailpit accepts retry.
        stop([worker])
        unavailable = resources.enter_context(socket.socket())
        unavailable.bind(("127.0.0.1", 0))
        bad_worker = start_worker(
            processes,
            env,
            endpoints,
            namespace,
            "bulk",
            NOTIFICATIONS_SMTP_PORT=str(unavailable.getsockname()[1]),
        )
        retry_user = profile()
        retry_campaign = api + path + str(uuid4())
        past_due = datetime.now(UTC) - timedelta(days=1)
        retry_batch = {
            "batch_id": str(uuid4()),
            "created_by_user_id": AUTHOR_ID,
            "items": [item(retry_user, due=past_due)],
        }
        assert response(retry_campaign + "/batches", retry_batch)[0] == 202
        assert response(retry_campaign + "/close", {"expected_batches": 1})[0] == 200
        wait_for(
            lambda: query(
                "SELECT 1 FROM deliveries WHERE user_id=%s "
                "AND attempt_count >= 1 AND status='retry'",
                (retry_user,),
            )
        )
        stop([bad_worker])
        snapshot = query(
            "SELECT payload,message_id FROM deliveries WHERE user_id=%s", (retry_user,)
        )
        contacts.update(retry_user, f"changed-{run}@example.org")
        worker = start_worker(processes, env, endpoints, namespace, "bulk")
        wait_for(lambda: status(retry_campaign)["pending"] == 0)
        assert status(retry_campaign)["status"] == "completed"
        assert (
            query("SELECT payload,message_id FROM deliveries WHERE user_id=%s", (retry_user,))
            == snapshot
        )
        assert query(
            "SELECT source_available_at FROM campaign_recipients WHERE user_id=%s", (retry_user,)
        ) == [(past_due,)]
        assert response(retry_campaign + "/batches", retry_batch)[0] == 202
        wait_for(lambda: len(emails()) == 4)
        retry_message = next(
            m for m in emails() if any(str(retry_user) in r["Address"] for r in m["To"])
        )
        assert retry_message["Subject"].strip() == literal_subject.strip()
        assert status(retry_campaign)["sent"] == 1
        # The worker uses actual TCP SMTP; the fixture withholds DATA's final response.
        # This tests uncertainty, not provider acceptance. Mailpit success was checked above.
        stop([worker])
        smtp = resources.enter_context(socketserver.TCPServer(("127.0.0.1", 0), UnconfirmedSMTP))
        smtp.messages = []
        smtp_thread = threading.Thread(target=smtp.serve_forever, daemon=True)
        smtp_thread.start()
        resources.callback(smtp_thread.join, 5)
        resources.callback(smtp.shutdown)
        uncertain_user, late_optout = profile(), profile()
        uncertain_campaign = api + path + str(uuid4())
        uncertain_batch = {
            "batch_id": str(uuid4()),
            "created_by_user_id": AUTHOR_ID,
            "items": [item(uncertain_user), item(late_optout)],
        }
        assert response(uncertain_campaign + "/batches", uncertain_batch)[0] == 202
        assert response(uncertain_campaign + "/close", {"expected_batches": 1})[0] == 200
        query(
            "UPDATE profiles SET marketing=false WHERE user_id=%s RETURNING user_id",
            (late_optout,),
        )
        start_worker(
            processes,
            env,
            endpoints,
            namespace,
            "bulk",
            NOTIFICATIONS_SMTP_PORT=str(smtp.server_address[1]),
        )
        wait_for(lambda: status(uncertain_campaign)["pending"] == 0)
        uncertain = status(uncertain_campaign)
        assert uncertain["status"] == "failed"
        assert (
            uncertain["total"],
            uncertain["sent"],
            uncertain["failed"],
            uncertain["skipped"],
        ) == (2, 0, 1, 1)
        assert uncertain["failed_reasons"] == {"smtp_uncertain": 1}
        assert uncertain["skipped_reasons"] == {"subscription_disabled": 1}
        assert len(smtp.messages) == 3 and len(set(smtp.messages)) == 1
        assert query(
            "SELECT count(*),bool_and(a.duplicate_risk),bool_and(a.outcome='uncertain') "
            "FROM delivery_attempts a JOIN deliveries d USING(delivery_id) WHERE d.user_id=%s",
            (uncertain_user,),
        ) == [(3, True, True)]
        assert query(
            "SELECT count(*) FROM delivery_attempts a JOIN deliveries d USING(delivery_id) "
            "WHERE d.user_id=%s",
            (late_optout,),
        ) == [(0,)]
        assert response(uncertain_campaign + "/batches", uncertain_batch)[0] == 202
        assert len(emails()) == 4
        # Freeze only this test's Auth process: preparation retries without SMTP attempts.
        outage_user = profile()
        outage_campaign = api + path + str(uuid4())
        outage_batch = {
            "batch_id": str(uuid4()),
            "created_by_user_id": AUTHOR_ID,
            "items": [item(outage_user)],
        }
        stop(
            [
                child
                for child, _, _ in processes.children
                if child.poll() is None
                and child is not contacts.process
                and "notifications.delivery.worker" in child.args
            ]
        )
        start_worker(processes, env, endpoints, namespace, "bulk")
        os.killpg(contacts.process.pid, signal.SIGSTOP)
        try:
            assert response(outage_campaign + "/batches", outage_batch)[0] == 202
            assert response(outage_campaign + "/close", {"expected_batches": 1})[0] == 200
            wait_for(
                lambda: query(
                    "SELECT 1 FROM deliveries WHERE user_id=%s AND next_attempt_at > now()",
                    (outage_user,),
                )
            )
            assert query(
                "SELECT attempt_count,payload FROM deliveries WHERE user_id=%s", (outage_user,)
            ) == [(0, None)]
            assert status(outage_campaign)["pending"] == 1
        finally:
            os.killpg(contacts.process.pid, signal.SIGCONT)
        wait_for(lambda: status(outage_campaign)["pending"] == 0)
        assert status(outage_campaign)["status"] == "completed"
        wait_for(lambda: len(emails()) == 5)
        print(
            "campaign_total=7; sent=3; skipped=4; retry_sent=1; "
            "auth_recovery_sent=1; SMTP_messages=5; "
            f"uncertain=1; worker_skipped=1; UUID={run}"
        )
