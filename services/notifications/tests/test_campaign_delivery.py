"""Campaign delivery uses real PostgreSQL, isolated Redis and SMTP acceptance."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from contact_fixtures import contact, contact_profile
from jinja2 import FileSystemLoader
from sqlalchemy import select
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import expire, mail_messages, make_due
from test_delivery import limiter_settings as limiter_settings
from test_delivery_windows import clock

from notifications.config import Settings
from notifications.db.models import (
    Campaign,
    CampaignBatch,
    CampaignRecipient,
    Delivery,
    DeliveryAttempt,
    Outbox,
    Profile,
)
from notifications.db.session import Database
from notifications.delivery.worker import DeliveryWorker
from notifications.maintenance import Maintenance

SUBJECT = "  Предложение {{ first_name }} <b>личное</b>  "
BODY = "<script>alert(1)</script> & {{ 7 * 7 }}\r\n\rsecond\n"


@pytest.fixture
async def campaign(database_url, endpoints, delivery_limiter, monkeypatch):
    db = Database(database_url)
    settings = Settings(
        process="worker",
        database_url=database_url,
        rabbitmq_url=endpoints["RABBITMQ_URL"],
        smtp_host=endpoints["SMTP_HOST"],
        smtp_port=int(endpoints["SMTP_PORT"]),
        smtp_from="sender@example.org",
        smtp_starttls=False,
        fallback_locale="ru",
        fallback_tz="UTC",
    )
    worker = DeliveryWorker(
        db,
        settings,
        send_class="bulk",
        retry_seconds=1,
        max_attempts=2,
        limiter=delivery_limiter,
        max_concurrency=2,
        unavailable_retry_seconds=7,
    )
    await clock(worker, monkeypatch, datetime(2026, 10, 10, 12, tzinfo=UTC))
    cid, bid, uid, did = uuid4(), uuid4(), uuid4(), uuid4()
    source = datetime(2026, 1, 1, tzinfo=UTC)
    effective = source + timedelta(days=1)
    async with db.transaction() as s:
        s.add(
            contact_profile(
                user_id=uid,
                user_version=1,
                email=f"{uid}@example.org",
                first_name="<Anna>",
                last_name="& Smith",
                tz="UTC",
                likes=False,
                new_films=False,
                marketing=True,
            )
        )
        s.add(Campaign(campaign_id=cid, created_at=effective))
        await s.flush()
        s.add(
            CampaignBatch(
                campaign_id=cid,
                batch_id=bid,
                request_hash="a" * 64,
                item_count=1,
                accepted_at=effective,
            )
        )
        s.add(
            Delivery(
                delivery_id=did,
                logical_key=str(did),
                user_id=uid,
                kind="external",
                external_send_class="bulk",
                required_subscription="marketing",
                next_attempt_at=effective,
            )
        )
        await s.flush()
        s.add(
            CampaignRecipient(
                campaign_id=cid,
                batch_id=bid,
                user_id=uid,
                delivery_id=did,
                template_id="personal_offer",
                channel="email",
                subject=SUBJECT,
                text=BODY,
                source_available_at=source,
                effective_available_at=effective,
            )
        )
        s.add(Outbox(delivery_id=did, available_at=effective))
    try:
        yield db, worker, did, uid, cid
    finally:
        await db.close()


async def test_external_literal_snapshot_and_real_smtp(campaign, endpoints):
    db, worker, did, uid, _ = campaign
    async with db.transaction() as s:
        contact(uid).first_name = "<Current>"
    snapshot = await worker.prepare_delivery(did)
    assert snapshot.payload["subject"] == SUBJECT
    assert snapshot.payload["to"] == f"{uid}@example.org"
    assert snapshot.payload["reviews"] == []
    assert "&lt;Current&gt; &amp; Smith" in snapshot.payload["html"]
    assert (
        "&lt;script&gt;alert(1)&lt;/script&gt; &amp; {{ 7 * 7 }}<br><br>second<br>"
        in snapshot.payload["html"]
    )
    await worker.send_delivery(did)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert (delivery.status, delivery.send_class, delivery.required_subscription) == (
            "sent",
            "bulk",
            "marketing",
        )
        assert delivery.attempt_count == 1 and delivery.terminal_reason is None
    messages = await mail_messages(endpoints, uid)
    assert len(messages) == 1
    async with httpx.AsyncClient() as client:
        result = await client.get(
            endpoints["SMTP_API_URL"] + "/api/v1/message/" + messages[0]["ID"]
        )
        result.raise_for_status()
    assert result.json()["HTML"].strip() == snapshot.payload["html"].strip()
    assert result.json()["Subject"].strip() == SUBJECT.strip()


async def test_erased_terminal_campaign_cannot_be_prepared_or_sent_again(campaign, endpoints):
    db, worker, did, uid, cid = campaign
    await worker.send_delivery(did)
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        assert delivery.status == "sent"
        delivery.terminal_at = datetime.now(UTC) - timedelta(days=2)
    assert await Maintenance(db, batch_size=1, retention_seconds=86400).run_once() == (0, 1)
    assert await worker.prepare_delivery(did) is None
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        recipient = await session.get(CampaignRecipient, (cid, uid))
        attempt = await session.scalar(select(DeliveryAttempt))
        assert delivery.payload is None and delivery.attempt_count == 1
        assert recipient.subject is None and recipient.text is None
        assert attempt.outcome == "accepted" and attempt.finished_at is not None


async def test_renderer_rejects_missing_active_campaign_content(campaign):
    db, worker, did, uid, cid = campaign
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (cid, uid))
        recipient.subject = recipient.text = None
    with pytest.raises(ValueError, match="External notification content has been erased"):
        await worker.prepare_delivery(did)
    async with db.transaction() as session:
        delivery = await session.get(Delivery, did)
        assert delivery.status == "pending" and delivery.payload is None


async def test_external_renderer_exposes_literal_escaped_subject(campaign, monkeypatch, tmp_path):
    import notifications.delivery.render as renderer

    db, worker, did, uid, _ = campaign
    (tmp_path / "personal_offer.html").write_text(
        "<h1>{{ subject }}</h1><p>{{ first_name }} {{ last_name }}</p><p>{{ text }}</p>",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        renderer,
        "ENVIRONMENT",
        renderer.ENVIRONMENT.overlay(loader=FileSystemLoader(tmp_path), cache_size=0),
    )
    async with db.transaction() as session:
        payload = await renderer.render(
            session,
            await session.get(Delivery, did),
            await session.get(Profile, uid),
            worker.settings,
            contact(uid),
        )
    assert payload["subject"] == SUBJECT
    assert "<h1>  Предложение {{ first_name }} &lt;b&gt;личное&lt;/b&gt;  </h1>" in payload["html"]
    assert "<p>&lt;Anna&gt; &amp; Smith</p>" in payload["html"]
    assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; {{ 7 * 7 }}" in payload["html"]


@pytest.mark.parametrize("gate", ["before_prepare", "after_prepare", "during_redis"])
@pytest.mark.parametrize(
    "reason,status",
    [
        ("subscription_disabled", "cancelled"),
        ("profile_deleted", "cancelled"),
        ("invalid_timezone", "skipped"),
    ],
)
async def test_current_permission_persists_reason_without_attempt(
    campaign, endpoints, monkeypatch, gate, reason, status
):
    db, worker, did, uid, _ = campaign

    async def change_profile():
        async with db.transaction() as s:
            profile = await s.get(Profile, uid)
            if reason == "subscription_disabled":
                profile.marketing = False
            elif reason == "profile_deleted":
                profile.deleted_at = datetime.now(UTC)
                profile.marketing = False
                contact(uid).email = None
            else:
                profile.tz = None

    if gate == "after_prepare":
        await worker.prepare_delivery(did)
    if gate == "during_redis":
        acquire = worker.limiter.acquire

        async def changed(send_class):
            decision = await acquire(send_class)
            assert decision.allowed
            await change_profile()
            return decision

        monkeypatch.setattr(worker.limiter, "acquire", changed)
    else:
        await change_profile()
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert (delivery.status, delivery.terminal_reason) == (status, reason)
        assert delivery.attempt_count == 0
        assert delivery.lease_token is delivery.lease_until is None
        assert await s.scalar(select(DeliveryAttempt)) is None
    assert await mail_messages(endpoints, uid) == []


async def test_accepted_policy_survives_registry_changes(campaign, endpoints, monkeypatch):
    import notifications.template_registry as registry

    db, worker, did, uid, _ = campaign
    templates = dict(registry._TEMPLATES)
    templates["personal_offer"] = replace(
        templates["personal_offer"], send_class="transactional", required_subscription="likes"
    )
    monkeypatch.setattr(registry, "_TEMPLATES", templates)
    now = datetime(2026, 10, 10, 9, tzinfo=UTC)
    await clock(worker, monkeypatch, now)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.next_attempt_at == now.replace(hour=10)
        assert delivery.status == "prepared" and delivery.attempt_count == 0
        assert (await s.scalar(select(Outbox))).available_at == delivery.next_attempt_at
    await make_due(db, did)
    await clock(worker, monkeypatch, now.replace(hour=10))
    await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1


async def test_retry_reuses_snapshot_after_template_and_registry_changes(
    campaign, endpoints, monkeypatch, tmp_path
):
    import notifications.delivery.render as renderer
    import notifications.template_registry as registry

    db, worker, did, uid, _ = campaign
    template = tmp_path / "personal_offer.html"
    template.write_text("<p>Original {{ first_name }} {{ text }}</p>", encoding="utf-8")
    monkeypatch.setattr(
        renderer,
        "ENVIRONMENT",
        renderer.ENVIRONMENT.overlay(loader=FileSystemLoader(tmp_path), cache_size=0),
    )
    snapshot = await worker.prepare_delivery(did)
    smtp_port = worker.settings.smtp_port
    worker.settings.smtp_port = 1
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "retry" and delivery.terminal_reason is None
        message_id = delivery.message_id
    template.write_text("<p>Changed</p>", encoding="utf-8")
    monkeypatch.setattr(registry, "_TEMPLATES", {})
    async with db.transaction() as s:
        contact(uid).first_name = "Changed profile"
    worker.settings.smtp_port = smtp_port
    await make_due(db, did)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "sent" and delivery.attempt_count == 2
        assert delivery.payload == snapshot.payload and delivery.message_id == message_id
        recipient = await s.scalar(select(CampaignRecipient))
        assert recipient.source_available_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert recipient.effective_available_at == datetime(2026, 1, 2, tzinfo=UTC)
    assert len(await mail_messages(endpoints, uid)) == 1


async def test_redis_deferral_preserves_source_availability(campaign, endpoints):
    db, worker, did, uid, _ = campaign
    await worker.limiter._redis.delete(worker.limiter.settings.key)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        recipient = await s.scalar(select(CampaignRecipient))
        assert delivery.next_attempt_at == datetime(2026, 10, 10, 12, 0, 7, tzinfo=UTC)
        assert (await s.scalar(select(Outbox))).available_at == delivery.next_attempt_at
        assert delivery.attempt_count == 0 and delivery.lease_token is None
        assert recipient.source_available_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert recipient.effective_available_at == datetime(2026, 1, 2, tzinfo=UTC)
    assert await mail_messages(endpoints, uid) == []


async def test_known_retry_exhaustion_has_durable_reason(campaign):
    db, worker, did, _, _ = campaign
    worker.settings.smtp_port = 1
    await worker.send_delivery(did)
    await make_due(db, did)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert (delivery.status, delivery.terminal_reason, delivery.attempt_count) == (
            "failed",
            "attempts_exhausted",
            2,
        )
        attempts = list(
            await s.scalars(select(DeliveryAttempt).order_by(DeliveryAttempt.attempt_number))
        )
        assert [attempt.outcome for attempt in attempts] == ["retry", "failed"]
        assert not any(attempt.duplicate_risk for attempt in attempts)


async def test_permanent_failure_has_durable_reason(campaign):
    db, worker, did, uid, _ = campaign
    async with db.transaction() as s:
        contact(uid).email = "invalid-email"
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert (delivery.status, delivery.terminal_reason, delivery.attempt_count) == (
            "failed",
            "smtp_permanent",
            1,
        )


@asynccontextmanager
async def smtp_result(response):
    async def server(reader, writer):
        writer.write(b"220 local test\r\n")
        await writer.drain()
        in_data = False
        try:
            while line := await reader.readline():
                if in_data:
                    if line != b".\r\n":
                        continue
                    if response is not None:
                        writer.write(response)
                        await writer.drain()
                    break
                if line.upper().startswith(b"DATA"):
                    in_data = True
                    writer.write(b"354 send mail\r\n")
                else:
                    writer.write(b"250 OK\r\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    smtp = await asyncio.start_server(server, "127.0.0.1", 0)
    async with smtp:
        yield smtp.sockets[0].getsockname()[1]


@pytest.mark.parametrize(
    "response,reason,outcome,risk",
    [
        (b"550 permanent failure\r\n", "smtp_permanent", "failed", False),
        (b"451 temporary failure\r\n", "attempts_exhausted", "failed", False),
        (None, "smtp_uncertain", "uncertain", True),
    ],
)
async def test_final_smtp_response_has_durable_reason(campaign, response, reason, outcome, risk):
    db, worker, did, _, _ = campaign
    worker.max_attempts = 1
    async with smtp_result(response) as port:
        worker.settings.smtp_port = port
        await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        assert (delivery.status, delivery.terminal_reason, delivery.attempt_count) == (
            "failed",
            reason,
            1,
        )
        assert (attempt.outcome, attempt.duplicate_risk) == (outcome, risk)


async def test_exhausted_lease_recovery_records_uncertain_reason(campaign, endpoints, monkeypatch):
    db, worker, did, uid, _ = campaign
    worker.max_attempts = 1

    async def crash(*args):
        raise RuntimeError("lost final commit")

    monkeypatch.setattr(worker, "finish", crash)
    with pytest.raises(RuntimeError, match="lost final commit"):
        await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    await expire(db, did)
    assert await worker.recover() == 1
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        attempt = await s.scalar(select(DeliveryAttempt))
        assert (delivery.status, delivery.terminal_reason) == ("failed", "smtp_uncertain")
        assert attempt.outcome == "uncertain" and attempt.duplicate_risk
        assert delivery.lease_token is delivery.lease_until is None


@pytest.mark.parametrize("final_response", [b"550 permanent\r\n", b"451 temporary\r\n", None])
async def test_recovered_uncertainty_survives_failure_but_not_acceptance(
    campaign, endpoints, monkeypatch, final_response
):
    db, worker, did, uid, _ = campaign
    finish = worker.finish

    async def crash(*args):
        raise RuntimeError("lost final commit")

    monkeypatch.setattr(worker, "finish", crash)
    with pytest.raises(RuntimeError, match="lost final commit"):
        await worker.send_delivery(did)
    assert len(await mail_messages(endpoints, uid)) == 1
    await expire(db, did)
    assert await worker.recover() == 1
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        payload, message_id = delivery.payload, delivery.message_id
        assert delivery.status == "retry" and delivery.terminal_reason is None
        assert (await s.scalar(select(DeliveryAttempt))).outcome == "uncertain"
        contact(uid).first_name = "Changed after uncertain delivery"
    monkeypatch.setattr(worker, "finish", finish)
    await make_due(db, did)
    if final_response is None:
        await worker.send_delivery(did)
    else:
        async with smtp_result(final_response) as port:
            worker.settings.smtp_port = port
            await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        expected = ("sent", None) if final_response is None else ("failed", "smtp_uncertain")
        assert (delivery.status, delivery.terminal_reason) == expected
        assert delivery.payload == payload and delivery.message_id == message_id
        assert delivery.attempt_count == 2
        recipient = await s.scalar(select(CampaignRecipient))
        assert recipient.source_available_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert recipient.effective_available_at == datetime(2026, 1, 2, tzinfo=UTC)
    messages = await mail_messages(endpoints, uid)
    assert len(messages) == (2 if final_response is None else 1)
    assert len({message["MessageID"] for message in messages}) == 1


async def test_billing_notification_sent_to_smtp_at_night_without_marketing(
    database_url, endpoints, delivery_limiter, monkeypatch
):
    """T-21 (NTF-FR-01): billing_notification доставляется в Mailpit ночью при marketing=False."""
    db = Database(database_url)
    settings = Settings(
        process="worker",
        database_url=database_url,
        rabbitmq_url=endpoints["RABBITMQ_URL"],
        smtp_host=endpoints["SMTP_HOST"],
        smtp_port=int(endpoints["SMTP_PORT"]),
        smtp_from="sender@example.org",
        smtp_starttls=False,
        fallback_locale="ru",
        fallback_tz="UTC",
    )
    worker = DeliveryWorker(
        db,
        settings,
        send_class="transactional",
        retry_seconds=1,
        max_attempts=2,
        limiter=delivery_limiter,
        max_concurrency=2,
        unavailable_retry_seconds=7,
    )
    night_utc = datetime(2026, 11, 5, 2, 15, tzinfo=UTC)
    await clock(worker, monkeypatch, night_utc)
    cid, bid, uid, did = uuid4(), uuid4(), uuid4(), uuid4()
    async with db.transaction() as s:
        s.add(
            contact_profile(
                user_id=uid,
                user_version=1,
                email=f"{uid}@example.org",
                first_name="Иван",
                last_name="Петров",
                tz="Europe/Moscow",
                likes=False,
                new_films=False,
                marketing=False,
            )
        )
        s.add(Campaign(campaign_id=cid, created_at=night_utc))
        await s.flush()
        s.add(
            CampaignBatch(
                campaign_id=cid,
                batch_id=bid,
                request_hash="b" * 64,
                item_count=1,
                accepted_at=night_utc,
            )
        )
        s.add(
            Delivery(
                delivery_id=did,
                logical_key=f"campaign:{cid}:{uid}",
                user_id=uid,
                kind="external",
                external_send_class="transactional",
                required_subscription=None,
                next_attempt_at=night_utc,
            )
        )
        await s.flush()
        s.add(
            CampaignRecipient(
                campaign_id=cid,
                batch_id=bid,
                user_id=uid,
                delivery_id=did,
                template_id="billing_notification",
                channel="email",
                subject="Подписка «Премиум» оплачена",
                text="Оплата подтверждена.\nДоступ открыт до 05.12.2026 05:15 МСК.",
                source_available_at=None,
                effective_available_at=night_utc,
            )
        )
        s.add(Outbox(delivery_id=did, available_at=night_utc))
    try:
        await worker.send_delivery(did)
        async with db.transaction() as s:
            delivery = await s.get(Delivery, did)
            assert delivery is not None
            assert (delivery.status, delivery.send_class, delivery.required_subscription) == (
                "sent",
                "transactional",
                None,
            )
        messages = await mail_messages(endpoints, uid)
        assert len(messages) == 1
        assert messages[0]["Subject"] == "Подписка «Премиум» оплачена"
    finally:
        await db.close()


async def test_cancelled_billing_notification_is_skipped_by_worker_without_smtp(
    database_url, endpoints, delivery_limiter, monkeypatch
):
    """T-24 (NTF-FR-02, NTF-API-02): отменённая доставка не отправляется воркером в SMTP."""
    from notifications.campaigns.service import cancel_campaign

    db = Database(database_url)
    settings = Settings(
        process="worker",
        database_url=database_url,
        rabbitmq_url=endpoints["RABBITMQ_URL"],
        smtp_host=endpoints["SMTP_HOST"],
        smtp_port=int(endpoints["SMTP_PORT"]),
        smtp_from="sender@example.org",
        smtp_starttls=False,
        fallback_locale="ru",
        fallback_tz="UTC",
    )
    worker = DeliveryWorker(
        db,
        settings,
        send_class="transactional",
        retry_seconds=1,
        max_attempts=2,
        limiter=delivery_limiter,
        max_concurrency=2,
        unavailable_retry_seconds=7,
    )
    now_utc = datetime(2026, 11, 6, 12, 0, tzinfo=UTC)
    await clock(worker, monkeypatch, now_utc)
    cid, bid, uid, did = uuid4(), uuid4(), uuid4(), uuid4()
    async with db.transaction() as s:
        s.add(
            contact_profile(
                user_id=uid,
                user_version=1,
                email=f"{uid}@example.org",
                first_name="Анна",
                last_name="Смирнова",
                tz="Europe/Moscow",
                likes=False,
                new_films=False,
                marketing=False,
            )
        )
        s.add(Campaign(campaign_id=cid, created_by_user_id=uid, created_at=now_utc))
        await s.flush()
        s.add(
            CampaignBatch(
                campaign_id=cid,
                batch_id=bid,
                request_hash="c" * 64,
                item_count=1,
                accepted_at=now_utc,
            )
        )
        s.add(
            Delivery(
                delivery_id=did,
                logical_key=f"campaign:{cid}:{uid}",
                user_id=uid,
                kind="external",
                external_send_class="transactional",
                required_subscription=None,
                next_attempt_at=now_utc,
            )
        )
        await s.flush()
        s.add(
            CampaignRecipient(
                campaign_id=cid,
                batch_id=bid,
                user_id=uid,
                delivery_id=did,
                template_id="billing_notification",
                channel="email",
                subject="Срок подписки «Премиум» закончился",
                text="Срок действия вашей подписки истёк.",
                source_available_at=now_utc,
                effective_available_at=now_utc,
            )
        )
        s.add(Outbox(delivery_id=did, available_at=now_utc))

    try:
        # Отменяем кампанию до запуска воркера
        async with db.transaction() as s:
            await cancel_campaign(s, cid)

        await worker.send_delivery(did)
        async with db.transaction() as s:
            delivery = await s.get(Delivery, did)
            assert delivery is not None
            assert (delivery.status, delivery.terminal_reason, delivery.attempt_count) == (
                "cancelled",
                "cancelled_by_sender",
                0,
            )
        assert await mail_messages(endpoints, uid) == []
    finally:
        await db.close()
