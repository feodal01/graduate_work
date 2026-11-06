from dataclasses import replace
from datetime import UTC, datetime, time
from typing import Any
from uuid import uuid4

import httpx
import pytest
from contact_fixtures import contact_profile
from pydantic import SecretStr
from sqlalchemy import func, select

from notifications.config import Settings
from notifications.db.models import (
    Campaign,
    CampaignBatch,
    CampaignRecipient,
    Delivery,
    Outbox,
    Profile,
)

URL = "/api/v1/notifications/campaigns"
AUTHOR = uuid4()
API_KEY = "test-only-campaign-key"


def item(user_id=None, **changes) -> dict[str, Any]:
    return dict(
        user_id=str(user_id or uuid4()),
        template_id="personal_offer",
        subject=" Subject ",
        text="Text\n{{ literal }} <b>html</b>",
        type="email",
        **changes,
    )


def batch(*items) -> dict[str, Any]:
    return dict(batch_id=str(uuid4()), created_by_user_id=str(AUTHOR), items=list(items))


@pytest.fixture
async def campaign_api(database_url):
    from notifications.campaigns.app import create_campaign_app

    app = create_campaign_app(
        Settings(
            process="campaign-api",
            database_url=database_url,
            campaign_api_key=SecretStr(API_KEY),
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://campaigns",
            headers={"X-API-Key": API_KEY},
        ) as client:
            yield client, app.state.database


async def profiles(db, *users, **changes):
    async with db.transaction() as session:
        for user in users:
            values = dict(
                user_id=user,
                user_version=1,
                email="test@example.org",
                likes=True,
                new_films=True,
                marketing=True,
                tz="UTC",
            )
            values.update(changes)
            session.add(contact_profile(**values))


async def counts(db):
    async with db.transaction() as session:
        return [
            await session.scalar(select(func.count()).select_from(model))
            for model in (Campaign, CampaignBatch, CampaignRecipient, Delivery, Outbox)
        ]


@pytest.mark.parametrize("batch_size", [1, 2, 101])
async def test_campaign_batch_size_uses_app_settings(database_url, batch_size):
    from notifications.campaigns.app import create_campaign_app

    app = create_campaign_app(
        Settings(
            process="campaign-api",
            database_url=database_url,
            campaign_api_key=SecretStr(API_KEY),
            campaign_batch_size=batch_size,
        )
    )
    async with app.router.lifespan_context(app):
        users = [uuid4() for _ in range(batch_size + 1)]
        await profiles(app.state.database, *users)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://campaigns",
            headers={"X-API-Key": API_KEY},
        ) as client:
            too_many = await client.post(
                f"{URL}/{uuid4()}/batches", json=batch(*(item(uid) for uid in users))
            )
            assert too_many.status_code == 422
            assert too_many.json()["errors"][0]["loc"] == ["items"]
            assert await counts(app.state.database) == [0, 0, 0, 0, 0]
            accepted = await client.post(
                f"{URL}/{uuid4()}/batches", json=batch(*(item(uid) for uid in users[:-1]))
            )
            assert accepted.status_code == 202, accepted.text
            assert accepted.json()["item_count"] == batch_size


async def test_accept_replay_conflict_and_frozen_policy(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}/batches"
    first = await api.post(endpoint, json=body)
    assert first.status_code == 202, first.text
    assert first.json() == dict(
        campaign_id=str(campaign), batch_id=body["batch_id"], status="accepted", item_count=1
    )
    assert (await api.post(endpoint, json=body)).json() == first.json()
    async with db.transaction() as session:
        recipient = await session.scalar(select(CampaignRecipient))
        delivery = await session.scalar(select(Delivery))
        outbox = await session.scalar(select(Outbox))
        assert recipient.subject == body["items"][0]["subject"]
        assert recipient.source_available_at is None
        assert recipient.delivery_id == delivery.delivery_id == outbox.delivery_id
        assert (delivery.kind, delivery.send_class, delivery.required_subscription) == (
            "external",
            "bulk",
            "marketing",
        )
        assert delivery.logical_key == f"campaign:{campaign}:{user}"
        assert outbox.available_at == delivery.next_attempt_at >= recipient.effective_available_at
    body["items"][0]["text"] += "changed"
    response = await api.post(endpoint, json=body)
    assert (response.status_code, response.json()["error_code"]) == (409, "batch_content_conflict")
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_replay_after_deleted_optout_and_close(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}/batches"
    first = await api.post(endpoint, json=body)
    async with db.transaction() as session:
        profile = await session.get(Profile, user)
        profile.marketing = False
    assert (await api.post(endpoint, json=body)).json() == first.json()
    async with db.transaction() as session:
        profile = await session.get(Profile, user)
        profile.deleted_at = datetime.now(UTC)
        profile.likes = profile.new_films = False
    assert (
        await api.post(f"{URL}/{campaign}/close", json={"expected_batches": 1})
    ).status_code == 200
    assert (await api.post(endpoint, json=body)).json() == first.json()
    response = await api.post(endpoint, json=batch(item(user)))
    assert response.json()["error_code"] == "campaign_closed"


@pytest.mark.parametrize("registry_change", ["retired", "removed", "channel"])
async def test_saved_replay_survives_registry_change(campaign_api, monkeypatch, registry_change):
    import notifications.template_registry as registry

    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}/batches"
    first = await api.post(endpoint, json=body)
    assert first.status_code == 202
    assert (
        await api.post(f"{URL}/{campaign}/close", json={"expected_batches": 1})
    ).status_code == 200
    async with db.transaction() as session:
        profile = await session.get(Profile, user)
        profile.likes = profile.new_films = profile.marketing = False
        profile.deleted_at = datetime.now(UTC)
        before = [
            (await session.execute(select(model.__table__))).mappings().all()
            for model in (Campaign, CampaignBatch, CampaignRecipient, Delivery, Outbox)
        ]
    templates = dict(registry._TEMPLATES)
    if registry_change == "removed":
        del templates["personal_offer"]
    else:
        changes = {"external": False} if registry_change == "retired" else {"channel": "push"}
        templates["personal_offer"] = replace(
            templates["personal_offer"],
            send_class="transactional",
            required_subscription="likes",
            **changes,
        )
    monkeypatch.setattr(registry, "_TEMPLATES", templates)
    replay = await api.post(endpoint, json=body)
    assert replay.status_code == 202, replay.text
    assert replay.json() == first.json()
    body["items"][0]["text"] += " changed"
    conflict = await api.post(endpoint, json=body)
    assert (conflict.status_code, conflict.json()["error_code"]) == (409, "batch_content_conflict")
    async with db.transaction() as session:
        after = [
            (await session.execute(select(model.__table__))).mappings().all()
            for model in (Campaign, CampaignBatch, CampaignRecipient, Delivery, Outbox)
        ]
    assert after == before


@pytest.mark.parametrize(
    "invalid_template", ["unknown", "internal", "retired", "removed", "channel", "no_schema"]
)
async def test_new_batch_checks_template_admission_atomically(
    campaign_api, monkeypatch, invalid_template
):
    import notifications.template_registry as registry

    api, db = campaign_api
    user = uuid4()
    await profiles(db, user)
    templates = dict(registry._TEMPLATES)
    template_id, field = "personal_offer", "template_id"
    if invalid_template == "unknown":
        template_id = "missing"
    elif invalid_template == "internal":
        template_id = "welcome"
    elif invalid_template == "removed":
        del templates[template_id]
    elif invalid_template == "retired":
        templates[template_id] = replace(templates[template_id], external=False)
    elif invalid_template == "channel":
        templates[template_id] = replace(templates[template_id], channel="push")
        field = "type"
    else:
        template_id = "another_offer"
        templates[template_id] = replace(templates["personal_offer"], template_id=template_id)
    monkeypatch.setattr(registry, "_TEMPLATES", templates)
    invalid = item()
    invalid["template_id"] = template_id
    # Missing profiles must not hide an invalid template in a new batch.
    entries = (
        [invalid]
        if invalid_template in {"retired", "removed", "channel"}
        else [item(user), invalid]
    )
    response = await api.post(f"{URL}/{uuid4()}/batches", json=batch(*entries))
    assert response.status_code == 422, response.text
    assert response.json() == {
        "error_code": "validation_error",
        "detail": "Invalid request",
        "errors": [
            {
                "loc": ["items", len(entries) - 1, field],
                "type": "value_error",
                "msg": "Invalid value",
            }
        ],
    }
    assert await counts(db) == [0, 0, 0, 0, 0]


async def test_missing_profiles_and_duplicates_are_atomic(campaign_api):
    api, db = campaign_api
    known, missing, campaign = uuid4(), uuid4(), uuid4()
    await profiles(db, known)
    endpoint = f"{URL}/{campaign}/batches"
    response = await api.post(endpoint, json=batch(item(known), item(missing)))
    assert (response.status_code, response.json()["error_code"]) == (409, "profile_not_ready")
    assert response.json()["user_ids"] == [str(missing)]
    assert await counts(db) == [0, 0, 0, 0, 0]
    response = await api.post(endpoint, json=batch(item(known), item(known)))
    assert (response.status_code, response.json()["error_code"]) == (422, "validation_error")
    assert response.json()["errors"][0]["loc"] == ["items", 1, "user_id"]
    assert await counts(db) == [0, 0, 0, 0, 0]
    assert (await api.post(endpoint, json=batch(item(known)))).status_code == 202
    response = await api.post(endpoint, json=batch(item(known)))
    assert response.json()["error_code"] == "recipient_conflict"
    assert await counts(db) == [1, 1, 1, 1, 1]


@pytest.mark.parametrize(
    "field,value",
    [
        ("subject", ""),
        ("subject", "  "),
        ("subject", "secret\r\nBcc: victim"),
        ("subject", "\x00secret"),
        ("subject", "s" * 201),
        ("subject", 123),
        ("text", "\t\n"),
        ("text", "x" * 20001),
        ("text", False),
        ("template_id", "welcome"),
        ("template_id", "missing"),
        ("template_id", "x" * 65),
        ("type", "sms"),
        ("type", "push"),
        ("type", None),
        ("send_class", "transactional"),
        ("available_at", "2026-01-01T12:00:00"),
        ("available_at", 1234567890),
        ("available_at", "1234567890"),
        ("available_at", True),
        ("user_id", "bad"),
    ],
)
async def test_invalid_item_rejects_whole_batch_without_input_leak(campaign_api, field, value):
    api, db = campaign_api
    user = uuid4()
    await profiles(db, user)
    invalid = item()
    invalid[field] = value
    response = await api.post(f"{URL}/{uuid4()}/batches", json=batch(item(user), invalid))
    assert response.status_code == 422, response.text
    error = response.json()
    assert error["error_code"] == "validation_error"
    assert all(set(entry) == {"loc", "type", "msg"} for entry in error["errors"])
    assert "secret" not in response.text and "{{ literal }}" not in response.text
    assert await counts(db) == [0, 0, 0, 0, 0]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"batch_id": str(uuid4()), "items": []},
        {"batch_id": str(uuid4()), "items": [item()] * 101},
        {"batch_id": str(uuid4()), "items": [item()], "unknown": "secret"},
    ],
)
async def test_invalid_envelope(campaign_api, payload):
    api, _ = campaign_api
    response = await api.post(f"{URL}/{uuid4()}/batches", json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize("content_length", [None, "1", "1048577"])
async def test_streaming_body_limit(campaign_api, content_length):
    api, db = campaign_api

    async def chunks():
        for _ in range(17):
            yield b" " * 65536

    response = await api.post(
        f"{URL}/{uuid4()}/batches",
        content=chunks(),
        headers={} if content_length is None else {"content-length": content_length},
    )
    assert (response.status_code, response.json()["error_code"]) == (413, "request_too_large")
    assert await counts(db) == [0, 0, 0, 0, 0]


async def test_time_uuid_canonicalization_and_item_order(campaign_api):
    api, db = campaign_api
    users, campaign = [uuid4(), uuid4()], uuid4()
    await profiles(db, *users)
    body = batch(item(users[0]), item(users[1], available_at="2090-01-01T12:00:00+03:00"))
    endpoint = f"{URL}/{campaign}/batches"
    assert (await api.post(endpoint, json=body)).status_code == 202
    body["items"][0]["available_at"] = None
    body["items"][0]["user_id"] = str(users[0]).upper()
    body["batch_id"] = body["batch_id"].upper()
    body["items"][1]["available_at"] = "2090-01-01T09:00:00Z"
    assert (await api.post(endpoint, json=body)).status_code == 202
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, users[1]))
        assert (
            recipient.source_available_at
            == recipient.effective_available_at
            == datetime(2090, 1, 1, 9, tzinfo=UTC)
        )
    body["items"].reverse()
    assert (await api.post(endpoint, json=body)).json()["error_code"] == "batch_content_conflict"


async def test_skip_reasons_close_and_statistics(campaign_api):
    api, db = campaign_api
    users, campaign = [uuid4() for _ in range(4)], uuid4()
    await profiles(
        db, users[0], deleted_at=datetime.now(UTC), likes=False, new_films=False, marketing=False
    )
    await profiles(db, users[1], marketing=False, email=None)
    await profiles(db, users[2], email=None)
    await profiles(db, users[3], tz="not-a-zone")
    for user in users:
        assert (
            await api.post(f"{URL}/{campaign}/batches", json=batch(item(user)))
        ).status_code == 202
    status = (await api.get(f"{URL}/{campaign}")).json()
    assert status == dict(
        campaign_id=str(campaign),
        created_by_user_id=str(AUTHOR),
        intake_status="open",
        status="in_progress",
        accepted_batches=4,
        total=4,
        sent=0,
        failed=0,
        skipped=3,
        pending=1,
        skipped_reasons=dict(profile_deleted=1, subscription_disabled=1, invalid_timezone=1),
        failed_reasons={},
    )
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, users[3]))
        assert recipient.delivery_id is None and recipient.skip_reason == "invalid_timezone"
        recipient = await session.get(CampaignRecipient, (campaign, users[2]))
        delivery = await session.get(Delivery, recipient.delivery_id)
        delivery.status, delivery.terminal_reason = "failed", "smtp_uncertain"
        delivery.terminal_at = datetime.now(UTC)
    endpoint = f"{URL}/{campaign}/close"
    assert (await api.post(endpoint, json={"expected_batches": 3})).json()[
        "error_code"
    ] == "batch_count_mismatch"
    assert (await api.post(endpoint, json={"expected_batches": 4})).json() == dict(
        campaign_id=str(campaign), intake_status="closed"
    )
    assert (await api.post(endpoint, json={"expected_batches": 4})).status_code == 200
    assert (await api.post(endpoint, json={"expected_batches": 5})).json()[
        "error_code"
    ] == "batch_count_mismatch"
    status = (await api.get(f"{URL}/{campaign}")).json()
    assert (status["status"], status["failed"], status["pending"], status["failed_reasons"]) == (
        "failed",
        1,
        0,
        {"smtp_uncertain": 1},
    )


@pytest.mark.parametrize("expected", [0, -1, True, "1", 1.2, None])
async def test_invalid_close(campaign_api, expected):
    api, _ = campaign_api
    assert (
        await api.post(f"{URL}/{uuid4()}/close", json={"expected_batches": expected})
    ).status_code == 422


async def test_not_found_and_public_boundary(campaign_api, database_url):
    from notifications.main import create_app

    api, _ = campaign_api
    for response in [
        await api.get(f"{URL}/{uuid4()}"),
        await api.post(f"{URL}/{uuid4()}/close", json={"expected_batches": 1}),
    ]:
        assert (response.status_code, response.json()["error_code"]) == (404, "campaign_not_found")
    app = create_app(
        Settings(
            process="api",
            database_url=database_url,
            jwt_secret=SecretStr("secret"),
            jwt_algorithm="HS256",
            auth_introspection_url="https://auth.test/introspect",
            auth_introspection_api_key=SecretStr("test-service-key"),
            auth_introspection_timeout_seconds=1,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://public"
    ) as public:
        assert (
            await public.post(f"{URL}/{uuid4()}/batches", json=batch(item()))
        ).status_code == 404


@pytest.mark.parametrize("value", ["secret\x00text", "secret\ud800text"])
async def test_text_unrepresentable_in_postgres_is_validation_error(campaign_api, value):
    api, db = campaign_api
    user = uuid4()
    await profiles(db, user)
    value_item = item(user)
    value_item["text"] = value
    # JSON escapes deliberately preserve the invalid Unicode code point on the wire.
    import json

    response = await api.post(f"{URL}/{uuid4()}/batches", content=json.dumps(batch(value_item)))
    assert response.status_code == 422
    assert "secret" not in response.text
    assert await counts(db) == [0, 0, 0, 0, 0]


@pytest.mark.parametrize(
    "timestamp", ["0001-01-01T00:00:00+14:00", "9999-12-31T23:59:59-14:00", "9999-12-31T23:59:59Z"]
)
async def test_unschedulable_time_is_validation_error(campaign_api, timestamp):
    api, db = campaign_api
    user, later = uuid4(), uuid4()
    await profiles(db, user, later)
    response = await api.post(
        f"{URL}/{uuid4()}/batches", json=batch(item(user), item(later, available_at=timestamp))
    )
    assert response.status_code == 422
    assert response.json() == {
        "error_code": "validation_error",
        "detail": "Invalid request",
        "errors": [
            {"loc": ["items", 1, "available_at"], "type": "value_error", "msg": "Invalid value"}
        ],
    }
    assert await counts(db) == [0, 0, 0, 0, 0]


@pytest.mark.parametrize(
    "delivery_status,reason,expected",
    [
        ("sent", None, "completed"),
        ("cancelled", "profile_deleted", "skipped"),
        ("skipped", "no_email", "skipped"),
        ("failed", "smtp_permanent", "failed"),
        ("failed", "attempts_exhausted", "failed"),
        ("retry", None, "in_progress"),
    ],
)
async def test_closed_campaign_terminal_classification(
    campaign_api, delivery_status, reason, expected
):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    assert (await api.post(f"{URL}/{campaign}/batches", json=batch(item(user)))).status_code == 202
    async with db.transaction() as session:
        delivery = await session.scalar(select(Delivery))
        delivery.status, delivery.terminal_reason = delivery_status, reason
        if delivery_status == "sent":
            delivery.sent_at = datetime.now(UTC)
    await api.post(f"{URL}/{campaign}/close", json={"expected_batches": 1})
    result = (await api.get(f"{URL}/{campaign}")).json()
    assert result["status"] == expected
    assert (
        result["total"]
        == result["sent"] + result["failed"] + result["skipped"] + result["pending"]
        == 1
    )
    assert result["failed_reasons"] == ({reason: 1} if delivery_status == "failed" else {})
    assert result["skipped_reasons"] == (
        {reason: 1} if delivery_status in ("cancelled", "skipped") else {}
    )


async def test_exact_body_limit_and_valid_maximum_batch(campaign_api):
    import json

    api, db = campaign_api
    users = [uuid4() for _ in range(100)]
    await profiles(db, *users)
    body = batch(*(item(user) for user in users))
    body["items"][0]["subject"] = "s" * 200
    body["items"][0]["text"] = "x" * 20000
    encoded = json.dumps(body).encode()
    encoded += b" " * (1048576 - len(encoded))
    response = await api.post(f"{URL}/{uuid4()}/batches", content=encoded)
    assert response.status_code == 202
    assert response.json()["item_count"] == 100
    response = await api.post(f"{URL}/{uuid4()}/batches", content=encoded + b" ")
    assert response.status_code == 413
    assert await counts(db) == [1, 1, 100, 100, 100]


async def test_past_available_at_preserves_source_but_uses_acceptance_time(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user, available_at="2000-01-01T00:00:00Z"))
    assert (await api.post(f"{URL}/{campaign}/batches", json=body)).status_code == 202
    async with db.transaction() as session:
        recipient = await session.scalar(select(CampaignRecipient))
        accepted = await session.scalar(select(CampaignBatch))
        assert recipient.source_available_at == datetime(2000, 1, 1, tzinfo=UTC)
        assert recipient.effective_available_at == accepted.accepted_at
    body["items"][0].pop("available_at")
    assert (await api.post(f"{URL}/{campaign}/batches", json=body)).json()[
        "error_code"
    ] == "batch_content_conflict"


async def test_campaign_api_uses_configured_window(database_url):
    from notifications.campaigns.app import create_campaign_app

    app = create_campaign_app(
        Settings(
            process="campaign-api",
            database_url=database_url,
            campaign_api_key=SecretStr(API_KEY),
            send_window_start=time(8, 30),
            send_window_end=time(18, 45),
        )
    )
    uid = uuid4()
    async with app.router.lifespan_context(app):
        await profiles(app.state.database, uid, tz="Europe/Moscow")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://campaigns",
            headers={"X-API-Key": API_KEY},
        ) as client:
            response = await client.post(
                f"{URL}/{uuid4()}/batches",
                json=batch(item(uid, available_at="2099-01-01T16:00:00Z")),
            )
        assert response.status_code == 202
        async with app.state.database.transaction() as session:
            delivery = (await session.scalars(select(Delivery))).one()
            outbox = (await session.scalars(select(Outbox))).one()
            assert (
                delivery.next_attempt_at
                == outbox.available_at
                == datetime(2099, 1, 2, 5, 30, tzinfo=UTC)
            )


async def test_billing_notification_ignores_marketing_optout_and_daytime_window(campaign_api):
    """T-21 (NTF-FR-01, NTF-API-01): billing_notification принимается без marketing и окна."""
    api, db = campaign_api
    immediate_user, scheduled_user, campaign = uuid4(), uuid4(), uuid4()
    await profiles(
        db,
        immediate_user,
        scheduled_user,
        likes=False,
        new_films=False,
        marketing=False,
        tz="invalid-tz-still-ok-for-transactional",
    )
    future_night = "2099-01-01T23:15:00Z"
    body = batch(
        item(immediate_user)
        | {
            "template_id": "billing_notification",
            "subject": "Подписка «Премиум» оплачена",
            "text": "Доступ к новинкам открыт до 05.12.2026 15:00 МСК",
            "available_at": None,
        },
        item(scheduled_user)
        | {
            "template_id": "billing_notification",
            "subject": "Срок подписки «Премиум» закончился",
            "text": "Срок действия вашей подписки истёк",
            "available_at": future_night,
        },
    )
    response = await api.post(f"{URL}/{campaign}/batches", json=body)
    assert response.status_code == 202, response.text
    assert response.json()["item_count"] == 2

    async with db.transaction() as session:
        imm_rec = await session.get(CampaignRecipient, (campaign, immediate_user))
        sch_rec = await session.get(CampaignRecipient, (campaign, scheduled_user))
        assert imm_rec is not None and imm_rec.skip_reason is None
        assert sch_rec is not None and sch_rec.skip_reason is None

        imm_del = await session.get(Delivery, imm_rec.delivery_id)
        sch_del = await session.get(Delivery, sch_rec.delivery_id)
        assert imm_del is not None
        assert (imm_del.kind, imm_del.send_class, imm_del.required_subscription) == (
            "external",
            "transactional",
            None,
        )
        assert sch_del is not None
        assert (sch_del.kind, sch_del.send_class, sch_del.required_subscription) == (
            "external",
            "transactional",
            None,
        )
        # Запланированное на ночное время письмо НЕ сдвигается на утро 10:00
        sch_outbox = await session.scalar(
            select(Outbox).where(Outbox.delivery_id == sch_del.delivery_id)
        )
        assert sch_outbox is not None
        assert (
            sch_del.next_attempt_at
            == sch_outbox.available_at
            == datetime(2099, 1, 1, 23, 15, tzinfo=UTC)
        )


async def test_cancel_existing_scheduled_campaign_and_idempotent_replay(campaign_api):
    """T-24 (NTF-FR-02, NTF-API-02, NTF-NFR-R-01): отмена после планирования батча."""
    from notifications.db.models import CancelledCampaign

    api, db = campaign_api
    user_pending, user_sent, campaign = uuid4(), uuid4(), uuid4()
    await profiles(db, user_pending, user_sent, marketing=False)

    body = batch(
        item(user_pending)
        | {
            "template_id": "billing_notification",
            "subject": "Срок подписки «Премиум» закончился",
            "text": "Срок действия подписки истёк",
            "available_at": "2099-01-01T12:00:00Z",
        },
        item(user_sent)
        | {
            "template_id": "billing_notification",
            "subject": "Подписка оплачена",
            "text": "Уже отправленное письмо не должно меняться при cancel",
            "available_at": None,
        },
    )
    accept_res = await api.post(f"{URL}/{campaign}/batches", json=body)
    assert accept_res.status_code == 202

    # Помечаем вторую доставку как уже отправленную (sent), чтобы убедиться,
    # что cancel не трогает уже ушедшие в SMTP письма
    async with db.transaction() as session:
        sent_rec = await session.get(CampaignRecipient, (campaign, user_sent))
        assert sent_rec is not None and sent_rec.delivery_id is not None
        sent_del = await session.get(Delivery, sent_rec.delivery_id)
        assert sent_del is not None
        sent_del.status = "sent"
        sent_del.sent_at = datetime.now(UTC)
        sent_del.terminal_at = sent_del.sent_at

    # 1. Первый вызов cancel -> 200 OK
    cancel_1 = await api.post(f"{URL}/{campaign}/cancel")
    assert cancel_1.status_code == 200, cancel_1.text
    assert cancel_1.json() == {"campaign_id": str(campaign), "status": "cancelled"}

    # 2. Повторный вызов cancel -> 200 OK (идемпотентность NTF-NFR-R-01)
    cancel_2 = await api.post(f"{URL}/{campaign}/cancel")
    assert cancel_2.status_code == 200
    assert cancel_2.json() == {"campaign_id": str(campaign), "status": "cancelled"}

    async with db.transaction() as session:
        marker = await session.get(CancelledCampaign, campaign)
        assert marker is not None and marker.cancelled_at is not None

        pend_rec = await session.get(CampaignRecipient, (campaign, user_pending))
        assert pend_rec is not None and pend_rec.delivery_id is not None
        pend_del = await session.get(Delivery, pend_rec.delivery_id)
        assert pend_del is not None
        assert (pend_del.status, pend_del.terminal_reason) == (
            "cancelled",
            "cancelled_by_sender",
        )
        assert pend_del.terminal_at is not None
        assert pend_del.lease_token is None and pend_del.lease_until is None

        # Неопубликованная строка в outbox помечена published_at, чтобы publisher её не брал
        outbox_row = await session.scalar(
            select(Outbox).where(Outbox.delivery_id == pend_del.delivery_id)
        )
        assert outbox_row is not None and outbox_row.published_at is not None

        # Уже отправленное письмо осталось sent
        sent_del_after = await session.get(Delivery, sent_rec.delivery_id)
        assert sent_del_after is not None and sent_del_after.status == "sent"

    # 3. Идемпотентный повтор ТОГО ЖЕ батча после cancel возвращает 202
    replay_same = await api.post(f"{URL}/{campaign}/batches", json=body)
    assert replay_same.status_code == 202
    assert replay_same.json()["batch_id"] == body["batch_id"]

    # 4. Статус кампании отражает skipped_reasons={'cancelled_by_sender': 1} и sent=1
    status_res = await api.get(f"{URL}/{campaign}")
    assert status_res.status_code == 200
    status_json = status_res.json()
    assert status_json["sent"] == 1
    assert status_json["skipped"] == 1
    assert status_json["pending"] == 0
    assert status_json["skipped_reasons"] == {"cancelled_by_sender": 1}


async def test_preemptive_cancel_before_batch_intake_prevents_sending(campaign_api):
    """T-24 (инвариант 12, NTF-FR-02): cancel пришёл РАНЬШЕ планирования батча."""
    from notifications.db.models import CancelledCampaign

    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user, marketing=False)

    # 1. Вызываем cancel ДО создания кампании -> 200 OK, запись в cancelled_campaigns
    cancel_res = await api.post(f"{URL}/{campaign}/cancel")
    assert cancel_res.status_code == 200
    assert cancel_res.json() == {"campaign_id": str(campaign), "status": "cancelled"}

    async with db.transaction() as session:
        assert await session.get(CancelledCampaign, campaign) is not None
        assert await session.get(Campaign, campaign) is None

    # 2. Теперь приходит запоздавший батч планирования письма об окончании
    body = batch(
        item(user)
        | {
            "template_id": "billing_notification",
            "subject": "Срок подписки «Премиум» закончился",
            "text": "Срок действия вашей подписки истёк",
            "available_at": "2099-01-01T12:00:00Z",
        }
    )
    intake_res = await api.post(f"{URL}/{campaign}/batches", json=body)
    assert intake_res.status_code == 202, intake_res.text
    assert intake_res.json()["item_count"] == 1

    # Повтор того же батча идемпотентно возвращает 202, а с другим содержимым -> 409
    replay_res = await api.post(f"{URL}/{campaign}/batches", json=body)
    assert replay_res.status_code == 202

    conflict_body = dict(
        body, items=[item(user) | {"template_id": "billing_notification", "subject": "Другая тема"}]
    )
    conflict_res = await api.post(f"{URL}/{campaign}/batches", json=conflict_body)
    assert conflict_res.status_code == 409
    assert conflict_res.json()["error_code"] == "batch_content_conflict"

    # Доставка создана сразу в статусе cancelled (cancelled_by_sender), в outbox ничего нет
    async with db.transaction() as session:
        rec = await session.get(CampaignRecipient, (campaign, user))
        assert rec is not None and rec.delivery_id is not None
        delivery = await session.get(Delivery, rec.delivery_id)
        assert delivery is not None
        assert (delivery.status, delivery.terminal_reason) == (
            "cancelled",
            "cancelled_by_sender",
        )
        assert delivery.terminal_at is not None
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.delivery_id == delivery.delivery_id)
        )
        assert outbox_count == 0


async def test_cancel_campaign_unhappy_paths_auth_uuid_and_public_boundary(
    campaign_api, database_url
):
    """T-24 (NTF-NFR-SEC-01, NTF-NFR-IF-01, NTF-API-02): unhappy paths для cancel."""
    from notifications.campaigns.app import create_campaign_app
    from notifications.main import create_app

    api, _ = campaign_api
    cid = uuid4()

    # 1. Невалидный UUID в пути -> 422 validation_error
    bad_uuid = await api.post(f"{URL}/not-a-valid-uuid/cancel")
    assert bad_uuid.status_code == 422
    assert bad_uuid.json()["error_code"] == "validation_error"

    # 2. Отсутствующий или неверный X-API-Key -> 401 unauthorized
    app = create_campaign_app(
        Settings(
            process="campaign-api",
            database_url=database_url,
            campaign_api_key=SecretStr(API_KEY),
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://campaigns",
        ) as unauth_client:
            no_key = await unauth_client.post(f"{URL}/{cid}/cancel")
            assert no_key.status_code == 401
            assert no_key.json()["error_code"] == "unauthorized"

            wrong_key = await unauth_client.post(
                f"{URL}/{cid}/cancel",
                headers={"X-API-Key": "wrong-key"},
            )
            assert wrong_key.status_code == 401
            assert wrong_key.json()["error_code"] == "unauthorized"

    # 3. Публичный API процесс не экспонирует маршрут /cancel -> 404
    public_app = create_app(
        Settings(
            process="api",
            database_url=database_url,
            jwt_secret=SecretStr("secret"),
            jwt_algorithm="HS256",
            auth_introspection_url="https://auth.test/introspect",
            auth_introspection_api_key=SecretStr("test-service-key"),
            auth_introspection_timeout_seconds=1,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=public_app), base_url="http://public"
    ) as public_client:
        pub_res = await public_client.post(f"{URL}/{cid}/cancel", headers={"X-API-Key": API_KEY})
        assert pub_res.status_code == 404
