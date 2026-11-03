"""Campaign content retention against real PostgreSQL and the campaign API."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import null, select, text, update
from sqlalchemy.exc import IntegrityError
from test_campaigns_api import URL, batch, counts, item, profiles
from test_campaigns_api import campaign_api as campaign_api

from notifications.db.models import CampaignBatch, CampaignRecipient, Delivery
from notifications.maintenance import Maintenance


@pytest.mark.parametrize(
    ("status", "snapshot"),
    [
        (status, snapshot)
        for status in ("sent", "cancelled", "skipped", "failed")
        for snapshot in ("present", "cleared", "unprepared")
        if status != "sent" or snapshot != "unprepared"
    ],
)
async def test_terminal_campaign_erases_both_copies_and_preserves_replay(
    campaign_api, status, snapshot
):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}/batches"
    accepted = await api.post(endpoint, json=body)
    assert accepted.status_code == 202, accepted.text
    terminal_at = datetime.now(UTC) - timedelta(days=2)
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        delivery_id = delivery.delivery_id
        delivery.status = status
        delivery.terminal_at = terminal_at
        delivery.terminal_reason = None if status == "sent" else "no_email"
        delivery.sent_at = terminal_at if status == "sent" else None
        if snapshot != "unprepared":
            delivery.prepared_at = terminal_at - timedelta(hours=1)
            delivery.message_id = f"<{delivery_id}@example.test>"
            delivery.payload = (
                {"subject": recipient.subject, "text": recipient.text}
                if snapshot == "present"
                else null()
            )
        saved_batch = await session.get(CampaignBatch, (campaign, UUID(body["batch_id"])))
        request_hash = saved_batch.request_hash
    assert (
        await api.post(f"{URL}/{campaign}/close", json={"expected_batches": 1})
    ).status_code == 200
    statistics = (await api.get(f"{URL}/{campaign}")).json()
    row_counts = await counts(db)

    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    assert await maintenance.run_once() == (0, 1)
    assert await maintenance.run_once() == (0, 0)
    # Replays must still match the original hash and must never restore erased content.
    replay = await api.post(endpoint, json=body)
    assert replay.status_code == 202 and replay.json() == accepted.json()
    body["items"][0]["text"] += "changed"
    conflict = await api.post(endpoint, json=body)
    assert (conflict.status_code, conflict.json()["error_code"]) == (409, "batch_content_conflict")
    assert (await api.get(f"{URL}/{campaign}")).json() == statistics
    assert await counts(db) == row_counts
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, delivery_id)
        saved_batch = await session.get(CampaignBatch, (campaign, UUID(body["batch_id"])))
        assert delivery.payload is None
        assert recipient.subject is None and recipient.text is None
        assert recipient.delivery_id == delivery_id and recipient.skip_reason is None
        assert delivery.status == status and delivery.terminal_at == terminal_at
        assert saved_batch.request_hash == request_hash and saved_batch.item_count == 1
        if snapshot != "unprepared":
            assert delivery.message_id == f"<{delivery_id}@example.test>"
            assert delivery.prepared_at == terminal_at - timedelta(hours=1)


@pytest.mark.parametrize(
    "reason", ["profile_deleted", "subscription_disabled", "invalid_timezone", "no_email"]
)
async def test_intake_skips_expire_from_acceptance_with_no_delivery(campaign_api, reason):
    api, db = campaign_api
    expired, recent, campaign = uuid4(), uuid4(), uuid4()
    changes = {
        "profile_deleted": dict(
            deleted_at=datetime.now(UTC), likes=False, new_films=False, marketing=False
        ),
        "subscription_disabled": dict(marketing=False),
        "invalid_timezone": dict(tz="invalid"),
        "no_email": dict(marketing=False),
    }[reason]
    await profiles(db, expired, recent, **changes)
    # Future delivery dates must not postpone retention for an already final skip.
    bodies = [batch(item(user, available_at="2090-01-01T00:00:00Z")) for user in (expired, recent)]
    endpoint = f"{URL}/{campaign}/batches"
    responses = [await api.post(endpoint, json=body) for body in bodies]
    assert [response.status_code for response in responses] == [202, 202]
    async with db.transaction() as session:
        if reason == "no_email":
            # Historical intake skips predate fetching contacts in the worker.
            await session.execute(update(CampaignRecipient).values(skip_reason="no_email"))
        saved_batch = await session.get(CampaignBatch, (campaign, UUID(bodies[0]["batch_id"])))
        saved_batch.accepted_at = datetime.now(UTC) - timedelta(days=2)
        hashes = list(
            await session.scalars(
                select(CampaignBatch.request_hash).order_by(CampaignBatch.batch_id)
            )
        )
    statistics = (await api.get(f"{URL}/{campaign}")).json()
    row_counts = await counts(db)

    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    assert await maintenance.run_once() == (0, 1)
    assert await maintenance.run_once() == (0, 0)
    for body, response in zip(bodies, responses, strict=True):
        replay = await api.post(endpoint, json=body)
        assert replay.status_code == 202 and replay.json() == response.json()
    assert (await api.get(f"{URL}/{campaign}")).json() == statistics
    assert await counts(db) == row_counts == [1, 2, 2, 0, 0]
    async with db.transaction() as session:
        for user in (expired, recent):
            recipient = await session.get(CampaignRecipient, (campaign, user))
            assert recipient.delivery_id is None and recipient.skip_reason == reason
            expected = (
                (None, None)
                if user == expired
                else (bodies[1]["items"][0]["subject"], bodies[1]["items"][0]["text"])
            )
            assert (recipient.subject, recipient.text) == expected
        assert (
            list(
                await session.scalars(
                    select(CampaignBatch.request_hash).order_by(CampaignBatch.batch_id)
                )
            )
            == hashes
        )


@pytest.mark.parametrize(
    "status", ["pending", "prepared", "sending", "retry", "sent", "cancelled", "skipped", "failed"]
)
async def test_active_and_recent_terminal_campaigns_keep_content(campaign_api, status):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    assert (await api.post(f"{URL}/{campaign}/batches", json=body)).status_code == 202
    old = datetime.now(UTC) - timedelta(days=10)
    recent = datetime.now(UTC) - timedelta(hours=1)
    async with db.transaction() as session:
        saved_batch = await session.get(CampaignBatch, (campaign, UUID(body["batch_id"])))
        saved_batch.accepted_at = old
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        delivery.status = status
        delivery.prepared_at = old
        delivery.message_id = f"<{delivery.delivery_id}@example.test>"
        delivery.payload = {"subject": recipient.subject, "text": recipient.text}
        if status in ("sent", "cancelled", "skipped", "failed"):
            delivery.terminal_at = recent
        if status == "sent":
            delivery.sent_at = recent
    assert await Maintenance(db, batch_size=1, retention_seconds=86400).run_once() == (0, 0)
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        assert recipient.subject == body["items"][0]["subject"]
        assert recipient.text == body["items"][0]["text"]
        assert delivery.payload == {"subject": recipient.subject, "text": recipient.text}


async def test_locked_rows_do_not_block_other_campaign_cleanup(campaign_api):
    api, db = campaign_api
    users = [uuid4() for _ in range(4)]
    campaign = uuid4()
    await profiles(db, *users[:2])
    await profiles(db, *users[2:], marketing=False)
    assert (
        await api.post(f"{URL}/{campaign}/batches", json=batch(*(item(user) for user in users)))
    ).status_code == 202
    old = datetime.now(UTC) - timedelta(days=2)
    async with db.transaction() as session:
        await session.execute(update(CampaignBatch).values(accepted_at=old))
        await session.execute(update(Delivery).values(status="skipped", terminal_at=old))
        delivery_id = await session.scalar(
            select(CampaignRecipient.delivery_id).where(CampaignRecipient.user_id == users[0])
        )
    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    async with db.transaction() as owner:
        await owner.get(Delivery, delivery_id, with_for_update=True)
        await owner.get(CampaignRecipient, (campaign, users[2]), with_for_update=True)
        assert await maintenance.run_once() == (0, 2)
        async with db.transaction() as session:
            for index, user in enumerate(users):
                recipient = await session.get(CampaignRecipient, (campaign, user))
                assert (recipient.subject is None) == (index in (1, 3))
                assert (recipient.text is None) == (index in (1, 3))
    assert await maintenance.run_once() == (0, 2)
    assert await maintenance.run_once() == (0, 0)


async def test_snapshot_and_source_cleanup_roll_back_together(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    assert (await api.post(f"{URL}/{campaign}/batches", json=body)).status_code == 202
    old = datetime.now(UTC) - timedelta(days=2)
    payload = {"subject": body["items"][0]["subject"], "text": body["items"][0]["text"]}
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        delivery.status = "failed"
        delivery.terminal_at = delivery.prepared_at = old
        delivery.message_id = f"<{delivery.delivery_id}@example.test>"
        delivery.payload = payload
        # Make PostgreSQL reject the source update after the snapshot update.
        await session.execute(
            text(
                "ALTER TABLE campaign_recipients ADD CONSTRAINT reject_erasure "
                "CHECK(subject IS NOT NULL)"
            )
        )
    maintenance = Maintenance(db, batch_size=1, retention_seconds=86400)
    with pytest.raises(IntegrityError, match="reject_erasure"):
        await maintenance.run_once()
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        assert delivery.payload == payload
        assert (recipient.subject, recipient.text) == (payload["subject"], payload["text"])
        await session.execute(
            text("ALTER TABLE campaign_recipients DROP CONSTRAINT reject_erasure")
        )
    assert await maintenance.run_once() == (0, 1)
    async with db.transaction() as session:
        recipient = await session.get(CampaignRecipient, (campaign, user))
        delivery = await session.get(Delivery, recipient.delivery_id)
        assert delivery.payload is None
        assert recipient.subject is None and recipient.text is None
