"""Preparation retry and profile races use PostgreSQL, isolated Auth calls."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup

from notifications.clients.auth_contacts import ContactsError, UnavailableContact
from notifications.db.models import Delivery, DeliveryAttempt, Outbox, Profile


@pytest.mark.parametrize(
    "code", ["transport_failure", "http_failure", "invalid_response", "authorization_failure"]
)
async def test_contact_failure_requeues_preparation_without_smtp(setup, monkeypatch, code):
    db, worker, did, uid, _ = setup

    async def fail(ids):
        raise ContactsError(code)

    monkeypatch.setattr(worker.contacts, "fetch", fail)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.payload is None
        assert delivery.attempt_count == 0
        assert delivery.lease_token is delivery.lease_until is None
        assert delivery.terminal_at is None
        assert await s.scalar(select(DeliveryAttempt)) is None
        assert (await s.scalar(select(Outbox))).available_at == delivery.next_attempt_at


@pytest.mark.parametrize("change", ["delete", "optout", "timezone"])
async def test_profile_change_during_contact_fetch_cannot_prepare(setup, monkeypatch, change):
    db, worker, did, uid, _ = setup
    fetch = worker.contacts.fetch

    async def changing(ids):
        # This separate committed update would deadlock if HTTP held profile row locks.
        async with db.transaction() as s:
            profile = await s.get(Profile, uid, with_for_update=True)
            if change == "delete":
                profile.deleted_at = datetime.now(UTC)
                profile.likes = profile.new_films = profile.marketing = False
            elif change == "optout":
                profile.likes = False
            else:
                profile.tz = None
        return await fetch(ids)

    monkeypatch.setattr(worker.contacts, "fetch", changing)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == ("skipped" if change == "timezone" else "cancelled")
        assert delivery.terminal_at is not None
        assert delivery.payload is None and delivery.attempt_count == 0


async def test_unavailable_user_is_terminal_without_attempt(setup, monkeypatch):
    db, worker, did, uid, _ = setup

    async def unavailable(ids):
        return {uid: UnavailableContact(user_id=uid, status="unavailable")}

    monkeypatch.setattr(worker.contacts, "fetch", unavailable)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "skipped" and delivery.terminal_reason == "contact_unavailable"
        assert delivery.attempt_count == 0


async def test_no_email_is_decided_from_auth_without_smtp_attempt(setup):
    from contact_fixtures import contact

    db, worker, did, uid, _ = setup
    contact(uid).email = None
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "skipped" and delivery.terminal_reason == "no_email"
        assert delivery.attempt_count == 0


async def test_prepared_snapshot_never_refetches_contacts(setup, monkeypatch):
    db, worker, did, uid, _ = setup
    snapshot = await worker.prepare_delivery(did)
    assert snapshot is not None

    async def forbidden(ids):
        raise AssertionError("prepared delivery must use its saved contacts")

    monkeypatch.setattr(worker.contacts, "fetch", forbidden)
    await worker.send_delivery(did)
    async with db.transaction() as s:
        delivery = await s.get(Delivery, did)
        assert delivery.status == "sent"
        assert delivery.payload == snapshot.payload
