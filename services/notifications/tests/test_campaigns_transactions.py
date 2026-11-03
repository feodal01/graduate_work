import asyncio
from uuid import uuid4

from sqlalchemy import select
from test_campaigns_api import URL, batch, counts, item, profiles
from test_campaigns_api import campaign_api as campaign_api

from notifications.db.models import Campaign, Profile


async def test_concurrent_first_batch_replay_and_recipient_conflicts(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}/batches"
    responses = await asyncio.gather(*(api.post(endpoint, json=body) for _ in range(4)))
    assert [r.status_code for r in responses] == [202] * 4
    assert await counts(db) == [1, 1, 1, 1, 1]
    campaign = uuid4()
    responses = await asyncio.gather(
        *(api.post(f"{URL}/{campaign}/batches", json=batch(item(user))) for _ in range(2))
    )
    assert sorted(r.status_code for r in responses) == [202, 409]
    assert await counts(db) == [2, 2, 2, 2, 2]


async def test_overlapping_profiles_different_order_no_deadlock(campaign_api):
    api, db = campaign_api
    users = [uuid4() for _ in range(4)]
    await profiles(db, *users)
    responses = await asyncio.wait_for(
        asyncio.gather(
            api.post(f"{URL}/{uuid4()}/batches", json=batch(*(item(u) for u in users))),
            api.post(f"{URL}/{uuid4()}/batches", json=batch(*(item(u) for u in reversed(users)))),
        ),
        10,
    )
    assert [r.status_code for r in responses] == [202, 202]
    assert await counts(db) == [2, 2, 8, 8, 8]


async def test_batch_and_close_serialize_on_campaign(campaign_api):
    api, db = campaign_api
    users, campaign = [uuid4(), uuid4()], uuid4()
    await profiles(db, *users)
    assert (
        await api.post(f"{URL}/{campaign}/batches", json=batch(item(users[0])))
    ).status_code == 202
    async with db.transaction() as session:
        await session.scalar(
            select(Campaign).where(Campaign.campaign_id == campaign).with_for_update()
        )
        closing = asyncio.create_task(
            api.post(f"{URL}/{campaign}/close", json={"expected_batches": 1})
        )
        accepting = asyncio.create_task(
            api.post(f"{URL}/{campaign}/batches", json=batch(item(users[1])))
        )
        await asyncio.sleep(0.05)
        assert not closing.done() and not accepting.done()
    responses = await asyncio.gather(closing, accepting)
    assert sorted(r.status_code for r in responses) in ([200, 409], [202, 409])
    status = (await api.get(f"{URL}/{campaign}")).json()
    assert (status["intake_status"], status["accepted_batches"]) in [("closed", 1), ("open", 2)]


async def test_profile_lock_waits_and_observes_latest_optout(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    async with db.transaction() as session:
        profile = await session.scalar(
            select(Profile).where(Profile.user_id == user).with_for_update()
        )
        profile.marketing = False
        await session.flush()
        accepting = asyncio.create_task(
            api.post(f"{URL}/{campaign}/batches", json=batch(item(user)))
        )
        await asyncio.sleep(0.05)
        assert not accepting.done()
    assert (await accepting).status_code == 202
    assert await counts(db) == [1, 1, 1, 0, 0]


async def test_commit_failure_never_returns_202_and_rolls_back(campaign_api, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSessionTransaction

    api, db = campaign_api
    user = uuid4()
    await profiles(db, user)
    original = AsyncSessionTransaction.__aexit__

    async def fail_commit(self, exc_type, exc, tb):
        if exc_type is None:
            await self.rollback()
            raise RuntimeError("injected commit failure")
        return await original(self, exc_type, exc, tb)

    with monkeypatch.context() as patch:
        patch.setattr(AsyncSessionTransaction, "__aexit__", fail_commit)
        response = await api.post(f"{URL}/{uuid4()}/batches", json=batch(item(user)))
    assert response.status_code == 500
    assert await counts(db) == [0, 0, 0, 0, 0]


async def test_no_http_acknowledgement_while_commit_is_blocked(campaign_api, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSessionTransaction

    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    reached_commit, release_commit = asyncio.Event(), asyncio.Event()
    original = AsyncSessionTransaction.__aexit__

    async def delayed_commit(self, exc_type, exc, tb):
        if exc_type is None:
            reached_commit.set()
            await release_commit.wait()
        return await original(self, exc_type, exc, tb)

    with monkeypatch.context() as patch:
        patch.setattr(AsyncSessionTransaction, "__aexit__", delayed_commit)
        accepting = asyncio.create_task(
            api.post(f"{URL}/{campaign}/batches", json=batch(item(user)))
        )
        try:
            await asyncio.wait_for(reached_commit.wait(), 5)
            assert not accepting.done()
            # A separate actual connection cannot observe the uncommitted campaign.
            async with db.sessions() as session:
                assert await session.get(Campaign, campaign) is None
        finally:
            release_commit.set()
        assert (await accepting).status_code == 202
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_concurrent_same_batch_different_content(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    first = batch(item(user))
    second = {**first, "items": [dict(first["items"][0], text="different")]}
    responses = await asyncio.gather(
        *(api.post(f"{URL}/{campaign}/batches", json=body) for body in (first, second))
    )
    assert sorted(r.status_code for r in responses) == [202, 409]
    assert (
        next(r for r in responses if r.status_code == 409).json()["error_code"]
        == "batch_content_conflict"
    )
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_statistics_one_snapshot_during_terminal_changes(campaign_api):
    from datetime import UTC, datetime

    from sqlalchemy import event, update

    from notifications.db.models import Delivery

    api, db = campaign_api
    users, campaign = [uuid4() for _ in range(12)], uuid4()
    await profiles(db, *users)
    for offset in range(0, 12, 3):
        assert (
            await api.post(
                f"{URL}/{campaign}/batches",
                json=batch(*(item(u) for u in users[offset : offset + 3])),
            )
        ).status_code == 202
    await api.post(f"{URL}/{campaign}/close", json={"expected_batches": 4})
    # Count actual SQL executions for one GET, not a mocked service result.
    statements = []

    def record_statement(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(db.engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        response = await api.get(f"{URL}/{campaign}")
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", record_statement)
    assert response.status_code == 200
    assert len(statements) == 1
    assert response.json()["accepted_batches"] == 4

    async def finish_deliveries():
        for index, user in enumerate(users):
            async with db.transaction() as session:
                values = (
                    dict(status="sent", sent_at=datetime.now(UTC))
                    if index % 2 == 0
                    else dict(status="failed", terminal_reason="smtp_uncertain")
                )
                await session.execute(
                    update(Delivery).where(Delivery.user_id == user).values(**values)
                )
            await asyncio.sleep(0)

    changing = asyncio.create_task(finish_deliveries())
    while not changing.done():
        result = (await api.get(f"{URL}/{campaign}")).json()
        assert (
            result["total"]
            == sum(result[name] for name in ("sent", "failed", "skipped", "pending"))
            == 12
        )
        assert sum(result["failed_reasons"].values()) == result["failed"]
        assert result["accepted_batches"] == 4
    await changing
    result = (await api.get(f"{URL}/{campaign}")).json()
    assert (result["status"], result["sent"], result["failed"], result["pending"]) == (
        "partially_completed",
        6,
        6,
        0,
    )
