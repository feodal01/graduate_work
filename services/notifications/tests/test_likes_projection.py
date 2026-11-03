import asyncio
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select, update
from test_profiles import auth_event, profile_handler

from notifications.contracts import LikesEvent
from notifications.db.models import (
    BootstrapReceipt,
    KafkaOffset,
    LoadState,
    PendingReview,
    ProcessedEvent,
    ReviewCounter,
)
from notifications.db.session import Database, get_database


def likes(user_id, count=12, version=7, load_id=None, review_id="a" * 24):
    return LikesEvent(
        event_id=uuid4(),
        event_type="review.likes_updated",
        review_id=review_id,
        user_id=user_id,
        likes_count=count,
        likes_version=version,
        load_id=load_id,
    )


def manifest(events):
    return hashlib.sha256(
        "".join(
            f"{event.review_id},{event.event_id}\n"
            for event in sorted(events, key=lambda event: event.review_id)
        ).encode()
    ).hexdigest()


@pytest.fixture
async def db(database_url, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_PROCESS", "bootstrap")
    monkeypatch.setenv("NOTIFICATIONS_DATABASE_URL", database_url)
    get_database.cache_clear()
    database = Database(database_url)
    yield database
    await database.close()
    if get_database.cache_info().currsize:
        await get_database().close()
    get_database.cache_clear()


async def test_live_before_baseline_and_replay_after_retention(db, database_url):
    from notifications.likes.events import apply_likes

    user = uuid4()
    bootstrap = likes(user, load_id="initial")
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user))
        await apply_likes(session, likes(user, 15, 8))
        await apply_likes(session, bootstrap)
    async with db.transaction() as session:
        counter = await session.get(ReviewCounter, bootstrap.review_id)
        assert (
            counter.likes_count,
            counter.last_sent_likes_count,
            counter.last_notification_sent_at,
        ) == (15, 12, None)
        await session.execute(delete(ProcessedEvent))
        await apply_likes(session, likes(user, 18, 9))
        await apply_likes(session, bootstrap)
    async with db.transaction() as session:
        counter = await session.get(ReviewCounter, bootstrap.review_id)
        assert (counter.likes_count, counter.likes_version, counter.last_sent_likes_count) == (
            18,
            9,
            12,
        )
        receipt = await session.get(BootstrapReceipt, ("initial", bootstrap.review_id))
        assert receipt.event_id == bootstrap.event_id
        assert receipt.applied_at is not None


@pytest.mark.parametrize("deleted_first", [False, True])
async def test_pending_preserves_latest_and_original_baseline_then_deletion(
    db, database_url, deleted_first
):
    from notifications.ingest.bootstrap import finalize_load, register_load
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    await register_load("initial", 1, manifest([event]))
    handler = profile_handler(database_url)
    if deleted_first:
        async with db.transaction() as session:
            await handler(session, auth_event(user, kind="user.deleted"))
    async with db.transaction() as session:
        await apply_likes(session, likes(user, 14, 8))
        await apply_likes(session, likes(user, 18, 10))
        await apply_likes(session, event)
        await apply_likes(session, likes(user, 16, 9))
    if not deleted_first:
        assert await finalize_load("initial") is False
        async with db.transaction() as session:
            row = await session.get(PendingReview, event.review_id)
            assert (row.likes_count, row.likes_version, row.baseline_likes_count) == (18, 10, 12)
            await handler(session, auth_event(user, kind="user.deleted"))
    assert await finalize_load("initial") is True
    async with db.transaction() as session:
        assert await session.get(PendingReview, event.review_id) is None
        receipt = await session.get(BootstrapReceipt, ("initial", event.review_id))
        assert receipt.applied_at is not None


async def test_activation_on_update_and_live_never_changes_sent_baseline(db, database_url):
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    async with db.transaction() as session:
        await apply_likes(session, likes(user, 18, 10))
        await apply_likes(session, event)
        await profile_handler(database_url)(session, auth_event(user))
    sent = datetime(2026, 1, 1, tzinfo=UTC)
    async with db.transaction() as session:
        counter = await session.get(ReviewCounter, event.review_id)
        assert (
            counter.user_id,
            counter.likes_count,
            counter.likes_version,
            counter.last_sent_likes_count,
        ) == (user, 18, 10, 12)
        assert await session.get(PendingReview, event.review_id) is None
        await session.execute(
            update(ReviewCounter).values(last_sent_likes_count=17, last_notification_sent_at=sent)
        )
        await apply_likes(session, likes(user, 20, 11))
        await apply_likes(session, event)
        await apply_likes(session, likes(user, 3, 1, review_id="b" * 24))
    async with db.transaction() as session:
        counter = await session.get(ReviewCounter, event.review_id)
        assert (
            counter.likes_count,
            counter.last_sent_likes_count,
            counter.last_notification_sent_at,
        ) == (20, 17, sent)
        new = await session.get(ReviewCounter, "b" * 24)
        assert (new.last_sent_likes_count, new.last_notification_sent_at) == (0, None)


async def test_finalize_missing_rows_hash_mismatch_and_receipt_conflict(db, database_url):
    from notifications.ingest.bootstrap import finalize_load, register_load
    from notifications.likes.events import apply_likes

    user = uuid4()
    events = [likes(user, load_id="initial", review_id=char * 24) for char in "ab"]
    await register_load("initial", 2, manifest(events))
    await register_load("initial", 2, manifest(events))
    with pytest.raises(ValueError, match="manifest"):
        await register_load("initial", 1, manifest(events))
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user))
        await apply_likes(session, events[1])
    assert await finalize_load("initial") is False
    async with db.transaction() as session:
        await apply_likes(session, events[0])
    assert await finalize_load("initial") is True
    assert await finalize_load("initial") is True
    with pytest.raises(ValueError, match="event_id"):
        async with db.transaction() as session:
            await apply_likes(session, likes(user, load_id="initial"))
    with pytest.raises(ValueError, match="completed"):
        async with db.transaction() as session:
            await apply_likes(session, likes(user, load_id="initial", review_id="c" * 24))
    bad = likes(user, load_id="bad")
    await register_load("bad", 1, "0" * 64)
    async with db.transaction() as session:
        await apply_likes(session, bad)
    assert await finalize_load("bad") is False
    with pytest.raises(ValueError, match="registered"):
        await finalize_load("unknown")


async def test_all_projection_writes_rollback_with_marker_and_offset(db, database_url):
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    with pytest.raises(RuntimeError):
        async with db.transaction() as session:
            await apply_likes(session, event)
            await profile_handler(database_url)(session, auth_event(user))
            session.add(ProcessedEvent(event_id=event.event_id))
            session.add(KafkaOffset(topic="ugc", partition=0, next_offset=1))
            await session.flush()
            raise RuntimeError("rollback")
    async with db.transaction() as session:
        for model in (BootstrapReceipt, PendingReview, ReviewCounter, ProcessedEvent, KafkaOffset):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.parametrize("first", ["auth", "likes"])
async def test_profile_creation_race_cannot_strand_pending(db, database_url, first):
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    entered = asyncio.Event()
    release = asyncio.Event()
    handler = profile_handler(database_url)

    async def action(session, kind):
        if kind == "auth":
            await handler(session, auth_event(user))
        else:
            await apply_likes(session, event)

    async def one():
        async with db.transaction() as session:
            await action(session, first)
            entered.set()
            await release.wait()

    async def two():
        await entered.wait()
        async with db.transaction() as session:
            await action(session, "likes" if first == "auth" else "auth")

    task_one = asyncio.create_task(one())
    task_two = asyncio.create_task(two())
    await entered.wait()
    await asyncio.sleep(0.05)
    assert not task_two.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(task_one, task_two), timeout=5)
    async with db.transaction() as session:
        assert await session.get(PendingReview, event.review_id) is None
        assert (await session.get(ReviewCounter, event.review_id)).last_sent_likes_count == 12
        assert (await session.get(BootstrapReceipt, ("initial", event.review_id))).applied_at


async def test_concurrent_versions_and_bootstrap(db, database_url):
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user))

    async def consume(item):
        async with db.transaction() as session:
            await apply_likes(session, item)

    await asyncio.wait_for(
        asyncio.gather(
            *(consume(item) for item in [likes(user, 18, 10), event, likes(user, 15, 8), event])
        ),
        5,
    )
    async with db.transaction() as session:
        row = await session.get(ReviewCounter, event.review_id)
        assert (row.likes_count, row.likes_version, row.last_sent_likes_count) == (18, 10, 12)


async def test_cli_import_strict_manifest_and_empty_load(db, database_url, tmp_path):
    async def cli(*args):
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "notifications.ingest.bootstrap",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                "NOTIFICATIONS_PROCESS": "bootstrap",
                "NOTIFICATIONS_DATABASE_URL": database_url,
            },
        )
        stdout, stderr = await proc.communicate()
        return proc.returncode, stdout.decode(), stderr.decode()

    code, _, error = await cli("--help")
    assert code == 0, error
    path = tmp_path / "manifest.json"
    payload = {
        "load_id": "empty",
        "expected_count": 0,
        "manifest_hash": hashlib.sha256(b"").hexdigest(),
    }
    for invalid in (
        {**payload, "extra": 1},
        {**payload, "expected_count": True},
        {**payload, "expected_count": "0"},
        {**payload, "manifest_hash": "x"},
    ):
        path.write_text(json.dumps(invalid))
        assert (await cli("register", "--manifest", str(path)))[0] != 0
    path.write_text(json.dumps(payload))
    assert (await cli("register", "--manifest", str(path)))[0] == 0
    code, output, error = await cli("finalize", "--load-id", "empty")
    assert code == 0, error
    assert json.loads(output) == {"load_id": "empty", "completed": True}
    async with db.transaction() as session:
        assert (await session.get(LoadState, "empty")).completed_at is not None


async def test_finalize_waits_for_atomic_receipt_commit(db, database_url):
    from notifications.ingest.bootstrap import finalize_load, register_load
    from notifications.likes.events import apply_likes

    user = uuid4()
    event = likes(user, load_id="initial")
    await register_load("initial", 1, manifest([event]))
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user))
        await apply_likes(session, event)
        finalizing = asyncio.create_task(finalize_load("initial"))
        await asyncio.sleep(0.05)
        assert not finalizing.done()
    assert await asyncio.wait_for(finalizing, 5) is True


async def test_second_load_cannot_replace_first_baseline(db, database_url):
    from notifications.likes.events import apply_likes

    user = uuid4()
    async with db.transaction() as session:
        await apply_likes(session, likes(user, load_id="first"))
        await apply_likes(session, likes(user, 18, 9, load_id="second"))
        await profile_handler(database_url)(session, auth_event(user))
    async with db.transaction() as session:
        row = await session.get(ReviewCounter, "a" * 24)
        assert (row.likes_count, row.last_sent_likes_count) == (18, 12)
        await apply_likes(session, likes(user, 20, 10, load_id="third"))
    async with db.transaction() as session:
        row = await session.get(ReviewCounter, "a" * 24)
        assert (row.likes_count, row.last_sent_likes_count) == (20, 12)
        receipts = (await session.scalars(select(BootstrapReceipt))).all()
        assert len(receipts) == 3
        assert all(receipt.applied_at is not None for receipt in receipts)


def test_receipt_migration_rejects_missing_source_ids(database_url):
    import psycopg
    from conftest import migrate

    migrate(database_url, "downgrade", "001_initial")
    with psycopg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://")) as conn:
        conn.execute("INSERT INTO bootstrap_receipts(load_id,entity_id) VALUES ('old','review')")
    with pytest.raises(AssertionError, match="original event_id is unavailable"):
        migrate(database_url, "upgrade", "head")
    with psycopg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://")) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
            "001_initial",
        )
        conn.execute("DELETE FROM bootstrap_receipts")
    migrate(database_url, "upgrade", "head")
