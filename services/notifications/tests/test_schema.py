from uuid import uuid4

import pytest
from contact_fixtures import contact_profile
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


def test_models_exist_before_migrations():
    from notifications.db.models import Base

    assert "profiles" in Base.metadata.tables


async def test_transaction_rolls_back_every_effect(database_url):
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        with pytest.raises(RuntimeError):
            async with db.transaction() as session:
                await session.execute(
                    text("INSERT INTO processed_events(event_id) VALUES (:id)"), {"id": uuid4()}
                )
                user_id, delivery_id = uuid4(), uuid4()
                await session.execute(
                    text(
                        "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
                        "VALUES (:id,1,false,false,false)"
                    ),
                    {"id": user_id},
                )
                await session.execute(
                    text(
                        "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind) "
                        "VALUES (:id,'welcome',:user_id,'welcome')"
                    ),
                    {"id": delivery_id, "user_id": user_id},
                )
                await session.execute(
                    text("INSERT INTO outbox(outbox_id,delivery_id) VALUES (:id,:delivery_id)"),
                    {"id": uuid4(), "delivery_id": delivery_id},
                )
                await session.execute(
                    text(
                        "INSERT INTO kafka_offsets(topic,partition,next_offset) VALUES ('auth',0,1)"
                    )
                )
                raise RuntimeError("crash before commit")
        async with db.transaction() as session:
            for table in ("processed_events", "profiles", "deliveries", "outbox", "kafka_offsets"):
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        await db.close()


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
        "SELECT user_id,1,false,false,false FROM profiles",
        "INSERT INTO review_counters(review_id,user_id,likes_count,likes_version) "
        "VALUES ('bbbbbbbbbbbbbbbbbbbbbbbb', '00000000-0000-0000-0000-000000000000',0,0)",
        "INSERT INTO review_counters(review_id,user_id,likes_count,likes_version) "
        "SELECT review_id,user_id,0,0 FROM review_counters",
        "INSERT INTO processed_events(event_id) SELECT event_id FROM processed_events",
        "INSERT INTO kafka_offsets(topic,partition,next_offset) VALUES ('auth.users.v1',0,1)",
        "INSERT INTO bootstrap_receipts(load_id,entity_id,event_id) "
        "VALUES ('initial','review-1','00000000-0000-0000-0000-000000000001')",
        "UPDATE review_counters SET likes_count=-1",
        "UPDATE profiles SET deleted_at=now(),likes=true",
        "UPDATE kafka_offsets SET next_offset=-1",
    ],
)
async def test_database_rejects_invalid_state(database_url, statement):
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        async with db.transaction() as session:
            user_id = uuid4()
            await session.execute(
                text(
                    "INSERT INTO profiles(user_id,user_version,likes,new_films,"
                    "marketing) VALUES (:id,1,false,false,false)"
                ),
                {"id": user_id},
            )
            await session.execute(
                text(
                    "INSERT INTO review_counters(review_id,user_id,likes_count,"
                    "likes_version) VALUES ('aaaaaaaaaaaaaaaaaaaaaaaa',:id,0,0)"
                ),
                {"id": user_id},
            )
            await session.execute(
                text("INSERT INTO processed_events(event_id) VALUES (:id)"), {"id": uuid4()}
            )
            await session.execute(
                text(
                    "INSERT INTO kafka_offsets(topic,partition,next_offset) "
                    "VALUES ('auth.users.v1',0,0)"
                )
            )
            await session.execute(
                text(
                    "INSERT INTO bootstrap_receipts(load_id,entity_id,event_id) "
                    "VALUES ('initial','review-1','00000000-0000-0000-0000-000000000001')"
                )
            )
        with pytest.raises(IntegrityError):
            async with db.transaction() as session:
                await session.execute(text(statement))
    finally:
        await db.close()


def test_migration_downgrade_and_reapply(database_url):
    import psycopg
    from conftest import migrate

    migrate(database_url, "downgrade", "base")
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        row = conn.execute("SELECT to_regclass('profiles')").fetchone()
        assert row is not None and row[0] is None
    migrate(database_url, "upgrade", "head")
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        row = conn.execute("SELECT to_regclass('review_counters')").fetchone()
        assert row is not None and row[0] is not None


async def seed_delivery(db):
    """Persist a real recipient, counter, scheduled snapshot and delivery."""
    from datetime import UTC, datetime

    from notifications.db.models import Delivery, ReviewCounter, ScheduleItem, ScheduleRun

    user_id, run_id, delivery_id = uuid4(), uuid4(), uuid4()
    async with db.transaction() as session:
        session.add(
            contact_profile(
                user_id=user_id, user_version=1, likes=True, new_films=False, marketing=False
            )
        )
        await session.flush()
        session.add(
            ReviewCounter(
                review_id="a" * 24,
                user_id=user_id,
                likes_count=15,
                likes_version=8,
                last_sent_likes_count=12,
            )
        )
        session.add(
            ScheduleRun(
                run_id=run_id, kind="likes", period_start=datetime(2026, 10, 10, tzinfo=UTC)
            )
        )
        await session.flush()
        session.add(
            ScheduleItem(run_id=run_id, review_id="a" * 24, user_id=user_id, likes_count=15)
        )
        session.add(
            Delivery(
                delivery_id=delivery_id,
                logical_key=f"likes:{run_id}:{user_id}",
                user_id=user_id,
                run_id=run_id,
                kind="likes",
            )
        )
    return user_id, run_id, delivery_id


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO schedule_runs(run_id,kind,period_start) "
        "SELECT gen_random_uuid(),kind,period_start FROM schedule_runs",
        "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind) "
        "SELECT gen_random_uuid(),logical_key,user_id,kind FROM deliveries",
        "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,run_id) "
        "SELECT gen_random_uuid(),'duplicate',user_id,kind,run_id FROM deliveries",
        "UPDATE deliveries SET status='unknown'",
        "UPDATE deliveries SET status='sent'",
        "UPDATE deliveries SET lease_token=gen_random_uuid()",
        "UPDATE deliveries SET payload='{}'::jsonb",
        "INSERT INTO delivery_items(item_id,delivery_id,review_id,likes_count) "
        "SELECT gen_random_uuid(),delivery_id,'aaaaaaaaaaaaaaaaaaaaaaaa',NULL FROM deliveries",
    ],
)
async def test_durable_work_constraints(database_url, statement):
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        await seed_delivery(db)
        with pytest.raises(IntegrityError):
            async with db.transaction() as session:
                await session.execute(text(statement))
    finally:
        await db.close()


async def test_durable_reservations_outbox_and_bootstrap_survive_cleanup(database_url):
    from datetime import UTC, datetime

    from notifications.db.models import (
        BootstrapReceipt,
        DeliveryItem,
        Outbox,
        PendingReview,
        ScheduleItem,
        ScheduleRun,
    )
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        user_id, _, delivery_id = await seed_delivery(db)
        second_run = uuid4()
        async with db.transaction() as session:
            session.add(
                ScheduleRun(
                    run_id=second_run, kind="likes", period_start=datetime(2026, 10, 11, tzinfo=UTC)
                )
            )
            session.add(DeliveryItem(delivery_id=delivery_id, review_id="a" * 24, likes_count=15))
            session.add(Outbox(delivery_id=delivery_id))
            session.add(BootstrapReceipt(load_id="initial", entity_id="a" * 24, event_id=uuid4()))
            session.add(
                PendingReview(
                    review_id="b" * 24,
                    user_id=uuid4(),
                    likes_count=18,
                    likes_version=9,
                    baseline_likes_count=12,
                    baseline_load_id="initial",
                )
            )
        with pytest.raises(IntegrityError):
            async with db.transaction() as session:
                session.add(
                    ScheduleItem(
                        run_id=second_run, review_id="a" * 24, user_id=user_id, likes_count=18
                    )
                )
        with pytest.raises(IntegrityError):
            async with db.transaction() as session:
                session.add(Outbox(delivery_id=delivery_id))
        async with db.transaction() as session:
            await session.execute(text("DELETE FROM processed_events"))
            await session.execute(text("DELETE FROM outbox"))
            assert await session.scalar(text("SELECT count(*) FROM bootstrap_receipts")) == 1
            row = (
                await session.execute(
                    text(
                        "SELECT likes_count, likes_version, "
                        "baseline_likes_count FROM pending_reviews"
                    )
                )
            ).one()
            assert tuple(row) == (18, 9, 12)
    finally:
        await db.close()


async def test_migration_matches_orm_metadata(database_url):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from notifications.db.models import Base
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        with pytest.warns(
            UserWarning,
            match=r"^Computed default on deliveries\.send_class cannot be modified$",
        ):
            async with db.engine.connect() as connection:
                differences = await connection.run_sync(
                    lambda conn: compare_metadata(
                        MigrationContext.configure(
                            conn, opts={"compare_type": True, "compare_server_default": True}
                        ),
                        Base.metadata,
                    )
                )
        assert differences == []
    finally:
        await db.close()


@pytest.mark.parametrize("schema_source", ["migration", "metadata"])
async def test_defaulted_run_delivery_and_campaign_statuses(database_url, schema_source):
    from notifications.db.models import Base, Campaign
    from notifications.db.session import Database

    db = Database(database_url)
    try:
        if schema_source == "metadata":
            async with db.engine.begin() as connection:
                status_defaults = text(
                    "SELECT table_name, column_default FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name = 'status' "
                    "AND table_name IN ('schedule_runs', 'deliveries') ORDER BY table_name"
                )
                migrated_defaults = (await connection.execute(status_defaults)).all()
                await connection.run_sync(Base.metadata.drop_all)
                await connection.run_sync(Base.metadata.create_all)
                model_defaults = (await connection.execute(status_defaults)).all()
                assert model_defaults == migrated_defaults
        await seed_delivery(db)
        async with db.transaction() as session:
            campaign = Campaign(campaign_id=uuid4())
            session.add(campaign)
            await session.flush()
            assert campaign.intake_status == "open"
            assert campaign.created_at is not None
            assert campaign.expected_batches is None and campaign.closed_at is None
            assert await session.scalar(text("SELECT status FROM schedule_runs")) == "pending"
            assert await session.scalar(text("SELECT status FROM deliveries")) == "pending"
    finally:
        await db.close()
