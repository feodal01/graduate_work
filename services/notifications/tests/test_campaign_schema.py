"""Campaign constraints and populated PostgreSQL migration regression."""

from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from conftest import migrate
from psycopg.rows import DictRow, dict_row


@pytest.fixture
def campaign_connection(database_url):
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        user_id, campaign_id, batch_id, delivery_id = [uuid4() for _ in range(4)]
        conn.execute(
            "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
            "VALUES (%s,1,false,false,true)",
            (user_id,),
        )
        conn.execute("INSERT INTO campaigns(campaign_id) VALUES (%s)", (campaign_id,))
        conn.execute(
            "INSERT INTO campaign_batches(campaign_id,batch_id,request_hash,item_count) "
            "VALUES (%s,%s,%s,1)",
            (campaign_id, batch_id, "a" * 64),
        )
        conn.execute(
            "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,external_send_class,"
            "required_subscription) VALUES (%s,%s,%s,'external','bulk','marketing')",
            (delivery_id, str(delivery_id), user_id),
        )
        conn.execute(
            "INSERT INTO campaign_recipients(campaign_id,batch_id,user_id,delivery_id,"
            "template_id,channel,subject,text,effective_available_at) "
            "VALUES (%s,%s,%s,%s,'personal_offer','email','Offer','Body',now())",
            (campaign_id, batch_id, user_id, delivery_id),
        )
        conn.commit()
        yield conn


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE campaigns SET intake_status='invalid'",
        "UPDATE campaigns SET expected_batches=0",
        "UPDATE campaigns SET intake_status='closed'",
        "UPDATE campaign_batches SET item_count=0",
        "UPDATE campaign_batches SET item_count=-1",
        "UPDATE campaign_batches SET request_hash='invalid'",
        "UPDATE campaign_batches SET campaign_id=gen_random_uuid()",
        "INSERT INTO campaign_batches SELECT * FROM campaign_batches",
        "INSERT INTO campaign_recipients SELECT * FROM campaign_recipients",
        "UPDATE campaign_recipients SET batch_id=gen_random_uuid()",
        "UPDATE campaign_recipients SET user_id=gen_random_uuid()",
        "UPDATE campaign_recipients SET delivery_id=gen_random_uuid()",
        "UPDATE campaign_recipients SET delivery_id=NULL",
        "UPDATE campaign_recipients SET skip_reason='no_email'",
        "UPDATE campaign_recipients SET delivery_id=NULL,skip_reason='unknown'",
        "UPDATE campaign_recipients SET channel='sms'",
        "UPDATE campaign_recipients SET subject=NULL",
        "UPDATE campaign_recipients SET text=NULL",
        "UPDATE campaign_recipients SET effective_available_at=NULL",
        "UPDATE deliveries SET external_send_class=NULL",
        "UPDATE deliveries SET external_send_class='invalid'",
        "UPDATE deliveries SET required_subscription='invalid'",
        "UPDATE deliveries SET terminal_reason='invalid'",
        "UPDATE deliveries SET kind='welcome'",
    ],
)
def test_campaign_constraints(campaign_connection, statement):
    with pytest.raises(psycopg.IntegrityError):
        campaign_connection.execute(statement)


def test_external_classes_generated_and_policy_snapshot(campaign_connection):
    conn = campaign_connection
    for send_class in ("transactional", "service", "bulk"):
        conn.execute("UPDATE deliveries SET external_send_class=%s", (send_class,))
        assert conn.execute("SELECT send_class FROM deliveries").fetchone() == (send_class,)
    conn.execute("UPDATE deliveries SET required_subscription=NULL")
    conn.commit()
    with pytest.raises(psycopg.errors.GeneratedAlways):
        conn.execute("UPDATE deliveries SET send_class='service'")
    conn.rollback()
    conn.execute("UPDATE deliveries SET terminal_reason='smtp_uncertain'")
    conn.execute(
        "UPDATE deliveries SET kind='likes', external_send_class=NULL, required_subscription=NULL"
    )
    assert conn.execute("SELECT terminal_reason,send_class FROM deliveries").fetchone() == (
        "smtp_uncertain",
        "service",
    )


def test_external_delivery_cannot_use_schedule_run(campaign_connection):
    conn = campaign_connection
    run_id = uuid4()
    conn.execute(
        "INSERT INTO schedule_runs(run_id,kind,period_start) VALUES (%s,'likes',now())", (run_id,)
    )
    conn.commit()
    with pytest.raises(psycopg.IntegrityError):
        conn.execute("UPDATE deliveries SET run_id=%s", (run_id,))


def test_recipient_delivery_is_unique_and_batch_fk_is_composite(campaign_connection):
    conn = campaign_connection
    other_campaign = uuid4()
    conn.execute("INSERT INTO campaigns(campaign_id) VALUES (%s)", (other_campaign,))
    conn.execute(
        "INSERT INTO campaign_batches(campaign_id,batch_id,request_hash,item_count) "
        "SELECT %s,batch_id,request_hash,item_count FROM campaign_batches",
        (other_campaign,),
    )
    conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(
            "INSERT INTO campaign_recipients SELECT %s,user_id,batch_id,delivery_id,"
            "template_id,channel,subject,text,source_available_at,"
            "effective_available_at,skip_reason "
            "FROM campaign_recipients",
            (other_campaign,),
        )
    conn.rollback()
    conn.execute("UPDATE campaign_recipients SET delivery_id=NULL,skip_reason='no_email'")
    conn.execute("DELETE FROM campaign_batches WHERE campaign_id=%s", (other_campaign,))
    conn.commit()
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute("UPDATE campaign_recipients SET campaign_id=%s", (other_campaign,))


def test_tombstones_and_original_availability_are_preserved(campaign_connection):
    conn = campaign_connection
    source = datetime(2026, 10, 10, tzinfo=UTC)
    effective = datetime(2026, 10, 11, tzinfo=UTC)
    conn.execute("UPDATE profiles SET marketing=false,deleted_at=now()")
    conn.execute(
        "UPDATE campaign_recipients SET delivery_id=NULL,skip_reason='profile_deleted',"
        "source_available_at=%s,effective_available_at=%s",
        (source, effective),
    )
    conn.execute("UPDATE campaigns SET intake_status='closed',expected_batches=1,closed_at=now()")
    assert conn.execute(
        "SELECT source_available_at,effective_available_at,skip_reason FROM campaign_recipients"
    ).fetchone() == (source, effective, "profile_deleted")


def test_internal_policy_columns_stay_null(campaign_connection):
    conn = campaign_connection
    conn.execute(
        "UPDATE deliveries SET kind='likes',external_send_class=NULL,required_subscription=NULL"
    )
    conn.commit()
    for assignment in ("external_send_class='bulk'", "required_subscription='likes'"):
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute("UPDATE deliveries SET " + assignment)


def test_send_class_cannot_be_explicitly_inserted(campaign_connection):
    with pytest.raises(psycopg.errors.GeneratedAlways):
        campaign_connection.execute(
            "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,send_class) "
            "SELECT gen_random_uuid(),'explicit-class',user_id,'likes','service' FROM profiles"
        )


def test_downgrade_refuses_to_destroy_external_deliveries(campaign_connection, database_url):
    conn = campaign_connection
    with pytest.raises(AssertionError, match="Cannot downgrade campaigns"):
        migrate(database_url, "downgrade", "003")
    assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
        "009_film_digest_selection",
    )
    assert conn.execute(
        "SELECT count(*) FROM campaign_recipients JOIN deliveries USING(delivery_id)"
    ).fetchone() == (1,)


def test_configurable_batch_migration_and_safe_downgrade(campaign_connection, database_url):
    conn = campaign_connection
    migrate(database_url, "downgrade", "005_campaign_author")
    with pytest.raises(psycopg.IntegrityError):
        conn.execute("UPDATE campaign_batches SET item_count=101")
    conn.rollback()
    migrate(database_url, "upgrade", "head")
    conn.execute("UPDATE campaign_batches SET item_count=101")
    conn.commit()
    with pytest.raises(AssertionError, match="item_count_range"):
        migrate(database_url, "downgrade", "005_campaign_author")
    assert conn.execute("SELECT item_count FROM campaign_batches").fetchone() == (101,)
    assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
        "009_film_digest_selection",
    )


def test_populated_003_upgrade_downgrade_preserves_existing_work(database_url):
    migrate(database_url, "downgrade", "003")
    url = database_url.replace("+asyncpg", "")
    tables = ("deliveries", "outbox", "schedule_runs", "schedule_recipients", "delivery_items")
    with psycopg.Connection[DictRow].connect(url, row_factory=dict_row) as conn:
        for kind in ("welcome", "likes", "new_films"):
            user_id, delivery_id, run_id = uuid4(), uuid4(), uuid4()
            conn.execute(
                "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
                "VALUES (%s,1,true,true,true)",
                (user_id,),
            )
            if kind != "welcome":
                conn.execute(
                    "INSERT INTO schedule_runs(run_id,kind,period_start) VALUES (%s,%s,now())",
                    (run_id, kind),
                )
                conn.execute(
                    "INSERT INTO schedule_recipients(run_id,user_id) VALUES (%s,%s)",
                    (run_id, user_id),
                )
            conn.execute(
                "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,run_id,status,"
                "payload,message_id,prepared_at,next_attempt_at) "
                'VALUES (%s,%s,%s,%s,%s,\'prepared\',\'{"subject":"Frozen","html":"Body"}\','
                "%s,now(),now()+interval '3 days')",
                (
                    delivery_id,
                    str(delivery_id),
                    user_id,
                    kind,
                    run_id if kind != "welcome" else None,
                    str(delivery_id),
                ),
            )
            conn.execute(
                "INSERT INTO outbox(outbox_id,delivery_id,available_at) "
                "SELECT %s,delivery_id,next_attempt_at FROM deliveries WHERE delivery_id=%s",
                (uuid4(), delivery_id),
            )
            if kind == "likes":
                conn.execute(
                    "INSERT INTO review_counters(review_id,user_id,likes_count,likes_version) "
                    "VALUES (%s,%s,5,1)",
                    ("a" * 24, user_id),
                )
                conn.execute(
                    "INSERT INTO delivery_items(item_id,delivery_id,review_id,likes_count) "
                    "VALUES (%s,%s,%s,5)",
                    (uuid4(), delivery_id, "a" * 24),
                )
        before = {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
        indexes = conn.execute(
            "SELECT indexname,indexdef FROM pg_indexes WHERE tablename IN ('deliveries','outbox') "
            "ORDER BY indexname"
        ).fetchall()
    for direction, revision in (("upgrade", "head"), ("downgrade", "003"), ("upgrade", "head")):
        migrate(database_url, direction, revision)
        with psycopg.Connection[DictRow].connect(url, row_factory=dict_row) as conn:
            for table in tables:
                after = conn.execute(f"SELECT * FROM {table}").fetchall()
                if table == "deliveries" and direction == "upgrade":
                    for row in after:
                        for name in (
                            "external_send_class",
                            "required_subscription",
                            "terminal_reason",
                            "terminal_at",
                        ):
                            assert row.pop(name) is None
                if table == "schedule_runs" and direction == "upgrade":
                    for row in after:
                        assert row.pop("films_per_digest") is None
                assert after == before[table]
            assert (
                conn.execute(
                    "SELECT indexname,indexdef FROM pg_indexes "
                    "WHERE tablename IN ('deliveries','outbox') ORDER BY indexname"
                ).fetchall()
                == indexes
            )


def test_populated_004_author_upgrade_preserves_history_and_hashes(database_url):
    migrate(database_url, "downgrade", "004_campaigns")
    campaign, batch = uuid4(), uuid4()
    old_hash = "b" * 64
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        conn.execute("INSERT INTO campaigns(campaign_id) VALUES (%s)", (campaign,))
        conn.execute(
            "INSERT INTO campaign_batches(campaign_id,batch_id,request_hash,item_count) "
            "VALUES (%s,%s,%s,1)",
            (campaign, batch, old_hash),
        )
        conn.commit()
        before = conn.execute("SELECT * FROM campaign_batches").fetchall()
        conn.commit()
        migrate(database_url, "upgrade", "head")
        assert conn.execute(
            "SELECT created_by_user_id FROM campaigns WHERE campaign_id=%s", (campaign,)
        ).fetchone() == (None,)
        assert conn.execute("SELECT * FROM campaign_batches").fetchall() == before
        # Author identity has no recipient/profile FK: an unrelated UUID is valid.
        conn.execute("UPDATE campaigns SET created_by_user_id=%s", (uuid4(),))


def test_content_retention_migration_preserves_existing_campaigns(
    campaign_connection, database_url
):
    conn = campaign_connection
    migrate(database_url, "downgrade", "007_contact_privacy")
    tables = ("campaigns", "campaign_batches", "campaign_recipients", "deliveries")
    before = {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
    conn.commit()
    for direction, revision in (
        ("upgrade", "head"),
        ("downgrade", "007_contact_privacy"),
        ("upgrade", "head"),
    ):
        migrate(database_url, direction, revision)
        assert {
            table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in tables
        } == before
        conn.commit()


def test_content_retention_downgrade_refuses_erased_text(campaign_connection, database_url):
    conn = campaign_connection
    conn.execute("UPDATE deliveries SET status='skipped', terminal_at=now()")
    conn.execute("UPDATE campaign_recipients SET subject=NULL,text=NULL")
    tables = ("campaigns", "campaign_batches", "campaign_recipients", "deliveries")
    before = {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
    conn.commit()
    with pytest.raises(AssertionError, match="Restore a pre-008 backup"):
        migrate(database_url, "downgrade", "007_contact_privacy")
    assert {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in tables} == before
    assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
        "009_film_digest_selection",
    )
    assert conn.execute(
        "SELECT column_name,is_nullable FROM information_schema.columns "
        "WHERE table_name='campaign_recipients' AND column_name IN ('subject','text') "
        "ORDER BY column_name"
    ).fetchall() == [("subject", "YES"), ("text", "YES")]


def test_cancelled_campaigns_table_and_cancelled_by_sender_constraint(
    campaign_connection, database_url
):
    """T-24 (14-er-notifications-db-target, NTF-FR-02): схема cancelled_campaigns и CHECK."""
    conn = campaign_connection
    cid = uuid4()
    conn.execute("INSERT INTO cancelled_campaigns(campaign_id) VALUES (%s)", (cid,))
    conn.execute(
        "UPDATE deliveries SET status='cancelled', terminal_reason='cancelled_by_sender', "
        "terminal_at=now()"
    )
    conn.commit()
    with pytest.raises(AssertionError, match="Restore a pre-009 backup"):
        migrate(database_url, "downgrade", "008_campaign_content_retention")
    assert conn.execute(
        "SELECT count(*) FROM cancelled_campaigns WHERE campaign_id=%s", (cid,)
    ).fetchone() == (1,)
