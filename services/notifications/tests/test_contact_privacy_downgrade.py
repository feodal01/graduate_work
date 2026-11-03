"""Rollback refuses data states that the previous schema cannot represent."""

from uuid import uuid4

import psycopg
import pytest
from conftest import migrate


@pytest.mark.parametrize(
    "reason,redacted", [("invalid_timezone", False), ("contact_unavailable", False), (None, True)]
)
def test_incompatible_downgrade_preserves_schema_and_history(database_url, reason, redacted):
    uid, did = uuid4(), uuid4()
    url = database_url.replace("+asyncpg", "")
    with psycopg.connect(url) as connection:
        connection.execute(
            "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
            "VALUES (%s,7,NULL,true,true,true)",
            (uid,),
        )
        connection.execute(
            "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,status,terminal_reason,"
            "terminal_at,prepared_at,message_id) VALUES (%s,%s,%s,'welcome','skipped',%s,now(),"
            "CASE WHEN %s THEN now() END,CASE WHEN %s THEN '<retained@test>' END)",
            (did, str(did), uid, reason, redacted, redacted),
        )
        connection.commit()
        before = connection.execute(
            "SELECT * FROM deliveries WHERE delivery_id=%s", (did,)
        ).fetchone()
        constraints = connection.execute(
            "SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid='deliveries'::regclass ORDER BY conname"
        ).fetchall()
    with pytest.raises(AssertionError, match="Restore a pre-007 backup"):
        migrate(database_url, "downgrade", "006_configurable_batch_size")
    with psycopg.connect(url) as connection:
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (
            "009_film_digest_selection",
        )
        assert (
            connection.execute("SELECT * FROM deliveries WHERE delivery_id=%s", (did,)).fetchone()
            == before
        )
        assert (
            connection.execute(
                "SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid='deliveries'::regclass ORDER BY conname"
            ).fetchall()
            == constraints
        )
        assert connection.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name='profiles' AND column_name='email'"
        ).fetchone() == (0,)


def test_compatible_populated_downgrade_preserves_history_without_restoring_contacts(database_url):
    uid, did = uuid4(), uuid4()
    url = database_url.replace("+asyncpg", "")
    with psycopg.connect(url) as connection:
        connection.execute(
            "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
            "VALUES (%s,7,'UTC',true,true,true)",
            (uid,),
        )
        connection.execute(
            "INSERT INTO deliveries(delivery_id,logical_key,user_id,kind,status,"
            "terminal_reason,terminal_at) "
            "VALUES (%s,%s,%s,'welcome','skipped','no_email',now())",
            (did, str(did), uid),
        )
        connection.commit()
    migrate(database_url, "downgrade", "006_configurable_batch_size")
    with psycopg.connect(url) as connection:
        assert connection.execute(
            "SELECT user_version,tz,email,first_name,last_name,locale FROM profiles "
            "WHERE user_id=%s",
            (uid,),
        ).fetchone() == (7, "UTC", None, None, None, None)
        assert connection.execute(
            "SELECT status,terminal_reason FROM deliveries WHERE delivery_id=%s", (did,)
        ).fetchone() == ("skipped", "no_email")


def test_campaign_reason_rejects_downgrade_without_changing_recipient(database_url):
    uid, campaign, batch = uuid4(), uuid4(), uuid4()
    url = database_url.replace("+asyncpg", "")
    with psycopg.connect(url) as connection:
        connection.execute(
            "INSERT INTO profiles(user_id,user_version,likes,new_films,marketing) "
            "VALUES (%s,1,true,true,true)",
            (uid,),
        )
        connection.execute("INSERT INTO campaigns(campaign_id) VALUES (%s)", (campaign,))
        connection.execute(
            "INSERT INTO campaign_batches(campaign_id,batch_id,request_hash,item_count) "
            "VALUES (%s,%s,%s,1)",
            (campaign, batch, "a" * 64),
        )
        connection.execute(
            "INSERT INTO campaign_recipients(campaign_id,user_id,batch_id,template_id,channel,"
            "subject,text,effective_available_at,skip_reason) VALUES (%s,%s,%s,'personal_offer',"
            "'email','subject','text',now(),'invalid_timezone')",
            (campaign, uid, batch),
        )
        connection.commit()
        before = connection.execute("SELECT * FROM campaign_recipients").fetchall()
    with pytest.raises(AssertionError, match="Restore a pre-007 backup"):
        migrate(database_url, "downgrade", "006_configurable_batch_size")
    with psycopg.connect(url) as connection:
        assert connection.execute("SELECT * FROM campaign_recipients").fetchall() == before
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (
            "009_film_digest_selection",
        )
