from uuid import uuid4

import pytest

from notifications.contracts import AuthEvent
from notifications.db.models import Profile
from notifications.delivery.policy import skip_reason


def test_minimal_event_discards_legacy_contact_fields():
    event = AuthEvent.model_validate(
        dict(
            event_id=str(uuid4()),
            event_type="user.registered",
            user_id=str(uuid4()),
            user_version=1,
            tz="UTC",
            email="private@example.test",
            first_name="Private",
            last_name=None,
            locale="ru",
        )
    )
    assert "email" not in event.model_dump()
    assert "email" not in Profile.__table__.columns


def test_email_is_not_local_eligibility_and_bad_timezone_is_terminal():
    profile = Profile(user_id=uuid4(), tz=None, likes=True)
    assert skip_reason(profile, None, send_class="transactional") is None
    assert skip_reason(profile, "likes", send_class="service") == "invalid_timezone"


async def test_contacts_validate_complete_unique_response():
    import httpx

    from notifications.clients.auth_contacts import AuthContactsClient, ContactsError

    user_id = uuid4()

    async def handle(request):
        assert request.headers["X-Service-Key"] == "test-key"
        return httpx.Response(200, json={"items": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = AuthContactsClient("https://auth.test/contacts", "test-key", 2, 10, http=http)
        with pytest.raises(ContactsError, match="invalid_response"):
            await client._fetch(http, [user_id])


def test_populated_contact_migration_preserves_local_state(database_url):
    import psycopg
    from conftest import migrate

    migrate(database_url, "downgrade", "006_configurable_batch_size")
    uid = uuid4()
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        conn.execute(
            "INSERT INTO profiles(user_id,user_version,email,first_name,last_name,locale,tz,"
            "likes,new_films,marketing,deleted_at) VALUES (%s,19,'old@example.test','Private',"
            "'Surname','ru','Europe/Moscow',false,false,false,now())",
            (uid,),
        )
        before = conn.execute(
            "SELECT user_id,user_version,tz,likes,new_films,marketing,deleted_at "
            "FROM profiles WHERE user_id=%s",
            (uid,),
        ).fetchone()
        conn.commit()
    migrate(database_url, "upgrade", "head")
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        assert (
            conn.execute(
                "SELECT user_id,user_version,tz,likes,new_films,marketing,deleted_at "
                "FROM profiles WHERE user_id=%s",
                (uid,),
            ).fetchone()
            == before
        )
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='profiles'"
            )
        }
        assert not columns & {"email", "first_name", "last_name", "locale"}
