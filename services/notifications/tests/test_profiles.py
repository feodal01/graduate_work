import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from notifications.config import Settings
from notifications.contracts import AuthEvent
from notifications.db.models import Delivery, Outbox, Profile
from notifications.db.session import Database


def auth_event(user_id, version=1, kind="user.profile_updated", email="user@example.org"):
    data = dict(event_id=uuid4(), event_type=kind, user_id=user_id, user_version=version)
    if kind != "user.deleted":
        data.update(email=email, first_name="First", last_name="Last", locale="ru", tz="UTC")
    return AuthEvent.model_validate(data)


def profile_handler(database_url):
    from notifications.profiles.events import AuthProfileHandler

    return AuthProfileHandler(
        Settings(
            process="consumer",
            database_url=database_url,
            kafka_bootstrap_servers="unused:9092",
            initial_likes=True,
            initial_new_films=False,
            initial_marketing=True,
        )
    )


@pytest.fixture
async def db(database_url):
    database = Database(database_url)
    try:
        yield database
    finally:
        await database.close()


async def test_updated_creates_without_welcome_and_preserves_preferences(db, database_url):
    handler = profile_handler(database_url)
    user_id = uuid4()
    async with db.transaction() as session:
        await handler(session, auth_event(user_id))
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert (profile.likes, profile.new_films, profile.marketing) == (True, False, True)
        await session.execute(
            update(Profile)
            .where(Profile.user_id == user_id)
            .values(
                likes=False,
                new_films=True,
                marketing=False,
            )
        )
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 2, email="new@example.org"))
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert not hasattr(profile, "email")
        assert (profile.likes, profile.new_films, profile.marketing) == (False, True, False)
        assert await session.scalar(select(func.count()).select_from(Delivery)) == 0


@pytest.mark.parametrize("existing", [False, True])
async def test_delete_tombstone_and_stale_registration(db, database_url, existing):
    handler = profile_handler(database_url)
    user_id = uuid4()
    if existing:
        async with db.transaction() as session:
            await handler(session, auth_event(user_id))
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 3, "user.deleted"))
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 2, "user.registered"))
        await handler(session, auth_event(user_id, 3))
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert profile.user_version == 3
        assert profile.deleted_at is not None
        assert (profile.likes, profile.new_films, profile.marketing) == (False, False, False)
        assert not hasattr(profile, "email")
        assert await session.scalar(select(func.count()).select_from(Delivery)) == 0


@pytest.mark.parametrize(
    "email, status, outbox_count",
    [
        ("user@example.org", "pending", 1),
        (None, "pending", 1),
    ],
)
async def test_registered_deduplicates_welcome(db, database_url, email, status, outbox_count):
    handler = profile_handler(database_url)
    user_id = uuid4()

    async def apply(version):
        async with db.transaction() as session:
            await handler(session, auth_event(user_id, version, "user.registered", email))

    await asyncio.gather(apply(1), apply(1))
    await apply(2)
    async with db.transaction() as session:
        deliveries = (await session.scalars(select(Delivery))).all()
        assert len(deliveries) == 1
        assert deliveries[0].status == status
        assert deliveries[0].kind == "welcome"
        assert await session.scalar(select(func.count()).select_from(Outbox)) == outbox_count


async def test_late_registration_after_update_does_not_send(db, database_url):
    handler = profile_handler(database_url)
    user_id = uuid4()
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 2))
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 1, "user.registered"))
    async with db.transaction() as session:
        assert await session.scalar(select(func.count()).select_from(Delivery)) == 0


async def test_concurrent_versions_keep_newest(db, database_url):
    handler = profile_handler(database_url)
    user_id = uuid4()

    async def apply(version):
        async with db.transaction() as session:
            await handler(session, auth_event(user_id, version, email=f"v{version}@example.org"))

    await asyncio.gather(apply(2), apply(1))
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert profile.user_version == 2
        assert not hasattr(profile, "email")


async def test_shared_transaction_rollback_includes_welcome(db, database_url):
    handler = profile_handler(database_url)
    with pytest.raises(RuntimeError, match="crash"):
        async with db.transaction() as session:
            await handler(session, auth_event(uuid4(), 1, "user.registered"))
            raise RuntimeError("crash")
    async with db.transaction() as session:
        for model in (Profile, Delivery, Outbox):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_public_apply_auth_uses_explicit_environment_flags(db, monkeypatch, database_url):
    from notifications.profiles.events import apply_auth

    values = dict(
        PROCESS="consumer",
        DATABASE_URL=database_url,
        KAFKA_BOOTSTRAP_SERVERS="unused:9092",
        INITIAL_LIKES="false",
        INITIAL_NEW_FILMS="true",
        INITIAL_MARKETING="false",
    )
    for name, value in values.items():
        monkeypatch.setenv(f"NOTIFICATIONS_{name}", value)
    user_id = uuid4()
    async with db.transaction() as session:
        await apply_auth(session, auth_event(user_id))
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert (profile.likes, profile.new_films, profile.marketing) == (False, True, False)
