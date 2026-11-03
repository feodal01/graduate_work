from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import select
from test_campaigns_api import URL, batch, counts, item, profiles
from test_campaigns_api import campaign_api as campaign_api

from notifications.config import Settings
from notifications.db.models import Campaign


@pytest.mark.parametrize("key", [None, "", "   "])
def test_campaign_api_requires_nonempty_secret(key):
    values = dict(
        process="campaign-api",
        database_url="postgresql+asyncpg://localhost/test",
    )
    if key is not None:
        values["campaign_api_key"] = key
    with pytest.raises(ValidationError):
        Settings.model_validate(values)


@pytest.mark.parametrize("key", ["", "wrong", "revoked"])
async def test_key_required_before_create_replay_close_status(campaign_api, key):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}"
    before = await counts(db)
    assert (
        await api.post(endpoint + "/batches", json=body, headers={"X-API-Key": key})
    ).status_code == 401
    assert await counts(db) == before
    assert (await api.post(endpoint + "/batches", json=body)).status_code == 202
    before = await counts(db)
    for method, suffix, payload in [
        ("POST", "/batches", body),
        ("POST", "/close", {"expected_batches": 1}),
        ("GET", "", None),
    ]:
        result = await api.request(
            method, endpoint + suffix, json=payload, headers={"X-API-Key": key}
        )
        assert result.status_code == 401
    assert await counts(db) == before
    assert (await api.get(endpoint)).json()["intake_status"] == "open"


async def test_author_identity_and_required_new_campaign(campaign_api):
    api, db = campaign_api
    users, campaign = [uuid4(), uuid4()], uuid4()
    await profiles(db, *users)
    body = batch(item(users[0]))
    endpoint = f"{URL}/{campaign}"
    without_author = {k: v for k, v in body.items() if k != "created_by_user_id"}
    assert (await api.post(endpoint + "/batches", json=without_author)).status_code == 422
    assert (await api.post(endpoint + "/batches", json=body)).status_code == 202
    assert (await api.get(endpoint)).json()["created_by_user_id"] == body["created_by_user_id"]
    changed = {**body, "created_by_user_id": str(uuid4())}
    assert (await api.post(endpoint + "/batches", json=changed)).json()[
        "error_code"
    ] == "batch_content_conflict"
    changed.update(batch_id=str(uuid4()), items=[item(users[1])])
    assert (await api.post(endpoint + "/batches", json=changed)).json()[
        "error_code"
    ] == "campaign_author_conflict"
    async with db.transaction() as session:
        saved = await session.scalar(select(Campaign).where(Campaign.campaign_id == campaign))
        assert saved.created_by_user_id == UUID(body["created_by_user_id"])


async def test_missing_key_header_is_unauthorized(campaign_api):
    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    saved_body = batch(item(user))
    endpoint = f"{URL}/{campaign}"
    assert (await api.post(endpoint + "/batches", json=saved_body)).status_code == 202
    api.headers.pop("X-API-Key")
    for method, suffix, body in [
        ("POST", "/batches", saved_body),
        ("POST", "/close", {"expected_batches": 1}),
        ("GET", "", None),
    ]:
        response = await api.request(method, endpoint + suffix, json=body)
        assert response.status_code == 401
    assert (await api.post(f"{URL}/{uuid4()}/batches", json=batch(item()))).status_code == 401
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_rotation_revokes_old_key_even_for_saved_replay(campaign_api, database_url):
    import httpx
    from test_campaigns_api import API_KEY

    from notifications.campaigns.app import create_campaign_app

    api, db = campaign_api
    user, campaign = uuid4(), uuid4()
    await profiles(db, user)
    body = batch(item(user))
    endpoint = f"{URL}/{campaign}"
    assert (await api.post(endpoint + "/batches", json=body)).status_code == 202
    replacement = "replacement-test-only-key"
    app = create_campaign_app(
        Settings(
            process="campaign-api",
            database_url=database_url,
            campaign_api_key=SecretStr(replacement),
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://rotated"
        ) as rotated:
            for method, suffix, payload in [
                ("POST", "/batches", body),
                ("POST", "/close", {"expected_batches": 1}),
                ("GET", "", None),
            ]:
                rejected = await rotated.request(
                    method, endpoint + suffix, json=payload, headers={"X-API-Key": API_KEY}
                )
                assert rejected.status_code == 401
                assert API_KEY not in rejected.text and replacement not in rejected.text
            assert (
                await rotated.post(
                    endpoint + "/batches", json=body, headers={"X-API-Key": replacement}
                )
            ).status_code == 202
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_first_batches_different_authors_serialize(campaign_api):
    import asyncio

    api, db = campaign_api
    users, campaign = [uuid4(), uuid4()], uuid4()
    await profiles(db, *users)
    bodies = [dict(batch(item(user)), created_by_user_id=str(uuid4())) for user in users]
    results = await asyncio.gather(
        *(api.post(f"{URL}/{campaign}/batches", json=body) for body in bodies)
    )
    assert sorted(result.status_code for result in results) == [202, 409]
    winner = next(
        body for body, result in zip(bodies, results, strict=True) if result.status_code == 202
    )
    loser = next(result for result in results if result.status_code == 409)
    assert loser.json()["error_code"] == "campaign_author_conflict"
    assert (await api.get(f"{URL}/{campaign}")).json()["created_by_user_id"] == winner[
        "created_by_user_id"
    ]
    assert await counts(db) == [1, 1, 1, 1, 1]


async def test_legacy_exact_hash_replay_only(campaign_api):
    import hashlib
    import json

    from notifications.db.models import CampaignBatch

    api, db = campaign_api
    campaign, batch_id = uuid4(), uuid4()
    legacy = {"batch_id": str(batch_id), "items": [item()]}
    legacy["items"][0]["available_at"] = None
    old_hash = hashlib.sha256(
        json.dumps(legacy, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    async with db.transaction() as session:
        session.add(Campaign(campaign_id=campaign))
        await session.flush()
        session.add(
            CampaignBatch(
                campaign_id=campaign, batch_id=batch_id, request_hash=old_hash, item_count=1
            )
        )
    endpoint = f"{URL}/{campaign}"
    assert (await api.post(endpoint + "/batches", json=legacy)).status_code == 202
    key = api.headers.pop("X-API-Key")
    assert (await api.post(endpoint + "/batches", json=legacy)).status_code == 401
    api.headers["X-API-Key"] = key
    changed_text = {**legacy, "items": [dict(legacy["items"][0], text="changed")]}
    response = await api.post(endpoint + "/batches", json=changed_text)
    assert response.status_code == 409
    assert response.json()["error_code"] == "batch_content_conflict"
    changed = {**legacy, "created_by_user_id": str(uuid4())}
    assert (await api.post(endpoint + "/batches", json=changed)).json()[
        "error_code"
    ] == "batch_content_conflict"
    for author in [None, str(uuid4())]:
        new_batch = {**legacy, "batch_id": str(uuid4())}
        if author is not None:
            new_batch["created_by_user_id"] = author
        response = await api.post(endpoint + "/batches", json=new_batch)
        assert response.status_code == 409
        assert response.json()["error_code"] == "campaign_author_conflict"
    assert (await api.get(endpoint)).json()["created_by_user_id"] is None
    assert (await api.post(endpoint + "/close", json={"expected_batches": 1})).status_code == 200
    assert (await api.post(endpoint + "/batches", json=legacy)).status_code == 202
    async with db.transaction() as session:
        saved = await session.get(CampaignBatch, (campaign, batch_id))
        assert saved.request_hash == old_hash
        assert (await session.get(Campaign, campaign)).created_by_user_id is None
    assert await counts(db) == [1, 1, 0, 0, 0]


def test_campaign_secret_masked_in_config_and_missing_neighbor_error(monkeypatch):
    secret = "q7"
    monkeypatch.setenv("NOTIFICATIONS_CAMPAIGN_API_KEY", secret)
    configured = Settings(
        process="campaign-api",
        database_url="postgresql+asyncpg://localhost/test",
    )
    assert configured.campaign_api_key is not None
    assert configured.campaign_api_key.get_secret_value() == secret
    assert secret not in repr(configured)
    monkeypatch.delenv("NOTIFICATIONS_CAMPAIGN_API_KEY")
    with pytest.raises(ValidationError) as exc:
        Settings(process="campaign-api", database_url="postgresql+asyncpg://localhost/test")
    assert secret not in str(exc.value)


@pytest.mark.parametrize("author", [None, "not-a-uuid", 123])
async def test_invalid_explicit_author_rejected_atomically(campaign_api, author):
    api, db = campaign_api
    body = dict(batch(item()), created_by_user_id=author)
    response = await api.post(f"{URL}/{uuid4()}/batches", json=body)
    assert response.status_code == 422
    assert await counts(db) == [0, 0, 0, 0, 0]


@pytest.mark.parametrize("key", [None, ""])
def test_real_campaign_api_process_rejects_unconfigured_key(key):
    import os
    import subprocess
    import sys

    env = {
        name: value for name, value in os.environ.items() if not name.startswith("NOTIFICATIONS_")
    }
    env.update(
        NOTIFICATIONS_PROCESS="campaign-api",
        NOTIFICATIONS_DATABASE_URL="postgresql+asyncpg://localhost/test",
        NOTIFICATIONS_INVALID_TZ_RETRY_SECONDS="60",
    )
    if key is not None:
        env["NOTIFICATIONS_CAMPAIGN_API_KEY"] = key
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "notifications.campaigns.app:create_campaign_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            "0",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "campaign_api_key" in result.stderr
