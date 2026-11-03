import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from test_profiles import auth_event, profile_handler

from notifications.config import Settings
from notifications.db.models import Profile
from notifications.db.session import Database

SECRET = "test-notifications-secret-at-least-32-bytes"
URL = "/api/v1/notifications/preferences/me"


def headers(user_id, **overrides):
    now = datetime.now(UTC)
    claims = dict(
        sub=str(user_id),
        jti=str(uuid4()),
        type="access",
        iat=int(now.timestamp()),
        exp=int((now + timedelta(minutes=10)).timestamp()),
    )
    claims.update(overrides)
    claims = {key: value for key, value in claims.items() if value is not None}
    return {"Authorization": "Bearer " + jwt.encode(claims, SECRET, algorithm="HS256")}


@pytest.fixture
def auth_response():
    return {"status": 200, "body": None, "error": None, "requests": []}


@pytest.fixture
async def api(database_url, auth_response):
    from notifications.main import create_app

    app = create_app(
        Settings(
            process="api",
            database_url=database_url,
            jwt_secret=SecretStr(SECRET),
            jwt_algorithm="HS256",
            auth_introspection_url="https://auth.test/api/v1/internal/introspect",
            auth_introspection_api_key=SecretStr("test-service-key"),
            auth_introspection_timeout_seconds=1,
        )
    )

    def introspect(request):
        auth_response["requests"].append(request)
        if auth_response["error"] is not None:
            raise auth_response["error"]
        body = auth_response["body"]
        if body is None:
            claims = jwt.decode(json.loads(request.content)["token"], SECRET, algorithms=["HS256"])
            body = {"active": True, "sub": claims["sub"]}
        return httpx.Response(auth_response["status"], json=body)

    async with app.router.lifespan_context(app):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(introspect)) as auth_http,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://notifications"
            ) as client,
        ):
            app.state.auth_introspection.http = auth_http
            yield client


@pytest.fixture
async def db(database_url):
    database = Database(database_url)
    try:
        yield database
    finally:
        await database.close()


async def test_get_patch_only_own_flags(api, db, database_url):
    user_id, other_id = uuid4(), uuid4()
    handler = profile_handler(database_url)
    async with db.transaction() as session:
        for identity in (user_id, other_id):
            await handler(session, auth_event(identity))
    response = await api.get(URL, headers=headers(user_id))
    assert response.json() == dict(likes=True, new_films=False, marketing=True)
    response = await api.patch(URL, headers=headers(user_id), json={"likes": False})
    assert response.status_code == 200
    assert response.json() == dict(likes=False, new_films=False, marketing=True)
    response = await api.patch(
        URL, headers=headers(user_id), json={"user_id": str(other_id), "marketing": False}
    )
    assert response.status_code == 422
    response = await api.patch(
        URL + f"?user_id={other_id}", headers=headers(user_id), json={"marketing": False}
    )
    assert response.status_code == 200
    async with db.transaction() as session:
        other = await session.get(Profile, other_id)
        assert (other.likes, other.marketing) == (True, True)


@pytest.mark.parametrize(
    "payload", [{}, {"likes": None}, {"likes": 1}, {"likes": "false"}, {"unknown": False}]
)
async def test_patch_rejects_invalid_subset(api, payload):
    response = await api.patch(URL, headers=headers(uuid4()), json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize("method", ["GET", "PATCH"])
async def test_profile_lifecycle_errors(api, db, database_url, method):
    user_id = uuid4()
    kwargs = {"json": {"likes": True}} if method == "PATCH" else {}
    response = await api.request(method, URL, headers=headers(user_id), **kwargs)
    assert (response.status_code, response.json()["detail"]) == (409, "profile_not_ready")
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user_id, 2, "user.deleted"))
    response = await api.request(method, URL, headers=headers(user_id), **kwargs)
    assert (response.status_code, response.json()["detail"]) == (410, "profile_deleted")


@pytest.mark.parametrize(
    "overrides",
    [
        {"type": "refresh"},
        {"exp": 1},
        {"sub": "admin"},
        {"jti": "invalid"},
        {"iat": []},
        {"exp": []},
        {"iat": 1e100},
        {"exp": 1e100},
        *[{field: None} for field in ("sub", "jti", "type", "iat", "exp")],
    ],
)
async def test_invalid_token_claims(api, overrides):
    response = await api.get(URL, headers=headers(uuid4(), **overrides))
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize("authorization", [None, "Basic anything", "Bearer invalid"])
async def test_missing_malformed_token(api, authorization):
    response = await api.get(
        URL,
        headers={}
        if authorization is None
        else {
            "Authorization": authorization,
        },
    )
    assert response.status_code == 401


async def test_foreign_signature_and_algorithm_rejected(api):
    now = int(datetime.now(UTC).timestamp())
    claims = dict(sub=str(uuid4()), jti=str(uuid4()), type="access", iat=now, exp=now + 60)
    for secret, algorithm in [(SECRET + "other", "HS256"), (SECRET * 2, "HS512"), ("", "none")]:
        token = jwt.encode(claims, secret, algorithm=algorithm)
        response = await api.get(URL, headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401


async def test_patch_waiting_for_profile_update_keeps_preference(api, db, database_url):
    handler = profile_handler(database_url)
    user_id = uuid4()
    async with db.transaction() as session:
        await handler(session, auth_event(user_id))
    async with db.transaction() as session:
        await handler(session, auth_event(user_id, 2, email="new@example.org"))
        patch = asyncio.create_task(api.patch(URL, headers=headers(user_id), json={"likes": False}))
        # While the profile row lock is held, PATCH cannot finish its write.
        await asyncio.sleep(0.05)
        assert not patch.done()
    assert (await patch).status_code == 200
    async with db.transaction() as session:
        profile = await session.get(Profile, user_id)
        assert not hasattr(profile, "email")
        assert profile.likes is False


async def test_profile_update_waiting_for_patch_preserves_flags(api, db, database_url):
    handler = profile_handler(database_url)
    user_id = uuid4()
    async with db.transaction() as session:
        await handler(session, auth_event(user_id))

    async def update_profile():
        async with db.transaction() as session:
            await handler(session, auth_event(user_id, 2, email="new@example.org"))

    async with db.transaction() as session:
        profile = await session.scalar(
            select(Profile).where(Profile.user_id == user_id).with_for_update()
        )
        profile.likes = False
        await session.flush()
        updating = asyncio.create_task(update_profile())
        await asyncio.sleep(0.05)
        assert not updating.done()
    await updating
    response = await api.get(URL, headers=headers(user_id))
    assert response.json()["likes"] is False


async def test_concurrent_patches_preserve_both_changes(api, db, database_url):
    user_id = uuid4()
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user_id))
    first, second = await asyncio.gather(
        api.patch(URL, headers=headers(user_id), json={"likes": False}),
        api.patch(URL, headers=headers(user_id), json={"new_films": True}),
    )
    assert first.status_code == second.status_code == 200
    response = await api.get(URL, headers=headers(user_id))
    assert response.json() == dict(likes=False, new_films=True, marketing=True)


@pytest.mark.parametrize("method", ["GET", "PATCH"])
async def test_revocation_is_checked_on_every_request(api, db, database_url, auth_response, method):
    user_id = uuid4()
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user_id))
    bearer = headers(user_id)
    assert (await api.get(URL, headers=bearer)).status_code == 200
    auth_response["body"] = {"active": False}
    kwargs = {"json": {"likes": False}} if method == "PATCH" else {}
    result = await api.request(method, URL, headers=bearer, **kwargs)
    assert result.status_code == 401
    assert result.headers["www-authenticate"] == "Bearer"
    assert len(auth_response["requests"]) == 2
    async with db.transaction() as session:
        assert (await session.get(Profile, user_id)).likes is True


@pytest.mark.parametrize(
    "failure",
    [{"status": code} for code in (401, 403, 429, 500, 503, 302)]
    + [
        {"body": {}},
        {"body": {"active": "false"}},
        {"body": {"active": True}},
        {"body": {"active": True, "sub": str(uuid4())}},
        {"body": {"active": False, "sub": str(uuid4())}},
        {"error": httpx.ConnectError("private transport detail")},
        {"error": httpx.ReadTimeout("private timeout detail")},
    ],
)
async def test_auth_failure_never_changes_preferences(
    api, db, database_url, auth_response, failure
):
    user_id = uuid4()
    async with db.transaction() as session:
        await profile_handler(database_url)(session, auth_event(user_id))
    auth_response.update(failure)
    result = await api.patch(URL, headers=headers(user_id), json={"likes": False})
    assert result.status_code == 503
    assert result.json() == {"detail": "Authentication service unavailable"}
    async with db.transaction() as session:
        assert (await session.get(Profile, user_id)).likes is True


async def test_passes_token_service_credentials_and_request_id(api, auth_response):
    bearer = headers(uuid4())
    await api.get(URL, headers={**bearer, "X-Request-Id": "preferences-check"})
    request = auth_response["requests"][0]
    assert request.method == "POST"
    assert str(request.url) == "https://auth.test/api/v1/internal/introspect"
    assert request.headers["X-Service-Key"] == "test-service-key"
    assert request.headers["X-Request-Id"] == "preferences-check"
    assert json.loads(request.content) == {"token": bearer["Authorization"].removeprefix("Bearer ")}
