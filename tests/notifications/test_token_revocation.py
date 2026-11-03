"""Access revocation across real Auth and Notifications HTTP processes."""

import json
import os
import signal
from contextlib import ExitStack
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from conftest import compose, published_port
from test_end_to_end import SECRET, ContactsAuth, Processes, http, introspection_env

PREFERENCES = "/api/v1/notifications/preferences/me"
PASSWORD = "Unusual-7-Cinema!Password"


def response(url, token, *, method="GET", data=None):
    request = Request(
        url,
        data=None if data is None else json.dumps(data).encode(),
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "X-Request-Id": "token-revocation-test",
        },
        method=method,
    )
    try:
        result = urlopen(request, timeout=10)
    except HTTPError as error:
        result = error
    with result:
        body = result.read()
        return result.status, json.loads(body) if body else None


def remove_sessions(user_id):
    keys = compose(
        "exec", "-T", "redis", "redis-cli", "--scan", "--pattern", f"{{auth-session:{user_id}}}:*"
    ).splitlines()
    if keys:
        compose("exec", "-T", "redis", "redis-cli", "DEL", *keys)


@pytest.fixture
def stand(tmp_path):
    endpoints = {
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": str(published_port("postgres", 5432)),
        "POSTGRES_USER": "notifications_test",
        "POSTGRES_PASSWORD": "notifications_test",
        "REDIS_HOST": "127.0.0.1",
        "REDIS_PORT": str(published_port("redis", 6379)),
    }
    pg_base = (
        "postgresql://notifications_test:notifications_test@127.0.0.1:"
        + endpoints["POSTGRES_PORT"]
        + "/"
    )
    processes = Processes(tmp_path)
    with ExitStack() as resources:
        admin = resources.enter_context(
            psycopg.connect(pg_base + "notifications_test_admin", autocommit=True)
        )
        name = "notifications_test_" + uuid4().hex
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        resources.callback(
            admin.execute, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
        )
        try:
            auth = ContactsAuth(processes, endpoints, resources, admin, pg_base)
            login = "revocation-" + uuid4().hex
            user = http(
                auth.url + "/api/v1/auth/signup",
                {"login": login, "password": PASSWORD, "tz": "Europe/Moscow"},
            )
            user_id = user["id"]
            resources.callback(remove_sessions, user_id)
            pair = http(auth.url + "/api/v1/auth/login", {"login": login, "password": PASSWORD})
            env = {
                "NOTIFICATIONS_PROCESS": "api",
                "NOTIFICATIONS_DATABASE_URL": (pg_base + name).replace(
                    "postgresql://", "postgresql+asyncpg://"
                ),
                "NOTIFICATIONS_JWT_SECRET": SECRET,
                "NOTIFICATIONS_JWT_ALGORITHM": "HS256",
                **introspection_env(auth.url),
            }
            processes.run("notifications", env, "alembic", "upgrade", "head")
            with psycopg.connect(pg_base + name) as db:
                db.execute(
                    "INSERT INTO profiles(user_id,user_version,tz,likes,new_films,marketing) "
                    "VALUES (%s,1,'Europe/Moscow',true,false,false)",
                    (user_id,),
                )
            api = processes.api(
                "notifications", env, "notifications.main:create_app", "/openapi.json"
            )
            yield {
                "auth": auth,
                "api": api,
                "pair": pair,
                "login": login,
                "user_id": user_id,
                "database": pg_base + name,
            }
        finally:
            processes.close()


def test_logout_revokes_same_token_for_preferences(stand):
    access = stand["pair"]["access_token"]
    url = stand["api"] + PREFERENCES
    assert response(url, access, method="PATCH", data={"likes": False})[0] == 200
    assert (
        response(
            stand["auth"].url + "/api/v1/auth/logout",
            access,
            method="POST",
            data={"refresh_token": stand["pair"]["refresh_token"]},
        )[0]
        == 204
    )
    assert response(stand["auth"].url + "/api/v1/auth/profile", access)[0] == 401
    assert response(url, access, method="PATCH", data={"likes": True})[0] == 401
    assert response(url, access)[0] == 401
    with psycopg.connect(stand["database"]) as db:
        assert db.execute(
            "SELECT likes FROM profiles WHERE user_id=%s", (stand["user_id"],)
        ).fetchone() == (False,)
    fresh = http(
        stand["auth"].url + "/api/v1/auth/login",
        {"login": stand["login"], "password": PASSWORD},
    )
    assert response(url, fresh["access_token"], method="PATCH", data={"likes": True})[0] == 200


@pytest.mark.parametrize("action", ["password_change", "delete_account"])
def test_account_change_revokes_token_for_preferences(stand, action):
    access = stand["pair"]["access_token"]
    url = stand["api"] + PREFERENCES
    assert response(url, access, method="PATCH", data={"likes": False})[0] == 200
    if action == "password_change":
        changed = response(
            stand["auth"].url + "/api/v1/auth/profile",
            access,
            method="PATCH",
            data={"new_password": "Another-9-Unusual!Password"},
        )
        assert changed[0] == 200
    else:
        assert (
            response(stand["auth"].url + "/api/v1/auth/profile", access, method="DELETE")[0] == 204
        )
    assert response(url, access, method="PATCH", data={"likes": True})[0] == 401
    assert response(url, access)[0] == 401
    with psycopg.connect(stand["database"]) as db:
        assert db.execute(
            "SELECT likes FROM profiles WHERE user_id=%s", (stand["user_id"],)
        ).fetchone() == (False,)


def test_auth_outage_cannot_bypass_session_verification(stand):
    access = stand["pair"]["access_token"]
    url = stand["api"] + PREFERENCES
    assert response(url, access, method="PATCH", data={"likes": False})[0] == 200
    os.killpg(stand["auth"].process.pid, signal.SIGTERM)
    stand["auth"].process.wait(timeout=10)
    assert response(url, access, method="PATCH", data={"likes": True})[0] == 503
    assert response(url, access)[0] == 503
    with psycopg.connect(stand["database"]) as db:
        assert db.execute(
            "SELECT likes FROM profiles WHERE user_id=%s", (stand["user_id"],)
        ).fetchone() == (False,)
