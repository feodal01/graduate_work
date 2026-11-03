import os
import subprocess
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[3]
SERVICE = Path(__file__).resolve().parents[1]


@pytest.fixture
def database_url():
    # Discover only PostgreSQL: other services are not dependencies of this suite.
    port = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            "notifications-integration",
            "-f",
            str(ROOT / "tests/notifications/compose.yaml"),
            "port",
            "postgres",
            "5432",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert port.startswith("127.0.0.1:")
    name = "notifications_test_" + uuid4().hex
    admin_url = f"postgresql://notifications_test:notifications_test@{port}"
    with psycopg.connect(admin_url + "/notifications_test_admin", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        url = admin_url.replace("postgresql://", "postgresql+asyncpg://") + "/" + name
        try:
            migrate(url, "upgrade", "head")
            yield url
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def migrate(url, direction, revision):
    result = subprocess.run(
        ["alembic", direction, revision],
        cwd=SERVICE,
        env={**os.environ, "NOTIFICATIONS_DATABASE_URL": url},
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


@pytest.fixture(autouse=True)
def auth_contact_service(monkeypatch, request):
    """Existing worker scenarios isolate Auth; client HTTP has separate wire tests."""
    from contact_fixtures import CONTACTS, contact

    from notifications.clients.auth_contacts import AuthContactsClient

    for name, value in {
        "URL": "https://auth.example.test/contacts",
        "API_KEY": "test-service-key",
        "TIMEOUT_SECONDS": "5",
        "RETRY_SECONDS": "60",
        "BATCH_SIZE": "100",
    }.items():
        monkeypatch.setenv("NOTIFICATIONS_AUTH_CONTACTS_" + name, value)

    async def fetch(self, user_ids):
        return {uid: contact(uid) for uid in user_ids}

    # Wire tests opt out by calling the real _fetch method directly.
    if request.node.path.name != "test_auth_contacts.py":
        monkeypatch.setattr(AuthContactsClient, "fetch", fetch)
    yield CONTACTS
    CONTACTS.clear()
