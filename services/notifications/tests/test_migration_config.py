"""Migration settings validate before Alembic opens a database connection."""

import os
import subprocess
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

SERVICE = Path(__file__).resolve().parents[1]
DATABASE_URL = "postgresql+asyncpg://migrator:migration-password@localhost/migrations"


@pytest.fixture(autouse=True)
def migration_environment(monkeypatch):
    for name in os.environ:
        if name.startswith("NOTIFICATIONS_"):
            monkeypatch.delenv(name)


def test_migrations_require_only_database_url(monkeypatch):
    from notifications.db.settings import DatabaseSettings

    monkeypatch.setenv("NOTIFICATIONS_DATABASE_URL", DATABASE_URL)
    # Required database URL is loaded from env by BaseSettings.
    settings = DatabaseSettings()  # pyright: ignore[reportCallIssue]
    assert isinstance(settings.database_url, SecretStr)
    assert settings.database_url.get_secret_value() == DATABASE_URL
    assert "migration-password" not in repr(settings)


def test_migrations_require_database_url():
    from notifications.db.settings import DatabaseSettings

    with pytest.raises(ValidationError, match="database_url"):
        DatabaseSettings()  # pyright: ignore[reportCallIssue] -- env-loaded required field


@pytest.mark.parametrize(
    "url", ["", "invalid-url-migration-password", "postgresql://u:p@host:bad/db"]
)
def test_migrations_reject_invalid_url_without_exposing_input(monkeypatch, url):
    from notifications.db.settings import DatabaseSettings

    monkeypatch.setenv("NOTIFICATIONS_DATABASE_URL", url)
    with pytest.raises(ValidationError) as caught:
        DatabaseSettings()  # pyright: ignore[reportCallIssue] -- env-loaded required field
    assert "database_url" in str(caught.value)
    assert "migration-password" not in str(caught.value)
    assert "u:p" not in str(caught.value)


@pytest.mark.parametrize("url", [DATABASE_URL, "postgresql://u:p@localhost/db?sslmode=require"])
def test_migrations_preserve_valid_sqlalchemy_urls(monkeypatch, url):
    from notifications.db.settings import DatabaseSettings

    monkeypatch.setenv("NOTIFICATIONS_DATABASE_URL", url)
    settings = DatabaseSettings()  # pyright: ignore[reportCallIssue] -- env-loaded required field
    assert settings.database_url.get_secret_value() == url


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
def test_alembic_validates_settings_before_configuring_database(offline):
    command = ["alembic", "upgrade", "001"]
    if offline:
        command.append("--sql")
    result = subprocess.run(
        command,
        cwd=SERVICE,
        env={**os.environ, "NOTIFICATIONS_DATABASE_URL": "invalid-url-migration-password"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "ValidationError" in result.stderr
    assert "database_url" in result.stderr
    assert "migration-password" not in result.stdout + result.stderr


def test_alembic_offline_requires_no_application_settings():
    result = subprocess.run(
        ["alembic", "upgrade", "001", "--sql"],
        cwd=SERVICE,
        env={**os.environ, "NOTIFICATIONS_DATABASE_URL": DATABASE_URL},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "CREATE TABLE profiles" in result.stdout
    assert "migration-password" not in result.stdout + result.stderr
