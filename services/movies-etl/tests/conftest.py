import os
import tempfile
from pathlib import Path

# Settings() создаётся при импорте postgres_to_es.settings, поэтому обязательные
# переменные окружения нужно задать до импорта пакета. Значения безопасные:
# unit-тесты подменяют сеть; integration fixture заменяет настройки ниже,
# а переменные окружения имеют приоритет над локальным .env.
os.environ.setdefault("POSTGRES_DB", "movies_test")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("SQL_HOST", "127.0.0.1")
os.environ.setdefault("SQL_PORT", "5432")
os.environ.setdefault("ELASTICSEARCH_URL", "http://127.0.0.1:9200")
os.environ.setdefault("ELASTICSEARCH_INDEX", "movies_test")
os.environ.setdefault(
    "STATE_DATABASE_FILE_PATH",
    str(Path(tempfile.gettempdir()) / f"movies-etl-test-{os.getpid()}.sqlite"),
)
# Если мок вдруг не сработает, тест должен упасть сразу, а не висеть на ретраях.
os.environ.setdefault("POSTGRES_BACKOFF__MAX_TRIES", "1")
os.environ.setdefault("ELASTICSEARCH_BACKOFF__MAX_TRIES", "1")

import json
import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from pydantic import SecretStr

from postgres_to_es.settings import settings


@pytest.fixture
def publication_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, object]]:
    """Opt-in real services; never connect to or drop the application's database."""
    if os.environ.get("MOVIES_PUBLICATION_INTEGRATION") != "1":
        pytest.fail(
            "Selected integration test requires MOVIES_PUBLICATION_INTEGRATION=1 "
            "and tests/notifications/compose.yaml; use -m 'not integration' for unit-only checks"
        )
    root = Path(__file__).resolve().parents[3]
    output = subprocess.run(
        [sys.executable, str(root / "tests/notifications/stack_endpoints.py")],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    endpoints = json.loads(output.stdout)
    dsl = {
        "host": endpoints["POSTGRES_HOST"],
        "port": endpoints["POSTGRES_PORT"],
        "user": endpoints["POSTGRES_USER"],
        "password": endpoints["POSTGRES_PASSWORD"],
        "dbname": "notifications_test_admin",
    }
    name = f"notifications_test_{uuid4().hex}"
    with closing(psycopg2.connect(**dsl)) as admin:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        dsl["dbname"] = name
        for key in ("host", "port", "user"):
            monkeypatch.setattr(
                settings, f"postgres_{key}", int(dsl[key]) if key == "port" else dsl[key]
            )
        monkeypatch.setattr(settings, "postgres_password", SecretStr(str(dsl["password"])))
        monkeypatch.setattr(settings, "postgres_db", name)
        monkeypatch.setattr(settings, "elasticsearch_url", endpoints["ELASTICSEARCH_URL"])
        try:
            with closing(psycopg2.connect(**dsl)) as conn, conn, conn.cursor() as cursor:
                cursor.execute("""
                    CREATE SCHEMA content;
                    CREATE TABLE content.film_work (
                        id uuid PRIMARY KEY, title text NOT NULL, description text,
                        rating double precision, creation_date date, type text DEFAULT 'movie',
                        created timestamptz DEFAULT now(), modified timestamptz DEFAULT now()
                    );
                    CREATE TABLE content.genre (id uuid PRIMARY KEY, name text,
                        modified timestamptz DEFAULT now());
                    CREATE TABLE content.person (id uuid PRIMARY KEY, full_name text,
                        modified timestamptz DEFAULT now());
                    CREATE TABLE content.genre_film_work (id uuid PRIMARY KEY,
                        film_work_id uuid REFERENCES content.film_work(id),
                        genre_id uuid REFERENCES content.genre(id));
                    CREATE TABLE content.person_film_work (id uuid PRIMARY KEY,
                        film_work_id uuid REFERENCES content.film_work(id),
                        person_id uuid REFERENCES content.person(id), role text);
                """)
            yield dsl
        finally:
            with admin.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
                )
