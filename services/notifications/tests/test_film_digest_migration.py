"""Preserve existing issue membership and refuse a lossy rollback."""

from uuid import uuid4

import psycopg
import pytest
from conftest import migrate


def test_selection_migration_preserves_assigned_and_unassigned_films(database_url):
    migrate(database_url, "downgrade", "008_campaign_content_retention")
    with psycopg.connect(database_url.replace("+asyncpg", "")) as conn:
        run = uuid4()
        conn.execute(
            "INSERT INTO schedule_runs(run_id,kind,period_start) VALUES (%s,'new_films',now())",
            (run,),
        )
        for assignment in (run, None):
            film_id = uuid4()
            conn.execute(
                "INSERT INTO film_cards(film_id,film_version,catalog_published_at,"
                "catalog_updated_at,title,genres) VALUES (%s,1,now(),now(),'Film','[]')",
                (film_id,),
            )
            conn.execute(
                "INSERT INTO film_announcements(film_id,assigned_run_id) VALUES (%s,%s)",
                (film_id, assignment),
            )
        before = conn.execute(
            "SELECT film_id,detected_at,assigned_run_id FROM film_announcements ORDER BY film_id"
        ).fetchall()
        conn.commit()
        for direction, revision in (
            ("upgrade", "head"),
            ("downgrade", "008_campaign_content_retention"),
            ("upgrade", "head"),
        ):
            migrate(database_url, direction, revision)
            assert (
                conn.execute(
                    "SELECT film_id,detected_at,assigned_run_id "
                    "FROM film_announcements ORDER BY film_id"
                ).fetchall()
                == before
            )
            if direction == "upgrade":
                assert conn.execute(
                    "SELECT included_in_digest FROM film_announcements"
                ).fetchall() == [(True,), (True,)]
                assert conn.execute("SELECT films_per_digest FROM schedule_runs").fetchone() == (
                    None,
                )
            conn.commit()
        conn.execute(
            "UPDATE film_announcements SET included_in_digest=false WHERE assigned_run_id=%s",
            (run,),
        )
        conn.commit()
        with pytest.raises(AssertionError, match="Restore a pre-009 backup"):
            migrate(database_url, "downgrade", "008_campaign_content_retention")
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
            "010_campaign_cancellation",
        )
        assert conn.execute(
            "SELECT included_in_digest FROM film_announcements WHERE assigned_run_id=%s", (run,)
        ).fetchone() == (False,)
        conn.commit()
        for assignment in (
            "films_per_digest=0",
            "films_per_digest=-1",
            "films_per_digest=20,kind='likes'",
        ):
            with pytest.raises(psycopg.errors.CheckViolation):
                with conn.transaction():
                    conn.execute("UPDATE schedule_runs SET " + assignment)
