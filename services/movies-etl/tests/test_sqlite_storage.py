from pathlib import Path

from postgres_to_es.storage import ETLJobsTable, ProducerCursorsTable, SQLiteFileDatabase


def _enqueue_job(
    jobs: ETLJobsTable, job_id: str, *, now: str, created_at: str | None = None
) -> None:
    jobs.enqueue(
        job_id=job_id,
        source_table="person",
        first_id="00000000-0000-0000-0000-000000000001",
        first_modified="2021-01-01T00:00:00",
        last_id="00000000-0000-0000-0000-000000000002",
        last_modified="2021-01-01T00:01:00",
        row_count=2,
        now=created_at or now,
    )


def test_database_persists_across_reopen(tmp_path: Path) -> None:
    # Arrange
    db_path = tmp_path / "state.sqlite"
    database = SQLiteFileDatabase(str(db_path))
    ProducerCursorsTable(database).advance_cursor(
        source_table="person",
        last_modified="2021-01-01T00:00:00",
        last_id="00000000-0000-0000-0000-000000000001",
        now="2026-06-30T23:00:00",
    )

    # Act
    reopened = ProducerCursorsTable(SQLiteFileDatabase(str(db_path)))
    cursor = reopened.get_cursor("person")

    # Assert
    assert db_path.exists()
    assert cursor == {
        "source_table": "person",
        "last_modified": "2021-01-01T00:00:00",
        "last_id": "00000000-0000-0000-0000-000000000001",
        "updated_at": "2026-06-30T23:00:00",
    }


def test_advance_cursor_overwrites_previous_position(tmp_path: Path) -> None:
    # Arrange
    cursors = ProducerCursorsTable(SQLiteFileDatabase(str(tmp_path / "state.sqlite")))

    # Act
    cursors.advance_cursor(
        source_table="person",
        last_modified="2021-01-01T00:00:00",
        last_id="00000000-0000-0000-0000-000000000001",
        now="2026-06-30T23:00:00",
    )
    cursors.advance_cursor(
        source_table="person",
        last_modified="2021-01-02T00:00:00",
        last_id="00000000-0000-0000-0000-000000000002",
        now="2026-06-30T23:01:00",
    )
    cursor = cursors.get_cursor("person")

    # Assert
    assert cursor == {
        "source_table": "person",
        "last_modified": "2021-01-02T00:00:00",
        "last_id": "00000000-0000-0000-0000-000000000002",
        "updated_at": "2026-06-30T23:01:00",
    }


def test_enqueue_is_idempotent(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)

    # Act
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:05:00")
    rows = database.fetch_all("SELECT id, status FROM etl_jobs WHERE id = ?", ("job-1",))

    # Assert
    assert rows == [{"id": "job-1", "status": "pending"}]


def test_claim_next_locks_pending_job(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")

    # Act
    claimed = jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:01:00",
        locked_until="2026-06-30T23:06:00",
    )

    # Assert
    assert claimed is not None
    assert claimed["id"] == "job-1"
    assert claimed["status"] == "processing"
    assert claimed["locked_by"] == "worker-1"
    assert claimed["locked_until"] == "2026-06-30T23:06:00"
    assert claimed["attempts"] == 1


def test_claim_next_returns_none_when_nothing_claimable(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")
    jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:01:00",
        locked_until="2026-06-30T23:06:00",
    )

    # Act: lease ещё не истёк
    second = jobs.claim_next(
        worker_id="worker-2",
        now="2026-06-30T23:02:00",
        locked_until="2026-06-30T23:07:00",
    )

    # Assert
    assert second is None


def test_claim_next_reclaims_stale_processing_job(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")
    jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:01:00",
        locked_until="2026-06-30T23:06:00",
    )

    # Act: воркер «упал», лиз истёк → другой воркер забирает работу
    reclaimed = jobs.claim_next(
        worker_id="worker-2",
        now="2026-06-30T23:10:00",
        locked_until="2026-06-30T23:15:00",
    )

    # Assert
    assert reclaimed is not None
    assert reclaimed["id"] == "job-1"
    assert reclaimed["locked_by"] == "worker-2"
    assert reclaimed["attempts"] == 2


def test_mark_done_finalizes_job(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")
    jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:01:00",
        locked_until="2026-06-30T23:06:00",
    )

    # Act
    jobs.mark_done(job_id="job-1", now="2026-06-30T23:02:00")
    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))

    # Assert
    assert row is not None
    assert row["status"] == "done"
    assert row["finished_at"] == "2026-06-30T23:02:00"
    assert row["locked_by"] is None
    assert row["locked_until"] is None


def test_mark_failed_retries_then_fails_when_attempts_exhausted(tmp_path: Path) -> None:
    # Arrange
    database = SQLiteFileDatabase(str(tmp_path / "state.sqlite"))
    jobs = ETLJobsTable(database)
    _enqueue_job(jobs, "job-1", now="2026-06-30T23:00:00")
    max_attempts = 2

    # Act: попытка 1 → attempts=1 < 2 → возврат в pending
    jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:01:00",
        locked_until="2026-06-30T23:06:00",
    )
    jobs.mark_failed(
        job_id="job-1",
        error="boom-1",
        now="2026-06-30T23:02:00",
        max_attempts=max_attempts,
    )
    after_first = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))

    # попытка 2 → attempts=2 >= 2 → терминальный failed
    jobs.claim_next(
        worker_id="worker-1",
        now="2026-06-30T23:03:00",
        locked_until="2026-06-30T23:08:00",
    )
    jobs.mark_failed(
        job_id="job-1",
        error="boom-2",
        now="2026-06-30T23:04:00",
        max_attempts=max_attempts,
    )
    after_second = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))

    # Assert
    assert after_first is not None
    assert after_first["status"] == "pending"
    assert after_first["last_error"] == "boom-1"
    assert after_first["finished_at"] is None
    assert after_first["locked_by"] is None

    assert after_second is not None
    assert after_second["status"] == "failed"
    assert after_second["last_error"] == "boom-2"
    assert after_second["finished_at"] == "2026-06-30T23:04:00"
