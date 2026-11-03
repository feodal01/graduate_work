from pathlib import Path

import pytest

import postgres_to_es.pipelines.worker.pipeline as worker_pipeline_module
from postgres_to_es.pipelines.worker.pipeline import _WORKER_ID, WorkerResult, run_worker_pipeline
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import settings
from postgres_to_es.storage import ETLJobsTable, SQLiteFileDatabase


def _enqueue(jobs: ETLJobsTable, job_id: str) -> None:
    jobs.enqueue(
        job_id=job_id,
        source_table="film_work",
        first_id="00000000-0000-0000-0000-000000000001",
        first_modified="2021-01-01T00:00:00",
        last_id="00000000-0000-0000-0000-000000000002",
        last_modified="2021-01-01T00:01:00",
        row_count=2,
        now="2026-06-30T23:00:00",
    )


def _use_temp_database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> SQLiteFileDatabase:
    db_path = tmp_path / "state.sqlite"
    monkeypatch.setattr(settings, "state_database_file_path", str(db_path))
    return SQLiteFileDatabase(str(db_path))


@pytest.fixture(autouse=True)
def _noop_producer_and_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    # По умолчанию producer и load_documents — no-op: большинство тестов здесь
    # проверяют claim/extract/transform (у producer и loader свои тесты), и
    # без этого каждый такой тест полез бы в реальный Postgres/Elasticsearch.
    monkeypatch.setattr(worker_pipeline_module, "run_producer_pipeline", lambda: [])
    monkeypatch.setattr(worker_pipeline_module, "load_documents", lambda documents: None)
    monkeypatch.setattr(
        worker_pipeline_module,
        "prepare_publication",
        lambda film_id, now: MovieDocument(id=str(film_id), title="Prepared"),
    )


def test_run_worker_runs_producer_before_claiming_so_new_jobs_are_picked_up_immediately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: producer кладёт job прямо во время этого же тика, до claim_next
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)

    def fake_producer() -> list[str]:
        _enqueue(jobs, "job-from-producer")
        return ["fake-producer-result"]

    monkeypatch.setattr(worker_pipeline_module, "run_producer_pipeline", fake_producer)
    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", lambda job: [])
    monkeypatch.setattr(worker_pipeline_module, "transform_rows", lambda rows: [])

    # Act
    result = run_worker_pipeline()

    # Assert: ту же job, которую только что создал producer, забрали и обработали
    assert result is not None
    assert result.job["id"] == "job-from-producer"
    assert result.producer_results == ["fake-producer-result"]


def test_run_worker_returns_none_when_queue_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _use_temp_database(monkeypatch, tmp_path)

    # Act
    result = run_worker_pipeline()

    # Assert
    assert result is None


def test_run_worker_claims_job_extracts_and_transforms_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    fake_rows = [{"fw_id": "film-1"}]
    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", lambda job: fake_rows)
    monkeypatch.setattr(
        worker_pipeline_module,
        "transform_rows",
        lambda rows: [MovieDocument(id="00000000-0000-0000-0000-000000000001", title="Extracted")],
    )
    monkeypatch.setattr(settings, "worker_lease_seconds", 120)

    # Act
    result = run_worker_pipeline()

    # Assert
    assert isinstance(result, WorkerResult)
    assert result.producer_results == []
    assert result.job["id"] == "job-1"
    assert result.documents == [
        MovieDocument(id="00000000-0000-0000-0000-000000000001", title="Prepared")
    ]

    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))
    assert row is not None
    assert row["status"] == "done"
    assert row["finished_at"] is not None
    assert row["locked_by"] is None
    assert row["attempts"] == 1


def test_run_worker_locks_job_with_worker_id_while_processing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: заглядываем в БД прямо во время обработки, до mark_done
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    captured: dict[str, object] = {}

    def fake_extract(job: dict[str, object]) -> list[dict[str, object]]:
        row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", (job["id"],))
        assert row is not None
        captured["status"] = row["status"]
        captured["locked_by"] = row["locked_by"]
        return []

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", fake_extract)
    monkeypatch.setattr(worker_pipeline_module, "transform_rows", lambda rows: [])

    # Act
    run_worker_pipeline()

    # Assert
    assert captured["status"] == "processing"
    assert captured["locked_by"] == _WORKER_ID


def test_run_worker_claims_only_one_job_per_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", lambda job: [])
    monkeypatch.setattr(worker_pipeline_module, "transform_rows", lambda rows: [])
    monkeypatch.setattr(settings, "worker_lease_seconds", 300)

    # Act: вторая попытка не должна забрать ту же job — лиз ещё не истёк
    first = run_worker_pipeline()
    second = run_worker_pipeline()

    # Assert
    assert first is not None and first.job["id"] == "job-1"
    assert second is None


def test_run_worker_marks_job_failed_when_extraction_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: иначе job осталась бы в processing навсегда, а ошибка пропала бы
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    def fake_extract(job: dict[str, object]) -> list[dict[str, object]]:
        raise ValueError("extraction boom")

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", fake_extract)
    monkeypatch.setattr(settings, "job_max_attempts", 5)

    # Act
    with pytest.raises(ValueError, match="extraction boom"):
        run_worker_pipeline()

    # Assert: меньше max_attempts — возвращена в pending для ретрая, ошибка сохранена
    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))
    assert row is not None
    assert row["status"] == "pending"
    assert row["last_error"] == "extraction boom"
    assert row["locked_by"] is None


def test_run_worker_marks_job_failed_when_transform_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", lambda job: [{"fw_id": "f1"}])

    def fake_transform(rows: list[object]) -> list[object]:
        raise ValueError("unknown role")

    monkeypatch.setattr(worker_pipeline_module, "transform_rows", fake_transform)
    monkeypatch.setattr(settings, "job_max_attempts", 5)

    # Act
    with pytest.raises(ValueError, match="unknown role"):
        run_worker_pipeline()

    # Assert
    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))
    assert row is not None
    assert row["status"] == "pending"
    assert row["last_error"] == "unknown role"
    assert row["locked_by"] is None


def test_run_worker_marks_job_failed_when_load_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", lambda job: [{"fw_id": "f1"}])
    monkeypatch.setattr(
        worker_pipeline_module,
        "transform_rows",
        lambda rows: [MovieDocument(id="00000000-0000-0000-0000-000000000001", title="Extracted")],
    )

    def fake_load(documents: list[object]) -> None:
        raise RuntimeError("elasticsearch unavailable")

    monkeypatch.setattr(worker_pipeline_module, "load_documents", fake_load)
    monkeypatch.setattr(settings, "job_max_attempts", 5)

    # Act
    with pytest.raises(RuntimeError, match="elasticsearch unavailable"):
        run_worker_pipeline()

    # Assert: job не должна быть done — данные не подтверждены загруженными в ES
    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))
    assert row is not None
    assert row["status"] == "pending"
    assert row["last_error"] == "elasticsearch unavailable"


def test_run_worker_marks_job_permanently_failed_after_max_attempts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    database = _use_temp_database(monkeypatch, tmp_path)
    jobs = ETLJobsTable(database)
    _enqueue(jobs, "job-1")

    def fake_extract(job: dict[str, object]) -> list[dict[str, object]]:
        raise ValueError("boom")

    monkeypatch.setattr(worker_pipeline_module, "extract_job_rows", fake_extract)
    monkeypatch.setattr(settings, "job_max_attempts", 1)

    # Act: claim_next увеличивает attempts до 1, что уже >= max_attempts
    with pytest.raises(ValueError, match="boom"):
        run_worker_pipeline()

    # Assert: терминальный failed, дальше не переподхватывается
    row = database.fetch_one("SELECT * FROM etl_jobs WHERE id = ?", ("job-1",))
    assert row is not None
    assert row["status"] == "failed"
    assert row["finished_at"] is not None

    assert run_worker_pipeline() is None
