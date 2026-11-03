import datetime
from pathlib import Path

import pytest

import postgres_to_es.pipelines.producer.pipeline as pipeline_module
from postgres_to_es.pipelines.producer.extractor import ModifiedTableBatch
from postgres_to_es.pipelines.producer.pipeline import ProducerTableResult
from postgres_to_es.storage import SQLiteFileDatabase

BASE_MODIFIED = datetime.datetime(2021, 1, 1, 0, 0, 0)


def _make_batch(index: int, count: int) -> ModifiedTableBatch:
    first = BASE_MODIFIED + datetime.timedelta(minutes=index)
    last = first + datetime.timedelta(seconds=count)
    return ModifiedTableBatch(
        first_id=f"00000000-0000-0000-0000-0000000000{index:02d}",
        first_modified=first,
        last_id=f"00000000-0000-0000-0000-0000000001{index:02d}",
        last_modified=last,
        count=count,
    )


class _FakeConnection:
    """Заглушка вместо реального psycopg2-соединения.

    run_producer_pipeline теперь сам открывает соединение (одно на весь
    прогон по таблицам), поэтому мока одной fetch_next_modified_batch уже
    недостаточно — иначе тест попытается достучаться до настоящего
    Postgres по адресу из .env (например, "theatre-db", недоступного вне
    docker-сети) и зависнет на реальных backoff-ретраях.
    """

    autocommit: bool = False

    def close(self) -> None:
        pass


def _install_fake_extractor(
    monkeypatch: pytest.MonkeyPatch,
    batches_by_table: dict[str, list[ModifiedTableBatch]],
) -> list[tuple[str, datetime.datetime, str, int]]:
    """Подменяет extractor, отдавая заранее заданные батчи по таблицам.

    Возвращает список вызовов для проверки порядка курсора.
    """
    calls: list[tuple[str, datetime.datetime, str, int]] = []
    queues = {table: list(items) for table, items in batches_by_table.items()}

    def fake_fetch(
        conn: object,
        table_name: str,
        last_modified: datetime.datetime,
        last_id: str,
        batch_size: int,
    ) -> ModifiedTableBatch | None:
        calls.append((table_name, last_modified, last_id, batch_size))
        queue = queues.get(table_name, [])
        if not queue:
            return None
        return queue.pop(0)

    monkeypatch.setattr(pipeline_module, "fetch_next_modified_batch", fake_fetch)
    monkeypatch.setattr(pipeline_module.psycopg2, "connect", lambda **kwargs: _FakeConnection())
    return calls


def test_producer_enqueues_one_job_per_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    db_path = tmp_path / "state.sqlite"
    person_batches = [_make_batch(0, 500), _make_batch(1, 500), _make_batch(2, 17)]
    _install_fake_extractor(
        monkeypatch,
        {"person": person_batches, "film_work": [], "genre": []},
    )

    # Act
    result = pipeline_module.run_producer_pipeline(str(db_path))

    # Assert: по одному job на каждый батч
    assert result == [
        ProducerTableResult(source_table="film_work", jobs_enqueued=0),
        ProducerTableResult(source_table="genre", jobs_enqueued=0),
        ProducerTableResult(source_table="person", jobs_enqueued=3),
    ]

    database = SQLiteFileDatabase(str(db_path))
    jobs = database.fetch_all("SELECT row_count, status FROM etl_jobs ORDER BY first_modified")
    assert [job["row_count"] for job in jobs] == [500, 500, 17]
    assert all(job["status"] == "pending" for job in jobs)


def test_producer_advances_cursor_by_modified_and_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    db_path = tmp_path / "state.sqlite"
    person_batches = [_make_batch(0, 500), _make_batch(1, 500)]
    calls = _install_fake_extractor(
        monkeypatch,
        {"person": person_batches, "film_work": [], "genre": []},
    )

    # Act
    pipeline_module.run_producer_pipeline(str(db_path))

    # Assert: каждый следующий запрос идёт от (last_modified, last_id) предыдущего батча
    person_calls = [call for call in calls if call[0] == "person"]
    assert person_calls[1][1] == person_batches[0].last_modified
    assert person_calls[1][2] == person_batches[0].last_id
    assert person_calls[2][1] == person_batches[1].last_modified
    assert person_calls[2][2] == person_batches[1].last_id

    database = SQLiteFileDatabase(str(db_path))
    cursor = database.fetch_one(
        "SELECT last_modified, last_id FROM producer_cursors WHERE source_table = ?",
        ("person",),
    )
    assert cursor == {
        "last_modified": person_batches[1].last_modified.isoformat(),
        "last_id": person_batches[1].last_id,
    }


def test_producer_rerun_after_crash_does_not_duplicate_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: первый прогон кладёт 2 батча
    db_path = tmp_path / "state.sqlite"
    first_batches = [_make_batch(0, 500), _make_batch(1, 500)]
    _install_fake_extractor(
        monkeypatch,
        {"person": list(first_batches), "film_work": [], "genre": []},
    )
    pipeline_module.run_producer_pipeline(str(db_path))

    # Act: повторный прогон "видит" те же батчи заново (как будто курсор не учли)
    _install_fake_extractor(
        monkeypatch,
        {"person": list(first_batches), "film_work": [], "genre": []},
    )
    pipeline_module.run_producer_pipeline(str(db_path))

    # Assert: enqueue идемпотентен по job_id → дублей нет
    database = SQLiteFileDatabase(str(db_path))
    rows = database.fetch_all("SELECT id FROM etl_jobs")
    assert len(rows) == 2
