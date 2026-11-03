import datetime
import logging
import uuid
from contextlib import closing
from dataclasses import dataclass
from typing import cast

import psycopg2
import psycopg2.extensions

from postgres_to_es.db import get_postgres_dsl
from postgres_to_es.pipelines.producer.extractor import (
    MODIFIED_TABLES,
    ModifiedTableBatch,
    fetch_next_modified_batch,
)
from postgres_to_es.settings import BackoffSettings, postgres_backoff_settings, settings
from postgres_to_es.storage import ETLJobsTable, ProducerCursorsTable, SQLiteFileDatabase

logger = logging.getLogger(__name__)

POSTGRES_BACKOFF_SETTINGS = cast(BackoffSettings, postgres_backoff_settings)

DEFAULT_LAST_MODIFIED = datetime.datetime(1970, 1, 1)
DEFAULT_LAST_ID = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True)
class ProducerTableResult:
    """Итог прогона producer-а по одной исходной таблице."""

    source_table: str
    jobs_enqueued: int


def _get_cursor(
    cursors: ProducerCursorsTable,
    table_name: str,
) -> tuple[datetime.datetime, str]:
    cursor = cursors.get_cursor(table_name)
    if cursor is None:
        return DEFAULT_LAST_MODIFIED, DEFAULT_LAST_ID
    return datetime.datetime.fromisoformat(cursor["last_modified"]), cursor["last_id"]


def _job_id(table_name: str, batch: ModifiedTableBatch) -> str:
    # Детерминированный id: два producer-а, вычислившие один и тот же батч,
    # получат одинаковый job_id — гонка безопасна, см. enqueue/advance_cursor.
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            ":".join(
                (
                    table_name,
                    batch.first_modified.isoformat(),
                    batch.first_id,
                    batch.last_modified.isoformat(),
                    batch.last_id,
                )
            ),
        )
    )


def _enqueue_batch(
    table_name: str,
    batch: ModifiedTableBatch,
    cursors: ProducerCursorsTable,
    jobs: ETLJobsTable,
) -> None:
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    jobs.enqueue(
        job_id=_job_id(table_name, batch),
        source_table=table_name,
        first_id=batch.first_id,
        first_modified=batch.first_modified.isoformat(),
        last_id=batch.last_id,
        last_modified=batch.last_modified.isoformat(),
        row_count=batch.count,
        now=now,
    )
    cursors.advance_cursor(
        source_table=table_name,
        last_modified=batch.last_modified.isoformat(),
        last_id=batch.last_id,
        now=now,
    )
    logger.debug(
        "Enqueued batch: table=%s rows=%s range=(%s, %s) -> (%s, %s)",
        table_name,
        batch.count,
        batch.first_modified,
        batch.first_id,
        batch.last_modified,
        batch.last_id,
    )


def _run_producer_table_pipeline(
    conn: psycopg2.extensions.connection,
    table_name: str,
    cursors: ProducerCursorsTable,
    jobs: ETLJobsTable,
    batch_size: int,
) -> ProducerTableResult:
    last_modified, last_id = _get_cursor(cursors, table_name)
    jobs_enqueued = 0

    while True:
        batch = fetch_next_modified_batch(conn, table_name, last_modified, last_id, batch_size)
        if batch is None:
            return ProducerTableResult(source_table=table_name, jobs_enqueued=jobs_enqueued)

        _enqueue_batch(table_name, batch, cursors, jobs)
        last_modified, last_id = batch.last_modified, batch.last_id
        jobs_enqueued += 1


@POSTGRES_BACKOFF_SETTINGS.for_postgres()
def _run_all_tables_pipeline(
    cursors: ProducerCursorsTable,
    jobs: ETLJobsTable,
    batch_size: int,
) -> list[ProducerTableResult]:
    """Один Postgres-коннекшн на весь прогон по всем таблицам.

    При ошибке backoff ретраит целиком эту функцию, то есть пересоздаёт
    соединение и заново проходит таблицы — это безопасно: все запросы
    здесь read-only, а enqueue в SQLite идемпотентен по job_id, так что
    уже обработанные батчи просто не будут найдены повторно (курсор
    в SQLite уже продвинут).
    """
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn:
        conn.autocommit = True
        return [
            _run_producer_table_pipeline(conn, table_name, cursors, jobs, batch_size)
            for table_name in sorted(MODIFIED_TABLES)
        ]


def run_producer_pipeline(
    database_file_path: str | None = None,
) -> list[ProducerTableResult]:
    database_file_path = database_file_path or settings.state_database_file_path
    batch_size = settings.producer_batch_size
    database = SQLiteFileDatabase(database_file_path)
    cursors = ProducerCursorsTable(database)
    jobs = ETLJobsTable(database)
    return _run_all_tables_pipeline(cursors, jobs, batch_size)
