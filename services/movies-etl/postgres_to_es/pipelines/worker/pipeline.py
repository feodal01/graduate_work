import datetime
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any

from postgres_to_es.pipelines.extractor.pipeline import extract_job_rows
from postgres_to_es.pipelines.loader.pipeline import load_documents
from postgres_to_es.pipelines.producer.pipeline import ProducerTableResult, run_producer_pipeline
from postgres_to_es.pipelines.transform.pipeline import transform_rows
from postgres_to_es.publication import prepare_publication
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import settings
from postgres_to_es.storage import ETLJobsTable, SQLiteFileDatabase

logger = logging.getLogger(__name__)

# Идентификатор этого процесса-воркера для locked_by: стабилен на всё время
# его жизни, чтобы по etl_jobs.locked_by можно было понять, какой воркер
# держит (или держал) job, если она зависла.
_WORKER_ID = f"{os.getpid()}:{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class WorkerResult:
    """Итог одного тика: что нашёл producer и что обработали из очереди."""

    producer_results: list[ProducerTableResult]
    job: dict[str, Any]
    documents: list[MovieDocument]


def run_worker_pipeline() -> WorkerResult | None:
    """Прогнать producer, затем заклеймить следующую job и обработать её.

    producer ищет изменения в Postgres и кладёт новые job-ы в очередь —
    делаем это в начале тика, чтобы свежие job-ы сразу могли быть
    заклеймлены ниже, без отдельного запуска. Ошибки producer-а не
    привязаны к конкретной job (он сканирует сразу несколько таблиц), и
    поэтому не перехватываются здесь — пробрасываются как есть, как и
    giveup-ошибки Postgres.

    Единственное место, где job клеймится и где фиксируется её итог:
    каждый шаг (extract_job_rows, transform_rows, load_documents) — чистая
    функция без побочных эффектов на job, поэтому одного try/except здесь
    достаточно, вместо дублирования mark_failed/mark_done в каждом шаге.
    """
    # 1. producer ищет изменения в Postgres и кладёт новые job-ы в очередь
    producer_results = run_producer_pipeline()

    # 2. job клеймится из очереди
    jobs = ETLJobsTable(SQLiteFileDatabase(settings.state_database_file_path))

    now = datetime.datetime.now(datetime.timezone.utc)
    locked_until = now + datetime.timedelta(seconds=settings.worker_lease_seconds)
    job = jobs.claim_next(
        worker_id=_WORKER_ID,
        now=now.isoformat(),
        locked_until=locked_until.isoformat(),
    )
    if job is None:
        logger.debug("No pending job, queue is empty")
        return None

    logger.debug(
        "Claimed job: id=%s table=%s rows=%s attempt=%s",
        job["id"],
        job["source_table"],
        job["row_count"],
        job["attempts"],
    )

    # 3. job обрабатывается и преобразуется в документы
    try:
        rows = extract_job_rows(job)
        documents = transform_rows(rows)
        logger.debug(
            "Transformed job %s: %s row(s) -> %s document(s)",
            job["id"],
            len(rows),
            len(documents),
        )
    except Exception as exc:
        jobs.mark_failed(
            job_id=job["id"],
            error=str(exc),
            now=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            max_attempts=settings.job_max_attempts,
        )
        raise

    # 4. документы загружаются в Elasticsearch
    try:
        documents = [
            prepare_publication(
                uuid.UUID(document.id), datetime.datetime.now(datetime.timezone.utc)
            )
            for document in documents
        ]
        load_documents(documents)
    except Exception as exc:
        jobs.mark_failed(
            job_id=job["id"],
            error=str(exc),
            now=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            max_attempts=settings.job_max_attempts,
        )
        raise

    jobs.mark_done(job_id=job["id"], now=datetime.datetime.now(datetime.timezone.utc).isoformat())
    return WorkerResult(producer_results=producer_results, job=job, documents=documents)
