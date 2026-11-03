"""Точка входа сервиса: `python -m postgres_to_es`.

Бесконечно опрашивает очередь через run_worker_pipeline(). Job, упавшая
после исчерпания ретраев, уже зафиксирована как failed/pending в
storage самим run_worker_pipeline (см. mark_failed) — здесь достаточно
залогировать исключение и продолжить цикл, не останавливая сервис
из-за одной сломанной job-ы.

Ctrl+C (KeyboardInterrupt) и SystemExit сюда не попадают — `except
Exception` их не перехватывает, так что локально сервис по-прежнему
останавливается штатно.
"""

import logging
import time

from postgres_to_es.logging_config import configure_logging
from postgres_to_es.pipelines.worker.pipeline import run_worker_pipeline
from postgres_to_es.publication import migrate_publication_state
from postgres_to_es.settings import settings

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    migrate_publication_state()
    logger.info("ETL worker started")

    while True:
        try:
            result = run_worker_pipeline()
        except Exception:
            logger.exception("Worker tick failed, continuing")
            time.sleep(settings.worker_poll_interval_seconds)
            continue

        if result is None:
            time.sleep(settings.worker_poll_interval_seconds)
            continue

        logger.info(
            "Job done: table=%s rows=%s documents=%s",
            result.job["source_table"],
            result.job["row_count"],
            len(result.documents),
        )


if __name__ == "__main__":
    main()
