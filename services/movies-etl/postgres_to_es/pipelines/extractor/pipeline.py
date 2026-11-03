from contextlib import closing
from typing import Any, cast

import psycopg2

from postgres_to_es.db import get_postgres_dsl
from postgres_to_es.pipelines.extractor.queries import (
    ExtractedFilmWorkRow,
    affected_film_work_ids,
    fetch_film_work_rows,
)
from postgres_to_es.settings import BackoffSettings, postgres_backoff_settings

POSTGRES_BACKOFF_SETTINGS = cast(BackoffSettings, postgres_backoff_settings)


@POSTGRES_BACKOFF_SETTINGS.for_postgres()
def extract_job_rows(job: dict[str, Any]) -> list[ExtractedFilmWorkRow]:
    """Извлечь сджойненные строки фильмов для заклеймленной job.

    Чистый шаг пайплайна: не клеймит job и не трогает её статус — этим
    владеет worker, который вызывает этот шаг внутри своего try/except.

    Один Postgres-коннекшн на оба запроса; при обрыве backoff ретраит всю
    функцию целиком (переоткрывая соединение), что безопасно — оба
    запроса read-only и идемпотентны.
    """
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn:
        conn.autocommit = True
        film_work_ids = affected_film_work_ids(conn, job)
        return fetch_film_work_rows(conn, film_work_ids)
