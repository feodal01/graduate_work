from postgres_to_es.pipelines.extractor.queries import ExtractedFilmWorkRow
from postgres_to_es.pipelines.transform.aggregate import (
    build_movie_payload,
    group_rows_by_film_work,
)
from postgres_to_es.schemas import MovieDocument


def transform_rows(rows: list[ExtractedFilmWorkRow]) -> list[MovieDocument]:
    """Группирует плоские строки в документы и валидирует их через Pydantic.

    Чистый шаг пайплайна: не знает про job/SQLite. Ошибка валидации (как и
    неизвестная person_film_work.role — см. build_movie_payload) — это
    giveup-случай, как и POSTGRES_GIVEUP в settings.py: постоянная проблема
    данных/кода, а не транзиентный сбой одной записи. Поэтому она не
    перехватывается здесь и пробрасывается наружу — её фиксацией на job
    занимается worker, который вызывает этот шаг внутри своего try/except.
    """
    return [
        MovieDocument.model_validate(build_movie_payload(fw_id, film_rows))
        for fw_id, film_rows in group_rows_by_film_work(rows).items()
    ]
