import datetime
from dataclasses import dataclass

import psycopg2.extensions
from psycopg2 import sql

MODIFIED_TABLES = frozenset({"film_work", "genre", "person"})


@dataclass(frozen=True)
class ModifiedTableBatch:
    first_id: str
    first_modified: datetime.datetime
    last_id: str
    last_modified: datetime.datetime
    count: int


def fetch_next_modified_batch(
    conn: psycopg2.extensions.connection,
    table_name: str,
    last_modified: datetime.datetime,
    last_id: str,
    batch_size: int,
) -> ModifiedTableBatch | None:
    """Следующий батч до batch_size изменённых строк после курсора (modified, id)."""
    if table_name not in MODIFIED_TABLES:
        raise ValueError(f"Table {table_name!r} does not support modified cursor")

    query = sql.SQL("""
    WITH batch AS (
        SELECT
            id,
            modified
        FROM {table}
        WHERE (modified, id) > (%s, %s::uuid)
        ORDER BY modified, id
        LIMIT %s
    )
    SELECT
        (array_agg(id ORDER BY modified, id))[1] AS first_id,
        (array_agg(modified ORDER BY modified, id))[1] AS first_modified,
        (array_agg(id ORDER BY modified DESC, id DESC))[1] AS last_id,
        (array_agg(modified ORDER BY modified DESC, id DESC))[1] AS last_modified,
        count(*) AS count
    FROM batch;
    """).format(table=sql.Identifier("content", table_name))

    params = (last_modified, last_id, batch_size)

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()

    # Агрегат без GROUP BY всегда возвращает ровно одну строку; проверка
    # на None нужна только для типов (fetchone() -> tuple | None).
    if row is None:
        return None

    first_id, first_modified, last_id, last_modified, count = row

    if count == 0:
        return None

    return ModifiedTableBatch(
        first_id=first_id,
        first_modified=first_modified,
        last_id=last_id,
        last_modified=last_modified,
        count=count,
    )
