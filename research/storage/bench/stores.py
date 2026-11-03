"""Одинаковые операции над MongoDB и PostgreSQL: схема, загрузка и запросы.

Запросы повторяют UGC API: сводка оценок фильма — один запрос с числом оценок,
средней оценкой, лайками и дизлайками (`services/ugc-api/src/services/ratings.py`),
списки пользователя сортируются по времени, новые первыми.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

import numpy as np
import numpy.typing as npt
import psycopg
from bson.binary import Binary, UuidRepresentation
from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient

from bench.dataset import DISLIKE_SCORE, LIKE_SCORE, to_datetime
from bench.settings import Settings

DATABASE = 'ugc'
PAGE_SIZE = 50

Array = npt.NDArray[np.generic]


@dataclass(frozen=True)
class Summary:
    ratings_count: int
    average_score: float | None
    likes: int
    dislikes: int


class Store(Protocol):
    name: str

    def user_likes(self, user_id: UUID) -> list[UUID]: ...

    def film_summary(self, film_id: UUID) -> Summary: ...

    def user_bookmarks(self, user_id: UUID) -> list[UUID]: ...

    def set_rating(self, user_id: UUID, film_id: UUID, score: int, now: datetime) -> None: ...

    def close(self) -> None: ...


def _uuid_rows(ids: Array) -> list[UUID]:
    return [UUID(bytes=row.tobytes()) for row in ids]


class MongoStore:
    name = 'mongo'

    def __init__(self, uri: str | None = None) -> None:
        if uri is None:
            uri = Settings().mongo_uri.get_secret_value()
        self.client: MongoClient[dict[str, Any]] = MongoClient(
            uri,
            uuidRepresentation='standard',
        )
        database = self.client[DATABASE]
        self.ratings = database['ratings']
        self.bookmarks = database['bookmarks']

    def user_likes(self, user_id: UUID) -> list[UUID]:
        cursor = (
            self.ratings.find(
                {'user_id': user_id, 'score': LIKE_SCORE},
                {'_id': 0, 'film_id': 1, 'updated_at': 1},
            )
            .sort('updated_at', DESCENDING)
            .limit(PAGE_SIZE)
        )
        return [document['film_id'] for document in cursor]

    def film_summary(self, film_id: UUID) -> Summary:
        pipeline: list[dict[str, Any]] = [
            {'$match': {'film_id': film_id}},
            {
                '$group': {
                    '_id': None,
                    'ratings_count': {'$sum': 1},
                    'average_score': {'$avg': '$score'},
                    'likes': {'$sum': {'$cond': [{'$eq': ['$score', LIKE_SCORE]}, 1, 0]}},
                    'dislikes': {'$sum': {'$cond': [{'$eq': ['$score', DISLIKE_SCORE]}, 1, 0]}},
                },
            },
        ]
        rows = list(self.ratings.aggregate(pipeline))
        if not rows:
            return Summary(0, None, 0, 0)
        row = rows[0]
        return Summary(row['ratings_count'], row['average_score'], row['likes'], row['dislikes'])

    def user_bookmarks(self, user_id: UUID) -> list[UUID]:
        cursor = (
            self.bookmarks.find({'user_id': user_id}, {'_id': 0, 'film_id': 1, 'created_at': 1})
            .sort('created_at', DESCENDING)
            .limit(PAGE_SIZE)
        )
        return [document['film_id'] for document in cursor]

    def set_rating(self, user_id: UUID, film_id: UUID, score: int, now: datetime) -> None:
        self.ratings.update_one(
            {'user_id': user_id, 'film_id': film_id},
            {'$set': {'score': score, 'updated_at': now}, '$setOnInsert': {'created_at': now}},
            upsert=True,
        )

    def close(self) -> None:
        self.client.close()

    # Загрузка набора.

    def reset(self) -> None:
        self.ratings.drop()
        self.bookmarks.drop()

    def create_indexes(self) -> None:
        self.ratings.create_indexes(
            [
                IndexModel([('user_id', ASCENDING), ('film_id', ASCENDING)], unique=True),
                IndexModel([('film_id', ASCENDING), ('score', ASCENDING)]),
                IndexModel(
                    [('user_id', ASCENDING), ('score', ASCENDING), ('updated_at', DESCENDING)],
                ),
            ],
        )
        self.bookmarks.create_indexes(
            [
                IndexModel([('user_id', ASCENDING), ('film_id', ASCENDING)], unique=True),
                IndexModel([('user_id', ASCENDING), ('created_at', DESCENDING)]),
            ],
        )

    def load_ratings(self, data: dict[str, Array], start: int, stop: int) -> None:
        users = _binary_rows(data['user_ids'])
        films = _binary_rows(data['film_ids'])
        documents = [
            {
                'user_id': users[user],
                'film_id': films[film],
                'score': int(score),
                'created_at': to_datetime(int(created)),
                'updated_at': to_datetime(int(updated)),
            }
            for user, film, score, created, updated in zip(
                data['rating_user'][start:stop].tolist(),
                data['rating_film'][start:stop].tolist(),
                data['rating_score'][start:stop].tolist(),
                data['rating_created'][start:stop].tolist(),
                data['rating_updated'][start:stop].tolist(),
                strict=True,
            )
        ]
        self.ratings.insert_many(documents, ordered=False)

    def load_bookmarks(self, data: dict[str, Array], start: int, stop: int) -> None:
        users = _binary_rows(data['user_ids'])
        films = _binary_rows(data['film_ids'])
        documents = [
            {'user_id': users[user], 'film_id': films[film], 'created_at': to_datetime(created)}
            for user, film, created in zip(
                data['bookmark_user'][start:stop].tolist(),
                data['bookmark_film'][start:stop].tolist(),
                data['bookmark_created'][start:stop].tolist(),
                strict=True,
            )
        ]
        self.bookmarks.insert_many(documents, ordered=False)

    def sizes(self) -> dict[str, dict[str, int]]:
        result = {}
        for collection in (self.ratings, self.bookmarks):
            stats = next(collection.aggregate([{'$collStats': {'storageStats': {}}}]))
            storage = stats['storageStats']
            result[collection.name] = {
                'rows': int(storage['count']),
                'data_bytes': int(storage['storageSize']),
                'index_bytes': int(storage['totalIndexSize']),
            }
        return result


_BINARY_CACHE: dict[int, list[Binary]] = {}


def _binary_rows(ids: Array) -> list[Binary]:
    """UUID в BSON Binary subtype 4, как их пишет PyMongo с uuidRepresentation=standard."""
    key = id(ids)
    if key not in _BINARY_CACHE:
        _BINARY_CACHE[key] = [
            Binary.from_uuid(value, UuidRepresentation.STANDARD) for value in _uuid_rows(ids)
        ]
    return _BINARY_CACHE[key]


class PostgresStore:
    name = 'postgres'

    def __init__(self, dsn: str | None = None) -> None:
        if dsn is None:
            dsn = Settings().postgres_dsn.get_secret_value()
        # prepare_threshold=0: запросы подготавливаются с первого вызова, как
        # кэшируются планы в MongoDB.
        self.connection = psycopg.connect(dsn, autocommit=True, prepare_threshold=0)

    def user_likes(self, user_id: UUID) -> list[UUID]:
        rows = self.connection.execute(
            'SELECT film_id, updated_at FROM ratings'
            ' WHERE user_id = %s AND score = %s ORDER BY updated_at DESC LIMIT %s',
            (user_id, LIKE_SCORE, PAGE_SIZE),
        ).fetchall()
        return [row[0] for row in rows]

    def film_summary(self, film_id: UUID) -> Summary:
        row = self.connection.execute(
            'SELECT count(*), avg(score)::float8,'
            ' count(*) FILTER (WHERE score = %s), count(*) FILTER (WHERE score = %s)'
            ' FROM ratings WHERE film_id = %s',
            (LIKE_SCORE, DISLIKE_SCORE, film_id),
        ).fetchone()
        assert row is not None
        return Summary(row[0], row[1], row[2], row[3])

    def user_bookmarks(self, user_id: UUID) -> list[UUID]:
        rows = self.connection.execute(
            'SELECT film_id, created_at FROM bookmarks'
            ' WHERE user_id = %s ORDER BY created_at DESC LIMIT %s',
            (user_id, PAGE_SIZE),
        ).fetchall()
        return [row[0] for row in rows]

    def set_rating(self, user_id: UUID, film_id: UUID, score: int, now: datetime) -> None:
        self.connection.execute(
            'INSERT INTO ratings (user_id, film_id, score, created_at, updated_at)'
            ' VALUES (%s, %s, %s, %s, %s)'
            ' ON CONFLICT (user_id, film_id)'
            ' DO UPDATE SET score = EXCLUDED.score, updated_at = EXCLUDED.updated_at',
            (user_id, film_id, score, now, now),
        )

    def close(self) -> None:
        self.connection.close()

    # Загрузка набора.

    def reset(self) -> None:
        self.connection.execute('DROP TABLE IF EXISTS ratings, bookmarks')
        self.connection.execute(
            'CREATE TABLE ratings ('
            ' user_id uuid NOT NULL, film_id uuid NOT NULL,'
            ' score smallint NOT NULL CHECK (score BETWEEN 0 AND 10),'
            ' created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)',
        )
        self.connection.execute(
            'CREATE TABLE bookmarks ('
            ' user_id uuid NOT NULL, film_id uuid NOT NULL, created_at timestamptz NOT NULL)',
        )

    def create_indexes(self) -> None:
        for statement in (
            'ALTER TABLE ratings ADD PRIMARY KEY (user_id, film_id)',
            'CREATE INDEX ratings_film_score ON ratings (film_id, score)',
            'CREATE INDEX ratings_user_score_updated ON ratings (user_id, score, updated_at DESC)',
            'ALTER TABLE bookmarks ADD PRIMARY KEY (user_id, film_id)',
            'CREATE INDEX bookmarks_user_created ON bookmarks (user_id, created_at DESC)',
            # Карта видимости нужна для index-only scan, статистика — для планов.
            'VACUUM (ANALYZE) ratings',
            'VACUUM (ANALYZE) bookmarks',
        ):
            self.connection.execute(statement)

    def load_ratings(self, data: dict[str, Array], start: int, stop: int) -> None:
        users = _uuid_cache(data['user_ids'])
        films = _uuid_cache(data['film_ids'])
        with self.connection.cursor() as cursor:
            with cursor.copy(
                'COPY ratings (user_id, film_id, score, created_at, updated_at)'
                ' FROM STDIN (FORMAT BINARY)',
            ) as copy:
                copy.set_types(['uuid', 'uuid', 'int2', 'timestamptz', 'timestamptz'])
                for user, film, score, created, updated in zip(
                    data['rating_user'][start:stop].tolist(),
                    data['rating_film'][start:stop].tolist(),
                    data['rating_score'][start:stop].tolist(),
                    data['rating_created'][start:stop].tolist(),
                    data['rating_updated'][start:stop].tolist(),
                    strict=True,
                ):
                    row = (users[user], films[film], score)
                    copy.write_row((*row, to_datetime(created), to_datetime(updated)))

    def load_bookmarks(self, data: dict[str, Array], start: int, stop: int) -> None:
        users = _uuid_cache(data['user_ids'])
        films = _uuid_cache(data['film_ids'])
        with self.connection.cursor() as cursor:
            with cursor.copy(
                'COPY bookmarks (user_id, film_id, created_at) FROM STDIN (FORMAT BINARY)',
            ) as copy:
                copy.set_types(['uuid', 'uuid', 'timestamptz'])
                for user, film, created in zip(
                    data['bookmark_user'][start:stop].tolist(),
                    data['bookmark_film'][start:stop].tolist(),
                    data['bookmark_created'][start:stop].tolist(),
                    strict=True,
                ):
                    copy.write_row((users[user], films[film], to_datetime(created)))

    def sizes(self) -> dict[str, dict[str, int]]:
        result = {}
        for table in ('ratings', 'bookmarks'):
            row = self.connection.execute(
                f'SELECT (SELECT count(*) FROM {table}),'
                ' pg_table_size(%s::regclass), pg_indexes_size(%s::regclass)',
                (table, table),
            ).fetchone()
            assert row is not None
            result[table] = {'rows': row[0], 'data_bytes': row[1], 'index_bytes': row[2]}
        return result


_UUID_CACHE: dict[int, list[UUID]] = {}


def _uuid_cache(ids: Array) -> list[UUID]:
    key = id(ids)
    if key not in _UUID_CACHE:
        _UUID_CACHE[key] = _uuid_rows(ids)
    return _UUID_CACHE[key]


STORES: dict[str, type[MongoStore] | type[PostgresStore]] = {
    'mongo': MongoStore,
    'postgres': PostgresStore,
}


def open_store(name: str) -> MongoStore | PostgresStore:
    return STORES[name]()
