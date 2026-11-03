"""Синтетический набор оценок и закладок, общий для обоих хранилищ.

Набор строится детерминированно из SEED и сохраняется в data/dataset.npz, чтобы
MongoDB и PostgreSQL получили одни и те же строки. Популярность фильмов и
активность пользователей распределены по степенному закону: у фильма 0 больше
всего оценок, длинный хвост фильмов оценивают редко.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import numpy as np
import numpy.typing as npt

N_USERS = 100_000
N_FILMS = 10_000
N_RATINGS = 10_000_000
N_BOOKMARKS = 2_000_000
# Фильмы 0…POPULAR_FILMS-1 самые популярные: у каждого десятки тысяч оценок.
POPULAR_FILMS = 100
SEED = 42

LIKE_SCORE = 10
DISLIKE_SCORE = 0
# Лайк и дизлайк — крайние оценки, остальная доля делится поровну между 1…9.
SCORE_WEIGHTS = [0.15] + [0.5 / 9] * 9 + [0.35]

FILM_EXPONENT = 0.7
USER_EXPONENT = 0.3
PERIOD_END = int(datetime(2026, 9, 1, tzinfo=UTC).timestamp())
PERIOD_SECONDS = 2 * 365 * 24 * 3600
MAX_UPDATE_DELAY_SECONDS = 30 * 24 * 3600

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / 'data' / 'dataset.npz'
RESULTS_DIR = ROOT / 'results'

IntArray = npt.NDArray[np.int64]


@dataclass(frozen=True)
class Ids:
    users: list[UUID]
    films: list[UUID]


def _power_law(size: int, exponent: float) -> npt.NDArray[np.float64]:
    weights = 1.0 / np.arange(1, size + 1, dtype=np.float64) ** exponent
    return weights / weights.sum()


def _unique_pairs(
    rng: np.random.Generator,
    count: int,
    user_exponent: float,
) -> tuple[IntArray, IntArray]:
    """Пары (пользователь, фильм) без повторов: уникальный ключ в обоих хранилищах."""
    user_p = _power_law(N_USERS, user_exponent)
    film_p = _power_law(N_FILMS, FILM_EXPONENT)
    keys: IntArray = np.empty(0, dtype=np.int64)
    while keys.size < count:
        need = int((count - keys.size) * 1.3) + 1000
        users = rng.choice(N_USERS, size=need, p=user_p).astype(np.int64)
        films = rng.choice(N_FILMS, size=need, p=film_p).astype(np.int64)
        keys = np.unique(np.concatenate([keys, users * N_FILMS + films]))
    keys = rng.permutation(keys)[:count]
    return keys // N_FILMS, keys % N_FILMS


def _timestamps(rng: np.random.Generator, count: int) -> IntArray:
    return PERIOD_END - rng.integers(0, PERIOD_SECONDS, size=count, dtype=np.int64)


def build() -> None:
    rng = np.random.default_rng(SEED)
    user_ids = rng.integers(0, 256, size=(N_USERS, 16), dtype=np.uint8)
    film_ids = rng.integers(0, 256, size=(N_FILMS, 16), dtype=np.uint8)

    rating_user, rating_film = _unique_pairs(rng, N_RATINGS, USER_EXPONENT)
    rating_score = rng.choice(11, size=N_RATINGS, p=SCORE_WEIGHTS).astype(np.int8)
    rating_created = _timestamps(rng, N_RATINGS)
    delay = rng.integers(0, MAX_UPDATE_DELAY_SECONDS, size=N_RATINGS, dtype=np.int64)
    rating_updated = np.minimum(rating_created + delay, PERIOD_END)

    bookmark_user, bookmark_film = _unique_pairs(rng, N_BOOKMARKS, USER_EXPONENT)
    bookmark_created = _timestamps(rng, N_BOOKMARKS)

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    np.savez(
        DATA_FILE,
        user_ids=user_ids,
        film_ids=film_ids,
        rating_user=rating_user.astype(np.int32),
        rating_film=rating_film.astype(np.int32),
        rating_score=rating_score,
        rating_created=rating_created,
        rating_updated=rating_updated,
        bookmark_user=bookmark_user.astype(np.int32),
        bookmark_film=bookmark_film.astype(np.int32),
        bookmark_created=bookmark_created,
    )


def load() -> dict[str, npt.NDArray[np.generic]]:
    with np.load(DATA_FILE) as data:
        return {name: data[name] for name in data.files}


def load_ids() -> Ids:
    with np.load(DATA_FILE) as data:
        return Ids(
            users=[UUID(bytes=row.tobytes()) for row in data['user_ids']],
            films=[UUID(bytes=row.tobytes()) for row in data['film_ids']],
        )


def to_datetime(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, UTC)


def describe(data: dict[str, npt.NDArray[np.generic]]) -> dict[str, object]:
    """Распределение данных для отчёта: сколько строк у фильма и у пользователя."""
    per_film = np.bincount(data['rating_film'].astype(np.int64), minlength=N_FILMS)
    per_user = np.bincount(data['rating_user'].astype(np.int64), minlength=N_USERS)
    bookmarks_per_user = np.bincount(data['bookmark_user'].astype(np.int64), minlength=N_USERS)
    scores = data['rating_score']
    return {
        'users': N_USERS,
        'films': N_FILMS,
        'ratings': int(scores.size),
        'bookmarks': int(data['bookmark_user'].size),
        'likes_share': float(np.mean(scores == LIKE_SCORE)),
        'dislikes_share': float(np.mean(scores == DISLIKE_SCORE)),
        'ratings_per_film': _spread(per_film),
        'ratings_per_popular_film': _spread(per_film[:POPULAR_FILMS]),
        'ratings_per_user': _spread(per_user),
        'bookmarks_per_user': _spread(bookmarks_per_user),
    }


def _spread(values: IntArray) -> dict[str, int]:
    return {
        'min': int(values.min()),
        'median': int(np.median(values)),
        'max': int(values.max()),
    }
