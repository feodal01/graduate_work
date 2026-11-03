import logging

from pydantic import BaseModel, RootModel, ValidationError

from db.cache.base import AbstractCache
from db.storage.base import AbstractStorage
from db.storage.query import Match, SearchQuery, Sort, Term
from models.film import Film

logger = logging.getLogger(__name__)

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmList(RootModel[list[Film]]):
    """Обёртка для JSON-массива фильмов в кэше"""


class FilmService:
    def __init__(self, storage: AbstractStorage[Film], cache: AbstractCache) -> None:
        self.storage = storage
        self.cache = cache

    async def _get_cached[T: BaseModel](self, key: str, model: type[T]) -> T | None:
        raw = await self.cache.get(key)
        if raw is None:
            return None
        try:
            return model.model_validate_json(raw)
        except ValidationError:
            logger.warning('Invalid cache payload for %s', key, exc_info=True)
            return None

    async def list_films(
        self,
        sort: str,
        genre: str | None,
        page_number: int,
        page_size: int,
    ) -> list[Film]:
        cache_key = f'film-list:{sort}:{genre or "all"}:{page_number}:{page_size}'
        cached = await self._get_cached(cache_key, FilmList)
        if cached is not None:
            return cached.root

        films = await self.storage.search(
            SearchQuery(
                page_number=page_number,
                page_size=page_size,
                filters=((Term('genres.id', genre),) if genre is not None else ()),
                sort=(Sort('imdb_rating', descending=sort.startswith('-')),),
            )
        )
        await self._cache_films(cache_key, films)
        return films

    async def search_by_title(
        self,
        query: str,
        page_number: int,
        page_size: int,
    ) -> list[Film]:
        cache_key = f'film-search:{query.strip().casefold()}:{page_number}:{page_size}'
        cached = await self._get_cached(cache_key, FilmList)
        if cached is not None:
            return cached.root

        films = await self.storage.search(
            SearchQuery(
                page_number=page_number,
                page_size=page_size,
                match=Match('title', query, fuzziness='AUTO'),
            )
        )
        await self._cache_films(cache_key, films)
        return films

    async def get_by_id(self, film_id: str) -> Film | None:
        cache_key = f'film:{film_id}'
        cached = await self._get_cached(cache_key, Film)
        if cached is not None:
            return cached

        film = await self.storage.get_by_id(film_id)
        if not film:
            return None

        await self.cache.set(
            cache_key,
            film.model_dump_json(),
            FILM_CACHE_EXPIRE_IN_SECONDS,
        )

        return film

    async def _cache_films(self, key: str, films: list[Film]) -> None:
        await self.cache.set(
            key,
            FilmList(films).model_dump_json(),
            FILM_CACHE_EXPIRE_IN_SECONDS,
        )
