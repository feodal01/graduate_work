from typing import cast

from fastapi import Depends, Request

from clients.auth import AuthClient
from db.cache.base import AbstractCache
from db.health import HealthCheck
from db.storage.base import AbstractStorage
from models.film import Film
from services.film import FilmService


async def get_film_storage(request: Request) -> AbstractStorage[Film]:
    return cast(AbstractStorage[Film], request.app.state.film_storage)


async def get_cache(request: Request) -> AbstractCache:
    return cast(AbstractCache, request.app.state.cache)


async def get_auth_client(request: Request) -> AuthClient:
    return cast(AuthClient, request.app.state.auth_client)


async def get_storage_health(request: Request) -> HealthCheck:
    return cast(HealthCheck, request.app.state.film_storage)


async def get_film_service(
    storage: AbstractStorage[Film] = Depends(get_film_storage),
    cache: AbstractCache = Depends(get_cache),
) -> FilmService:
    return FilmService(storage, cache)
