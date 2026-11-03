from typing import Annotated, Any

from fastapi import Depends, Query
from pymongo import AsyncMongoClient

from core.config import settings

_mongo_client: AsyncMongoClient[dict[str, Any]] | None = None


def set_mongo_client(client: AsyncMongoClient[dict[str, Any]] | None) -> None:
    global _mongo_client
    _mongo_client = client


def get_mongo_client() -> AsyncMongoClient[dict[str, Any]]:
    if _mongo_client is None:
        raise RuntimeError('MongoDB client is not initialized')
    return _mongo_client


class Pagination:
    """Параметры пагинации в том же виде, что у Movies API."""

    def __init__(
        self,
        page_number: Annotated[
            int,
            Query(ge=1, description='Номер страницы (начиная с 1)', examples=[1]),
        ] = 1,
        page_size: Annotated[
            int,
            Query(
                ge=1,
                le=settings.max_page_size,
                description=f'Размер страницы (1…{settings.max_page_size})',
                examples=[50],
            ),
        ] = 50,
    ) -> None:
        self.page_number = page_number
        self.page_size = page_size

    @property
    def skip(self) -> int:
        return (self.page_number - 1) * self.page_size


PaginationDep = Annotated[Pagination, Depends()]
