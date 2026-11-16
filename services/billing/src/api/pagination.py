"""Общие параметры постраничных списков (`limit`, `offset`).

Значения по умолчанию и ограничения заданы один раз в настройках, поэтому
все списки меняются вместе.
"""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Query

from core.config import settings


@dataclass(frozen=True, slots=True)
class Pagination:
    limit: int
    offset: int


def get_pagination(
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=settings.pagination.max_page_size,
            description='Сколько записей вернуть на странице',
        ),
    ] = settings.pagination.default_page_size,
    offset: Annotated[
        int,
        Query(ge=0, description='Сколько записей пропустить от начала списка'),
    ] = 0,
) -> Pagination:
    return Pagination(limit=limit, offset=offset)


PaginationParams = Annotated[Pagination, Depends(get_pagination)]
