import logging
from datetime import date
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from clients.auth import (
    AuthClient,
    AuthContractError,
    AuthRateLimitedError,
    AuthTokenRejectedError,
    AuthUnavailableError,
)
from core.config import settings
from dependencies import get_auth_client, get_film_service
from security.jwt import VerifiedAccessToken, optional_jwt_bearer
from services.film import FilmService
from services.film_access import can_read_during_auth_failure

router = APIRouter()
logger = logging.getLogger(__name__)


class ResponseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class UUIDMixin(ResponseModel):
    uuid: UUID = Field(
        validation_alias=AliasChoices('id', 'uuid'),
        description='Идентификатор сущности',
        examples=['b31592e5-673d-46dc-a561-9446438aea0f'],
    )


class Person(UUIDMixin):
    full_name: str = Field(
        validation_alias=AliasChoices('name', 'full_name'),
        description='Полное имя',
        examples=['Ashley Parker Angel'],
    )


class Genre(UUIDMixin):
    name: str = Field(description='Название жанра', examples=['Action'])


class FilmMixin(ResponseModel):
    title: str = Field(
        description='Название кинопроизведения',
        examples=['Lunar: The Silver Star'],
    )
    imdb_rating: float | None = Field(
        description='Рейтинг IMDb',
        examples=[9.2],
    )


class Film(UUIDMixin, FilmMixin):
    description: str | None = Field(
        description='Описание кинопроизведения',
        examples=['A teenager named Alex sets out to become the Dragonmaster.'],
    )
    genre: list[Genre] = Field(
        validation_alias=AliasChoices('genres', 'genre'),
        description='Жанры',
    )
    actors: list[Person] = Field(description='Актёры')
    writers: list[Person] = Field(description='Сценаристы')
    directors: list[Person] = Field(description='Режиссёры')


class FilmShort(UUIDMixin, FilmMixin):
    """Краткая карточка фильма для списков и поиска."""


class Pagination:
    """Валидация и хранение параметров пагинации."""

    def __init__(
        self,
        page_number: Annotated[
            int,
            Query(
                ge=1,
                description='Номер страницы (начиная с 1)',
                examples=[1],
            ),
        ] = 1,
        page_size: Annotated[
            int,
            Query(
                ge=1,
                le=settings.max_page_size_api_response,
                description=f'Размер страницы (1…{settings.max_page_size_api_response})',
                examples=[50],
            ),
        ] = 50,
    ):
        if page_number * page_size > settings.max_elasticsearch_result_window:
            raise HTTPException(
                status_code=422,
                detail=(
                    f'Pagination depth exceeded max size {settings.max_elasticsearch_result_window}'
                ),
            )

        self.page_number = page_number
        self.page_size = page_size


FILM_SHORT_EXAMPLE = {
    'uuid': '223e4317-e89b-22d3-f3b6-426614174000',
    'title': 'Billion Star Hotel',
    'imdb_rating': 6.1,
}


@router.get(
    '',
    response_model=list[FilmShort],
    summary='Список кинопроизведений',
    description=(
        'Популярные кинопроизведения. Сортировка по `imdb_rating`, '
        'опциональная фильтрация по жанру.'
    ),
    response_description='Список фильмов',
    status_code=200,
    responses={
        200: {
            'description': 'Список фильмов',
            'content': {
                'application/json': {
                    'example': [
                        {
                            'uuid': '524e4331-e14b-24d3-a156-426614174003',
                            'title': 'Lunar: The Silver Star',
                            'imdb_rating': 9.2,
                        },
                        FILM_SHORT_EXAMPLE,
                    ],
                },
            },
        },
    },
)
async def film_list(
    pagination: Annotated[Pagination, Depends(Pagination)],
    genre: Annotated[
        UUID | None,
        Query(description='Фильтр по UUID жанра', examples=[None]),
    ] = None,
    sort: Annotated[
        Literal['-imdb_rating', 'imdb_rating'],
        Query(
            description='Сортировка по рейтингу: `-imdb_rating` — по убыванию',
            examples=['-imdb_rating'],
        ),
    ] = '-imdb_rating',
    film_service: FilmService = Depends(get_film_service),
) -> list[FilmShort]:
    films = await film_service.list_films(
        sort=sort,
        genre=str(genre) if genre is not None else None,
        page_number=pagination.page_number,
        page_size=pagination.page_size,
    )
    return [FilmShort.model_validate(film) for film in films]


@router.get(
    '/search/',
    response_model=list[FilmShort],
    summary='Поиск кинопроизведений',
    description='Полнотекстовый поиск по названию (`query`).',
    response_description='Найденные фильмы',
    status_code=200,
    responses={
        200: {
            'description': 'Найденные фильмы',
            'content': {
                'application/json': {
                    'example': [
                        FILM_SHORT_EXAMPLE,
                        {
                            'uuid': '524e4331-e14b-24d3-a456-426614174001',
                            'title': 'Wishes on a Falling Star',
                            'imdb_rating': 8.5,
                        },
                    ],
                },
            },
        },
    },
)
async def film_search(
    query: Annotated[
        str,
        Query(description='Строка поиска по названию', examples=['star']),
    ],
    pagination: Annotated[Pagination, Depends(Pagination)],
    film_service: FilmService = Depends(get_film_service),
) -> list[FilmShort]:
    films = await film_service.search_by_title(
        query,
        page_number=pagination.page_number,
        page_size=pagination.page_size,
    )
    return [FilmShort.model_validate(film) for film in films]


@router.get(
    '/{film_id}/',
    response_model=Film,
    summary='Карточка кинопроизведения',
    description='Полная информация о фильме по UUID.',
    response_description='Фильм найден',
    status_code=200,
    responses={
        200: {
            'description': 'Фильм найден',
            'model': Film,
            'content': {
                'application/json': {
                    'example': {
                        'uuid': 'b31592e5-673d-46dc-a561-9446438aea0f',
                        'title': 'Lunar: The Silver Star',
                        'imdb_rating': 9.2,
                        'description': (
                            'From the village of Burg, a teenager named Alex '
                            'sets out to become the Dragonmaster.'
                        ),
                        'genre': [
                            {
                                'uuid': '6f822a92-7b51-4753-8d00-ecfedf98a937',
                                'name': 'Action',
                            },
                        ],
                        'actors': [
                            {
                                'uuid': 'afbdbaca-04e2-44ca-8bef-da1ae4d84cdf',
                                'full_name': 'Ashley Parker Angel',
                            },
                        ],
                        'writers': [
                            {
                                'uuid': '1bd9a00b-9596-49a3-afbe-f39a632a09a9',
                                'full_name': 'Toshio Akashi',
                            },
                        ],
                        'directors': [
                            {
                                'uuid': '4a893a97-e713-4936-9dd4-c8ca437ab483',
                                'full_name': 'Toshio Akashi',
                            },
                        ],
                    },
                },
            },
        },
        401: {'description': 'Access-токен недействителен'},
        403: {'description': 'Доступ к фильму запрещён'},
        404: {'description': 'Фильм не найден'},
    },
)
async def film_details(
    request: Request,
    film_id: Annotated[
        UUID,
        Path(description='UUID кинопроизведения'),
    ],
    verified_token: Annotated[
        VerifiedAccessToken | None,
        Depends(optional_jwt_bearer),
    ],
    film_service: Annotated[FilmService, Depends(get_film_service)],
    auth_client: Annotated[AuthClient, Depends(get_auth_client)],
) -> Film:
    film = await film_service.get_by_id(str(film_id))
    if not film:
        raise HTTPException(status_code=404, detail='Film not found')

    try:
        allowed = await auth_client.can_read_film(
            creation_date=film.creation_date,
            access_token=(verified_token.value if verified_token is not None else None),
            request_id=(request.headers.get('X-Request-Id') or uuid4().hex),
        )
    except AuthTokenRejectedError as exc:
        raise HTTPException(
            status_code=401,
            detail='Invalid or expired token',
            headers={'WWW-Authenticate': 'Bearer'},
        ) from exc
    except AuthRateLimitedError as exc:
        logger.warning(
            'Auth rate limited Movies API; applying local film policy film_id=%s retry_after=%s',
            film_id,
            exc.retry_after_seconds,
        )
        allowed = can_read_during_auth_failure(
            film.creation_date,
            today=date.today(),
        )
    except AuthUnavailableError:
        logger.warning(
            'Auth unavailable; applying local film policy film_id=%s',
            film_id,
        )
        allowed = can_read_during_auth_failure(
            film.creation_date,
            today=date.today(),
        )
    except AuthContractError:
        logger.error('Auth contract error film_id=%s', film_id)
        allowed = False

    if not allowed:
        raise HTTPException(status_code=403, detail='Film access denied')

    return Film.model_validate(film)
