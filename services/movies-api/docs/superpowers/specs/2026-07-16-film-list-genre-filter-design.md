# Film List and Genre Filter Design

## Goal

Implement `GET /api/v1/films` for popular films, with optional filtering by a
genre UUID, while preserving the short film response contract from `api.md`.

## API Contract

The endpoint accepts:

- `genre: UUID | None` — optional genre filter;
- `sort: "-imdb_rating" | "imdb_rating"` — default `-imdb_rating`;
- `page_number: int` — default `1`, minimum `1`;
- `page_size: int` — default `50`, minimum `1`.

The endpoint is registered at the empty path inside the existing films router,
so the effective URL is exactly `/api/v1/films`. It returns `list[FilmShort]`.
An empty result is a successful `200` response with `[]`. Invalid UUIDs, sort
values, and non-positive pagination values produce FastAPI validation errors
with status `422`.

## Elasticsearch Query

`FilmService.list_films` applies:

- `from = (page_number - 1) * page_size`;
- `size = page_size`;
- ascending or descending `imdb_rating` sorting with missing values last;
- `match_all` when `genre` is absent;
- a `nested` filter with path `genres` and a term on `genres.id` when `genre`
  is present.

The nested query matches the `movies_20260714_genre_ids` snapshot mapping,
where `genres` is a strict nested field and `genres.id` is a keyword.

## Cache

The list operation reuses the generic Redis helpers in `FilmService`.
The key format is:

`film-list:{sort}:{genre-or-all}:{page_number}:{page_size}`

Cached values use the existing `FILM_LIST_ADAPTER`; empty lists are cached and
the default TTL remains 300 seconds.

## Tests

- The exact `/api/v1/films` route returns the short response contract.
- API parameters are validated and forwarded to the service.
- Elasticsearch receives pagination, sort, and `match_all` without a genre.
- Elasticsearch receives the nested `genres.id` filter with a genre.
- Repeated identical list requests use Redis without a second Elasticsearch
  call.
- Different filters and pages do not share cache entries.
- The full existing test suite continues to pass.
