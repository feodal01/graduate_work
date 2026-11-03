# Film List and Genre Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the exact `GET /api/v1/films` contract with popularity sorting, optional nested genre filtering, pagination, and Redis caching.

**Architecture:** Add one thin route returning `FilmShort` objects and one `FilmService.list_films` method that owns cache-aside behavior and the Elasticsearch query. Reuse the existing generic Redis helpers and `FILM_LIST_ADAPTER`.

**Tech Stack:** Python 3.14, FastAPI 0.139, Elasticsearch 8.6, redis-py asyncio, Pydantic 2, pytest.

## Global Constraints

- Preserve the exact `/api/v1/films` path and short response contract.
- Treat `genre` as an optional UUID.
- Accept only `-imdb_rating` and `imdb_rating` sorting.
- Default to `page_number=1` and `page_size=50`; both must be positive.
- Filter the strict nested `genres` field by `genres.id`.
- Cache results for 300 seconds, including empty lists.

---

### Task 1: Elasticsearch Listing and Cache

**Files:**
- Create: `tests/test_film_list_service.py`
- Modify: `src/services/film.py`

**Interfaces:**
- Produces: `FilmService.list_films(sort: str, genre: str | None, page_number: int, page_size: int) -> list[Film]`

- [x] **Step 1: Write failing service tests**

Cover pagination, descending/ascending sorting, `match_all`, nested `genres.id`
filtering, cache hits, and cache-key separation between genres.

- [x] **Step 2: Verify RED**

Run: `uv run pytest -q tests/test_film_list_service.py`

Expected: failures because `FilmService.list_films` does not exist.

- [x] **Step 3: Implement the minimal service method**

Build the key `film-list:{sort}:{genre-or-all}:{page_number}:{page_size}` and
the Elasticsearch body:

```python
payload = {
    'from': (page_number - 1) * page_size,
    'size': page_size,
    'sort': [{'imdb_rating': {'order': order, 'missing': '_last'}}],
    'query': query,
}
```

Use `match_all` without a genre and a nested term query on `genres.id` with a
genre. Read and write through `_get_from_cache` and `_put_to_cache`.

- [x] **Step 4: Verify GREEN**

Run: `uv run pytest -q tests/test_film_list_service.py`

Expected: all service tests pass.

---

### Task 2: Exact FastAPI Route and Contract

**Files:**
- Create: `tests/test_film_list.py`
- Modify: `src/api/v1/films.py`

**Interfaces:**
- Consumes: `FilmService.list_films`
- Produces: `GET /api/v1/films -> list[FilmShort]`

- [x] **Step 1: Write failing API tests**

Cover the exact path without redirect, short response fields, optional genre,
parameter forwarding, invalid sort, invalid UUID, and non-positive pagination.

- [x] **Step 2: Verify RED**

Run: `uv run pytest -q tests/test_film_list.py`

Expected: `404` because the route does not exist.

- [x] **Step 3: Implement the minimal route**

Register `@router.get("")`, use `Literal["-imdb_rating", "imdb_rating"]`,
`UUID | None`, and `Query(ge=1)`, then convert service results through
`_transform_film_to_short_response`.

- [x] **Step 4: Verify GREEN**

Run: `uv run pytest -q tests/test_film_list.py`

Expected: all API tests pass.

---

### Task 3: Full Verification

**Files:**
- Verify: `src/services/film.py`
- Verify: `src/api/v1/films.py`
- Verify: `tests/test_film_list_service.py`
- Verify: `tests/test_film_list.py`

**Interfaces:**
- Consumes: the completed film-list feature
- Produces: test and static-check evidence

- [x] **Step 1: Run all tests**

Run: `uv run pytest -q`

Expected: all tests pass.

- [x] **Step 2: Run static sanity checks**

Run: `uv run python -m compileall -q src tests && git diff --check`

Expected: exit code `0` with no output.
