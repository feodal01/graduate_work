# Generic Film Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace film-specific Redis helpers with reusable key/value helpers used by film details and text search.

**Architecture:** Keep cache-aside orchestration in `FilmService`. Two private methods perform only Redis I/O for caller-provided keys and values; public service methods own cache-key construction and Pydantic serialization.

**Tech Stack:** Python 3.14, FastAPI, redis-py asyncio, Pydantic 2, pytest.

## Global Constraints

- Do not change HTTP routes or response contracts.
- Use a five-minute default TTL (`300` seconds).
- Use `film:{film_id}` for detail keys.
- Use `film-search:{normalized_query}:{page_number}:{page_size}` for search keys.
- Preserve fail-fast Redis error behavior.

---

### Task 1: Characterize Film Detail Cache Behavior

**Files:**
- Create: `tests/test_film_service_cache.py`
- Test: `tests/test_film_service_cache.py`

**Interfaces:**
- Consumes: `FilmService.get_by_id(film_id: str) -> Film | None`
- Produces: regression coverage for detail cache hit and cache miss

- [x] **Step 1: Write cache-hit and cache-miss tests**

Use an in-memory Redis stub and Elasticsearch stub. Verify that a cached film is returned without Elasticsearch and that an Elasticsearch result is stored with key `film:{film_id}` and TTL `300`.

- [x] **Step 2: Run characterization tests**

Run: `uv run pytest -q tests/test_film_service_cache.py`

Expected before the key refactor: the new namespaced-key assertions fail because the existing code uses the bare film UUID.

---

### Task 2: Introduce Generic Redis Helpers

**Files:**
- Modify: `src/services/film.py`
- Test: `tests/test_film_service_cache.py`
- Test: `tests/test_film_search.py`

**Interfaces:**
- Produces: `_get_from_cache(key: str) -> bytes | str | None`
- Produces: `_put_to_cache(key: str, value: bytes | str, ttl: int = 300) -> None`

- [x] **Step 1: Replace specialized helpers**

Implement raw Redis I/O only:

```python
async def _get_from_cache(self, key: str) -> bytes | str | None:
    return await self.redis.get(key)


async def _put_to_cache(
    self,
    key: str,
    value: bytes | str,
    ttl: int = FILM_CACHE_EXPIRE_IN_SECONDS,
) -> None:
    await self.redis.set(key, value, ttl)
```

- [x] **Step 2: Migrate detail lookup**

Build `film:{film_id}`, deserialize cached data with `Film.model_validate_json`, and serialize Elasticsearch results with `film.model_dump_json()`.

- [x] **Step 3: Migrate text search**

Use the existing search key, deserialize through `FILM_LIST_ADAPTER.validate_json`, and serialize through `FILM_LIST_ADAPTER.dump_json` while calling the same generic helpers.

- [x] **Step 4: Run focused tests**

Run: `uv run pytest -q tests/test_film_service_cache.py tests/test_film_search.py`

Expected: all focused tests pass.

---

### Task 3: Verify the Refactoring

**Files:**
- Verify: `src/services/film.py`
- Verify: `tests/test_film_service_cache.py`
- Verify: `tests/test_film_search.py`

**Interfaces:**
- Consumes: the completed generic cache implementation
- Produces: verification evidence

- [x] **Step 1: Run the full test suite**

Run: `uv run pytest -q`

Expected: all tests pass.

- [x] **Step 2: Run static sanity checks**

Run: `uv run python -m compileall -q src tests && git diff --check`

Expected: exit code `0` with no output.
