# Generic Film Cache Design

## Goal

Reuse one pair of Redis access methods for film lookup by ID and film search,
while allowing every caller to provide its own cache key and serialized value.

## Design

`FilmService` keeps two private infrastructure helpers:

- `_get_from_cache(key)` returns the raw cached value or `None`;
- `_put_to_cache(key, value, ttl=300)` stores the supplied value with a TTL.

Serialization remains in the public service methods because they know the
expected value type: `Film` for ID lookup and `list[Film]` for search.

Cache keys use separate namespaces:

- film details: `film:{film_id}`;
- search: `film-search:{normalized_query}:{page_number}:{page_size}`.

Search queries are normalized with `strip().casefold()`. Pagination stays in
the key so different result pages cannot collide. Empty search results are
cached to avoid repeating Elasticsearch requests that return no hits.

## Data Flow

Both public methods use cache-aside:

1. Build the operation-specific key.
2. Read raw data through `_get_from_cache(key)`.
3. Deserialize and return on a cache hit.
4. Query Elasticsearch on a cache miss.
5. Serialize and store through `_put_to_cache(key, value)`.
6. Return the Elasticsearch result.

Redis errors retain the current fail-fast behavior; adding fallback behavior
is outside this refactoring.

## Tests

- Film lookup by ID reads a cached film without calling Elasticsearch.
- Film lookup by ID writes an Elasticsearch result to Redis.
- Repeated search reads the cached list without calling Elasticsearch again.
- Search cache keys remain distinct across pages.
- Empty search results remain cacheable.
- The complete test suite continues to pass.
