# Docker Compose Snapshot Refresh Design

## Goal

Make the API container start successfully, align its Docker healthcheck with
the existing health endpoint, and recreate only the local Elasticsearch data
volume so the current snapshot is restored on startup.

## Image Startup

The Docker image continues to copy application code to `/app/src`. After the
copy, `WORKDIR /app/src` becomes the runtime working directory, allowing the
existing `uvicorn main:app` command to import `/app/src/main.py` and its
top-level packages. `PYTHONUNBUFFERED` uses the canonical `ENV key=value`
syntax.

## Compose Healthcheck

The API healthcheck requests the route that already exists:

`http://localhost:8000/api/v1/health/`

It uses Python's standard-library `urllib.request`, so no extra binary is
installed in the slim image. The check has explicit interval, timeout, and
retry settings.

## Volume Refresh

Only the Compose project volume named
`async_api_sprint_1_elasticsearch_data` is removed. Other Elasticsearch
volumes and Redis are outside the deletion scope.

The sequence is:

1. Stop and remove the current Compose containers without deleting volumes.
2. Remove `async_api_sprint_1_elasticsearch_data` explicitly.
3. Build and start the complete Compose stack.
4. Allow `elasticsearch-init` to restore
   `movies_20260714_genre_ids` into the newly empty Elasticsearch data path.

## Verification

- `api`, `elasticsearch`, and `redis` report healthy.
- `elasticsearch-init` exits with code `0`.
- The `movies` mapping reports `genres.type = nested` and
  `genres.properties.id.type = keyword`.
- `/api/v1/health/` returns `200`.
- `/api/v1/films`, genre filtering, title search, and film detail return their
  documented successful responses through the published API port.
- The full unit test suite remains green.
