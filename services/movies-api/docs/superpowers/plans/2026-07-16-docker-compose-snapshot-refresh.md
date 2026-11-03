# Docker Compose Snapshot Refresh Implementation Plan

> **For Codex:** Execute this plan task by task and verify every command before claiming completion.

**Goal:** Make the API container start correctly, expose a working Compose healthcheck, and recreate only the project Elasticsearch data volume so the current snapshot with genre IDs is restored.

**Architecture:** Keep the existing Compose topology and images. Run the FastAPI application from `/app/src`, probe the existing `/api/v1/health/` endpoint, and preserve Redis and unrelated Docker volumes while recreating the project's Elasticsearch volume.

**Tech Stack:** Docker, Docker Compose, FastAPI/Uvicorn, Elasticsearch 8.6.2, Redis 7.

---

### Task 1: Correct the API container startup and healthcheck

**Files:**
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`

- [x] Set the runtime working directory to `/app/src` after copying the application source.
- [x] Normalize `PYTHONUNBUFFERED` to the `KEY=value` Dockerfile form.
- [x] Point the API healthcheck at `http://localhost:8000/api/v1/health/`.
- [x] Add explicit healthcheck interval, timeout, and retry settings.
- [x] Validate the rendered Compose configuration:

```bash
docker compose config
```

- [x] Build the API image and verify that `main` imports from its runtime working directory:

```bash
docker compose build api
docker run --rm async_api_sprint_1-api python -c "import main; print(main.app.title)"
```

### Task 2: Recreate only the project Elasticsearch data volume

**Files:**
- No file changes.

- [x] Stop the Compose project without deleting volumes automatically:

```bash
docker compose down
```

- [x] Confirm that the exact target volume is `async_api_sprint_1_elasticsearch_data`.
- [x] Delete only that volume:

```bash
docker volume rm async_api_sprint_1_elasticsearch_data
```

- [x] Build and start the full stack:

```bash
docker compose up -d --build
```

- [x] Wait until Elasticsearch and Redis are healthy, the init service exits with code 0, and the API becomes healthy:

```bash
docker compose ps -a
```

### Task 3: Verify the restored snapshot and public API

**Files:**
- Verify: `tests/`

- [x] Verify the health endpoint:

```bash
curl -fsS http://127.0.0.1:8000/api/v1/health/
```

- [x] Verify that `movies.genres` is nested and contains a keyword `id` field:

```bash
curl -fsS http://127.0.0.1:9200/movies/_mapping
```

- [x] Verify list, genre filtering, title search, and detail endpoints against the restored data.
- [x] Run the complete automated test suite:

```bash
uv run pytest -q
```

- [x] Run final source and diff checks:

```bash
uv run python -m compileall -q src tests
git diff --check
git status --short --branch
```
