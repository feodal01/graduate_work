# syntax=docker/dockerfile:1

FROM python:3.14.2-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN pip install \
    --disable-pip-version-check \
    --no-cache-dir \
    --root-user-action=ignore \
    uv==0.9.18

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project


FROM builder AS test

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY src ./src
COPY tests ./tests
COPY alembic.ini ./alembic.ini

ENV PYTHONPATH=/app/src

CMD ["uv", "run", "pytest", "tests", "-q"]


FROM python:3.14.2-slim AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 billing \
    && useradd --uid 10001 --gid billing --no-create-home billing

WORKDIR /app

COPY --from=builder --chown=billing:billing /app/.venv /app/.venv
COPY --chown=billing:billing src ./src
COPY --chown=billing:billing alembic.ini ./alembic.ini

USER billing

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; request = urllib.request.Request('http://127.0.0.1:8000/api/v1/health/', headers={'X-Request-Id': 'billing-image-health'}); urllib.request.urlopen(request, timeout=2)"]

CMD ["uvicorn", "main:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8000"]
