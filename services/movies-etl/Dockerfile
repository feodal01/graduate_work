# syntax=docker/dockerfile:1

FROM python:3.12-slim-trixie AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN pip install \
    --disable-pip-version-check \
    --no-cache-dir \
    --root-user-action=ignore \
    uv==0.9.18

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project


FROM python:3.12-slim-trixie AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /opt/venv /opt/venv
COPY --chown=nonroot:nonroot postgres_to_es ./postgres_to_es

# /app/state — точка монтирования volume для durable state.sqlite. Создаём
# и передаём владение nonroot заранее: новый named volume при первом
# монтировании наследует права каталога, уже существующего в образе.
RUN mkdir -p /app/state && chown -R nonroot:nonroot /app

USER nonroot

CMD ["python", "-m", "postgres_to_es"]
