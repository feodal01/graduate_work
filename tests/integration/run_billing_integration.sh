#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROJECT_NAME="billing-integration-check"

export GATEWAY_PORT=18081
export JAEGER_UI_PORT=18687
export JAEGER_OTLP_HTTP_PORT=14319

ENV_FILE="${REPOSITORY_ROOT}/.env.example"
if [[ -f "${REPOSITORY_ROOT}/.env" ]]; then
    ENV_FILE="${REPOSITORY_ROOT}/.env"
fi

compose() {
    docker compose \
        --project-name "${PROJECT_NAME}" \
        --env-file "${ENV_FILE}" \
        --project-directory "${REPOSITORY_ROOT}" \
        "$@"
}

cleanup() {
    compose down --volumes --remove-orphans
}

trap cleanup EXIT INT TERM

cleanup
compose up -d --build \
    auth-db auth-redis auth-migrate auth-bootstrap auth \
    billing-db billing-migrate billing-api billing-worker \
    notifications-db notifications-migrate notifications-campaign-api \
    theatre-db admin-migrate admin-collectstatic admin \
    jaeger
compose up -d --no-deps gateway

python3 "${SCRIPT_DIR}/test_billing_integration.py" \
    --repository-root "${REPOSITORY_ROOT}" \
    --project-name "${PROJECT_NAME}" \
    --env-file "${ENV_FILE}" \
    --base-url "http://127.0.0.1:${GATEWAY_PORT}" \
    --jaeger-url "http://127.0.0.1:${JAEGER_UI_PORT}"
