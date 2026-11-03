#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROJECT_NAME="auth-integration-check"

export GATEWAY_PORT=18080
export JAEGER_UI_PORT=18686
export JAEGER_OTLP_HTTP_PORT=14318

compose() {
    docker compose \
        --project-name "${PROJECT_NAME}" \
        --env-file "${REPOSITORY_ROOT}/.env.example" \
        --project-directory "${REPOSITORY_ROOT}" \
        "$@"
}

cleanup() {
    compose down --volumes --remove-orphans
}

trap cleanup EXIT INT TERM

cleanup
compose up -d --build

python3 "${SCRIPT_DIR}/test_auth_integration.py" \
    --repository-root "${REPOSITORY_ROOT}" \
    --project-name "${PROJECT_NAME}" \
    --base-url "http://127.0.0.1:${GATEWAY_PORT}"
