#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
service_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file="$script_dir/docker-compose.yaml"
environment_file="$script_dir/functional.env"

cleanup() {
    docker compose \
        --env-file "$environment_file" \
        -f "$compose_file" \
        down -v --remove-orphans
}

trap cleanup EXIT

build_option=--build
pull_option=missing
if [ "${AUTH_FUNCTIONAL_SKIP_BUILD:-0}" = 1 ]; then
    build_option=--no-build
    pull_option=never
fi

docker compose \
    --env-file "$environment_file" \
    -f "$compose_file" \
    up -d "$build_option" --pull "$pull_option" --wait --wait-timeout 90

set -a
. "$environment_file"
set +a

cd "$service_dir"
if [ "$#" -eq 0 ]; then
    set -- functional_tests
fi
uv run pytest -q "$@"
