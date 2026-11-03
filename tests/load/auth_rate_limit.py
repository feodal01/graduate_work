"""Verify the Auth and Nginx rate limits against the real Docker stack."""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from threading import Barrier
from time import perf_counter
from typing import Final
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

AUTH_REQUESTS: Final = 10
AUTH_EXPECTED_FORWARDED: Final = 5
REFRESH_REQUESTS: Final = 6
REFRESH_EXPECTED_ROTATIONS: Final = 5
GATEWAY_REQUESTS: Final = 100
PASSWORD: Final = 'Load-Test-Password-2026!'


class LoadTestFailure(Exception):
    """A real response violated the configured rate-limit contract."""


@dataclass(frozen=True, slots=True)
class ResponseResult:
    """Store the observable result of one HTTP request."""

    status: int
    retry_after: str | None
    body: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Store the evidence produced by one concurrent scenario."""

    name: str
    requests: int
    duration_seconds: float
    requests_per_second: float
    statuses: dict[int, int]
    retry_after_values: dict[str, int]


@dataclass(frozen=True, slots=True)
class RefreshScenarioResult:
    """Store evidence that refresh buckets are stable and user-specific."""

    name: str
    requests: int
    duration_seconds: float
    requests_per_second: float
    statuses: dict[int, int]
    retry_after_values: dict[str, int]
    other_user_status: int


def _send_json(
    *,
    base_url: str,
    path: str,
    payload: dict[str, object],
    request_id: str,
) -> ResponseResult:
    request = Request(
        f'{base_url.rstrip("/")}{path}',
        data=json.dumps(payload).encode(),
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'auth-rate-limit-load-test',
            'X-Request-Id': request_id,
        },
        method='POST',
    )

    try:
        with urlopen(request, timeout=10) as response:
            body: object = json.loads(response.read())
            return ResponseResult(
                status=response.status,
                retry_after=response.headers.get('Retry-After'),
                body=body if isinstance(body, dict) else None,
            )
    except HTTPError as error:
        with error:
            return ResponseResult(
                status=error.code,
                retry_after=error.headers.get('Retry-After'),
            )


def _send_login(
    *,
    base_url: str,
    login: str,
    request_id: str | None,
    start: Barrier,
) -> ResponseResult:
    payload = json.dumps({'login': login, 'password': PASSWORD}).encode()
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'auth-rate-limit-load-test',
    }
    if request_id is not None:
        headers['X-Request-Id'] = request_id
    request = Request(
        f'{base_url.rstrip("/")}/api/v1/auth/login',
        data=payload,
        headers=headers,
        method='POST',
    )

    start.wait()
    try:
        with urlopen(request, timeout=10) as response:
            return ResponseResult(
                status=response.status,
                retry_after=response.headers.get('Retry-After'),
            )
    except HTTPError as error:
        with error:
            return ResponseResult(
                status=error.code,
                retry_after=error.headers.get('Retry-After'),
            )


def _run_concurrently(
    *,
    name: str,
    base_url: str,
    logins: list[str],
    request_ids: list[str | None],
) -> ScenarioResult:
    request_count = len(logins)
    start = Barrier(request_count + 1)
    started_at = perf_counter()
    with ThreadPoolExecutor(max_workers=request_count) as executor:
        futures = [
            executor.submit(
                _send_login,
                base_url=base_url,
                login=login,
                request_id=request_id,
                start=start,
            )
            for login, request_id in zip(logins, request_ids, strict=True)
        ]
        start.wait()
        results = [future.result() for future in futures]
    duration = perf_counter() - started_at
    statuses = Counter(result.status for result in results)
    retry_after_values = Counter(
        result.retry_after
        for result in results
        if result.retry_after is not None
    )
    return ScenarioResult(
        name=name,
        requests=request_count,
        duration_seconds=round(duration, 3),
        requests_per_second=round(request_count / duration, 2),
        statuses=dict(sorted(statuses.items())),
        retry_after_values=dict(sorted(retry_after_values.items())),
    )


def _is_positive_integer(value: str | None) -> bool:
    if value is None:
        return False
    try:
        return int(value) > 0
    except ValueError:
        return False


def _verify_auth_token_bucket(result: ScenarioResult) -> None:
    expected_rejected = AUTH_REQUESTS - AUTH_EXPECTED_FORWARDED
    if result.statuses != {401: AUTH_EXPECTED_FORWARDED, 429: expected_rejected}:
        raise LoadTestFailure(
            'Auth Token Bucket expected statuses '
            f'{{401: {AUTH_EXPECTED_FORWARDED}, 429: {expected_rejected}}}, '
            f'got {result.statuses}'
        )
    if sum(result.retry_after_values.values()) != expected_rejected:
        raise LoadTestFailure('Every Auth 429 response must contain Retry-After')
    if not all(_is_positive_integer(value) for value in result.retry_after_values):
        raise LoadTestFailure('Auth Retry-After must be a positive integer')


def _verify_nginx_rate_limit(result: ScenarioResult) -> None:
    if result.statuses.get(401, 0) == 0:
        raise LoadTestFailure(
            f'Nginx did not forward any request to Auth; got {result.statuses}'
        )
    if result.statuses.get(429, 0) == 0:
        raise LoadTestFailure(
            f'Nginx did not reject any request; got {result.statuses}'
        )
    if set(result.statuses) != {401, 429}:
        raise LoadTestFailure(f'Nginx scenario returned {result.statuses}')
    rejected = result.statuses[429]
    if sum(result.retry_after_values.values()) != rejected:
        raise LoadTestFailure('Every Nginx 429 response must contain Retry-After')
    if not all(_is_positive_integer(value) for value in result.retry_after_values):
        raise LoadTestFailure('Nginx Retry-After must be a positive integer')


def _create_refresh_session(
    *,
    base_url: str,
    login: str,
    request_id_prefix: str,
) -> str:
    signup = _send_json(
        base_url=base_url,
        path='/api/v1/auth/signup',
        payload={
            'login': login,
            'password': PASSWORD,
            'first_name': None,
            'last_name': None,
            'tz': 'Europe/Moscow',
        },
        request_id=f'{request_id_prefix}-signup',
    )
    if signup.status != 201:
        raise LoadTestFailure(
            f'Refresh scenario signup returned HTTP {signup.status}'
        )

    login_response = _send_json(
        base_url=base_url,
        path='/api/v1/auth/login',
        payload={'login': login, 'password': PASSWORD},
        request_id=f'{request_id_prefix}-login',
    )
    refresh_token = (
        login_response.body.get('refresh_token')
        if login_response.body is not None
        else None
    )
    if login_response.status != 200 or not isinstance(refresh_token, str):
        raise LoadTestFailure(
            f'Refresh scenario login returned HTTP {login_response.status}'
        )
    return refresh_token


def _run_refresh_user_bucket(
    *,
    base_url: str,
    run_id: str,
) -> RefreshScenarioResult:
    refresh_token = _create_refresh_session(
        base_url=base_url,
        login=f'load-refresh-a-{run_id}',
        request_id_prefix=f'load-refresh-a-{run_id}',
    )
    results: list[ResponseResult] = []
    started_at = perf_counter()
    for index in range(REFRESH_REQUESTS):
        result = _send_json(
            base_url=base_url,
            path='/api/v1/auth/refresh',
            payload={'refresh_token': refresh_token},
            request_id=f'load-refresh-a-{run_id}-{index}',
        )
        results.append(result)
        next_refresh_token = (
            result.body.get('refresh_token')
            if result.body is not None
            else None
        )
        if result.status == 200 and isinstance(next_refresh_token, str):
            refresh_token = next_refresh_token
    duration = perf_counter() - started_at

    other_refresh_token = _create_refresh_session(
        base_url=base_url,
        login=f'load-refresh-b-{run_id}',
        request_id_prefix=f'load-refresh-b-{run_id}',
    )
    other_user = _send_json(
        base_url=base_url,
        path='/api/v1/auth/refresh',
        payload={'refresh_token': other_refresh_token},
        request_id=f'load-refresh-b-{run_id}-0',
    )

    statuses = Counter(result.status for result in results)
    retry_after_values = Counter(
        result.retry_after
        for result in results
        if result.retry_after is not None
    )
    return RefreshScenarioResult(
        name='refresh_user_bucket',
        requests=len(results),
        duration_seconds=round(duration, 3),
        requests_per_second=round(len(results) / duration, 2),
        statuses=dict(sorted(statuses.items())),
        retry_after_values=dict(sorted(retry_after_values.items())),
        other_user_status=other_user.status,
    )


def _verify_refresh_user_bucket(result: RefreshScenarioResult) -> None:
    expected_statuses = {
        200: REFRESH_EXPECTED_ROTATIONS,
        429: REFRESH_REQUESTS - REFRESH_EXPECTED_ROTATIONS,
    }
    if result.statuses != expected_statuses:
        raise LoadTestFailure(
            f'Refresh Token Bucket expected {expected_statuses}, '
            f'got {result.statuses}'
        )
    if sum(result.retry_after_values.values()) != 1:
        raise LoadTestFailure('Refresh 429 response must contain Retry-After')
    if not all(_is_positive_integer(value) for value in result.retry_after_values):
        raise LoadTestFailure('Refresh Retry-After must be a positive integer')
    if result.other_user_status != 200:
        raise LoadTestFailure(
            'A second user from the same load generator must have an independent '
            f'refresh bucket; got HTTP {result.other_user_status}'
        )


def main() -> int:
    """Run all scenarios and return a process status suitable for CI."""
    auth_base_url = os.environ.get('AUTH_BASE_URL', 'http://auth:8000')
    gateway_base_url = os.environ.get('GATEWAY_BASE_URL', 'http://gateway')
    run_id = uuid4().hex

    auth_result = _run_concurrently(
        name='auth_token_bucket',
        base_url=auth_base_url,
        logins=[f'load-auth-{run_id}'] * AUTH_REQUESTS,
        request_ids=[f'load-auth-{run_id}-{index}' for index in range(AUTH_REQUESTS)],
    )
    _verify_auth_token_bucket(auth_result)

    refresh_result = _run_refresh_user_bucket(
        base_url=auth_base_url,
        run_id=run_id,
    )
    _verify_refresh_user_bucket(refresh_result)

    gateway_result = _run_concurrently(
        name='nginx_rate_limit',
        base_url=gateway_base_url,
        logins=[
            f'load-gateway-{run_id}-{index}' for index in range(GATEWAY_REQUESTS)
        ],
        request_ids=[
            f'load-gateway-{run_id}-{index}' for index in range(GATEWAY_REQUESTS)
        ],
    )
    _verify_nginx_rate_limit(gateway_result)

    print(
        json.dumps(
            {
                'result': 'PASS',
                'scenarios': [
                    asdict(auth_result),
                    asdict(refresh_result),
                    asdict(gateway_result),
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except LoadTestFailure as error:
        print(f'FAIL: {error}', file=sys.stderr)
        raise SystemExit(1) from error
