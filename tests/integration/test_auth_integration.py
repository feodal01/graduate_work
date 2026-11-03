from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

PUBLIC_FILM_ID = '11111111-1111-4111-8111-111111111111'
EARLY_FILM_ID = '22222222-2222-4222-8222-222222222222'
UNDATED_FILM_ID = '33333333-3333-4333-8333-333333333333'
ADMIN_LOGIN = 'admin'
ADMIN_PASSWORD = 'Cinema-Admin-2026!'
READER_LOGIN = 'integration-reader'
READER_PASSWORD = 'T9!vQ2#xL7@mP4$z'


@dataclass(frozen=True)
class HttpResult:
    status: int
    body: bytes
    url: str
    headers: dict[str, str]

    def json(self) -> Any:
        """Decode the response body as JSON."""
        return json.loads(self.body)

    def text(self) -> str:
        """Decode the response body as UTF-8 text."""
        return self.body.decode('utf-8')

    def header(self, name: str) -> str | None:
        """Return one response header without case sensitivity."""
        normalized_name = name.casefold()
        return next(
            (
                value
                for header_name, value in self.headers.items()
                if header_name.casefold() == normalized_name
            ),
            None,
        )


class Browser:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip('/')
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies),
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> HttpResult:
        """Send an HTTP request and return regular and error responses."""
        url = f'{self.base_url}{path}'
        request = urllib.request.Request(
            url,
            data=data,
            headers=headers or {},
            method=method,
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                return HttpResult(
                    status=response.status,
                    body=response.read(),
                    url=response.geturl(),
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            return HttpResult(
                status=exc.code,
                body=exc.read(),
                url=exc.geturl(),
                headers=dict(exc.headers.items()),
            )

    def form_post(
        self,
        path: str,
        fields: dict[str, str | list[str]],
        *,
        csrf_page: str | None = None,
    ) -> HttpResult:
        """Load a Django form and submit it with its CSRF token."""
        page_path = csrf_page or path
        page = self.request('GET', page_path)
        require_status(page, 200, f'GET {page_path}')
        token = extract_csrf_token(page.text())
        payload: dict[str, str | list[str]] = {
            'csrfmiddlewaretoken': token,
            **fields,
        }
        return self._post_form(path, payload, referer=page_path)

    def session_form_post(
        self,
        path: str,
        fields: dict[str, str | list[str]],
        *,
        referer: str,
    ) -> HttpResult:
        """Submit a form with the CSRF cookie from the active session."""
        token = next(
            (cookie.value for cookie in self.cookies if cookie.name == 'csrftoken'),
            None,
        )
        if token is None:
            raise AssertionError('Django session does not contain a CSRF cookie')
        payload: dict[str, str | list[str]] = {
            'csrfmiddlewaretoken': token,
            **fields,
        }
        return self._post_form(path, payload, referer=referer)

    def _post_form(
        self,
        path: str,
        payload: dict[str, str | list[str]],
        *,
        referer: str,
    ) -> HttpResult:
        return self.request(
            'POST',
            path,
            data=urllib.parse.urlencode(payload, doseq=True).encode(),
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': f'{self.base_url}{referer}',
            },
        )


class IntegrationCheck:
    def __init__(
        self,
        *,
        repository_root: Path,
        project_name: str,
        base_url: str,
    ) -> None:
        self.repository_root = repository_root
        self.project_name = project_name
        self.base_url = base_url.rstrip('/')
        self.compose_environment = {
            **os.environ,
            'GATEWAY_PORT': '18080',
            'JAEGER_UI_PORT': '18686',
            'JAEGER_OTLP_HTTP_PORT': '14318',
        }
        self.jaeger_url = 'http://127.0.0.1:18686'
        self.env = read_env(repository_root / '.env.example')

    def run(self) -> None:
        """Run the healthy, degraded, and restored integration scenarios."""
        self._wait_for_stack()
        self._check_login_history_migration_cycle()
        self._check_tracing()
        self._check_auth_token_bucket()
        time.sleep(2)
        self._check_nginx_rate_limit()
        time.sleep(3)
        self._create_films()
        self._wait_for_films()

        admin_access, admin_profile = self._login_auth(
            ADMIN_LOGIN,
            ADMIN_PASSWORD,
        )
        counts_before_admin = self._local_user_counts()
        reader_profile = self._api(
            'POST',
            '/api/v1/auth/signup',
            payload={
                'login': READER_LOGIN,
                'password': READER_PASSWORD,
                'first_name': 'Integration',
                'last_name': 'Reader',
                'tz': 'Europe/Moscow',
            },
            expected=201,
        ).json()

        if counts_before_admin != self._local_user_counts():
            raise AssertionError('Auth signup changed Admin user tables')

        roles = self._api(
            'GET',
            '/api/v1/auth/roles',
            access_token=admin_access,
            expected=200,
        ).json()
        subscriber = next(role for role in roles if role['name'] == 'subscriber')
        self._api(
            'POST',
            f"/api/v1/auth/users/{reader_profile['id']}/roles",
            payload={'role_id': subscriber['id']},
            access_token=admin_access,
            expected=204,
        )
        self._login_auth(READER_LOGIN, READER_PASSWORD)
        reader_access, _ = self._login_auth(READER_LOGIN, READER_PASSWORD)
        self._check_login_history_partitions(
            reader_access,
            reader_profile['id'],
        )

        self._check_movies_with_healthy_auth(reader_access)
        browser, local_user_id = self._check_admin_login(
            counts_before_admin,
            admin_profile['id'],
        )
        self._check_admin_audit(browser, local_user_id)
        self._wait_for_rate_limit_refill('ADMIN')
        self._check_admin_role_management(browser, admin_access, 'before-stop')

        self._compose('stop', 'auth')
        self._check_degradation(browser)

        self._compose('start', 'auth')
        self._wait_for_http('/auth/health', 200)
        require_status(
            self._http(
                'GET',
                f'/api/v1/films/{EARLY_FILM_ID}/',
                access_token=reader_access,
            ),
            200,
            'subscriber film after Auth restart',
        )
        restored_browser, _ = self._login_admin()
        self._wait_for_rate_limit_refill('ADMIN')
        self._check_admin_role_management(
            restored_browser,
            self._login_auth(ADMIN_LOGIN, ADMIN_PASSWORD)[0],
            'after-restart',
        )
        self._check_movies_rate_limit_degradation(reader_access)
        self._check_rate_limit_fail_open()
        print('Auth integration check passed.')

    def _check_login_history_migration_cycle(self) -> None:
        self._login_auth(ADMIN_LOGIN, ADMIN_PASSWORD)
        before = self._login_history_snapshot()
        if before == '[]':
            raise AssertionError('Auth login did not create login history')

        self._compose('stop', 'auth')
        try:
            self._run_auth_migration('downgrade', '20260829_0003')
            if self._auth_database_value(
                "SELECT relkind FROM pg_class "
                "WHERE oid = 'login_history'::regclass;"
            ) != 'r':
                raise AssertionError('downgrade did not restore a plain table')
            if self._login_history_snapshot() != before:
                raise AssertionError('downgrade changed login history rows')

            self._run_auth_migration('upgrade', 'head')
            if self._auth_database_value(
                "SELECT relkind FROM pg_class "
                "WHERE oid = 'login_history'::regclass;"
            ) != 'p':
                raise AssertionError('upgrade did not restore partitioning')
            if self._login_history_snapshot() != before:
                raise AssertionError('upgrade changed login history rows')
            self._check_duplicate_id_downgrade_guard()
        finally:
            self._run_auth_migration('upgrade', 'head')
            self._compose('start', 'auth')
            self._wait_for_http('/auth/health', 200)

    def _check_duplicate_id_downgrade_guard(self) -> None:
        guard_user_id = str(uuid4())
        guard_login = f'integration-downgrade-guard-{uuid4().hex}'
        self._auth_database_value(
            'WITH source AS ('
            'SELECT id, logged_at, ip_address, user_agent '
            'FROM login_history LIMIT 1'
            '), guard_user AS ('
            'INSERT INTO users '
            '(id, login, password, first_name, last_name, '
            'is_superuser, created_at) '
            f"VALUES ('{guard_user_id}'::uuid, '{guard_login}', NULL, "
            "NULL, NULL, false, now()) RETURNING id"
            ') '
            'INSERT INTO login_history '
            '(id, user_id, logged_at, ip_address, user_agent) '
            'SELECT source.id, guard_user.id, source.logged_at, '
            'source.ip_address, source.user_agent '
            'FROM source CROSS JOIN guard_user;'
        )

        result = self._compose_result(
            'run',
            '--rm',
            'auth-migrate',
            'alembic',
            'downgrade',
            '20260829_0003',
        )
        output = f'{result.stdout}{result.stderr}'
        if result.returncode == 0 or 'duplicate id values' not in output:
            raise AssertionError(
                'downgrade did not reject duplicate login history IDs:\n'
                f'{output}'
            )
        if self._auth_database_value(
            "SELECT relkind FROM pg_class "
            "WHERE oid = 'login_history'::regclass;"
        ) != 'p':
            raise AssertionError('failed downgrade changed the partitioned table')

        self._auth_database_value(
            'DELETE FROM login_history '
            f"WHERE user_id = '{guard_user_id}'::uuid; "
            f"DELETE FROM users WHERE id = '{guard_user_id}'::uuid;"
        )

    def _run_auth_migration(self, action: str, revision: str) -> None:
        self._compose(
            'run',
            '--rm',
            'auth-migrate',
            'alembic',
            action,
            revision,
        )

    def _login_history_snapshot(self) -> str:
        return self._auth_database_value(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object("
            "'id', id::text, 'user_id', user_id::text, "
            "'logged_at', logged_at, 'ip_address', ip_address, "
            "'user_agent', user_agent) ORDER BY user_id, id)::text, '[]') "
            'FROM login_history;'
        )

    def _check_login_history_partitions(
        self,
        access_token: str,
        user_id: str,
    ) -> None:
        normalized_user_id = str(UUID(user_id))
        leaf_count = self._auth_database_value(
            "SELECT count(*) FROM "
            "pg_partition_tree('login_history'::regclass) "
            'WHERE isleaf;'
        )
        if leaf_count != '16':
            raise AssertionError(
                f'Expected 16 login history partitions, got {leaf_count}'
            )
        bounds = self._auth_database_value(
            "SELECT child.relname || '|' || "
            'pg_get_expr(child.relpartbound, child.oid) '
            'FROM pg_class AS child '
            'JOIN pg_inherits AS inheritance '
            'ON inheritance.inhrelid = child.oid '
            "WHERE inheritance.inhparent = 'login_history'::regclass "
            'ORDER BY child.relname;'
        ).splitlines()
        expected_bounds = [
            f'login_history_p{remainder:02d}|'
            f'FOR VALUES WITH (modulus 16, remainder {remainder})'
            for remainder in range(16)
        ]
        if bounds != expected_bounds:
            raise AssertionError(f'Unexpected login history partitions: {bounds}')

        row_location = self._auth_database_value(
            "SELECT count(*)::text || '|' || "
            "count(DISTINCT tableoid)::text || '|' || "
            'min(tableoid::regclass::text) '
            'FROM login_history '
            f"WHERE user_id = '{normalized_user_id}'::uuid;"
        )
        row_count, table_count, table_name = row_location.split('|', 2)
        if row_count != '2' or table_count != '1':
            raise AssertionError(
                f'User history is not isolated in one partition: {row_location}'
            )
        if re.fullmatch(r'login_history_p\d{2}', table_name) is None:
            raise AssertionError(
                f'Unexpected login history table: {table_name}'
            )

        plan = self._auth_database_value(
            'EXPLAIN (COSTS OFF) '
            'SELECT id, user_id, logged_at, ip_address, user_agent '
            'FROM login_history '
            f"WHERE user_id = '{normalized_user_id}'::uuid "
            'ORDER BY logged_at DESC;'
        )
        planned_partitions = set(re.findall(r'login_history_p\d{2}', plan))
        if planned_partitions != {table_name}:
            raise AssertionError(
                f'Partition pruning did not select one table:\n{plan}'
            )

        history = self._api(
            'GET',
            '/api/v1/auth/login-history',
            access_token=access_token,
            expected=200,
        ).json()
        if len(history) != 2:
            raise AssertionError(f'Unexpected login history response: {history}')
        if any(item['user_id'] != normalized_user_id for item in history):
            raise AssertionError('Login history mixed different users')
        logged_at = [item['logged_at'] for item in history]
        if logged_at != sorted(logged_at, reverse=True):
            raise AssertionError('Login history is not newest first')

    def _check_auth_token_bucket(self) -> None:
        login = f'integration-token-bucket-{uuid4().hex}'
        results: list[HttpResult] = []
        for _ in range(6):
            results.append(
                self._http(
                    'POST',
                    '/api/v1/auth/login',
                    payload={
                        'login': login,
                        'password': 'Invalid-Password-2026!',
                    },
                )
            )
            time.sleep(0.05)

        statuses = [result.status for result in results]
        if statuses != [401, 401, 401, 401, 401, 429]:
            raise AssertionError(
                f'Auth Token Bucket returned unexpected statuses: {statuses}'
            )
        rejected = results[-1]
        if rejected.json() != {'detail': 'Too many requests'}:
            raise AssertionError('Auth Token Bucket returned unexpected JSON')
        retry_after = rejected.header('Retry-After')
        if retry_after is None or not retry_after.isdigit() or int(retry_after) < 1:
            raise AssertionError(
                f'Auth Token Bucket returned invalid Retry-After: {retry_after}'
            )

    def _check_nginx_rate_limit(self) -> None:
        prefix = f'integration-nginx-limit-{uuid4().hex}'

        def send_login(sequence: int) -> HttpResult:
            return self._http(
                'POST',
                '/api/v1/auth/login',
                payload={
                    'login': f'{prefix}-{sequence}',
                    'password': 'Invalid-Password-2026!',
                },
            )

        with ThreadPoolExecutor(max_workers=100) as executor:
            results = list(executor.map(send_login, range(100)))

        unexpected = sorted(
            {result.status for result in results} - {401, 429}
        )
        if unexpected:
            raise AssertionError(
                f'Nginx rate limit returned unexpected statuses: {unexpected}'
            )
        rejected = [result for result in results if result.status == 429]
        if not rejected:
            raise AssertionError('Nginx did not reject the parallel burst')
        if any(result.header('Retry-After') != '1' for result in rejected):
            raise AssertionError('Nginx 429 response has an invalid Retry-After')
        gateway_logs = self._compose('logs', 'gateway')
        if 'limit_req_status=REJECTED' not in gateway_logs:
            raise AssertionError('gateway log does not contain a rejected request')

    def _wait_for_stack(self) -> None:
        for path in ('/admin/health', '/auth/health', '/movies/health'):
            self._wait_for_http(path, 200, timeout=240)
        require_status(
            self._http('GET', '/api/v1/films?page_size=1'),
            200,
            'film list',
        )

    def _check_tracing(self) -> None:
        trace_id = '00112233445566778899aabbccddeeff'
        result = self._http(
            'GET',
            '/auth/health',
            headers={
                'traceparent': (
                    f'00-{trace_id}-0123456789abcdef-01'
                ),
            },
        )
        require_status(result, 200, 'sampled Auth health')

        trace = self._wait_for_trace(trace_id)
        request_id = self._auth_request_id(trace)
        gateway_logs = self._compose('logs', 'gateway')
        if f'request_id={request_id}' not in gateway_logs:
            raise AssertionError(
                'gateway access-log does not contain the traced request ID'
            )

        direct_check = self._compose(
            'exec',
            '-T',
            'auth',
            'python',
            '-c',
            (
                "import urllib.error, urllib.request\n"
                "try:\n"
                "    urllib.request.urlopen("
                "'http://127.0.0.1:8000/api/v1/health/', timeout=2)\n"
                "except urllib.error.HTTPError as exc:\n"
                "    print(f'{exc.code}|{exc.read().decode()}')\n"
            ),
        ).strip()
        if direct_check != '400|{"detail":"X-Request-Id is required"}':
            raise AssertionError(
                f'Auth accepted a request without X-Request-Id: {direct_check}'
            )

        self._compose('stop', 'jaeger')
        unavailable_export = self._http(
            'GET',
            '/auth/health',
            headers={
                'traceparent': (
                    '00-ffeeddccbbaa99887766554433221100-'
                    'fedcba9876543210-01'
                ),
            },
        )
        require_status(
            unavailable_export,
            200,
            'Auth health while Jaeger is unavailable',
        )
        self._compose('start', 'jaeger')

    def _wait_for_trace(self, trace_id: str) -> dict[str, Any]:
        url = f'{self.jaeger_url}/api/traces/{trace_id}'
        deadline = time.monotonic() + 30
        last_result = 'no trace data'
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    payload = json.load(response)
                traces = payload.get('data')
                if isinstance(traces, list) and traces:
                    trace = traces[0]
                    if isinstance(trace, dict):
                        return trace
                last_result = str(payload)[:500]
            except (urllib.error.URLError, json.JSONDecodeError) as exc:
                last_result = str(exc)
            time.sleep(0.5)
        raise AssertionError(
            f'Jaeger did not return trace {trace_id}: {last_result}'
        )

    @staticmethod
    def _auth_request_id(trace: dict[str, Any]) -> str:
        processes = trace.get('processes')
        spans = trace.get('spans')
        if not isinstance(processes, dict) or not isinstance(spans, list):
            raise AssertionError('Jaeger trace has an unexpected structure')
        for span in spans:
            if not isinstance(span, dict):
                continue
            process = processes.get(span.get('processID'))
            if not isinstance(process, dict) or process.get('serviceName') != 'auth':
                continue
            tags = span.get('tags')
            if not isinstance(tags, list):
                continue
            for tag in tags:
                if (
                    isinstance(tag, dict)
                    and tag.get('key') == 'http.request_id'
                    and isinstance(tag.get('value'), str)
                    and tag['value']
                ):
                    return tag['value']
        raise AssertionError(
            'Auth server span does not contain http.request_id'
        )

    def _create_films(self) -> None:
        code = (
            "from datetime import date; "
            "from uuid import UUID; "
            "from movies.models import FilmWork; "
            f"items=[('{PUBLIC_FILM_ID}','Integration Public',date(2020,1,1)),"
            f"('{EARLY_FILM_ID}','Integration Early',date(2026,8,1)),"
            f"('{UNDATED_FILM_ID}','Integration Undated',None)]; "
            "[FilmWork.objects.update_or_create(id=UUID(item[0]), defaults={"
            "'title':item[1], 'description':'integration fixture', "
            "'creation_date':item[2], 'rating':8.0, 'type':'movie'}) "
            "for item in items]; print('films-ready')"
        )
        result = self._compose(
            'exec',
            '-T',
            'admin',
            'python',
            'manage.py',
            'shell',
            '-c',
            code,
        )
        if 'films-ready' not in result:
            raise AssertionError('Django did not create film fixtures')

    def _wait_for_films(self) -> None:
        query = json.dumps(
            {
                'query': {
                    'ids': {
                        'values': [PUBLIC_FILM_ID, EARLY_FILM_ID, UNDATED_FILM_ID]
                    }
                }
            }
        )
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            output = self._compose(
                'exec',
                '-T',
                'elasticsearch',
                'curl',
                '-fsS',
                'http://127.0.0.1:9200/movies/_count',
                '-H',
                'Content-Type: application/json',
                '-d',
                query,
                check=False,
            )
            try:
                if json.loads(output).get('count') == 3:
                    return
            except json.JSONDecodeError:
                pass
            time.sleep(2)
        raise AssertionError('Movies ETL did not index the three film fixtures')

    def _check_movies_with_healthy_auth(self, reader_access: str) -> None:
        require_status(
            self._http('GET', f'/api/v1/films/{PUBLIC_FILM_ID}/'),
            200,
            'anonymous public film',
        )
        for film_id in (EARLY_FILM_ID, UNDATED_FILM_ID):
            require_status(
                self._http('GET', f'/api/v1/films/{film_id}/'),
                403,
                'anonymous restricted film',
            )
            require_status(
                self._http(
                    'GET',
                    f'/api/v1/films/{film_id}/',
                    access_token=reader_access,
                ),
                200,
                'subscriber restricted film',
            )
        require_status(
            self._http(
                'GET',
                f'/api/v1/films/{PUBLIC_FILM_ID}/',
                access_token='invalid-token',
            ),
            401,
            'invalid bearer token',
        )

    def _check_movies_rate_limit_degradation(self, reader_access: str) -> None:
        time.sleep(5)

        def get_early_film(_sequence: int) -> HttpResult:
            return self._http(
                'GET',
                f'/api/v1/films/{EARLY_FILM_ID}/',
                access_token=reader_access,
            )

        with ThreadPoolExecutor(max_workers=100) as executor:
            results = list(executor.map(get_early_film, range(100)))

        statuses = {result.status for result in results}
        if statuses != {200, 403}:
            raise AssertionError(
                'Movies rate-limit degradation must return both 200 and 403; '
                f'got {sorted(statuses)}'
            )
        auth_logs = self._compose('logs', 'auth')
        if 'policy=service allowed=false' not in auth_logs:
            raise AssertionError(
                'Auth log does not contain a rejected service policy request'
            )

    def _check_rate_limit_fail_open(self) -> None:
        self._compose('stop', 'auth-redis')
        try:
            result = self._http(
                'POST',
                '/api/v1/auth/signup',
                payload={
                    'login': f'integration-fail-open-{uuid4().hex}',
                    'password': 'Fail-Open-Password-2026!',
                    'first_name': 'Fail',
                    'last_name': 'Open',
                    'tz': 'Europe/Moscow',
                },
            )
            require_status(result, 201, 'Auth signup while Redis is unavailable')
            auth_logs = self._compose('logs', 'auth')
            if 'policy=signup store_available=false' not in auth_logs:
                raise AssertionError(
                    'Auth log does not contain the rate-limit fail-open decision'
                )
        finally:
            self._compose('start', 'auth-redis')
        self._wait_for_http('/auth/health', 200)

    def _check_admin_login(
        self,
        counts_before: tuple[int, int],
        auth_user_id: str,
    ) -> tuple[Browser, int]:
        browser, login_result = self._login_admin()
        require_status(login_result, 200, 'first Admin login')
        if not login_result.url.endswith('/admin/'):
            raise AssertionError('Admin login did not redirect to the index')

        expected_counts = (counts_before[0] + 1, counts_before[1] + 1)
        actual_counts = self._local_user_counts()
        if actual_counts != expected_counts:
            raise AssertionError(
                f'first Admin login counts {actual_counts}, expected {expected_counts}'
            )

        identity = self._database_value(
            "SELECT u.id::text || '|' || i.auth_user_id::text || '|' || "
            "u.password FROM auth_user AS u JOIN users_authidentity AS i "
            "ON i.user_id = u.id ORDER BY i.id DESC LIMIT 1;"
        )
        local_user_id_text, stored_auth_id, password = identity.split('|', 2)
        if stored_auth_id != auth_user_id:
            raise AssertionError('AuthIdentity contains another Auth UUID')
        if not password.startswith('!'):
            raise AssertionError('technical Django user has a usable password')

        logout = browser.session_form_post(
            '/admin/logout/',
            {},
            referer='/admin/',
        )
        require_status(logout, 200, 'Admin logout')
        self._wait_for_rate_limit_refill('ADMIN')
        browser, repeated_login = self._login_admin(browser)
        require_status(repeated_login, 200, 'repeated Admin login')
        if self._local_user_counts() != expected_counts:
            raise AssertionError('repeated Admin login created a duplicate')
        repeated_identity = self._database_value(
            "SELECT u.id::text || '|' || i.auth_user_id::text FROM auth_user AS u "
            "JOIN users_authidentity AS i ON i.user_id = u.id "
            f"WHERE i.auth_user_id = '{auth_user_id}';"
        )
        if repeated_identity != f'{local_user_id_text}|{auth_user_id}':
            raise AssertionError('repeated Admin login changed the local identity')
        return browser, int(local_user_id_text)

    def _login_admin(
        self,
        browser: Browser | None = None,
    ) -> tuple[Browser, HttpResult]:
        active_browser = browser or Browser(self.base_url)
        result = active_browser.form_post(
            '/admin/login/',
            {
                'username': ADMIN_LOGIN,
                'password': ADMIN_PASSWORD,
                'next': '/admin/',
            },
        )
        return active_browser, result

    def _check_admin_audit(self, browser: Browser, local_user_id: int) -> None:
        genre_name = 'Integration Audit Genre'
        before = int(
            self._database_value(
                "SELECT count(*) FROM content.genre "
                f"WHERE name = '{genre_name}';"
            )
        )
        result = browser.form_post(
            '/admin/movies/genre/add/',
            {'name': genre_name, 'description': 'integration audit', '_save': 'Save'},
        )
        require_status(result, 200, 'Admin catalogue write')
        after = int(
            self._database_value(
                "SELECT count(*) FROM content.genre "
                f"WHERE name = '{genre_name}';"
            )
        )
        if after != before + 1:
            raise AssertionError('Admin did not create the catalogue record')
        log_user_id = int(
            self._database_value(
                "SELECT user_id FROM django_admin_log ORDER BY id DESC LIMIT 1;"
            )
        )
        if log_user_id != local_user_id:
            raise AssertionError('django_admin_log points to another local user')

    def _check_admin_role_management(
        self,
        browser: Browser,
        admin_access: str,
        suffix: str,
    ) -> None:
        role_name = f'integration-role-{suffix}'
        create = browser.form_post(
            '/admin/auth/roles/add/',
            {'name': role_name, 'permissions': ['roles:manage']},
        )
        require_status(create, 200, 'Admin role create')
        roles = self._api(
            'GET',
            '/api/v1/auth/roles',
            access_token=admin_access,
            expected=200,
        ).json()
        created_role = next((role for role in roles if role['name'] == role_name), None)
        if created_role is None:
            raise AssertionError('role created in Admin is absent in Auth')
        self._wait_for_rate_limit_refill('ADMIN')
        delete = browser.form_post(
            f"/admin/auth/roles/{created_role['id']}/delete/",
            {},
        )
        require_status(delete, 200, 'Admin role delete')
        remaining = self._api(
            'GET',
            '/api/v1/auth/roles',
            access_token=admin_access,
            expected=200,
        ).json()
        if any(role['id'] == created_role['id'] for role in remaining):
            raise AssertionError('role deleted in Admin remains in Auth')

    def _wait_for_rate_limit_refill(self, policy_name: str) -> None:
        rate = int(self.env[f'AUTH_RATE_LIMIT_{policy_name}_RATE'])
        window_seconds = int(
            self.env[f'AUTH_RATE_LIMIT_{policy_name}_WINDOW_SECONDS']
        )
        capacity = int(self.env[f'AUTH_RATE_LIMIT_{policy_name}_CAPACITY'])
        time.sleep(capacity * window_seconds / rate + 0.5)

    def _check_degradation(self, browser: Browser) -> None:
        running = set(
            self._compose('ps', '--status', 'running', '--services').splitlines()
        )
        if not {'gateway', 'movies-api'}.issubset(running):
            raise AssertionError('gateway or Movies API stopped with Auth')

        for path in (
            '/admin/health',
            '/movies/health',
            '/api/v1/films?page_size=1',
            '/api/v1/films/search/?query=Integration',
        ):
            require_status(self._http('GET', path), 200, f'degraded GET {path}')
        require_status(
            self._http('GET', f'/api/v1/films/{PUBLIC_FILM_ID}/'),
            200,
            'degraded public film',
        )
        for film_id in (EARLY_FILM_ID, UNDATED_FILM_ID):
            require_status(
                self._http('GET', f'/api/v1/films/{film_id}/'),
                403,
                'degraded restricted film',
            )

        local_counts = self._local_user_counts()
        _, unavailable_login = self._login_admin()
        require_status(unavailable_login, 503, 'Admin login while Auth is down')
        if self._local_user_counts() != local_counts:
            raise AssertionError('failed Admin login changed local user tables')

        genre_count = int(
            self._database_value(
                "SELECT count(*) FROM content.genre "
                "WHERE name = 'Integration Rejected Genre';"
            )
        )
        rejected = browser.session_form_post(
            '/admin/movies/genre/add/',
            {
                'name': 'Integration Rejected Genre',
                'description': 'must not be saved',
                '_save': 'Save',
            },
            referer='/admin/movies/genre/add/',
        )
        require_status(rejected, 503, 'Admin write while Auth is down')
        if int(
            self._database_value(
                "SELECT count(*) FROM content.genre "
                "WHERE name = 'Integration Rejected Genre';"
            )
        ) != genre_count:
            raise AssertionError('Admin wrote catalogue data while Auth was down')

        logout = browser.session_form_post(
            '/admin/logout/',
            {},
            referer='/admin/',
        )
        require_status(logout, 200, 'Admin logout while Auth is down')
        protected = browser.request('GET', '/admin/movies/genre/')
        require_status(protected, 200, 'request after local logout')
        query = urllib.parse.parse_qs(urllib.parse.urlparse(protected.url).query)
        if not protected.url.startswith(f'{self.base_url}/admin/login/') or query.get(
            'next'
        ) != ['/admin/movies/genre/']:
            raise AssertionError('local Admin session survived logout')

    def _login_auth(self, login: str, password: str) -> tuple[str, dict[str, Any]]:
        pair = self._api(
            'POST',
            '/api/v1/auth/login',
            payload={'login': login, 'password': password},
            expected=200,
        ).json()
        profile = self._api(
            'GET',
            '/api/v1/auth/profile',
            access_token=pair['access_token'],
            expected=200,
        ).json()
        return pair['access_token'], profile

    def _api(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        access_token: str | None = None,
        expected: int,
    ) -> HttpResult:
        result = self._http(
            method,
            path,
            payload=payload,
            access_token=access_token,
        )
        require_status(result, expected, f'{method} {path}')
        return result

    def _http(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        access_token: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResult:
        data = json.dumps(payload).encode() if payload is not None else None
        request_headers = dict(headers or {})
        if data is not None:
            request_headers['Content-Type'] = 'application/json'
        if access_token is not None:
            request_headers['Authorization'] = f'Bearer {access_token}'
        request = urllib.request.Request(
            f'{self.base_url}{path}',
            data=data,
            headers=request_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return HttpResult(
                    response.status,
                    response.read(),
                    response.geturl(),
                    dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            return HttpResult(
                exc.code,
                exc.read(),
                exc.geturl(),
                dict(exc.headers.items()),
            )

    def _wait_for_http(
        self,
        path: str,
        expected: int,
        *,
        timeout: float = 120,
    ) -> None:
        deadline = time.monotonic() + timeout
        last_status: int | str = 'connection failed'
        while time.monotonic() < deadline:
            try:
                result = self._http('GET', path)
                last_status = result.status
                if result.status == expected:
                    return
            except urllib.error.URLError as exc:
                last_status = str(exc)
            time.sleep(1)
        raise AssertionError(
            f'{path} did not return {expected}; last result: {last_status}'
        )

    def _local_user_counts(self) -> tuple[int, int]:
        result = self._database_value(
            "SELECT (SELECT count(*) FROM auth_user)::text || '|' || "
            "(SELECT count(*) FROM users_authidentity)::text;"
        )
        users, identities = result.split('|', 1)
        return int(users), int(identities)

    def _database_value(self, sql: str) -> str:
        return self._compose(
            'exec',
            '-T',
            'theatre-db',
            'psql',
            '-v',
            'ON_ERROR_STOP=1',
            '-U',
            self.env['THEATRE_POSTGRES_USER'],
            '-d',
            self.env['THEATRE_POSTGRES_DB'],
            '-Atc',
            sql,
        ).strip()

    def _auth_database_value(self, sql: str) -> str:
        return self._compose(
            'exec',
            '-T',
            'auth-db',
            'psql',
            '-v',
            'ON_ERROR_STOP=1',
            '-U',
            self.env['AUTH_POSTGRES_USER'],
            '-d',
            self.env['AUTH_POSTGRES_DB'],
            '-Atc',
            sql,
        ).strip()

    def _compose(self, *arguments: str, check: bool = True) -> str:
        result = self._compose_result(*arguments)
        if check and result.returncode != 0:
            command = result.args
            raise AssertionError(
                f"command failed: {' '.join(command)}\n"
                f'{result.stdout}{result.stderr}'
            )
        return result.stdout

    def _compose_result(
        self,
        *arguments: str,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            'docker',
            'compose',
            '--project-name',
            self.project_name,
            '--env-file',
            str(self.repository_root / '.env.example'),
            '--project-directory',
            str(self.repository_root),
            *arguments,
        ]
        return subprocess.run(
            command,
            cwd=self.repository_root,
            env=self.compose_environment,
            check=False,
            capture_output=True,
            text=True,
        )


def extract_csrf_token(page: str) -> str:
    """Extract the hidden CSRF value from a rendered Django form."""
    match = re.search(
        r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)',
        page,
    )
    if match is None:
        raise AssertionError('Django form does not contain a CSRF token')
    return match.group(1)


def require_status(result: HttpResult, expected: int, operation: str) -> None:
    """Fail with the response body when an HTTP status differs."""
    if result.status != expected:
        body = result.body.decode('utf-8', errors='replace')[:1000]
        raise AssertionError(
            f'{operation} returned {result.status}, expected {expected}: {body}'
        )


def read_env(path: Path) -> dict[str, str]:
    """Read the simple key-value environment template used by Compose."""
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        name, value = line.split('=', 1)
        values[name] = value.strip('"')
    return values


def main() -> None:
    """Parse command-line arguments and run the integration check."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository-root', type=Path, required=True)
    parser.add_argument('--project-name', required=True)
    parser.add_argument('--base-url', required=True)
    arguments = parser.parse_args()
    IntegrationCheck(
        repository_root=arguments.repository_root,
        project_name=arguments.project_name,
        base_url=arguments.base_url,
    ).run()


if __name__ == '__main__':
    main()
