from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class HttpResult:
    status: int
    body: bytes
    url: str
    headers: dict[str, str]
    set_cookies: list[str]

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


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        """Do not follow HTTP redirects automatically."""
        return None


class BillingIntegrationCheck:
    def __init__(
        self,
        *,
        repository_root: Path,
        project_name: str,
        env_file: Path,
        base_url: str,
        jaeger_url: str,
    ) -> None:
        self.repository_root = repository_root
        self.project_name = project_name
        self.env_file = env_file
        self.base_url = base_url.rstrip('/')
        self.jaeger_url = jaeger_url.rstrip('/')
        self.compose_environment = {
            **os.environ,
            'GATEWAY_PORT': self.base_url.rsplit(':', 1)[-1],
            'JAEGER_UI_PORT': self.jaeger_url.rsplit(':', 1)[-1],
            'JAEGER_OTLP_HTTP_PORT': os.environ.get(
                'JAEGER_OTLP_HTTP_PORT',
                '14319',
            ),
        }
        self.no_redirect_opener = urllib.request.build_opener(NoRedirectHandler())

    def run(self) -> None:
        """Run live Docker Compose checks for T-06, T-07, T-08, T-09, and T-10."""
        self._wait_for_billing_and_auth_health()
        self._check_one_time_migration_and_seeds()
        self._check_gateway_routing_and_internal_isolation()
        self._check_tracing_and_request_id()
        self._check_nginx_rate_limit_on_billing()
        self._check_db_invariants_in_live_postgres()
        self._check_login_logout_and_auth_degradation_t08()
        self._check_yookassa_adapter_in_container_t09()
        self._check_purchase_creation_t10()
        self._check_degradation_and_worker_graceful_shutdown()
        print('All Billing T-06 / T-07 / T-08 / T-09 / T-10 live Docker checks passed.')

    def _http(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        timeout: float = 10.0,
    ) -> HttpResult:
        url = f'{self.base_url}{path}'
        request = urllib.request.Request(
            url,
            data=data,
            headers=headers or {},
            method=method,
        )
        opener = (
            urllib.request.build_opener()
            if follow_redirects
            else self.no_redirect_opener
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                return HttpResult(
                    status=response.status,
                    body=response.read(),
                    url=response.geturl(),
                    headers=dict(response.headers.items()),
                    set_cookies=list(response.headers.get_all('Set-Cookie') or []),
                )
        except urllib.error.HTTPError as exc:
            return HttpResult(
                status=exc.code,
                body=exc.read(),
                url=exc.geturl(),
                headers=dict(exc.headers.items()),
                set_cookies=list(exc.headers.get_all('Set-Cookie') or []),
            )

    def _compose_result(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                'docker',
                'compose',
                '--project-name',
                self.project_name,
                '--env-file',
                str(self.env_file),
                '--project-directory',
                str(self.repository_root),
                *args,
            ],
            env=self.compose_environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def _compose(self, *args: str) -> str:
        result = self._compose_result(*args)
        if result.returncode != 0:
            raise AssertionError(
                f'docker compose {" ".join(args)} failed:\n'
                f'{result.stdout}\n{result.stderr}'
            )
        return result.stdout

    def _psql(self, sql_query: str) -> subprocess.CompletedProcess[str]:
        return self._compose_result(
            'exec',
            '-T',
            'billing-db',
            'psql',
            '-v',
            'ON_ERROR_STOP=1',
            '-tA',
            '-U',
            'billing',
            '-d',
            'billing',
            '-c',
            sql_query,
        )

    def _wait_for_billing_and_auth_health(self, timeout: float = 120.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                bil = self._http('GET', '/billing/health', timeout=3.0)
                auth = self._http('GET', '/auth/health', timeout=3.0)
                if (
                    bil.status == 200
                    and bil.json() == {'status': 'ok', 'database': 'ok'}
                    and auth.status == 200
                ):
                    return
            except Exception:
                pass
            time.sleep(1.0)
        raise AssertionError('Billing or Auth health endpoint did not become healthy')

    def _check_one_time_migration_and_seeds(self) -> None:
        """GW-FR-02, T-07, BIL-NFR-R-08: migration, 9 tables, tariffs & providers."""
        tables_res = self._psql(
            "SELECT string_agg(tablename, ',' ORDER BY tablename) "
            "FROM pg_tables WHERE schemaname = 'public';"
        )
        assert tables_res.returncode == 0, tables_res.stderr
        expected_tables = (
            'admin_actions,alembic_version,operation_events,outbox_tasks,'
            'payment_providers,payments,refunds,subscriptions,tariffs,'
            'user_access_versions'
        )
        if tables_res.stdout.strip() != expected_tables:
            raise AssertionError(f'Unexpected tables: {tables_res.stdout.strip()}')

        sync_commit = self._psql('SHOW synchronous_commit;').stdout.strip()
        if sync_commit != 'on':
            raise AssertionError(f'Expected synchronous_commit=on, got {sync_commit}')

        provider = self._psql(
            "SELECT code || '|' || title || '|' || is_active::text "
            "FROM payment_providers WHERE code = 'yookassa';"
        ).stdout.strip()
        if provider != 'yookassa|ЮKassa|true':
            raise AssertionError(f'Unexpected payment_provider seed: {provider}')

        tariff = self._psql(
            "SELECT code || '|' || price_kopeks::text || '|' || "
            "trim(currency) || '|' || duration_days::text "
            "FROM tariffs WHERE code = 'premium';"
        ).stdout.strip()
        if tariff != 'premium|29900|RUB|30':
            raise AssertionError(f'Unexpected tariff seed: {tariff}')

        self._compose('run', '--rm', 'billing-migrate', 'alembic', 'downgrade', 'base')
        self._compose('run', '--rm', 'billing-migrate', 'alembic', 'upgrade', 'head')

    def _check_gateway_routing_and_internal_isolation(self) -> None:
        """GW-FR-01, GW-API-01, GW-API-02, NFR-SEC-01: routing & internal 404."""
        health = self._http('GET', '/billing/health')
        if health.status != 200 or health.json() != {
            'status': 'ok',
            'database': 'ok',
        }:
            raise AssertionError(f'Unexpected /billing/health response: {health}')

        openapi = self._http('GET', '/billing/openapi.json')
        if openapi.status != 200 or '/api/v1/health/' not in openapi.json().get(
            'paths', {}
        ):
            raise AssertionError(f'Unexpected /billing/openapi.json: {openapi}')

        docs = self._http('GET', '/billing/docs')
        if docs.status != 200 or 'swagger-ui' not in docs.text():
            raise AssertionError('Swagger UI not served on /billing/docs')

        redirect = self._http('GET', '/billing', follow_redirects=False)
        if redirect.status != 301 or not (redirect.header('Location') or '').endswith(
            '/billing/'
        ):
            raise AssertionError(f'Expected 301 redirect from /billing: {redirect}')

        for internal_path in (
            '/api/v1/internal/billing/payments',
            '/api/v1/internal/billing/subscriptions',
            '/api/v1/internal/billing/outbox-tasks',
            '/api/v1/internal/billing/admin-actions',
        ):
            res = self._http(
                'GET',
                internal_path,
                headers={
                    'X-Service-Api-Key': 'local-service-key-change-me-00000000000000'
                },
            )
            if res.status != 404:
                raise AssertionError(
                    f'Internal path {internal_path} must return 404, got {res.status}'
                )

    def _check_tracing_and_request_id(self) -> None:
        """GW-NFR-S-01, NFR-S-01, NFR-U-02: X-Request-Id & Jaeger export."""
        direct_no_req_id = self._compose(
            'exec',
            '-T',
            'billing-api',
            'python',
            '-c',
            (
                'import urllib.error, urllib.request\n'
                'try:\n'
                '    urllib.request.urlopen('
                "'http://127.0.0.1:8000/api/v1/health/', timeout=2)\n"
                'except urllib.error.HTTPError as exc:\n'
                "    print(f'{exc.code}|{exc.read().decode()}')\n"
            ),
        ).strip()
        expected_err = (
            '400|{"error_code":"missing_request_id",'
            '"detail":"X-Request-Id is required"}'
        )
        if direct_no_req_id != expected_err:
            raise AssertionError(
                f'billing-api accepted request without X-Request-Id: {direct_no_req_id}'
            )

        trace_id = uuid4().hex
        sampled = self._http(
            'GET',
            '/billing/health',
            headers={'traceparent': f'00-{trace_id}-0123456789abcdef-01'},
        )
        if sampled.status != 200:
            raise AssertionError(f'Sampled /billing/health failed: {sampled}')

        deadline = time.monotonic() + 25.0
        found_request_id: str | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f'{self.jaeger_url}/api/traces/{trace_id}',
                    timeout=3.0,
                ) as resp:
                    payload = json.load(resp)
                traces = payload.get('data') or []
                if traces:
                    spans = traces[0].get('spans') or []
                    for span in spans:
                        for tag in span.get('tags') or []:
                            if tag.get('key') == 'http.request_id':
                                found_request_id = tag.get('value')
                                break
                if found_request_id:
                    break
            except Exception:
                pass
            time.sleep(0.5)

        if not found_request_id:
            raise AssertionError(
                f'Jaeger did not record trace {trace_id} with http.request_id'
            )

    def _check_nginx_rate_limit_on_billing(self) -> None:
        """GW-NFR-SEC-01, NFR-U-02: rate limiting on /billing/* returns 429."""

        def call_billing(_: int) -> HttpResult:
            return self._http('GET', '/billing/nonexistent-rate-limit-probe')

        with ThreadPoolExecutor(max_workers=80) as pool:
            results = list(pool.map(call_billing, range(80)))

        rejected = [r for r in results if r.status == 429]
        if not rejected:
            raise AssertionError('Nginx rate limit did not trigger 429 on /billing/*')
        for r in rejected:
            if r.header('Retry-After') != '1':
                raise AssertionError(f'Expected Retry-After: 1 on 429, got {r.headers}')
            body = r.json()
            if body.get('error_code') != 'rate_limit_exceeded':
                raise AssertionError(
                    f'Expected error_code=rate_limit_exceeded, got {body}'
                )
        time.sleep(2.0)

    def _check_db_invariants_in_live_postgres(self) -> None:
        """BIL-NFR-R-01, BIL-NFR-R-02, BIL-NFR-R-07: DB constraints in live Postgres."""
        user_id = str(uuid4())
        pay_1 = str(uuid4())
        pay_2 = str(uuid4())
        idem_1 = str(uuid4())
        idem_2 = str(uuid4())

        res1 = self._psql(
            'INSERT INTO payments '
            '(id, user_id, tariff_id, provider_code, amount_kopeks, '
            'currency, status, idempotency_key) VALUES '
            f"('{pay_1}', '{user_id}', "
            "'00000000-0000-0000-0000-000000000001', 'yookassa', "
            f"29900, 'RUB', 'pending', '{idem_1}');"
        )
        assert res1.returncode == 0, res1.stderr

        res2 = self._psql(
            'INSERT INTO payments '
            '(id, user_id, tariff_id, provider_code, amount_kopeks, '
            'currency, status, idempotency_key) VALUES '
            f"('{pay_2}', '{user_id}', "
            "'00000000-0000-0000-0000-000000000001', 'yookassa', "
            f"29900, 'RUB', 'pending', '{idem_2}');"
        )
        if res2.returncode == 0 or 'uq_payments_user_pending' not in res2.stderr:
            raise AssertionError(f'Duplicate pending payment was not rejected: {res2}')

        res_del = self._psql("DELETE FROM tariffs WHERE code = 'premium';")
        if res_del.returncode == 0 or 'payments_tariff_id_fkey' not in res_del.stderr:
            raise AssertionError(
                f'Deleting referenced tariff was not blocked by RESTRICT: {res_del}'
            )

        self._psql(f"DELETE FROM payments WHERE id = '{pay_1}';")

    def _check_login_logout_and_auth_degradation_t08(self) -> None:
        """T-08 (BIL-API-01, BIL-FR-12, BIL-NFR-SEC-03): E1 login, logout, E4 503."""
        login_page = self._http('GET', '/billing/login')
        if (
            login_page.status != 200
            or 'name="login"' not in login_page.text()
            or 'name="password"' not in login_page.text()
        ):
            raise AssertionError(f'GET /billing/login failed: {login_page}')

        bad_login = self._http(
            'POST',
            '/billing/login',
            data=urllib.parse.urlencode(
                {'login': 'admin', 'password': 'wrong-password-123'}
            ).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            follow_redirects=False,
        )
        if (
            bad_login.status != 401
            or 'Неверный логин или пароль' not in bad_login.text()
            or bad_login.set_cookies
        ):
            raise AssertionError(f'Expected 401 on wrong password: {bad_login}')

        good_login = self._http(
            'POST',
            '/billing/login',
            data=urllib.parse.urlencode(
                {'login': 'admin', 'password': 'Cinema-Admin-2026!'}
            ).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            follow_redirects=False,
        )
        if good_login.status != 303 or not (
            good_login.header('Location') or ''
        ).endswith('/billing/'):
            raise AssertionError(f'Expected 303 on valid login: {good_login}')
        if not any(
            'billing_access_token=' in c and 'HttpOnly' in c
            for c in good_login.set_cookies
        ):
            raise AssertionError(
                f'Missing HttpOnly access cookie: {good_login.set_cookies}'
            )
        if not any(
            'billing_refresh_token=' in c and 'HttpOnly' in c
            for c in good_login.set_cookies
        ):
            raise AssertionError(
                f'Missing HttpOnly refresh cookie: {good_login.set_cookies}'
            )

        cookie_pairs = [c.split(';', 1)[0] for c in good_login.set_cookies]
        cookie_header = '; '.join(cookie_pairs)
        logout_res = self._http(
            'POST',
            '/billing/logout',
            headers={'Cookie': cookie_header},
            follow_redirects=False,
        )
        if logout_res.status != 303 or not (
            logout_res.header('Location') or ''
        ).endswith('/billing/login'):
            raise AssertionError(
                f'Expected 303 to /billing/login on logout: {logout_res}'
            )
        if not any('Max-Age=0' in c for c in logout_res.set_cookies):
            raise AssertionError(
                f'Cookies were not cleared on logout: {logout_res.set_cookies}'
            )

        self._compose('stop', 'auth')
        try:
            degraded_login = self._http(
                'POST',
                '/billing/login',
                data=urllib.parse.urlencode(
                    {'login': 'admin', 'password': 'Cinema-Admin-2026!'}
                ).encode(),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                follow_redirects=False,
            )
            if (
                degraded_login.status != 503
                or 'Сервис авторизации временно недоступен' not in degraded_login.text()
            ):
                raise AssertionError(
                    f'Expected 503 E4 screen when Auth is down: {degraded_login}'
                )
        finally:
            self._compose('start', 'auth')
        self._wait_for_billing_and_auth_health()

    def _check_yookassa_adapter_in_container_t09(self) -> None:
        """T-09: verify YooKassaProviderAdapter inside billing-api container."""
        code = (
            'import asyncio\n'
            'from uuid import uuid4\n'
            'import httpx\n'
            'from core.config import YooKassaSettings\n'
            'from clients.payment_provider import (\n'
            '    CreateProviderPaymentRequest,\n'
            '    ProviderIntegrityMismatchError,\n'
            '    ProviderPermanentError,\n'
            '    ProviderReconciliationError,\n'
            '    YooKassaProviderAdapter,\n'
            ')\n'
            'async def check():\n'
            '    yk = YooKassaSettings()\n'
            '    pid = uuid4()\n'
            '    if yk.shop_id != "replace-with-test-yookassa-shop-id":\n'
            '        timeout = httpx.Timeout(yk.timeout_seconds)\n'
            '        async with httpx.AsyncClient(timeout=timeout) as client:\n'
            '            adapter = YooKassaProviderAdapter(\n'
            '                http_client=client,\n'
            '                shop_id=yk.shop_id,\n'
            '                secret_key=yk.secret_key.get_secret_value(),\n'
            '                base_url=str(yk.api_base_url),\n'
            '            )\n'
            '            req = CreateProviderPaymentRequest(\n'
            '                payment_id=pid,\n'
            '                idempotency_key=pid,\n'
            '                amount_kopeks=29900,\n'
            '                currency="RUB",\n'
            '                description="Live Docker T-09 check",\n'
            '                return_url=f"http://localhost/billing/payments/{pid}",\n'
            '            )\n'
            '            c1 = await adapter.create_payment(req)\n'
            '            c2 = await adapter.create_payment(req)\n'
            '            assert c1.provider_payment_id == c2.provider_payment_id\n'
            '            assert c1.domain_status == "pending" and c1.confirmation_url\n'
            '            fetched = await adapter.get_payment(\n'
            '                c1.provider_payment_id,\n'
            '                expected_payment_id=pid,\n'
            '                expected_amount_kopeks=29900,\n'
            '                expected_currency="RUB",\n'
            '            )\n'
            '            assert fetched.provider_payment_id == c1.provider_payment_id\n'
            '            try:\n'
            '                await adapter.get_payment(\n'
            '                    c1.provider_payment_id,\n'
            '                    expected_payment_id=pid,\n'
            '                    expected_amount_kopeks=19900,\n'
            '                    expected_currency="RUB",\n'
            '                )\n'
            '                raise AssertionError("Expected mismatch")\n'
            '            except ProviderIntegrityMismatchError:\n'
            '                pass\n'
            '            try:\n'
            '                await adapter.get_payment(str(uuid4()))\n'
            '                raise AssertionError("Expected 404")\n'
            '            except ProviderReconciliationError as exc:\n'
            '                assert not isinstance(exc, ProviderPermanentError)\n'
            '                assert exc.status_code == 404\n'
            '            print("live-yookassa-ok")\n'
            '    else:\n'
            '        print("mock-yookassa-ok")\n'
            'asyncio.run(check())\n'
        )
        out = self._compose('exec', '-T', 'billing-api', 'python', '-c', code).strip()
        if 'live-yookassa-ok' not in out and 'mock-yookassa-ok' not in out:
            raise AssertionError(f'YooKassa container check failed: {out}')

    def _check_purchase_creation_t10(self) -> None:
        """T-10 (BIL-FR-01..03, BIL-API-03, BIL-NFR-R-06): live purchase endpoints."""
        unauth_ui = self._http('POST', '/billing/subscribe', follow_redirects=False)
        if unauth_ui.status != 303 or not (unauth_ui.header('Location') or '').endswith(
            '/billing/login'
        ):
            raise AssertionError(
                f'Expected 303 to /billing/login on unauth subscribe: {unauth_ui}'
            )

        unauth_api = self._http('POST', '/api/v1/billing/payments')
        if (
            unauth_api.status != 401
            or unauth_api.json().get('error_code') != 'unauthenticated'
        ):
            raise AssertionError(f'Expected 401 on unauth API payments: {unauth_api}')

        login_res = self._http(
            'POST',
            '/billing/login',
            data=urllib.parse.urlencode(
                {'login': 'admin', 'password': 'Cinema-Admin-2026!'}
            ).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            follow_redirects=False,
        )
        assert login_res.status == 303, login_res
        cookie_header = '; '.join([c.split(';', 1)[0] for c in login_res.set_cookies])

        pay_1 = self._http(
            'POST',
            '/api/v1/billing/payments',
            headers={'Cookie': cookie_header},
        )
        if pay_1.status == 200:
            pay_1_data = pay_1.json()
            payment_id = pay_1_data['payment_id']
            if not pay_1_data.get('confirmation_url'):
                raise AssertionError(f'Missing confirmation_url in {pay_1_data}')

            sub_redirect = self._http(
                'POST',
                '/billing/subscribe',
                headers={'Cookie': cookie_header},
                follow_redirects=False,
            )
            if (
                sub_redirect.status != 303
                or sub_redirect.header('Location') != pay_1_data['confirmation_url']
            ):
                raise AssertionError(
                    f'Expected 303 to same confirmation_url: {sub_redirect}'
                )

            self._psql(
                f"DELETE FROM operation_events WHERE operation_id = '{payment_id}';"
            )
            self._psql(f"DELETE FROM payments WHERE id = '{payment_id}';")
        elif pay_1.status == 503:
            # Running with placeholder YooKassa credentials from .env.example
            self._psql("DELETE FROM operation_events WHERE operation_type = 'payment';")
            self._psql('DELETE FROM payments;')
        else:
            raise AssertionError(
                f'Unexpected status on POST /api/v1/billing/payments: {pay_1}'
            )

    def _check_degradation_and_worker_graceful_shutdown(self) -> None:
        """BIL-API-15, GW-NFR-R-01, BIL-NFR-R-09: DB outage yields 503; worker stops."""
        self._compose('stop', 'billing-db')
        try:
            degraded = self._http('GET', '/billing/health')
            if degraded.status != 503 or degraded.json() != {
                'status': 'unavailable',
                'database': 'unavailable',
            }:
                raise AssertionError(
                    f'Expected 503 when billing-db is stopped, got {degraded}'
                )
        finally:
            self._compose('start', 'billing-db')

        self._wait_for_billing_and_auth_health()

        self._compose('stop', 'billing-worker')
        worker_logs = self._compose('logs', '--tail=30', 'billing-worker')
        if (
            'Stopping billing-worker gracefully' not in worker_logs
            or 'billing-worker stopped' not in worker_logs
        ):
            raise AssertionError(
                f'billing-worker did not log graceful shutdown:\n{worker_logs}'
            )
        self._compose('start', 'billing-worker')


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Billing live Docker integration check."""
    parser = argparse.ArgumentParser(
        description='Run live Docker Compose integration checks for Billing.'
    )
    parser.add_argument('--repository-root', type=Path, required=True)
    parser.add_argument('--project-name', type=str, required=True)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--base-url', type=str, required=True)
    parser.add_argument('--jaeger-url', type=str, required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    checker = BillingIntegrationCheck(
        repository_root=args.repository_root,
        project_name=args.project_name,
        env_file=args.env_file,
        base_url=args.base_url,
        jaeger_url=args.jaeger_url,
    )
    checker.run()
