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
        """Run live Docker Compose checks for T-06..T-11."""
        self._wait_for_billing_and_auth_health()
        self._check_one_time_migration_and_seeds()
        self._check_gateway_routing_and_internal_isolation()
        self._check_tracing_and_request_id()
        self._check_nginx_rate_limit_on_billing()
        self._check_db_invariants_in_live_postgres()
        self._check_login_logout_and_auth_degradation_t08()
        self._check_yookassa_adapter_in_container_t09()
        self._check_purchase_creation_t10()
        self._check_reconciler_atomic_outcome_t11()
        self._check_payment_result_screen_t12()
        self._check_auth_subscription_access_t14()
        self._check_immediate_auth_access_delivery_t16()
        self._check_worker_background_reconcile_t17()
        self._check_yookassa_webhook_and_emulator_t18()
        self._check_outbox_worker_t19()
        self._check_notifications_and_expiration_scheduling_t20_t21_t22()
        self._check_my_subscription_screen_and_summary_t23()
        self._check_campaign_cancel_and_preemptive_cancel_t24()
        self._check_auth_user_lookup_t25()
        self._check_degradation_and_worker_graceful_shutdown()
        print('All Billing T-06..T-25 live Docker checks passed.')

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

    def _check_reconciler_atomic_outcome_t11(self) -> None:
        """T-11 (BIL-FR-04, BIL-FR-08, BIL-FR-13, inv. 6, 7, 11): live Reconciler."""
        code = (
            'import asyncio\n'
            'from uuid import uuid4\n'
            'from sqlalchemy import delete, select\n'
            'from clients.payment_provider import (\n'
            '    ProviderIntegrityMismatchError,\n'
            '    ProviderPaymentSnapshot,\n'
            ')\n'
            'from core.config import settings\n'
            'from db.sql.postgres import PostgresDatabase\n'
            'from domain.subscription import (\n'
            '    SubscriptionState,\n'
            '    compute_subscription_state,\n'
            ')\n'
            'from models import (\n'
            '    OperationEvent,\n'
            '    OutboxTask,\n'
            '    Payment,\n'
            '    Subscription,\n'
            '    UserAccessVersion,\n'
            ')\n'
            'from repositories.purchase import SQLAlchemyPurchaseRepository\n'
            'from services.reconciler import PaymentReconciler\n'
            'class LiveStubProvider:\n'
            '    provider_code = "yookassa"\n'
            '    def __init__(self, snap, err=None):\n'
            '        self.snap = snap\n'
            '        self.err = err\n'
            '    async def create_payment(self, request):\n'
            '        raise AssertionError("Not expected")\n'
            '    async def get_payment(self, pid, **kwargs):\n'
            '        if self.err:\n'
            '            raise self.err\n'
            '        return self.snap\n'
            'async def check_t11():\n'
            '    db = PostgresDatabase(settings.postgres.dsn)\n'
            '    repo = SQLAlchemyPurchaseRepository(db.sessionmaker)\n'
            '    uid = uuid4()\n'
            '    pay, _ = await repo.get_or_create_pending_payment(uid)\n'
            '    await repo.save_provider_payment_created(\n'
            '        pay.id,\n'
            '        provider_payment_id=f"yk-t11-{pay.id.hex[:8]}",\n'
            '        confirmation_url="https://yoomoney.ru/checkout/t11",\n'
            '        provider_details={"id": "yk-t11"},\n'
            '    )\n'
            '    # 1. Unhappy path: integrity mismatch keeps status pending\n'
            '    bad_rec = PaymentReconciler(\n'
            '        repository=repo,\n'
            '        payment_provider=LiveStubProvider(\n'
            '            None, ProviderIntegrityMismatchError("bad amount")\n'
            '        ),\n'
            '    )\n'
            '    try:\n'
            '        await bad_rec.reconcile_payment(\n'
            '            pay.id, source="reconcile", respect_throttle=False\n'
            '        )\n'
            '        raise AssertionError("Expected mismatch error")\n'
            '    except ProviderIntegrityMismatchError:\n'
            '        pass\n'
            '    still_pending = await repo.get_payment(pay.id)\n'
            '    assert still_pending and still_pending.status == "pending"\n'
            '    # 2. Happy path + race: succeeded creates 1 sub, v=1, 3 tasks\n'
            '    snap = ProviderPaymentSnapshot(\n'
            '        provider_code="yookassa",\n'
            '        provider_payment_id=f"yk-t11-{pay.id.hex[:8]}",\n'
            '        domain_status="succeeded",\n'
            '        amount_kopeks=29900,\n'
            '        currency="RUB",\n'
            '        confirmation_url=None,\n'
            '        failure_reason=None,\n'
            '        metadata_payment_id=str(pay.id),\n'
            '        provider_details={"status": "succeeded"},\n'
            '    )\n'
            '    good_rec = PaymentReconciler(\n'
            '        repository=repo,\n'
            '        payment_provider=LiveStubProvider(snap),\n'
            '    )\n'
            '    r1, r2 = await asyncio.gather(\n'
            '        good_rec.reconcile_payment(\n'
            '            pay.id, source="webhook", respect_throttle=False\n'
            '        ),\n'
            '        good_rec.reconcile_payment(\n'
            '            pay.id, source="reconcile", respect_throttle=False\n'
            '        ),\n'
            '    )\n'
            '    assert {r1.transitioned, r2.transitioned} == {True, False}\n'
            '    async with db.sessionmaker() as s:\n'
            '        sub = await s.scalar(\n'
            '            select(Subscription).where(\n'
            '                Subscription.payment_id == pay.id\n'
            '            )\n'
            '        )\n'
            '        assert sub is not None\n'
            '        state = compute_subscription_state(\n'
            '            ends_at=sub.ends_at, has_succeeded_refund=False\n'
            '        )\n'
            '        assert state == SubscriptionState.ACTIVE\n'
            '        ver = await s.get(UserAccessVersion, uid)\n'
            '        assert ver is not None and ver.last_version == 1\n'
            '        tasks = list(\n'
            '            (await s.scalars(\n'
            '                select(OutboxTask).where(\n'
            '                    OutboxTask.payment_id == pay.id\n'
            '                )\n'
            '            )).all()\n'
            '        )\n'
            '        assert len(tasks) == 3\n'
            '        await s.execute(\n'
            '            delete(OutboxTask).where(OutboxTask.payment_id == pay.id)\n'
            '        )\n'
            '        await s.execute(\n'
            '            delete(Subscription).where(\n'
            '                Subscription.payment_id == pay.id\n'
            '            )\n'
            '        )\n'
            '        await s.execute(\n'
            '            delete(UserAccessVersion).where(\n'
            '                UserAccessVersion.user_id == uid\n'
            '            )\n'
            '        )\n'
            '        await s.execute(\n'
            '            delete(OperationEvent).where(\n'
            '                OperationEvent.operation_id == pay.id\n'
            '            )\n'
            '        )\n'
            '        await s.execute(delete(Payment).where(Payment.id == pay.id))\n'
            '        await s.commit()\n'
            '    await db.close()\n'
            '    print("t11-reconciler-ok")\n'
            'asyncio.run(check_t11())\n'
        )
        out = self._compose('exec', '-T', 'billing-api', 'python', '-c', code).strip()
        if 't11-reconciler-ok' not in out:
            raise AssertionError(f'T-11 Reconciler live check failed: {out}')

    def _check_payment_result_screen_t12(self) -> None:
        """T-12 (BIL-FR-04, BIL-API-04, BIL-NFR-U-01): live E3 result screen."""
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

        # Get admin user_id via billing-api container using AuthGateway
        access_cookie = next(
            c.split(';', 1)[0].split('=', 1)[1]
            for c in login_res.set_cookies
            if 'billing_access_token=' in c
        )
        uid_code = (
            'import asyncio, httpx\n'
            'from clients.auth import HttpAuthGateway\n'
            'from core.config import settings\n'
            'async def get_uid():\n'
            '    async with httpx.AsyncClient() as c:\n'
            '        url = str(settings.auth_client.service_url)\n'
            '        gw = HttpAuthGateway(http_client=c, base_url=url)\n'
            f'        token = "{access_cookie}"\n'
            '        uid, _, _ = await gw.get_current_user(token, request_id="t12")\n'
            '        print(uid)\n'
            'asyncio.run(get_uid())\n'
        )
        admin_uid = self._compose(
            'exec', '-T', 'billing-api', 'python', '-c', uid_code
        ).strip()

        pay_id = str(uuid4())
        sub_id = str(uuid4())
        task_id = str(uuid4())
        idem_id = str(uuid4())

        # 1. Insert pending payment with fake provider_payment_id (not in YooKassa)
        self._psql(
            'INSERT INTO payments '
            '(id, user_id, tariff_id, provider_code, amount_kopeks, '
            'currency, status, idempotency_key, provider_payment_id, confirmation_url) '
            'VALUES '
            f"('{pay_id}', '{admin_uid}', "
            "'00000000-0000-0000-0000-000000000001', 'yookassa', "
            f"29900, 'RUB', 'pending', '{idem_id}', 'yk-fake-never-called', "
            "'https://yoomoney.ru/checkout/fake');"
        )
        try:
            # Block 4: pending -> "Проверяем оплату…" + meta refresh
            res_pending = self._http(
                'GET',
                f'/billing/payments/{pay_id}',
                headers={'Cookie': cookie_header},
            )
            if (
                res_pending.status != 200
                or 'Проверяем оплату…' not in res_pending.text()
                or 'http-equiv="refresh"' not in res_pending.text()
            ):
                raise AssertionError(
                    f'Expected pending block on E3 screen: {res_pending}'
                )

            # T-13: /continue on fake provider_payment_id -> 503 E4 screen
            res_cont_err = self._http(
                'GET',
                f'/billing/payments/{pay_id}/continue',
                headers={'Cookie': cookie_header},
                follow_redirects=False,
            )
            if (
                res_cont_err.status != 503
                or 'Не удалось связаться с платёжной системой'
                not in res_cont_err.text()
            ):
                raise AssertionError(
                    f'Expected 503 E4 on /continue with fake id: {res_cont_err}'
                )

            # Block 2: succeeded, outbox pending -> "Оплачено, доступ активируется"
            self._psql(
                f"UPDATE payments SET status = 'succeeded' WHERE id = '{pay_id}';"
            )
            self._psql(
                'INSERT INTO subscriptions '
                '(id, user_id, tariff_id, payment_id, starts_at, ends_at) VALUES '
                f"('{sub_id}', '{admin_uid}', "
                f"'00000000-0000-0000-0000-000000000001', '{pay_id}', "
                "'2026-11-04T12:00:00Z', '2026-12-04T12:00:00Z');"
            )
            self._psql(
                'INSERT INTO outbox_tasks '
                '(id, dedup_key, target_service, task_type, payment_id, '
                'user_id, access_version, payload, status) VALUES '
                f"('{task_id}', 'payment_succeeded:{pay_id}:auth_access', "
                f"'auth', 'sync_access', '{pay_id}', '{admin_uid}', 1, "
                "'{}'::jsonb, 'pending');"
            )
            res_act = self._http(
                'GET',
                f'/billing/payments/{pay_id}',
                headers={'Cookie': cookie_header},
            )
            if (
                res_act.status != 200
                or 'Оплачено, доступ активируется' not in res_act.text()
            ):
                raise AssertionError(
                    f'Expected activating block on E3 screen: {res_act}'
                )

            # Block 1: outbox completed -> "Оплата прошла, доступ открыт до …"
            self._psql(
                f"UPDATE outbox_tasks SET status = 'completed' WHERE id = '{task_id}';"
            )
            res_ready = self._http(
                'GET',
                f'/billing/payments/{pay_id}',
                headers={'Cookie': cookie_header},
            )
            if (
                res_ready.status != 200
                or 'Оплата прошла, доступ открыт до' not in res_ready.text()
            ):
                raise AssertionError(
                    f'Expected succeeded_ready block on E3 screen: {res_ready}'
                )

            # T-13: /continue on local succeeded -> 303 redirect to result page
            res_cont_ok = self._http(
                'GET',
                f'/billing/payments/{pay_id}/continue',
                headers={'Cookie': cookie_header},
                follow_redirects=False,
            )
            if res_cont_ok.status != 303 or not (
                res_cont_ok.header('Location') or ''
            ).endswith(f'/billing/payments/{pay_id}'):
                raise AssertionError(
                    f'Expected 303 to result page on /continue: {res_cont_ok}'
                )

            # Block 3: failed -> "Оплата не прошла" + human reason + retry form
            self._psql(f"DELETE FROM outbox_tasks WHERE payment_id = '{pay_id}';")
            self._psql(f"DELETE FROM subscriptions WHERE payment_id = '{pay_id}';")
            self._psql(
                "UPDATE payments SET status = 'failed', "
                "failure_reason = 'insufficient_funds' "
                f"WHERE id = '{pay_id}';"
            )
            res_failed = self._http(
                'GET',
                f'/billing/payments/{pay_id}',
                headers={'Cookie': cookie_header},
            )
            if (
                res_failed.status != 200
                or 'Оплата не прошла' not in res_failed.text()
                or 'Недостаточно средств на карте' not in res_failed.text()
            ):
                raise AssertionError(
                    f'Expected failed block on E3 screen: {res_failed}'
                )

            # Unhappy: unknown UUID -> 404 E4 screen
            res_404 = self._http(
                'GET',
                f'/billing/payments/{uuid4()}',
                headers={'Cookie': cookie_header},
            )
            if res_404.status != 404 or 'Платёж не найден' not in res_404.text():
                raise AssertionError(f'Expected 404 E4 screen: {res_404}')
        finally:
            self._psql(f"DELETE FROM outbox_tasks WHERE payment_id = '{pay_id}';")
            self._psql(f"DELETE FROM subscriptions WHERE payment_id = '{pay_id}';")
            self._psql(f"DELETE FROM payments WHERE id = '{pay_id}';")

    def _check_auth_subscription_access_t14(self) -> None:
        """T-14 (AUTH-FR-01, AUTH-FR-02, AUTH-API-01): live Auth subscription-access."""
        # Verify auth-migrate downgrade & upgrade of 20261104_0006
        self._compose('run', '--rm', 'auth-migrate', 'alembic', 'downgrade', '-1')
        self._compose('run', '--rm', 'auth-migrate', 'alembic', 'upgrade', 'head')

        # Run live HTTP checks from billing-api to auth:8000
        code = (
            'import asyncio, httpx\n'
            'from uuid import uuid4\n'
            'from core.config import ServiceSettings, settings\n'
            'async def check_t14():\n'
            '    base = str(settings.auth_client.service_url).rstrip("/")\n'
            '    key = ServiceSettings().api_key.get_secret_value()\n'
            '    async with httpx.AsyncClient(base_url=base) as c:\n'
            '        # 1. Create test user via signup\n'
            '        login = f"SubT14_{uuid4().hex[:8]}"\n'
            '        signup = await c.post(\n'
            '            "/api/v1/auth/signup",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT14-2026!",\n'
            '                "tz": "Europe/Moscow",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t14-signup"},\n'
            '        )\n'
            '        assert signup.status_code == 201, signup.text\n'
            '        uid = signup.json()["id"]\n'
            '        tok_res = await c.post(\n'
            '            "/api/v1/auth/login",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT14-2026!",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t14-login"},\n'
            '        )\n'
            '        token = tok_res.json()["access_token"]\n'
            '        svc_headers = {\n'
            '            "X-Request-Id": "t14-svc",\n'
            '            "X-Service-Api-Key": key,\n'
            '        }\n'
            '        # 2. Unauth / wrong key -> 401\n'
            '        r_401 = await c.put(\n'
            '            f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '            json={"version": 1, "expires_at": None},\n'
            '            headers={"X-Request-Id": "t14-401"},\n'
            '        )\n'
            '        assert r_401.status_code == 401\n'
            '        # 3. Unknown user -> 404\n'
            '        r_404 = await c.put(\n'
            '            f"/api/v1/internal/users/{uuid4()}/subscription-access",\n'
            '            json={"version": 1, "expires_at": None},\n'
            '            headers=svc_headers,\n'
            '        )\n'
            '        assert r_404.status_code == 404\n'
            '        # 4. Out-of-order versions 4 ("no access"), then 1..3 -> stays 4\n'
            '        r_v4 = await c.put(\n'
            '            f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '            json={"version": 4, "expires_at": None},\n'
            '            headers=svc_headers,\n'
            '        )\n'
            '        assert r_v4.status_code == 200\n'
            '        assert r_v4.json() == {"applied": True, "current_version": 4}\n'
            '        for v in (1, 2, 3, 4):\n'
            '            r_old = await c.put(\n'
            '                f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '                json={\n'
            '                    "version": v,\n'
            '                    "expires_at": "2027-01-01T00:00:00Z",\n'
            '                },\n'
            '                headers=svc_headers,\n'
            '            )\n'
            '            assert r_old.status_code == 200\n'
            '            assert r_old.json() == {\n'
            '                "applied": False,\n'
            '                "current_version": 4,\n'
            '            }\n'
            '        # 5. Version 5 grants access -> immediate permissions/check True\n'
            '        r_v5 = await c.put(\n'
            '            f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '            json={\n'
            '                "version": 5,\n'
            '                "expires_at": "2027-01-01T00:00:00Z",\n'
            '            },\n'
            '            headers=svc_headers,\n'
            '        )\n'
            '        assert r_v5.json() == {"applied": True, "current_version": 5}\n'
            '        chk_1 = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                **svc_headers,\n'
            '                "Authorization": f"Bearer {token}",\n'
            '            },\n'
            '        )\n'
            '        assert chk_1.json() == {"allowed": True}\n'
            '        # 6. Version 6 revokes access -> permissions/check False\n'
            '        r_v6 = await c.put(\n'
            '            f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '            json={"version": 6, "expires_at": None},\n'
            '            headers=svc_headers,\n'
            '        )\n'
            '        assert r_v6.json() == {"applied": True, "current_version": 6}\n'
            '        chk_2 = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                **svc_headers,\n'
            '                "Authorization": f"Bearer {token}",\n'
            '            },\n'
            '        )\n'
            '        assert chk_2.json() == {"allowed": False}\n'
            '        # 7. T-15 (AUTH-FR-03): expires_at in 2s expires automatically\n'
            '        from datetime import UTC, datetime, timedelta\n'
            '        exp_2s = (datetime.now(UTC) + timedelta(seconds=2)).isoformat()\n'
            '        r_v7 = await c.put(\n'
            '            f"/api/v1/internal/users/{uid}/subscription-access",\n'
            '            json={"version": 7, "expires_at": exp_2s},\n'
            '            headers=svc_headers,\n'
            '        )\n'
            '        assert r_v7.json() == {"applied": True, "current_version": 7}\n'
            '        chk_before = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                **svc_headers,\n'
            '                "Authorization": f"Bearer {token}",\n'
            '            },\n'
            '        )\n'
            '        assert chk_before.json() == {"allowed": True}\n'
            '        await asyncio.sleep(2.3)\n'
            '        chk_after = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                **svc_headers,\n'
            '                "Authorization": f"Bearer {token}",\n'
            '            },\n'
            '        )\n'
            '        assert chk_after.json() == {"allowed": False}\n'
            '        print("t14-auth-ok")\n'
            'asyncio.run(check_t14())\n'
        )
        out = self._compose('exec', '-T', 'billing-api', 'python', '-c', code).strip()
        if 't14-auth-ok' not in out:
            raise AssertionError(f'T-14 Auth subscription-access check failed: {out}')

    def _check_immediate_auth_access_delivery_t16(self) -> None:
        """T-16 (BIL-FR-11, BIL-NFR-R-03): live immediate outbox delivery to Auth."""
        code = (
            'import asyncio, httpx\n'
            'from uuid import UUID, uuid4\n'
            'from sqlalchemy import delete\n'
            'from clients.auth import HttpAuthGateway\n'
            'from clients.payment_provider import ProviderPaymentSnapshot\n'
            'from core.config import ServiceSettings, settings\n'
            'from db.sql.postgres import PostgresDatabase\n'
            'from models import (\n'
            '    OperationEvent,\n'
            '    OutboxTask,\n'
            '    Payment,\n'
            '    Subscription,\n'
            '    UserAccessVersion,\n'
            ')\n'
            'from repositories.purchase import SQLAlchemyPurchaseRepository\n'
            'from services.outbox_dispatcher import OutboxDispatcher\n'
            'from services.reconciler import PaymentReconciler\n'
            'class StubProv:\n'
            '    provider_code = "yookassa"\n'
            '    def __init__(self, pid):\n'
            '        self.pid = pid\n'
            '    async def create_payment(self, request):\n'
            '        raise AssertionError("Not expected")\n'
            '    async def get_payment(self, provider_payment_id, **kwargs):\n'
            '        return ProviderPaymentSnapshot(\n'
            '            provider_code="yookassa",\n'
            '            provider_payment_id=provider_payment_id,\n'
            '            domain_status="succeeded",\n'
            '            amount_kopeks=29900,\n'
            '            currency="RUB",\n'
            '            confirmation_url=None,\n'
            '            failure_reason=None,\n'
            '            metadata_payment_id=str(self.pid),\n'
            '            provider_details={"status": "succeeded"},\n'
            '        )\n'
            'async def check_t16():\n'
            '    base = str(settings.auth_client.service_url).rstrip("/")\n'
            '    key = ServiceSettings().api_key.get_secret_value()\n'
            '    db = PostgresDatabase(settings.postgres.dsn)\n'
            '    repo = SQLAlchemyPurchaseRepository(db.sessionmaker)\n'
            '    async with httpx.AsyncClient(base_url=base) as c:\n'
            '        login = f"SubT16_{uuid4().hex[:8]}"\n'
            '        signup = await c.post(\n'
            '            "/api/v1/auth/signup",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT16-2026!",\n'
            '                "tz": "Europe/Moscow",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t16-signup"},\n'
            '        )\n'
            '        uid = UUID(signup.json()["id"])\n'
            '        tok = (await c.post(\n'
            '            "/api/v1/auth/login",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT16-2026!",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t16-login"},\n'
            '        )).json()["access_token"]\n'
            '        pay, _ = await repo.get_or_create_pending_payment(uid)\n'
            '        await repo.save_provider_payment_created(\n'
            '            pay.id,\n'
            '            provider_payment_id=f"yk-t16-{pay.id.hex[:8]}",\n'
            '            confirmation_url="https://yoomoney.ru/checkout/t16",\n'
            '            provider_details={"id": "yk-t16"},\n'
            '        )\n'
            '        gw = HttpAuthGateway(\n'
            '            http_client=c, base_url=base, service_api_key=key\n'
            '        )\n'
            '        disp = OutboxDispatcher(repository=repo, auth_gateway=gw)\n'
            '        rec = PaymentReconciler(\n'
            '            repository=repo,\n'
            '            payment_provider=StubProv(pay.id),\n'
            '            outbox_dispatcher=disp,\n'
            '        )\n'
            '        out = await rec.reconcile_payment(\n'
            '            pay.id, source="webhook", respect_throttle=False\n'
            '        )\n'
            '        assert out.status == "succeeded" and out.transitioned\n'
            '        res_loc = await repo.get_local_payment_result_for_user(\n'
            '            pay.id, uid\n'
            '        )\n'
            '        assert res_loc is not None and res_loc.auth_access_delivered\n'
            '        chk = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                "X-Request-Id": "t16-chk",\n'
            '                "X-Service-Api-Key": key,\n'
            '                "Authorization": f"Bearer {tok}",\n'
            '            },\n'
            '        )\n'
            '        assert chk.json() == {"allowed": True}\n'
            '        async with db.sessionmaker() as s:\n'
            '            await s.execute(\n'
            '                delete(OutboxTask).where(\n'
            '                    OutboxTask.payment_id == pay.id\n'
            '                )\n'
            '            )\n'
            '            await s.execute(\n'
            '                delete(Subscription).where(\n'
            '                    Subscription.payment_id == pay.id\n'
            '                )\n'
            '            )\n'
            '            await s.execute(\n'
            '                delete(UserAccessVersion).where(\n'
            '                    UserAccessVersion.user_id == uid\n'
            '                )\n'
            '            )\n'
            '            await s.execute(\n'
            '                delete(OperationEvent).where(\n'
            '                    OperationEvent.operation_id == pay.id\n'
            '                )\n'
            '            )\n'
            '            await s.execute(delete(Payment).where(Payment.id == pay.id))\n'
            '            await s.commit()\n'
            '    await db.close()\n'
            '    print("t16-immediate-delivery-ok")\n'
            'asyncio.run(check_t16())\n'
        )
        out = self._compose('exec', '-T', 'billing-api', 'python', '-c', code).strip()
        if 't16-immediate-delivery-ok' not in out:
            raise AssertionError(f'T-16 immediate Auth delivery failed: {out}')

    def _check_worker_background_reconcile_t17(self) -> None:
        """T-17 (BIL-FR-05, inv. 10, 13, 15, 16): live worker background reconcile."""
        code = (
            'import asyncio\n'
            'from datetime import UTC, datetime, timedelta\n'
            'from uuid import uuid4\n'
            'from sqlalchemy import delete\n'
            'from clients.payment_provider import ProviderPaymentSnapshot\n'
            'from core.config import settings\n'
            'from db.sql.postgres import PostgresDatabase\n'
            'from models import (\n'
            '    DEFAULT_PROVIDER_CODE,\n'
            '    DEFAULT_TARIFF_ID,\n'
            '    DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '    OperationEvent,\n'
            '    OutboxTask,\n'
            '    Payment,\n'
            ')\n'
            'from repositories.purchase import SQLAlchemyPurchaseRepository\n'
            'from services.reconciler import PaymentReconciler\n'
            'class StubProvT17:\n'
            '    provider_code = "yookassa"\n'
            '    def __init__(self):\n'
            '        self.created = []\n'
            '    async def create_payment(self, req):\n'
            '        self.created.append(req.payment_id)\n'
            '        return ProviderPaymentSnapshot(\n'
            '            provider_code="yookassa",\n'
            '            provider_payment_id=f"yk-t17-{req.payment_id.hex[:8]}",\n'
            '            domain_status="pending",\n'
            '            amount_kopeks=req.amount_kopeks,\n'
            '            currency=req.currency,\n'
            '            confirmation_url="https://yoomoney.ru/checkout/t17",\n'
            '            failure_reason=None,\n'
            '            metadata_payment_id=str(req.payment_id),\n'
            '            provider_details={"status": "pending"},\n'
            '        )\n'
            '    async def get_payment(self, pid, **kwargs):\n'
            '        return ProviderPaymentSnapshot(\n'
            '            provider_code="yookassa",\n'
            '            provider_payment_id=pid,\n'
            '            domain_status="pending",\n'
            '            amount_kopeks=29900,\n'
            '            currency="RUB",\n'
            '            confirmation_url="https://yoomoney.ru/checkout/t17",\n'
            '            failure_reason=None,\n'
            '            metadata_payment_id=str(kwargs.get("expected_payment_id")),\n'
            '            provider_details={"status": "pending"},\n'
            '        )\n'
            'async def check_t17():\n'
            '    db = PostgresDatabase(settings.postgres.dsn)\n'
            '    repo = SQLAlchemyPurchaseRepository(db.sessionmaker)\n'
            '    now = datetime.now(UTC)\n'
            '    p_exp = uuid4()\n'
            '    p_rec = uuid4()\n'
            '    p_yng = uuid4()\n'
            '    async with db.sessionmaker() as s:\n'
            '        s.add_all([\n'
            '            Payment(\n'
            '                id=p_exp,\n'
            '                user_id=uuid4(),\n'
            '                tariff_id=DEFAULT_TARIFF_ID,\n'
            '                provider_code=DEFAULT_PROVIDER_CODE,\n'
            '                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '                status="pending",\n'
            '                idempotency_key=p_exp,\n'
            '                created_at=now - timedelta(hours=25),\n'
            '            ),\n'
            '            Payment(\n'
            '                id=p_rec,\n'
            '                user_id=uuid4(),\n'
            '                tariff_id=DEFAULT_TARIFF_ID,\n'
            '                provider_code=DEFAULT_PROVIDER_CODE,\n'
            '                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '                status="pending",\n'
            '                idempotency_key=p_rec,\n'
            '                created_at=now - timedelta(minutes=10),\n'
            '            ),\n'
            '            Payment(\n'
            '                id=p_yng,\n'
            '                user_id=uuid4(),\n'
            '                tariff_id=DEFAULT_TARIFF_ID,\n'
            '                provider_code=DEFAULT_PROVIDER_CODE,\n'
            '                amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '                status="pending",\n'
            '                idempotency_key=p_yng,\n'
            '                created_at=now,\n'
            '            ),\n'
            '        ])\n'
            '        await s.commit()\n'
            '    prov = StubProvT17()\n'
            '    rec = PaymentReconciler(repository=repo, payment_provider=prov)\n'
            '    await rec.reconcile_due_pending_payments(min_age_seconds=5.0)\n'
            '    exp_row = await repo.get_payment(p_exp)\n'
            '    rec_row = await repo.get_payment(p_rec)\n'
            '    yng_row = await repo.get_payment(p_yng)\n'
            '    assert exp_row and exp_row.status == "failed"\n'
            '    assert exp_row.failure_reason == "idempotency_key_expired"\n'
            '    assert rec_row and rec_row.provider_payment_id is not None\n'
            '    assert yng_row and yng_row.provider_payment_id is None\n'
            '    async with db.sessionmaker() as s:\n'
            '        for pid in (p_exp, p_rec, p_yng):\n'
            '            await s.execute(\n'
            '                delete(OutboxTask).where(OutboxTask.payment_id == pid)\n'
            '            )\n'
            '            await s.execute(\n'
            '                delete(OperationEvent).where(\n'
            '                    OperationEvent.operation_id == pid\n'
            '                )\n'
            '            )\n'
            '            await s.execute(delete(Payment).where(Payment.id == pid))\n'
            '        await s.commit()\n'
            '    await db.close()\n'
            '    print("t17-worker-reconcile-ok")\n'
            'asyncio.run(check_t17())\n'
        )
        out = self._compose(
            'exec', '-T', 'billing-worker', 'python', '-c', code
        ).strip()
        if 't17-worker-reconcile-ok' not in out:
            raise AssertionError(f'T-17 worker background reconcile failed: {out}')

    def _check_yookassa_webhook_and_emulator_t18(self) -> None:
        """T-18 (BIL-FR-07, BIL-API-05, BIL-NFR-SEC-04): live webhook & emulator."""
        # 1. Invalid webhook body -> 422
        bad_wh = self._http(
            'POST',
            '/api/v1/billing/webhooks/yookassa',
            data=json.dumps({'object': {'id': '   '}}).encode(),
            headers={'Content-Type': 'application/json'},
        )
        if bad_wh.status != 422:
            raise AssertionError(f'Expected 422 on blank object.id: {bad_wh}')

        # 2. Unknown provider_payment_id -> 200 OK
        unk_wh = self._http(
            'POST',
            '/api/v1/billing/webhooks/yookassa',
            data=json.dumps(
                {
                    'type': 'notification',
                    'event': 'payment.succeeded',
                    'object': {'id': f'yk-unk-{uuid4().hex[:8]}'},
                }
            ).encode(),
            headers={'Content-Type': 'application/json'},
        )
        if unk_wh.status != 200:
            raise AssertionError(f'Expected 200 on unknown webhook id: {unk_wh}')

        # 3. Login & create real pending payment in YooKassa test shop, then send
        # forged webhook claiming status='succeeded' via local emulator CLI
        login_res = self._http(
            'POST',
            '/billing/login',
            data=urllib.parse.urlencode(
                {'login': 'admin', 'password': 'Cinema-Admin-2026!'}
            ).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            follow_redirects=False,
        )
        cookie_header = '; '.join([c.split(';', 1)[0] for c in login_res.set_cookies])
        pay_res = self._http(
            'POST',
            '/api/v1/billing/payments',
            headers={'Cookie': cookie_header},
        )
        if pay_res.status == 200:
            pay_id = pay_res.json()['payment_id']
            yk_id = self._psql(
                f"SELECT provider_payment_id FROM payments WHERE id = '{pay_id}';"
            ).stdout.strip()
            try:
                # Run local webhook emulator inside billing-api against http://127.0.0.1:8000
                emu_out = self._compose(
                    'exec',
                    '-T',
                    'billing-api',
                    'python',
                    '-m',
                    'emulate_webhook',
                    '--id',
                    yk_id,
                    '--event',
                    'payment.succeeded',
                    '--base-url',
                    'http://127.0.0.1:8000',
                ).strip()
                if 'HTTP 200' not in emu_out:
                    raise AssertionError(f'Webhook emulator failed: {emu_out}')

                # Forged webhook claimed succeeded, GET returned pending -> pending
                db_state = self._psql(
                    f"SELECT status || '|' || (last_checked_at IS NOT NULL)::text "
                    f"FROM payments WHERE id = '{pay_id}';"
                ).stdout.strip()
                if db_state != 'pending|true':
                    raise AssertionError(
                        f'Forged webhook corrupted DB or skipped GET check: {db_state}'
                    )
            finally:
                self._psql(
                    f"DELETE FROM operation_events WHERE operation_id = '{pay_id}';"
                )
                self._psql(f"DELETE FROM payments WHERE id = '{pay_id}';")
        else:
            self._psql("DELETE FROM operation_events WHERE operation_type = 'payment';")
            self._psql('DELETE FROM payments;')

    def _check_outbox_worker_t19(self) -> None:
        """T-19 (BIL-FR-11, BIL-FR-14, BIL-NFR-R-04, BIL-NFR-S-01): outbox worker."""
        code = (
            'import asyncio, httpx\n'
            'from datetime import UTC, datetime, timedelta\n'
            'from uuid import UUID, uuid4\n'
            'from sqlalchemy import delete\n'
            'from core.config import ServiceSettings, settings\n'
            'from db.sql.postgres import PostgresDatabase\n'
            'from models import (\n'
            '    DEFAULT_PROVIDER_CODE,\n'
            '    DEFAULT_TARIFF_ID,\n'
            '    DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '    OutboxTask,\n'
            '    Payment,\n'
            ')\n'
            'async def check_t19():\n'
            '    base = str(settings.auth_client.service_url).rstrip("/")\n'
            '    key = ServiceSettings().api_key.get_secret_value()\n'
            '    db = PostgresDatabase(settings.postgres.dsn)\n'
            '    now = datetime.now(UTC)\n'
            '    ends = now + timedelta(days=30)\n'
            '    p_ok = uuid4()\n'
            '    p_404 = uuid4()\n'
            '    t_ok = uuid4()\n'
            '    t_stale = uuid4()\n'
            '    t_404 = uuid4()\n'
            '    async with httpx.AsyncClient(base_url=base) as c:\n'
            '        login = f"SubT19_{uuid4().hex[:8]}"\n'
            '        signup = await c.post(\n'
            '            "/api/v1/auth/signup",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT19-2026!",\n'
            '                "tz": "Europe/Moscow",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t19-signup"},\n'
            '        )\n'
            '        uid = UUID(signup.json()["id"])\n'
            '        tok = (await c.post(\n'
            '            "/api/v1/auth/login",\n'
            '            json={\n'
            '                "login": login,\n'
            '                "password": "Saffron-Citadel-SubT19-2026!",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t19-login"},\n'
            '        )).json()["access_token"]\n'
            '        async with db.sessionmaker() as s:\n'
            '            s.add_all([\n'
            '                Payment(\n'
            '                    id=p_ok,\n'
            '                    user_id=uid,\n'
            '                    tariff_id=DEFAULT_TARIFF_ID,\n'
            '                    provider_code=DEFAULT_PROVIDER_CODE,\n'
            '                    amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '                    status="succeeded",\n'
            '                    idempotency_key=p_ok,\n'
            '                ),\n'
            '                Payment(\n'
            '                    id=p_404,\n'
            '                    user_id=uuid4(),\n'
            '                    tariff_id=DEFAULT_TARIFF_ID,\n'
            '                    provider_code=DEFAULT_PROVIDER_CODE,\n'
            '                    amount_kopeks=DEFAULT_TARIFF_PRICE_KOPEKS,\n'
            '                    status="succeeded",\n'
            '                    idempotency_key=p_404,\n'
            '                ),\n'
            '            ])\n'
            '            await s.flush()\n'
            '            s.add_all([\n'
            '                OutboxTask(\n'
            '                    id=t_ok,\n'
            '                    dedup_key=f"t19_ok:{p_ok}",\n'
            '                    target_service="auth",\n'
            '                    task_type="sync_access",\n'
            '                    payment_id=p_ok,\n'
            '                    user_id=uid,\n'
            '                    access_version=2,\n'
            '                    payload={\n'
            '                        "user_id": str(uid),\n'
            '                        "version": 2,\n'
            '                        "expires_at": ends.isoformat(),\n'
            '                    },\n'
            '                    status="pending",\n'
            '                    next_retry_at=now - timedelta(seconds=1),\n'
            '                ),\n'
            '                OutboxTask(\n'
            '                    id=t_stale,\n'
            '                    dedup_key=f"t19_stale:{p_ok}",\n'
            '                    target_service="auth",\n'
            '                    task_type="sync_access",\n'
            '                    payment_id=p_ok,\n'
            '                    user_id=uid,\n'
            '                    access_version=1,\n'
            '                    payload={\n'
            '                        "user_id": str(uid),\n'
            '                        "version": 1,\n'
            '                        "expires_at": None,\n'
            '                    },\n'
            '                    status="pending",\n'
            '                    last_error="old error",\n'
            '                    next_retry_at=now + timedelta(seconds=1),\n'
            '                ),\n'
            '                OutboxTask(\n'
            '                    id=t_404,\n'
            '                    dedup_key=f"t19_404:{p_404}",\n'
            '                    target_service="auth",\n'
            '                    task_type="sync_access",\n'
            '                    payment_id=p_404,\n'
            '                    user_id=uuid4(),\n'
            '                    access_version=1,\n'
            '                    payload={"version": 1, "expires_at": None},\n'
            '                    status="pending",\n'
            '                    next_retry_at=now - timedelta(seconds=1),\n'
            '                ),\n'
            '            ])\n'
            '            await s.commit()\n'
            '        # Wait for live billing-worker container loop to process tasks\n'
            '        for _ in range(25):\n'
            '            await asyncio.sleep(0.4)\n'
            '            async with db.sessionmaker() as s:\n'
            '                r_ok = await s.get(OutboxTask, t_ok)\n'
            '                r_stale = await s.get(OutboxTask, t_stale)\n'
            '                r_404 = await s.get(OutboxTask, t_404)\n'
            '                if (\n'
            '                    r_ok and r_ok.status == "completed"\n'
            '                    and r_stale and r_stale.status == "completed"\n'
            '                    and r_404\n'
            '                    and r_404.status == "requires_intervention"\n'
            '                ):\n'
            '                    assert r_stale.last_error is None\n'
            '                    break\n'
            '        else:\n'
            '            raise AssertionError("t19 tasks not processed")\n'
            '        chk = await c.post(\n'
            '            "/api/v1/auth/permissions/check",\n'
            '            json={\n'
            '                "resource": "film",\n'
            '                "action": "read",\n'
            '                "attributes": {"creation_date": "9999-01-01"},\n'
            '            },\n'
            '            headers={\n'
            '                "X-Request-Id": "t19-chk",\n'
            '                "X-Service-Api-Key": key,\n'
            '                "Authorization": f"Bearer {tok}",\n'
            '            },\n'
            '        )\n'
            '        assert chk.json() == {"allowed": True}\n'
            '        async with db.sessionmaker() as s:\n'
            '            for pid in (p_ok, p_404):\n'
            '                await s.execute(\n'
            '                    delete(OutboxTask).where(\n'
            '                        OutboxTask.payment_id == pid\n'
            '                    )\n'
            '                )\n'
            '                await s.execute(\n'
            '                    delete(Payment).where(Payment.id == pid)\n'
            '                )\n'
            '            await s.commit()\n'
            '    await db.close()\n'
            '    print("t19-outbox-worker-ok")\n'
            'asyncio.run(check_t19())\n'
        )
        out = self._compose(
            'exec', '-T', 'billing-worker', 'python', '-c', code
        ).strip()
        if 't19-outbox-worker-ok' not in out:
            raise AssertionError(f'T-19 outbox worker live check failed: {out}')

    def _ntf_psql(self, sql_query: str) -> subprocess.CompletedProcess[str]:
        return self._compose_result(
            'exec',
            '-T',
            'notifications-db',
            'sh',
            '-c',
            f'psql -v ON_ERROR_STOP=1 -tA -U "$POSTGRES_USER" '
            f'-d "$POSTGRES_DB" -c "{sql_query}"',
        )

    def _check_notifications_and_expiration_scheduling_t20_t21_t22(self) -> None:
        """T-20, T-21, T-22 (NTF-FR-01, NTF-API-01, US-S-03): live campaign delivery."""
        code = (
            'import asyncio, httpx\n'
            'from uuid import UUID, uuid4\n'
            'from core.config import settings\n'
            'async def check_t22():\n'
            '    auth_url = str(settings.auth_client.service_url).rstrip("/")\n'
            '    async with httpx.AsyncClient() as hc:\n'
            '        signup = await hc.post(\n'
            '            f"{auth_url}/api/v1/auth/signup",\n'
            '            json={\n'
            '                "login": f"SubT22_{uuid4().hex[:8]}",\n'
            '                "password": "Saffron-Citadel-SubT22-2026!",\n'
            '                "tz": "Europe/Moscow",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t22-signup"},\n'
            '        )\n'
            '        real_uid = UUID(signup.json()["id"])\n'
            '    print(f"real_uid={real_uid}")\n'
            'asyncio.run(check_t22())\n'
        )
        uid_out = self._compose(
            'exec', '-T', 'billing-api', 'python', '-c', code
        ).strip()
        real_uid = next(
            line.split('=', 1)[1]
            for line in uid_out.splitlines()
            if line.startswith('real_uid=')
        )
        ins_real = self._ntf_psql(
            f'INSERT INTO profiles '
            f'(user_id, user_version, tz, likes, new_films, marketing) '
            f"VALUES ('{real_uid}', 1, 'Europe/Moscow', false, false, false) "
            f'ON CONFLICT (user_id) DO NOTHING;'
        )
        assert ins_real.returncode == 0, ins_real.stderr

        run_code = (
            'import asyncio, httpx\n'
            'from uuid import UUID\n'
            'from sqlalchemy import delete, select\n'
            'from clients.auth import HttpAuthGateway\n'
            'from clients.notifications import HttpNotificationsGateway\n'
            'from clients.payment_provider import ProviderPaymentSnapshot\n'
            'from core.config import ServiceSettings, settings\n'
            'from db.sql.postgres import PostgresDatabase\n'
            'from models import (\n'
            '    OperationEvent,\n'
            '    OutboxTask,\n'
            '    Payment,\n'
            '    Subscription,\n'
            '    UserAccessVersion,\n'
            ')\n'
            'from repositories.purchase import SQLAlchemyPurchaseRepository\n'
            'from services.outbox_dispatcher import OutboxDispatcher\n'
            'from services.reconciler import PaymentReconciler\n'
            'class StubProvT22:\n'
            '    provider_code = "yookassa"\n'
            '    def __init__(self, status, reason=None):\n'
            '        self.status = status\n'
            '        self.reason = reason\n'
            '    async def create_payment(self, req):\n'
            '        raise NotImplementedError\n'
            '    async def get_payment(self, pid, **kwargs):\n'
            '        return ProviderPaymentSnapshot(\n'
            '            provider_code="yookassa",\n'
            '            provider_payment_id=pid,\n'
            '            domain_status=self.status,\n'
            '            amount_kopeks=29900,\n'
            '            currency="RUB",\n'
            '            confirmation_url=None,\n'
            '            failure_reason=self.reason,\n'
            '            metadata_payment_id=str(kwargs.get("expected_payment_id")),\n'
            '            provider_details={"status": self.status},\n'
            '        )\n'
            'async def run_t22():\n'
            '    auth_url = str(settings.auth_client.service_url).rstrip("/")\n'
            '    ntf_cfg = settings.notifications_client\n'
            '    ntf_url = str(ntf_cfg.campaign_url).rstrip("/")\n'
            '    ntf_key = ntf_cfg.campaign_api_key.get_secret_value()\n'
            '    svc_key = ServiceSettings().api_key.get_secret_value()\n'
            '    db = PostgresDatabase(settings.postgres.dsn)\n'
            '    repo = SQLAlchemyPurchaseRepository(db.sessionmaker)\n'
            f'    uid = UUID("{real_uid}")\n'
            '    async with httpx.AsyncClient() as hc:\n'
            '        auth_gw = HttpAuthGateway(\n'
            '            http_client=hc, base_url=auth_url, service_api_key=svc_key\n'
            '        )\n'
            '        ntf_gw = HttpNotificationsGateway(\n'
            '            http_client=hc, base_url=ntf_url, api_key=ntf_key\n'
            '        )\n'
            '        disp = OutboxDispatcher(\n'
            '            repository=repo,\n'
            '            auth_gateway=auth_gw,\n'
            '            notifications_gateway=ntf_gw,\n'
            '        )\n'
            '        # 1. Failed payment -> payment_failed_email delivered\n'
            '        p_fail, _ = await repo.get_or_create_pending_payment(uid)\n'
            '        await repo.save_provider_payment_created(\n'
            '            p_fail.id,\n'
            '            provider_payment_id=f"yk-t22-f-{p_fail.id.hex[:8]}",\n'
            '            confirmation_url="https://yoomoney.ru/checkout/t22f",\n'
            '            provider_details={"id": "yk-t22-f"},\n'
            '        )\n'
            '        rec_f = PaymentReconciler(\n'
            '            repository=repo,\n'
            '            payment_provider=StubProvT22(\n'
            '                "failed", "insufficient_funds"\n'
            '            ),\n'
            '            outbox_dispatcher=disp,\n'
            '        )\n'
            '        await rec_f.reconcile_payment(\n'
            '            p_fail.id, source="webhook", respect_throttle=False\n'
            '        )\n'
            '        # 2. Succeeded -> sync_access + paid + expiration emails\n'
            '        p_ok, _ = await repo.get_or_create_pending_payment(uid)\n'
            '        await repo.save_provider_payment_created(\n'
            '            p_ok.id,\n'
            '            provider_payment_id=f"yk-t22-s-{p_ok.id.hex[:8]}",\n'
            '            confirmation_url="https://yoomoney.ru/checkout/t22s",\n'
            '            provider_details={"id": "yk-t22-s"},\n'
            '        )\n'
            '        rec_s = PaymentReconciler(\n'
            '            repository=repo,\n'
            '            payment_provider=StubProvT22("succeeded"),\n'
            '            outbox_dispatcher=disp,\n'
            '        )\n'
            '        await rec_s.reconcile_payment(\n'
            '            p_ok.id, source="webhook", respect_throttle=False\n'
            '        )\n'
            '        async with db.sessionmaker() as s:\n'
            '            tasks = list((\n'
            '                await s.scalars(\n'
            '                    select(OutboxTask).where(\n'
            '                        OutboxTask.user_id == uid\n'
            '                    )\n'
            '                )\n'
            '            ).all())\n'
            '            assert len(tasks) == 4\n'
            '            assert all(t.status == "completed" for t in tasks)\n'
            '            for pid in (p_fail.id, p_ok.id):\n'
            '                await s.execute(\n'
            '                    delete(OutboxTask).where(\n'
            '                        OutboxTask.payment_id == pid\n'
            '                    )\n'
            '                )\n'
            '                await s.execute(\n'
            '                    delete(Subscription).where(\n'
            '                        Subscription.payment_id == pid\n'
            '                    )\n'
            '                )\n'
            '                await s.execute(\n'
            '                    delete(OperationEvent).where(\n'
            '                        OperationEvent.operation_id == pid\n'
            '                    )\n'
            '                )\n'
            '                await s.execute(\n'
            '                    delete(Payment).where(Payment.id == pid)\n'
            '                )\n'
            '            await s.execute(\n'
            '                delete(UserAccessVersion).where(\n'
            '                    UserAccessVersion.user_id == uid\n'
            '                )\n'
            '            )\n'
            '            await s.commit()\n'
            '    await db.close()\n'
            '    print("t22-live-notifications-ok")\n'
            'asyncio.run(run_t22())\n'
        )
        run_out = self._compose(
            'exec', '-T', 'billing-api', 'python', '-c', run_code
        ).strip()
        if 't22-live-notifications-ok' not in run_out:
            raise AssertionError(f'T-20/T-21/T-22 live check failed: {run_out}')

        ntf_rows = self._ntf_psql(
            f'SELECT count(*) FROM campaign_recipients r '
            f'JOIN deliveries d ON d.delivery_id = r.delivery_id '
            f"WHERE r.user_id = '{real_uid}' "
            f"AND r.template_id = 'billing_notification' "
            f'AND r.skip_reason IS NULL '
            f"AND d.send_class = 'transactional' "
            f'AND d.required_subscription IS NULL;'
        ).stdout.strip()
        if ntf_rows != '3':
            raise AssertionError(f'Expected 3 transactional deliveries, got {ntf_rows}')

    def _check_my_subscription_screen_and_summary_t23(self) -> None:
        """T-23 (US-U-05, BIL-FR-09, BIL-API-02): live /billing/ and /me/summary."""
        # 1. Unauthenticated browser request redirects to /billing/login
        unauth = self._http('GET', '/billing/', follow_redirects=False)
        if unauth.status != 303 or '/billing/login' not in (
            unauth.header('location') or ''
        ):
            raise AssertionError(f'Expected 303 redirect to login, got {unauth}')

        unauth_api = self._http(
            'GET',
            '/api/v1/billing/me/summary',
            headers={'X-Request-Id': 't23-unauth'},
        )
        if unauth_api.status != 401:
            raise AssertionError(f'Expected 401 on /me/summary, got {unauth_api}')

        # 2. Sign up and log in user via /billing/login
        login = f'SubT23_{uuid4().hex[:8]}'
        password = 'Saffron-Citadel-SubT23-2026!'
        signup_code = (
            'import asyncio, httpx\n'
            'from core.config import settings\n'
            'async def s():\n'
            '    base = str(settings.auth_client.service_url).rstrip("/")\n'
            '    async with httpx.AsyncClient() as c:\n'
            '        r = await c.post(\n'
            '            f"{base}/api/v1/auth/signup",\n'
            '            json={\n'
            f'                "login": "{login}",\n'
            f'                "password": "{password}",\n'
            '                "tz": "Europe/Moscow",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t23-signup"},\n'
            '        )\n'
            '        print("uid=" + r.json()["id"])\n'
            'asyncio.run(s())\n'
        )
        out = self._compose(
            'exec', '-T', 'billing-api', 'python', '-c', signup_code
        ).strip()
        uid = next(
            line.split('=', 1)[1]
            for line in out.splitlines()
            if line.startswith('uid=')
        )

        form_data = urllib.parse.urlencode(
            {'login': login, 'password': password, 'next': '/billing/'}
        ).encode('utf-8')
        login_res = self._http(
            'POST',
            '/billing/login',
            data=form_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            follow_redirects=False,
        )
        assert login_res.status == 303, login_res.text()
        cookie_header = '; '.join(
            c.split(';', 1)[0] for c in login_res.set_cookies if '=' in c
        )

        # Rule 7: No subscription yet
        r7_html = self._http(
            'GET', '/billing/', headers={'Cookie': cookie_header}
        ).text()
        if 'Нет подписки' not in r7_html or 'Оформить за 299 ₽' not in r7_html:
            raise AssertionError(f'Expected Rule 7 on /billing/, got {r7_html}')

        r7_json = self._http(
            'GET',
            '/api/v1/billing/me/summary',
            headers={'Cookie': cookie_header, 'X-Request-Id': 't23-r7'},
        ).json()
        assert r7_json['rule_number'] == 7
        assert r7_json['status_display'] == 'Нет подписки'
        assert r7_json['can_subscribe'] is True

        # Insert active subscription + completed auth outbox task -> Rule 3
        pid = str(uuid4())
        sid = str(uuid4())
        tid = str(uuid4())
        sql = (
            f'INSERT INTO payments (id, user_id, tariff_id, provider_code, '
            f'amount_kopeks, currency, status, idempotency_key) VALUES '
            f"('{pid}', '{uid}', '00000000-0000-0000-0000-000000000001', "
            f"'yookassa', 29900, 'RUB', 'succeeded', '{pid}'); "
            f'INSERT INTO subscriptions (id, user_id, tariff_id, payment_id, '
            f'starts_at, ends_at) VALUES '
            f"('{sid}', '{uid}', '00000000-0000-0000-0000-000000000001', "
            f"'{pid}', now(), now() + interval '30 days'); "
            f'INSERT INTO outbox_tasks (id, dedup_key, target_service, task_type, '
            f'payment_id, user_id, access_version, payload, status) VALUES '
            f"('{tid}', 't23:{pid}', 'auth', 'sync_access', '{pid}', '{uid}', "
            f"1, '{{}}'::jsonb, 'completed');"
        )
        assert self._psql(sql).returncode == 0

        r3_html = self._http(
            'GET', '/billing/', headers={'Cookie': cookie_header}
        ).text()
        if 'Активна до ' not in r3_html or pid not in r3_html:
            raise AssertionError(
                f'Expected Rule 3 and payment in history, got {r3_html}'
            )

        r3_json = self._http(
            'GET',
            '/api/v1/billing/me/summary',
            headers={'Cookie': cookie_header, 'X-Request-Id': 't23-r3'},
        ).json()
        assert r3_json['rule_number'] == 3
        assert r3_json['can_subscribe'] is False
        assert len(r3_json['payments']) == 1
        assert r3_json['payments'][0]['payment_id'] == pid

        # Unhappy path: POST /billing/subscribe while Rule 3 active -> 409 error screen
        sub_409 = self._http(
            'POST',
            '/billing/subscribe',
            headers={'Cookie': cookie_header},
            follow_redirects=False,
        )
        if sub_409.status != 409 or 'Подписка уже активна' not in sub_409.text():
            raise AssertionError(
                f'Expected 409 error page on subscribe while active, got {sub_409}'
            )

        # Unhappy path: Auth outage on /billing/ and /me/summary -> 503 without leak
        self._compose('stop', 'auth')
        try:
            auth_down_ui = self._http(
                'GET',
                '/billing/',
                headers={'Cookie': cookie_header},
                follow_redirects=False,
            )
            if (
                auth_down_ui.status != 503
                or 'Сервис авторизации временно недоступен' not in auth_down_ui.text()
                or pid in auth_down_ui.text()
            ):
                raise AssertionError(
                    f'Expected 503 without data leak on /billing/, got {auth_down_ui}'
                )

            auth_down_json = self._http(
                'GET',
                '/api/v1/billing/me/summary',
                headers={'Cookie': cookie_header, 'X-Request-Id': 't23-auth-down'},
            )
            if (
                auth_down_json.status != 503
                or auth_down_json.json().get('error_code') != 'auth_unavailable'
            ):
                raise AssertionError(
                    f'Expected 503 on /me/summary, got {auth_down_json}'
                )
        finally:
            self._compose('start', 'auth')
            self._wait_for_billing_and_auth_health()

        # Clean up test records
        cleanup_sql = (
            f"DELETE FROM outbox_tasks WHERE payment_id = '{pid}'; "
            f"DELETE FROM subscriptions WHERE payment_id = '{pid}'; "
            f"DELETE FROM payments WHERE id = '{pid}';"
        )
        assert self._psql(cleanup_sql).returncode == 0

    def _check_campaign_cancel_and_preemptive_cancel_t24(self) -> None:
        """T-24 (NTF-FR-02, NTF-API-02, инв. 12): cancel existing & pre-emptive."""
        uid = str(uuid4())
        ins_res = self._ntf_psql(
            f'INSERT INTO profiles '
            f'(user_id, user_version, tz, likes, new_films, marketing) '
            f"VALUES ('{uid}', 1, 'Europe/Moscow', false, false, false) "
            f'ON CONFLICT (user_id) DO NOTHING;'
        )
        assert ins_res.returncode == 0, ins_res.stderr

        code = (
            'import asyncio, httpx\n'
            'from uuid import UUID, uuid4\n'
            'from clients.notifications import (\n'
            '    HttpNotificationsGateway,\n'
            '    NotificationsPermanentError,\n'
            ')\n'
            'from core.config import settings\n'
            'async def check_t24():\n'
            '    cfg = settings.notifications_client\n'
            '    url = str(cfg.campaign_url).rstrip("/")\n'
            '    key = cfg.campaign_api_key.get_secret_value()\n'
            f'    uid = UUID("{uid}")\n'
            '    c_after = uuid4()\n'
            '    b_after = uuid4()\n'
            '    c_before = uuid4()\n'
            '    b_before = uuid4()\n'
            '    async with httpx.AsyncClient() as hc:\n'
            '        gw = HttpNotificationsGateway(\n'
            '            http_client=hc, base_url=url, api_key=key\n'
            '        )\n'
            '        # 1. Schedule batch then cancel (normal order)\n'
            '        await gw.send_campaign_batch(\n'
            '            campaign_id=c_after,\n'
            '            batch_id=b_after,\n'
            '            user_id=uid,\n'
            '            template_id="billing_notification",\n'
            '            subject="Срок подписки «Премиум» закончился",\n'
            '            text="Текст письма",\n'
            '            available_at="2099-01-01T12:00:00Z",\n'
            '            request_id="t24-send-after",\n'
            '        )\n'
            '        await gw.cancel_campaign(c_after, request_id="t24-cancel-1")\n'
            '        await gw.cancel_campaign(c_after, request_id="t24-cancel-2")\n'
            '        # 2. Pre-emptive cancel BEFORE batch arrives (invariant 12)\n'
            '        await gw.cancel_campaign(c_before, request_id="t24-cancel-pre")\n'
            '        await gw.send_campaign_batch(\n'
            '            campaign_id=c_before,\n'
            '            batch_id=b_before,\n'
            '            user_id=uid,\n'
            '            template_id="billing_notification",\n'
            '            subject="Срок подписки «Премиум» закончился",\n'
            '            text="Текст письма",\n'
            '            available_at="2099-01-01T12:00:00Z",\n'
            '            request_id="t24-send-before",\n'
            '        )\n'
            '        # 3. Unhappy path: invalid API key -> 401\n'
            '        bad_gw = HttpNotificationsGateway(\n'
            '            http_client=hc, base_url=url, api_key="wrong-key"\n'
            '        )\n'
            '        try:\n'
            '            await bad_gw.cancel_campaign(c_after, request_id="t24-401")\n'
            '            raise AssertionError("Expected 401")\n'
            '        except NotificationsPermanentError as exc:\n'
            '            assert exc.status_code == 401\n'
            '    print(f"c_after={c_after}\\nc_before={c_before}")\n'
            'asyncio.run(check_t24())\n'
        )
        out = self._compose('exec', '-T', 'billing-api', 'python', '-c', code).strip()
        c_after = next(
            line.split('=', 1)[1]
            for line in out.splitlines()
            if line.startswith('c_after=')
        )
        c_before = next(
            line.split('=', 1)[1]
            for line in out.splitlines()
            if line.startswith('c_before=')
        )

        # Verify both campaigns have deliveries in status='cancelled',
        # terminal_reason='cancelled_by_sender', and 0 unpublished outbox rows
        ntf_verify = self._ntf_psql(
            f'SELECT count(*) FROM campaign_recipients r '
            f'JOIN deliveries d ON d.delivery_id = r.delivery_id '
            f"WHERE r.campaign_id IN ('{c_after}', '{c_before}') "
            f"AND d.status = 'cancelled' "
            f"AND d.terminal_reason = 'cancelled_by_sender';"
        ).stdout.strip()
        if ntf_verify != '2':
            raise AssertionError(
                f'Expected 2 cancelled_by_sender deliveries, got {ntf_verify}'
            )

        outbox_pending = self._ntf_psql(
            f'SELECT count(*) FROM outbox o '
            f'JOIN campaign_recipients r ON r.delivery_id = o.delivery_id '
            f"WHERE r.campaign_id IN ('{c_after}', '{c_before}') "
            f'AND o.published_at IS NULL;'
        ).stdout.strip()
        if outbox_pending != '0':
            raise AssertionError(
                f'Expected 0 unpublished outbox rows for cancelled campaigns, '
                f'got {outbox_pending}'
            )

    def _check_auth_user_lookup_t25(self) -> None:
        """T-25 (AUTH-FR-04, AUTH-API-02, AUTH-NFR-SEC-01): internal user lookup."""
        login = f'LookupT25_{uuid4().hex[:8]}'
        password = 'Saffron-Citadel-LookupT25-2026!'

        # External gateway must block /api/v1/internal/users/lookup -> 404
        ext_res = self._http(
            'POST',
            '/api/v1/internal/users/lookup',
            data=json.dumps({'login': login}).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
        )
        if ext_res.status != 404:
            raise AssertionError(
                f'Expected 404 on external /api/v1/internal/users/lookup, got {ext_res}'
            )

        code = (
            'import asyncio, httpx\n'
            'from uuid import uuid4\n'
            'from core.config import ServiceSettings, settings\n'
            'async def check_t25():\n'
            '    base = str(settings.auth_client.service_url).rstrip("/")\n'
            '    key = ServiceSettings().api_key.get_secret_value()\n'
            '    url = f"{base}/api/v1/internal/users/lookup"\n'
            '    async with httpx.AsyncClient() as hc:\n'
            '        sr = await hc.post(\n'
            '            f"{base}/api/v1/auth/signup",\n'
            '            json={\n'
            f'                "login": "{login}",\n'
            f'                "password": "{password}",\n'
            '                "tz": "Asia/Yekaterinburg",\n'
            '            },\n'
            '            headers={"X-Request-Id": "t25-signup"},\n'
            '        )\n'
            '        assert sr.status_code == 201, sr.text\n'
            '        uid = sr.json()["id"]\n'
            '        # 1. Unhappy: missing or wrong key -> 401\n'
            '        r401 = await hc.post(\n'
            '            url,\n'
            '            headers={"X-Request-Id": "t25-401"},\n'
            f'            json={{"login": "{login}"}},\n'
            '        )\n'
            '        assert r401.status_code == 401, r401.text\n'
            '        # 2. Unhappy: empty or invalid payload -> 422\n'
            '        r422 = await hc.post(\n'
            '            url,\n'
            '            headers={\n'
            '                "X-Service-Api-Key": key,\n'
            '                "X-Request-Id": "t25-422",\n'
            '            },\n'
            '            json={},\n'
            '        )\n'
            '        assert r422.status_code == 422, r422.text\n'
            '        # 3. Happy: exact login match\n'
            '        r_login = await hc.post(\n'
            '            url,\n'
            '            headers={\n'
            '                "X-Service-Api-Key": key,\n'
            '                "X-Request-Id": "t25-login",\n'
            '            },\n'
            f'            json={{"login": "{login}"}},\n'
            '        )\n'
            '        assert r_login.status_code == 200, r_login.text\n'
            '        assert r_login.json() == {\n'
            '            "items": [\n'
            '                {\n'
            '                    "user_id": uid,\n'
            f'                    "login": "{login}",\n'
            '                    "tz": "Asia/Yekaterinburg",\n'
            '                }\n'
            '            ]\n'
            '        }\n'
            '        # 4. Unhappy: unknown login -> empty list\n'
            '        r_none = await hc.post(\n'
            '            url,\n'
            '            headers={\n'
            '                "X-Service-Api-Key": key,\n'
            '                "X-Request-Id": "t25-none",\n'
            '            },\n'
            '            json={"login": "nonexistent_t25_user"},\n'
            '        )\n'
            '        assert r_none.status_code == 200\n'
            '        assert r_none.json() == {"items": []}\n'
            '        # 5. Happy: batch user_ids lookup\n'
            '        r_batch = await hc.post(\n'
            '            url,\n'
            '            headers={\n'
            '                "X-Service-Api-Key": key,\n'
            '                "X-Request-Id": "t25-batch",\n'
            '            },\n'
            '            json={"user_ids": [uid, str(uuid4())]},\n'
            '        )\n'
            '        assert r_batch.status_code == 200, r_batch.text\n'
            '        assert r_batch.json() == r_login.json()\n'
            'asyncio.run(check_t25())\n'
        )
        self._compose('exec', '-T', 'billing-api', 'python', '-c', code)

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
