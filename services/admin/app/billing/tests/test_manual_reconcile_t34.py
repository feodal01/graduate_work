from __future__ import annotations

from unittest.mock import patch
from uuid import UUID, uuid4

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


class ManualReconcileT34AdminTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='reconcile_admin',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.user.set_unusable_password()
        self.user.save()
        self.client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        session = self.client.session
        session[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        session[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        session.save()
        self.auth_client = FakeAdminAuthClient()

    def _request(
        self,
        method: str,
        url: str,
        data: dict[str, str] | None = None,
        *,
        billing_client: object | None = None,
    ):
        from billing.clients import get_billing_client

        b_client = billing_client or get_billing_client()
        with (
            patch(
                'users.auth_middleware.get_auth_client',
                return_value=self.auth_client,
            ),
            patch(
                'billing.views.get_billing_client',
                return_value=b_client,
            ),
        ):
            return getattr(self.client, method)(
                url,
                data=data or {},
                headers={'X-Request-Id': 't34-admin-req'},
            )

    def test_unauthenticated_and_csrf_protection_step(self) -> None:
        """T-34 (ADM-NFR-SEC-01): защита входа и CSRF на ручной сверке."""
        pid = uuid4()
        url = reverse('admin:billing_operation_reconcile', args=['payment', pid])

        anon = Client()
        self.assertEqual(anon.post(url).status_code, 302)

        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        sess = csrf_client.session
        sess[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        sess[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        sess.save()
        with patch(
            'users.auth_middleware.get_auth_client',
            return_value=self.auth_client,
        ):
            self.assertEqual(csrf_client.post(url).status_code, 403)

    def test_manual_reconcile_view_happy_and_unhappy_paths(self) -> None:
        """T-34 (ADM-FR-06, US-A-06, ADM-NFR-R-02): кнопка Проверить в ЮKassa."""
        from billing.clients import (
            BillingNotFoundError,
            BillingReconcileResult,
            BillingUnavailableError,
        )

        pid = UUID('11111111-1111-4111-8111-111111111111')
        rid = UUID('22222222-2222-4222-8222-222222222222')

        class ReconcileBilling:
            def __init__(self) -> None:
                self.mode = 'ok'
                self.calls: list[tuple[str, UUID]] = []

            def reconcile_operation(
                self,
                operation_type: str,
                operation_id: UUID,
                *,
                request_id: str,
            ) -> BillingReconcileResult:
                if self.mode == 'not_found':
                    raise BillingNotFoundError('not found')
                if self.mode == 'down':
                    raise BillingUnavailableError('YooKassa or Billing unavailable')
                self.calls.append((operation_type, operation_id))
                return BillingReconcileResult(
                    operation_type=operation_type,
                    operation_id=operation_id,
                    payment_id=pid,
                    status='succeeded',
                    transitioned=True,
                )

        bil = ReconcileBilling()
        pay_url = reverse('admin:billing_operation_reconcile', args=['payment', pid])
        ref_url = reverse('admin:billing_operation_reconcile', args=['refund', rid])

        # 1. GET перенаправляет на список платежей без вызова сверки
        self.assertEqual(self._request('get', pay_url, billing_client=bil).status_code, 302)
        self.assertEqual(bil.calls, [])

        # 2. Happy: POST сверки платежа и возврата перенаправляет в карточку платежа
        res_pay = self._request('post', pay_url, billing_client=bil)
        self.assertEqual(res_pay.status_code, 302)
        self.assertEqual(
            res_pay['Location'],
            reverse('admin:billing_payment_detail', args=[pid]),
        )

        res_ref = self._request('post', ref_url, billing_client=bil)
        self.assertEqual(res_ref.status_code, 302)
        self.assertEqual(
            res_ref['Location'],
            reverse('admin:billing_payment_detail', args=[pid]),
        )
        self.assertEqual(bil.calls, [('payment', pid), ('refund', rid)])

        # 3. Unhappy: невалидный тип операции -> 404
        bad_type_url = reverse('admin:billing_operation_reconcile', args=['invalid', pid])
        self.assertEqual(
            self._request('post', bad_type_url, billing_client=bil).status_code,
            404,
        )

        # 4. Unhappy: 404 если операция не найдена, 503 при сбое ЮKassa или Billing
        bil.mode = 'not_found'
        self.assertEqual(
            self._request('post', pay_url, billing_client=bil).status_code,
            404,
        )
        bil.mode = 'down'
        self.assertEqual(
            self._request('post', pay_url, billing_client=bil).status_code,
            503,
        )

    def test_billing_admin_client_reconcile_operation_contract(self) -> None:
        """T-34: проверка HTTP-клиента reconcile_operation (200, 404, 503)."""
        import httpx

        from billing.clients import (
            BillingAdminClient,
            BillingNotFoundError,
            BillingUnavailableError,
        )

        pid = UUID('11111111-1111-4111-8111-111111111111')
        mode = 'ok'

        def handler(request: httpx.Request) -> httpx.Response:
            if mode == 'ok':
                return httpx.Response(
                    200,
                    json={
                        'operation_type': 'payment',
                        'operation_id': str(pid),
                        'payment_id': str(pid),
                        'status': 'succeeded',
                        'transitioned': True,
                    },
                )
            if mode == '404':
                return httpx.Response(404, json={'error_code': 'operation_not_found'})
            return httpx.Response(
                503,
                json={'error_code': 'payment_provider_unavailable'},
            )

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        res = client.reconcile_operation('payment', pid, request_id='r1')
        self.assertEqual(res.status, 'succeeded')
        self.assertTrue(res.transitioned)

        mode = '404'
        with self.assertRaises(BillingNotFoundError):
            client.reconcile_operation('payment', pid, request_id='r2')

        mode = '503'
        with self.assertRaises(BillingUnavailableError):
            client.reconcile_operation('payment', pid, request_id='r3')
