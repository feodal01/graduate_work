from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.models import AuthIdentity
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


class OutboxActionsT32AdminTests(TestCase):
    def setUp(self) -> None:
        self.admin_auth_uuid = UUID('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')
        self.user = User.objects.create(
            username='outbox_admin',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.user.set_unusable_password()
        self.user.save()
        AuthIdentity.objects.create(
            user=self.user,
            auth_user_id=self.admin_auth_uuid,
        )
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
                headers={'X-Request-Id': 't32-admin-req'},
            )

    def test_unauthenticated_and_csrf_protection_step(self) -> None:
        """T-32 (ADM-NFR-SEC-01): защита входа и CSRF на действиях outbox."""
        tid = uuid4()
        retry_url = reverse('admin:billing_outbox_task_retry', args=[tid])
        close_url = reverse('admin:billing_outbox_task_close', args=[tid])

        anon = Client()
        self.assertEqual(anon.post(retry_url).status_code, 302)
        self.assertEqual(anon.get(close_url).status_code, 302)

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
            self.assertEqual(csrf_client.post(retry_url).status_code, 403)
            self.assertEqual(
                csrf_client.post(close_url, data={'reason': 'Причина'}).status_code,
                403,
            )

    def test_retry_and_close_views_happy_and_unhappy_paths(self) -> None:
        """T-32 (ADM-FR-07, US-A-02, ADM-NFR-R-02): кнопки Повторить и Закрыть с причиной."""
        from billing.clients import (
            BillingConflictError,
            BillingNotFoundError,
            BillingOutboxTaskItem,
            BillingUnavailableError,
        )

        tid = UUID('11111111-1111-4111-8111-111111111111')
        pid = UUID('22222222-2222-4222-8222-222222222222')
        uid = UUID('33333333-3333-4333-8333-333333333333')
        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)

        class ActionBilling:
            def __init__(self) -> None:
                self.mode = 'ok'
                self.retried: list[dict[str, object]] = []
                self.closed: list[dict[str, object]] = []

            def retry_outbox_task(
                self,
                task_id: UUID,
                *,
                admin_user_id: UUID,
                request_id: str,
            ) -> BillingOutboxTaskItem:
                if self.mode == 'not_found':
                    raise BillingNotFoundError('not found')
                if self.mode == 'conflict':
                    raise BillingConflictError('Задача уже завершена')
                if self.mode == 'down':
                    raise BillingUnavailableError('billing down')
                self.retried.append(
                    {
                        'task_id': task_id,
                        'admin_user_id': admin_user_id,
                        'request_id': request_id,
                    }
                )
                return BillingOutboxTaskItem(
                    id=task_id,
                    target_service='auth',
                    task_type='sync_access',
                    access_version=1,
                    status='completed',
                    attempts=1,
                    last_error=None,
                    created_at=t0,
                    updated_at=t0,
                    can_retry_or_close=False,
                    payment_id=pid,
                    user_id=uid,
                )

            def close_outbox_task(
                self,
                task_id: UUID,
                *,
                admin_user_id: UUID,
                reason: str,
                request_id: str,
            ) -> BillingOutboxTaskItem:
                if self.mode == 'not_found':
                    raise BillingNotFoundError('not found')
                if self.mode == 'conflict':
                    raise BillingConflictError(
                        'Закрытие доступно только для задачи в состоянии requires_intervention'
                    )
                if self.mode == 'down':
                    raise BillingUnavailableError('billing down')
                self.closed.append(
                    {
                        'task_id': task_id,
                        'admin_user_id': admin_user_id,
                        'reason': reason,
                        'request_id': request_id,
                    }
                )
                return BillingOutboxTaskItem(
                    id=task_id,
                    target_service='notifications',
                    task_type='payment_succeeded_email',
                    access_version=None,
                    status='closed',
                    attempts=20,
                    last_error='err',
                    created_at=t0,
                    updated_at=t0,
                    can_retry_or_close=False,
                    payment_id=pid,
                    user_id=uid,
                )

        bil = ActionBilling()
        retry_url = reverse('admin:billing_outbox_task_retry', args=[tid])
        close_url = reverse('admin:billing_outbox_task_close', args=[tid])

        # 1. GET на retry перенаправляет на список проблемных задач без выполнения действия
        r_get_retry = self._request('get', retry_url, billing_client=bil)
        self.assertEqual(r_get_retry.status_code, 302)
        self.assertEqual(bil.retried, [])

        # 2. Happy: POST на retry вызывает Billing и перенаправляет в карточку платежа
        r_post_retry = self._request('post', retry_url, billing_client=bil)
        self.assertEqual(r_post_retry.status_code, 302)
        self.assertEqual(
            r_post_retry['Location'],
            reverse('admin:billing_payment_detail', args=[pid]),
        )
        self.assertEqual(len(bil.retried), 1)
        self.assertEqual(bil.retried[0]['admin_user_id'], self.admin_auth_uuid)

        # 3. GET страницы закрытия с причиной -> 200 OK
        r_get_close = self._request('get', close_url, billing_client=bil)
        self.assertEqual(r_get_close.status_code, 200)
        self.assertContains(r_get_close, str(tid))

        # 4. Unhappy: POST закрытия с пустой причиной -> 400 (инвариант 9)
        for blank in ('', '   '):
            r_bad_close = self._request(
                'post',
                close_url,
                data={'reason': blank},
                billing_client=bil,
            )
            self.assertEqual(r_bad_close.status_code, 400)
            self.assertContains(
                r_bad_close,
                'Укажите причину закрытия',
                status_code=400,
            )
            self.assertEqual(bil.closed, [])

        # 5. Happy: POST закрытия с непустой причиной -> вызывает Billing и редиректит
        r_ok_close = self._request(
            'post',
            close_url,
            data={'reason': '  Клиент уведомлён вручную  '},
            billing_client=bil,
        )
        self.assertEqual(r_ok_close.status_code, 302)
        self.assertEqual(
            r_ok_close['Location'],
            reverse('admin:billing_payment_detail', args=[pid]),
        )
        self.assertEqual(len(bil.closed), 1)
        self.assertEqual(bil.closed[0]['reason'], 'Клиент уведомлён вручную')

        # 6. Unhappy: 409 при конфликте, 404 при отсутствии задачи, 503 при сбое Billing
        bil.mode = 'conflict'
        self.assertEqual(
            self._request(
                'post',
                close_url,
                data={'reason': 'Причина'},
                billing_client=bil,
            ).status_code,
            409,
        )
        bil.mode = 'not_found'
        self.assertEqual(
            self._request('post', retry_url, billing_client=bil).status_code,
            404,
        )
        self.assertEqual(
            self._request(
                'post',
                close_url,
                data={'reason': 'Причина'},
                billing_client=bil,
            ).status_code,
            404,
        )
        bil.mode = 'down'
        self.assertEqual(
            self._request('post', retry_url, billing_client=bil).status_code,
            503,
        )
        self.assertEqual(
            self._request(
                'post',
                close_url,
                data={'reason': 'Причина'},
                billing_client=bil,
            ).status_code,
            503,
        )

    def test_billing_admin_client_retry_and_close_outbox_contract(self) -> None:
        """T-32: проверка HTTP-клиента retry_outbox_task и close_outbox_task."""
        import httpx

        from billing.clients import BillingAdminClient

        tid = UUID('11111111-1111-4111-8111-111111111111')
        pid = UUID('22222222-2222-4222-8222-222222222222')
        uid = UUID('33333333-3333-4333-8333-333333333333')
        aid = UUID('44444444-4444-4444-8444-444444444444')

        def handler(request: httpx.Request) -> httpx.Response:
            status_val = 'completed' if request.url.path.endswith('/retry') else 'closed'
            return httpx.Response(
                200,
                json={
                    'id': str(tid),
                    'payment_id': str(pid),
                    'user_id': str(uid),
                    'target_service': 'auth',
                    'task_type': 'sync_access',
                    'access_version': 1,
                    'status': status_val,
                    'attempts': 1,
                    'last_error': None,
                    'created_at': '2026-11-05T10:00:00Z',
                    'updated_at': '2026-11-05T10:00:00Z',
                    'can_retry_or_close': False,
                },
            )

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        retried = client.retry_outbox_task(tid, admin_user_id=aid, request_id='r1')
        self.assertEqual(retried.status, 'completed')

        closed = client.close_outbox_task(
            tid,
            admin_user_id=aid,
            reason='Закрыто вручную',
            request_id='r2',
        )
        self.assertEqual(closed.status, 'closed')
