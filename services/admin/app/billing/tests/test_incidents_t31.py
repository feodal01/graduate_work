from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


class IncidentFiltersT31Tests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='incident_admin',
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

    def _get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        *,
        billing_client: object | None = None,
        lookup_client: object | None = None,
    ):
        from billing.clients import get_auth_lookup_client, get_billing_client

        b_client = billing_client or get_billing_client()
        l_client = lookup_client or get_auth_lookup_client()
        with (
            patch(
                'users.auth_middleware.get_auth_client',
                return_value=self.auth_client,
            ),
            patch(
                'billing.views.get_billing_client',
                return_value=b_client,
            ),
            patch(
                'billing.views.get_auth_lookup_client',
                return_value=l_client,
            ),
        ):
            return self.client.get(
                url,
                data=params or {},
                headers={'X-Request-Id': 't31-admin-req'},
            )

    def test_undelivered_tab_renders_tasks_search_and_degradation(self) -> None:
        """T-31 (ADM-FR-03, US-A-02, ADM-NFR-R-01, ADM-NFR-R-02): вкладка «Не доставлено»."""
        from billing.clients import (
            AuthLookupUnavailableError,
            BillingOutboxTaskItem,
            BillingOutboxTasksPage,
            BillingUnavailableError,
            UserLookupProfile,
        )

        user_id = UUID('11111111-1111-4111-8111-111111111111')
        pay_id = UUID('22222222-2222-4222-8222-222222222222')
        t_intervene = UUID('33333333-3333-4333-8333-333333333333')
        t_pending = UUID('44444444-4444-4444-8444-444444444444')
        created_utc = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)

        sample_tasks = (
            BillingOutboxTaskItem(
                id=t_intervene,
                target_service='auth',
                task_type='sync_access',
                access_version=1,
                status='requires_intervention',
                attempts=20,
                last_error='Auth HTTP 404 user not found',
                created_at=created_utc,
                updated_at=created_utc,
                can_retry_or_close=True,
                payment_id=pay_id,
                user_id=user_id,
            ),
            BillingOutboxTaskItem(
                id=t_pending,
                target_service='notifications',
                task_type='payment_succeeded_email',
                access_version=None,
                status='pending',
                attempts=5,
                last_error='Notifications HTTP 503',
                created_at=created_utc,
                updated_at=created_utc,
                can_retry_or_close=False,
                payment_id=pay_id,
                user_id=user_id,
            ),
        )

        class IncidentBilling:
            def __init__(self) -> None:
                self.fail = False
                self.last_kwargs: dict[str, object] = {}

            def list_outbox_tasks(self, **kwargs: object) -> BillingOutboxTasksPage:
                if self.fail:
                    raise BillingUnavailableError('billing down')
                self.last_kwargs = dict(kwargs)
                uid_filter = kwargs.get('user_id')
                if uid_filter is not None and uid_filter != user_id:
                    return BillingOutboxTasksPage(total=0, items=())
                return BillingOutboxTasksPage(total=2, items=sample_tasks)

        class IncidentLookup:
            def __init__(self) -> None:
                self.down = False
                self.batch_calls: list[tuple[UUID, ...]] = []

            def find_by_login(self, login: str, *, request_id: str) -> UserLookupProfile | None:
                if self.down:
                    raise AuthLookupUnavailableError('auth down')
                if login == 'alice_incident':
                    return UserLookupProfile(
                        user_id=user_id,
                        login='alice_incident',
                        tz='Asia/Yekaterinburg',
                    )
                return None

            def get_by_ids(
                self, user_ids: list[UUID], *, request_id: str
            ) -> dict[UUID, UserLookupProfile]:
                if self.down:
                    raise AuthLookupUnavailableError('auth down')
                self.batch_calls.append(tuple(user_ids))
                return {
                    user_id: UserLookupProfile(
                        user_id=user_id,
                        login='alice_incident',
                        tz='Asia/Yekaterinburg',
                    )
                }

        bil = IncidentBilling()
        lkp = IncidentLookup()

        # 1. Вкладка «Не доставлено» показывает обе задачи, кнопки «Повторить» и
        #    «Закрыть с причиной» для requires_intervention и ссылку на карточку платежа
        res = self._get(
            reverse('admin:billing_payments'),
            {'tab': 'undelivered', 'q': 'alice_incident'},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(bil.last_kwargs.get('user_id'), user_id)
        self.assertContains(res, 'alice_incident')
        self.assertContains(res, str(t_intervene))
        self.assertContains(res, str(t_pending))
        self.assertContains(res, 'Auth HTTP 404 user not found')
        self.assertContains(res, '05.11.2026 10:00 UTC')
        self.assertContains(res, '05.11.2026 15:00 (Asia/Yekaterinburg)')
        self.assertContains(res, reverse('admin:billing_payment_detail', args=[pay_id]))
        self.assertContains(res, 'Повторить')
        self.assertContains(res, 'Закрыть с причиной')

        # 2. Деградация при отказе Auth -> 200 OK с UUID и временем в UTC (ADM-NFR-R-01)
        lkp.down = True
        res_auth_down = self._get(
            reverse('admin:billing_payments'),
            {'tab': 'undelivered', 'q': str(user_id)},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res_auth_down.status_code, 200)
        self.assertContains(res_auth_down, str(user_id))
        self.assertContains(res_auth_down, '05.11.2026 10:00 UTC')

        # 3. Деградация при отказе Billing -> 503 (ADM-NFR-R-02)
        bil.fail = True
        res_bil_down = self._get(
            reverse('admin:billing_payments'),
            {'tab': 'undelivered'},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res_bil_down.status_code, 503)

    def test_billing_admin_client_list_outbox_tasks_contract(self) -> None:
        """T-31: проверка HTTP-клиента list_outbox_tasks (200 и невалидный JSON)."""
        import httpx

        from billing.clients import BillingAdminClient, BillingContractError

        tid = UUID('11111111-1111-4111-8111-111111111111')
        pid = UUID('22222222-2222-4222-8222-222222222222')
        uid = UUID('33333333-3333-4333-8333-333333333333')

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path.endswith('/outbox-tasks')
            return httpx.Response(
                200,
                json={
                    'total': 1,
                    'items': [
                        {
                            'id': str(tid),
                            'payment_id': str(pid),
                            'user_id': str(uid),
                            'target_service': 'auth',
                            'task_type': 'sync_access',
                            'access_version': 1,
                            'status': 'requires_intervention',
                            'attempts': 20,
                            'last_error': 'err',
                            'created_at': '2026-11-05T10:00:00Z',
                            'updated_at': '2026-11-05T10:00:00Z',
                            'can_retry_or_close': True,
                        }
                    ],
                },
            )

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        page = client.list_outbox_tasks(
            request_id='req-t31',
            user_id=uid,
            target_service='auth',
        )
        self.assertEqual(page.total, 1)
        self.assertEqual(page.items[0].id, tid)
        self.assertEqual(page.items[0].payment_id, pid)
        self.assertEqual(page.items[0].user_id, uid)

        bad_client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(
                transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={'bad': 1}))
            ),
        )
        with self.assertRaises(BillingContractError):
            bad_client.list_outbox_tasks(request_id='req-bad')
