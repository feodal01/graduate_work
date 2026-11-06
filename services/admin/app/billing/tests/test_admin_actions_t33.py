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


class AdminActionsLogT33Tests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='audit_admin',
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
                headers={'X-Request-Id': 't33-admin-req'},
            )

    def test_admin_actions_screen_filters_search_and_degradation(self) -> None:
        """T-33 (ADM-FR-08, US-A-05, ADM-NFR-R-01..02): журнал действий поддержки."""
        from billing.clients import (
            AuthLookupUnavailableError,
            BillingAdminActionItem,
            BillingAdminActionsPage,
            BillingUnavailableError,
            UserLookupProfile,
        )

        admin_id = UUID('11111111-1111-4111-8111-111111111111')
        pay_id = UUID('22222222-2222-4222-8222-222222222222')
        ref_id = UUID('33333333-3333-4333-8333-333333333333')
        act_id = UUID('44444444-4444-4444-8444-444444444444')
        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)

        sample_item = BillingAdminActionItem(
            id=act_id,
            admin_user_id=admin_id,
            action_type='refund_requested',
            target_type='refund',
            target_id=ref_id,
            payment_id=pay_id,
            target_status='succeeded',
            reason='Возврат по обращению в поддержку #55',
            created_at=t0,
        )

        class AuditBilling:
            def __init__(self) -> None:
                self.fail = False
                self.last_kwargs: dict[str, object] = {}

            def list_admin_actions(self, **kwargs: object) -> BillingAdminActionsPage:
                if self.fail:
                    raise BillingUnavailableError('billing down')
                self.last_kwargs = dict(kwargs)
                adm_filter = kwargs.get('admin_user_id')
                if adm_filter is not None and adm_filter != admin_id:
                    return BillingAdminActionsPage(total=0, items=())
                return BillingAdminActionsPage(total=1, items=(sample_item,))

        class AuditLookup:
            def __init__(self) -> None:
                self.down = False
                self.batch_calls: list[tuple[UUID, ...]] = []

            def find_by_login(self, login: str, *, request_id: str) -> UserLookupProfile | None:
                if self.down:
                    raise AuthLookupUnavailableError('auth down')
                if login == 'support_lead':
                    return UserLookupProfile(
                        user_id=admin_id,
                        login='support_lead',
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
                    admin_id: UserLookupProfile(
                        user_id=admin_id,
                        login='support_lead',
                        tz='Asia/Yekaterinburg',
                    )
                }

        bil = AuditBilling()
        lkp = AuditLookup()
        url = reverse('admin:billing_admin_actions')

        # 1. Фильтрация по логину администратора и типу действия
        res = self._get(
            url,
            {'q': 'support_lead', 'action_type': 'refund_requested'},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(bil.last_kwargs.get('admin_user_id'), admin_id)
        self.assertEqual(bil.last_kwargs.get('action_type'), 'refund_requested')
        self.assertContains(res, 'support_lead')
        self.assertContains(res, 'Возврат по обращению в поддержку #55')
        self.assertContains(res, '05.11.2026 10:00 UTC')
        self.assertContains(res, '05.11.2026 15:00 (Asia/Yekaterinburg)')
        self.assertContains(res, reverse('admin:billing_payment_detail', args=[pay_id]))
        self.assertContains(res, 'Возвращён')

        # 2. Невалидный диапазон дат -> 400
        bad_date = self._get(
            url,
            {'date_from': '2026-12-01', 'date_to': '2026-11-01'},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(bad_date.status_code, 400)

        # 3. При отказе Auth таблица работает с UUID и временем в UTC (ADM-NFR-R-01)
        lkp.down = True
        res_auth_down = self._get(
            url,
            {'q': str(admin_id)},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res_auth_down.status_code, 200)
        self.assertContains(res_auth_down, str(admin_id))
        self.assertContains(res_auth_down, '05.11.2026 10:00 UTC')

        # 4. При отказе Billing -> 503 (ADM-NFR-R-02)
        bil.fail = True
        res_bil_down = self._get(url, billing_client=bil, lookup_client=lkp)
        self.assertEqual(res_bil_down.status_code, 503)

    def test_billing_admin_client_list_admin_actions_contract(self) -> None:
        """T-33: проверка HTTP-клиента list_admin_actions."""
        import httpx

        from billing.clients import BillingAdminClient, BillingContractError

        aid = UUID('11111111-1111-4111-8111-111111111111')
        tid = UUID('22222222-2222-4222-8222-222222222222')
        pid = UUID('33333333-3333-4333-8333-333333333333')
        act_id = UUID('44444444-4444-4444-8444-444444444444')

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path.endswith('/admin-actions')
            return httpx.Response(
                200,
                json={
                    'total': 1,
                    'items': [
                        {
                            'id': str(act_id),
                            'admin_user_id': str(aid),
                            'action_type': 'outbox_closed',
                            'target_type': 'outbox_task',
                            'target_id': str(tid),
                            'payment_id': str(pid),
                            'target_status': 'closed',
                            'reason': 'Тестовая причина',
                            'created_at': '2026-11-05T10:00:00Z',
                        }
                    ],
                },
            )

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        page = client.list_admin_actions(
            request_id='req-t33',
            admin_user_id=aid,
            action_type='outbox_closed',
        )
        self.assertEqual(page.total, 1)
        self.assertEqual(page.items[0].id, act_id)
        self.assertEqual(page.items[0].payment_id, pid)
        self.assertEqual(page.items[0].target_status, 'closed')

        bad_client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(
                transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={'bad': 1}))
            ),
        )
        with self.assertRaises(BillingContractError):
            bad_client.list_admin_actions(request_id='req-bad')
