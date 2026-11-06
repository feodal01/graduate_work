from __future__ import annotations

from django.contrib import admin
from django.contrib.auth import logout as django_logout
from django.shortcuts import redirect, render
from django.urls import path

from billing import views as billing_views
from users import auth_views
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.auth_client import (
    AuthContractError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
    get_auth_client,
)
from users.auth_forms import (
    AuthAdminAuthenticationForm,
    AuthServiceRateLimited,
    AuthServiceUnavailable,
)
from users.rate_limit import rate_limited_response
from users.request_id import get_request_id


class IntegratedAdminSite(admin.AdminSite):
    login_form = AuthAdminAuthenticationForm
    index_template = 'admin/auth/integrated_index.html'
    site_header = 'Cinema administration'
    site_title = 'Cinema administration'
    index_title = 'Catalogue administration'

    def get_urls(self):
        custom_urls = [
            path(
                'auth/roles/',
                self.admin_view(auth_views.role_list),
                name='auth_role_list',
            ),
            path(
                'auth/roles/add/',
                self.admin_view(auth_views.role_add),
                name='auth_role_add',
            ),
            path(
                'auth/roles/<uuid:role_id>/change/',
                self.admin_view(auth_views.role_change),
                name='auth_role_change',
            ),
            path(
                'auth/roles/<uuid:role_id>/delete/',
                self.admin_view(auth_views.role_delete),
                name='auth_role_delete',
            ),
            path(
                'auth/user-roles/add/',
                self.admin_view(auth_views.user_role_add),
                name='auth_user_role_add',
            ),
            path(
                'auth/user-roles/delete/',
                self.admin_view(auth_views.user_role_delete),
                name='auth_user_role_delete',
            ),
            path(
                'billing/subscriptions/',
                self.admin_view(billing_views.subscriptions_list),
                name='billing_subscriptions',
            ),
            path(
                'billing/payments/',
                self.admin_view(billing_views.payments_list),
                name='billing_payments',
            ),
            path(
                'billing/payments/<uuid:payment_id>/',
                self.admin_view(billing_views.payment_detail),
                name='billing_payment_detail',
            ),
        ]
        return custom_urls + super().get_urls()

    def login(self, request, extra_context=None):
        try:
            return super().login(request, extra_context)
        except AuthServiceUnavailable:
            return render(
                request,
                'admin/auth/unavailable.html',
                status=503,
            )
        except AuthServiceRateLimited as exc:
            return rate_limited_response(request, exc.retry_after_seconds)

    def logout(self, request, extra_context=None):
        access_token = request.session.get(AUTH_ACCESS_TOKEN_SESSION_KEY)
        refresh_token = request.session.get(AUTH_REFRESH_TOKEN_SESSION_KEY)
        if isinstance(access_token, str) and isinstance(refresh_token, str):
            try:
                get_auth_client().logout(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    request_id=get_request_id(request),
                )
            except (
                AuthUnauthorizedError,
                AuthRateLimitedError,
                AuthUnavailableError,
                AuthContractError,
            ):
                pass
        django_logout(request)
        return redirect('admin:login')


integrated_admin_site = IntegratedAdminSite(name='admin')
