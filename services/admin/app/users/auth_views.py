from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from django.contrib import messages
from django.contrib.auth import logout as django_logout
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from users.auth_backend import AUTH_ACCESS_TOKEN_SESSION_KEY
from users.auth_client import (
    AuthContractError,
    AuthForbiddenError,
    AuthOperationError,
    AuthRateLimitedError,
    AuthUnauthorizedError,
    AuthUnavailableError,
    get_auth_client,
)
from users.auth_forms import RoleAssignmentForm, RoleForm
from users.request_id import get_request_id
from users.rate_limit import rate_limited_response


def role_list(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        'admin/auth/role_list.html',
        {'roles': request.auth_roles},
    )


def role_add(request: HttpRequest) -> HttpResponse:
    form = RoleForm(request.POST or None)
    if request.method == 'POST' and form.is_valid():
        result = _execute(
            request,
            form,
            lambda: get_auth_client().create_role(
                access_token=_access_token(request),
                name=form.cleaned_data['name'],
                permissions=set(form.cleaned_data['permissions']),
                request_id=get_request_id(request),
            ),
        )
        if result is not None:
            return result
        messages.success(request, 'Role created.')
        return redirect('admin:auth_role_list')
    return _render_role_form(request, form, 'Create role')


def role_change(request: HttpRequest, role_id: UUID) -> HttpResponse:
    role = _find_role(request, role_id)
    initial = {'name': role.name, 'permissions': role.permissions}
    form = RoleForm(request.POST or None, initial=initial)
    if request.method == 'POST' and form.is_valid():
        result = _execute(
            request,
            form,
            lambda: get_auth_client().update_role(
                access_token=_access_token(request),
                role_id=role_id,
                name=form.cleaned_data['name'],
                permissions=set(form.cleaned_data['permissions']),
                request_id=get_request_id(request),
            ),
        )
        if result is not None:
            return result
        messages.success(request, 'Role updated.')
        return redirect('admin:auth_role_list')
    return _render_role_form(request, form, 'Change role')


def role_delete(request: HttpRequest, role_id: UUID) -> HttpResponse:
    role = _find_role(request, role_id)
    if request.method == 'POST':
        result = _execute(
            request,
            None,
            lambda: get_auth_client().delete_role(
                access_token=_access_token(request),
                role_id=role_id,
                request_id=get_request_id(request),
            ),
        )
        if result is not None:
            return result
        messages.success(request, 'Role deleted.')
        return redirect('admin:auth_role_list')
    return render(
        request,
        'admin/auth/role_confirm_delete.html',
        {'role': role},
    )


def user_role_add(request: HttpRequest) -> HttpResponse:
    form = RoleAssignmentForm(request.POST or None, roles=request.auth_roles)
    if request.method == 'POST' and form.is_valid():
        result = _execute(
            request,
            form,
            lambda: get_auth_client().assign_role(
                access_token=_access_token(request),
                user_id=form.cleaned_data['user_id'],
                role_id=UUID(form.cleaned_data['role_id']),
                request_id=get_request_id(request),
            ),
        )
        if result is not None:
            return result
        messages.success(request, 'Role assigned.')
        return redirect('admin:auth_role_list')
    return _render_assignment_form(request, form, 'Assign role')


def user_role_delete(request: HttpRequest) -> HttpResponse:
    form = RoleAssignmentForm(request.POST or None, roles=request.auth_roles)
    if request.method == 'POST' and form.is_valid():
        result = _execute(
            request,
            form,
            lambda: get_auth_client().revoke_role(
                access_token=_access_token(request),
                user_id=form.cleaned_data['user_id'],
                role_id=UUID(form.cleaned_data['role_id']),
                request_id=get_request_id(request),
            ),
        )
        if result is not None:
            return result
        messages.success(request, 'Role revoked.')
        return redirect('admin:auth_role_list')
    return _render_assignment_form(request, form, 'Revoke role')


def _execute(
    request: HttpRequest,
    form,
    operation: Callable[[], object],
) -> HttpResponse | None:
    try:
        operation()
    except AuthUnauthorizedError:
        django_logout(request)
        return redirect('admin:login')
    except AuthForbiddenError:
        if form is not None:
            form.add_error(None, 'This action is forbidden.')
            return _render_form_for_type(request, form, status=403)
        return HttpResponse('This action is forbidden.', status=403)
    except AuthOperationError as exc:
        if form is not None:
            form.add_error(None, exc.detail)
            return _render_form_for_type(request, form)
        return HttpResponse(exc.detail, status=exc.status_code)
    except AuthRateLimitedError as exc:
        return rate_limited_response(request, exc.retry_after_seconds)
    except (AuthUnavailableError, AuthContractError):
        return render(
            request,
            'admin/auth/unavailable.html',
            status=503,
        )
    return None


def _render_form_for_type(
    request: HttpRequest,
    form,
    *,
    status: int = 200,
) -> HttpResponse:
    if isinstance(form, RoleAssignmentForm):
        return _render_assignment_form(request, form, 'Manage user role', status)
    return _render_role_form(request, form, 'Manage role', status)


def _render_role_form(
    request: HttpRequest,
    form: RoleForm,
    title: str,
    status: int = 200,
) -> HttpResponse:
    return render(
        request,
        'admin/auth/role_form.html',
        {'form': form, 'title': title},
        status=status,
    )


def _render_assignment_form(
    request: HttpRequest,
    form: RoleAssignmentForm,
    title: str,
    status: int = 200,
) -> HttpResponse:
    return render(
        request,
        'admin/auth/assignment_form.html',
        {'form': form, 'title': title},
        status=status,
    )


def _find_role(request: HttpRequest, role_id: UUID):
    for role in request.auth_roles:
        if role.id == role_id:
            return role
    raise Http404('Role not found')


def _access_token(request: HttpRequest) -> str:
    return request.session[AUTH_ACCESS_TOKEN_SESSION_KEY]
