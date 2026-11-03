from __future__ import annotations

from django import forms
from django.contrib.admin.forms import AdminAuthenticationForm
from django.contrib.auth import authenticate
from django.core.exceptions import ValidationError

from users.auth_backend import (
    AUTH_LOGIN_ERROR_ATTRIBUTE,
    AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE,
    AuthLoginError,
)

PERMISSION_CHOICES = (
    ('roles:manage', 'Manage roles'),
    ('user_roles:manage', 'Manage user roles'),
    ('films.early_access:read', 'Read early-access films'),
)


class AuthServiceUnavailable(Exception):
    """Вход нельзя завершить из-за технического отказа Auth."""


class AuthServiceRateLimited(Exception):
    """Auth временно ограничил запрос входа."""

    def __init__(self, retry_after_seconds: int | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__('Too many authentication requests')


class AuthAdminAuthenticationForm(AdminAuthenticationForm):
    username = forms.CharField(
        label='Login',
        max_length=255,
        widget=forms.TextInput(attrs={'autofocus': True}),
    )

    def __init__(self, request=None, *args, **kwargs) -> None:
        super().__init__(request, *args, **kwargs)
        self.fields['username'].max_length = 255
        self.fields['username'].widget.attrs['maxlength'] = 255

    def clean(self):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')
        if username is not None and password:
            self.user_cache = authenticate(
                self.request,
                username=username,
                password=password,
            )
            if self.user_cache is None:
                error = getattr(
                    self.request,
                    AUTH_LOGIN_ERROR_ATTRIBUTE,
                    AuthLoginError.INVALID_CREDENTIALS,
                )
                if error == AuthLoginError.UNAVAILABLE:
                    raise AuthServiceUnavailable
                if error == AuthLoginError.RATE_LIMITED:
                    raise AuthServiceRateLimited(
                        getattr(
                            self.request,
                            AUTH_LOGIN_RETRY_AFTER_ATTRIBUTE,
                            None,
                        )
                    )
                if error == AuthLoginError.FORBIDDEN:
                    raise ValidationError(
                        'This account does not have Admin access.',
                        code='forbidden',
                    )
                raise self.get_invalid_login_error()
            self.confirm_login_allowed(self.user_cache)
        return self.cleaned_data


class RoleForm(forms.Form):
    name = forms.CharField(max_length=100)
    permissions = forms.MultipleChoiceField(
        choices=PERMISSION_CHOICES,
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )


class RoleAssignmentForm(forms.Form):
    user_id = forms.UUIDField(label='Auth user UUID')
    role_id = forms.ChoiceField(label='Role')

    def __init__(self, *args, roles=(), **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.fields['role_id'].choices = [
            (str(role.id), role.name) for role in roles
        ]
