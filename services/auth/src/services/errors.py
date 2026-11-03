class InvalidCredentialsError(Exception):
    """Логин или пароль не прошли проверку."""

    def __init__(self) -> None:
        super().__init__('Invalid credentials')


class YandexOAuthNotConfiguredError(Exception):
    """Yandex OAuth is disabled or has no complete configuration."""

    def __init__(self) -> None:
        super().__init__('Yandex OAuth is not configured')


class SocialProviderNotFoundError(Exception):
    """The requested social provider has no registered resolver."""

    def __init__(self, provider_name: str) -> None:
        super().__init__(f"Social provider '{provider_name}' is not supported")


class OAuthStateGenerationError(Exception):
    """A unique OAuth state could not be stored."""

    def __init__(self) -> None:
        super().__init__('OAuth state could not be created')


class InvalidOAuthStateError(Exception):
    """OAuth state is absent, expired, or already consumed."""

    def __init__(self) -> None:
        super().__init__('OAuth state is invalid or expired')


class InvalidOAuthCodeError(Exception):
    """The provider rejected the one-time authorization code."""

    def __init__(self) -> None:
        super().__init__('OAuth code is invalid or expired')


class SocialAuthenticationRejectedError(Exception):
    """The user or provider rejected the social login."""

    def __init__(self) -> None:
        super().__init__('Yandex authentication was cancelled')


class InvalidOAuthCallbackError(Exception):
    """The callback does not contain exactly one provider result."""

    def __init__(self) -> None:
        super().__init__('OAuth callback is invalid')


class SocialProviderUnavailableError(Exception):
    """The social provider cannot complete the current operation."""

    def __init__(self) -> None:
        super().__init__('Yandex OAuth is unavailable')


class SocialProviderContractError(Exception):
    """The social provider returned an invalid response."""

    def __init__(self) -> None:
        super().__init__('Yandex OAuth returned an invalid response')


class InvalidRefreshTokenError(Exception):
    """Refresh JWT не может быть обменян на новую пару."""

    def __init__(self) -> None:
        super().__init__('Invalid refresh token')


class AccessTokenExpiredError(Exception):
    """Access JWT истёк."""

    def __init__(self) -> None:
        super().__init__('Token expired')


class InvalidAccessTokenError(Exception):
    """Access JWT не прошёл строгую проверку."""

    def __init__(self) -> None:
        super().__init__('Invalid token')


class AccessTokenRevokedError(Exception):
    """Access JWT отозван denylist, epoch или session mutation."""

    def __init__(self) -> None:
        super().__init__('Token revoked')


class PermissionDeniedError(Exception):
    """Текущему пользователю не разрешена административная операция."""

    def __init__(self) -> None:
        super().__init__('Forbidden')


class InvalidServiceCredentialsError(Exception):
    """Внутренний вызывающий сервис не прошёл аутентификацию."""

    def __init__(self) -> None:
        super().__init__('Invalid service credentials')


class RateLimitExceededError(Exception):
    """The selected Auth rate-limit bucket has no available token."""

    def __init__(self, retry_after_seconds: int) -> None:
        if (
            isinstance(retry_after_seconds, bool)
            or not isinstance(retry_after_seconds, int)
            or retry_after_seconds <= 0
        ):
            raise ValueError('Retry-After must be a positive integer')
        self.retry_after_seconds = retry_after_seconds
        super().__init__('Too many requests')
