class CacheUnavailableError(Exception):
    """Кэш недоступен."""

    def __init__(self, message: str = 'Cache is unavailable'):
        self.message = message
        super().__init__(self.message)


class SessionEpochMismatchError(Exception):
    """Access token belongs to an already revoked session epoch."""


class RateLimitStoreUnavailableError(Exception):
    """Redis cannot evaluate the rate-limit transition."""

    def __init__(self) -> None:
        super().__init__('Rate-limit store is unavailable')


class OAuthStateStoreUnavailableError(Exception):
    """Redis cannot save or consume an OAuth state."""

    def __init__(self) -> None:
        super().__init__('OAuth state store is unavailable')
