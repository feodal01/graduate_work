from secrets import compare_digest


class ServiceCredentials:
    """Constant-time verification for trusted internal service calls."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def verify(self, provided_key: str | None) -> bool:
        if provided_key is None:
            return False
        return compare_digest(provided_key, self._api_key)
