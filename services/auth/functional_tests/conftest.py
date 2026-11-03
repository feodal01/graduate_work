import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest


@dataclass(frozen=True, slots=True)
class FunctionalSettings:
    base_url: str
    service_api_key: str
    superuser_login: str
    superuser_password: str


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise pytest.UsageError(
            f'{name} is not set; run tests through ./functional_tests/run.sh',
        )
    return value


@pytest.fixture
def functional_settings() -> FunctionalSettings:
    """Читает адрес и тестовые учётные данные из functional.env."""
    return FunctionalSettings(
        base_url=_required_environment('AUTH_FUNCTIONAL_BASE_URL'),
        service_api_key=_required_environment('SERVICE_API_KEY'),
        superuser_login=_required_environment(
            'AUTH_FUNCTIONAL_SUPERUSER_LOGIN',
        ),
        superuser_password=_required_environment(
            'AUTH_FUNCTIONAL_SUPERUSER_PASSWORD',
        ),
    )


@pytest.fixture
async def http_client(
    functional_settings: FunctionalSettings,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Создаёт HTTP-клиент к запущенному контейнеру сервиса."""
    async with httpx.AsyncClient(
        base_url=functional_settings.base_url,
        timeout=10,
        headers={'X-Request-Id': 'auth-functional-request'},
    ) as client:
        yield client
