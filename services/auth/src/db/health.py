from abc import ABC, abstractmethod


class HealthCheck(ABC):
    """Проверка доступности бэкенда."""

    @abstractmethod
    async def ping(self) -> bool: ...
