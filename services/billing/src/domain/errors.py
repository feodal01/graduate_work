class BillingError(Exception):
    """Доменная ошибка Биллинга: нарушено правило или недоступна зависимость.

    Слой данных и доменные сервисы сообщают только код правила и текст.
    HTTP-статус назначает слой API (`api.errors`), поэтому репозитории
    не зависят от веб-слоя.
    """

    def __init__(self, detail: str, *, error_code: str) -> None:
        self.detail = detail
        self.error_code = error_code
        super().__init__(detail)


class EntityNotFoundError(BillingError):
    """Сущность, над которой просят выполнить операцию, не найдена."""


class RuleConflictError(BillingError):
    """Операция противоречит текущему состоянию (инварианты 1, 4, 5 и др.)."""


class DependencyUnavailableError(BillingError):
    """Платёжный провайдер или другая зависимость временно недоступны."""
