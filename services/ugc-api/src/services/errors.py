class NotFoundError(Exception):
    """Запрошенный объект не существует."""


class ForbiddenError(Exception):
    """Объект принадлежит другому пользователю."""


class ConflictError(Exception):
    """Объект уже существует."""


class WritesPausedError(Exception):
    """Изменение рецензий временно приостановлено для исходного снимка."""
