class LoginAlreadyTakenError(Exception):
    """Уникальный логин уже принадлежит другому пользователю."""


class RoleAlreadyExistsError(Exception):
    """Роль с таким именем уже существует."""


class RoleNotFoundError(Exception):
    """Роль не найдена."""


class UserNotFoundError(Exception):
    """Пользователь не найден."""


class RoleAlreadyAssignedError(Exception):
    """Роль уже назначена пользователю."""


class RoleAssignmentNotFoundError(Exception):
    """Назначение роли не найдено."""


class UnknownPermissionError(Exception):
    """Permission-код отсутствует в системном каталоге."""
