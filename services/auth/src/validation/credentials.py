from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated

from pydantic import AfterValidator
from zxcvbn import zxcvbn

MAX_PASSWORD_LENGTH = 72


class CredentialValidationError(ValueError):
    """Логин или пароль не соответствуют публичному контракту."""


def validate_login(value: str) -> str:
    """Проверить и нормализовать регистрозависимый логин."""
    normalized = value.strip()
    if not 3 <= len(normalized) <= 255:
        raise CredentialValidationError(
            'login must contain from 3 to 255 characters',
        )
    if any(character.isspace() for character in normalized):
        raise CredentialValidationError('login must not contain whitespace')
    return normalized


def validate_password_input(value: str) -> str:
    """Ограничить стоимость последующей проверки техническим пределом."""
    if len(value) > MAX_PASSWORD_LENGTH:
        raise CredentialValidationError(
            'password must contain no more than 72 characters',
        )
    return value


@dataclass(frozen=True, slots=True)
class PasswordPolicy:
    min_length: int
    min_score: int

    def __post_init__(self) -> None:
        if not 1 <= self.min_length <= MAX_PASSWORD_LENGTH:
            raise ValueError('min_length must be between 1 and 72')
        if not 0 <= self.min_score <= 4:
            raise ValueError('min_score must be between 0 and 4')

    def validate(
        self,
        value: str,
        *,
        login: str | None = None,
        user_inputs: Iterable[str | None] = (),
    ) -> str:
        """Проверить длину и практическую угадываемость нового пароля."""
        validate_password_input(value)
        if len(value) < self.min_length:
            raise CredentialValidationError(
                f'password must contain at least {self.min_length} characters',
            )
        if login is not None and value.casefold() == login.casefold():
            raise CredentialValidationError('password must not match login')

        context = [item for item in (login, *user_inputs) if item is not None]
        result = zxcvbn(value, user_inputs=context)
        if result['score'] < self.min_score:
            raise CredentialValidationError('password is too easy to guess')
        return value


Login = Annotated[str, AfterValidator(validate_login)]
Password = Annotated[str, AfterValidator(validate_password_input)]
