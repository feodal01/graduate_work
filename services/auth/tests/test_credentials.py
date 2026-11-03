import pytest

from validation.credentials import (
    CredentialValidationError,
    PasswordPolicy,
    validate_login,
)


@pytest.fixture
def password_policy() -> PasswordPolicy:
    """Создаёт политику паролей с тестовыми ограничениями."""
    return PasswordPolicy(min_length=8, min_score=3)


def test_login_is_trimmed_and_remains_case_sensitive() -> None:
    """Удаление trim или lower-casing логина должно сломать контракт."""
    assert validate_login('  Alice  ') == 'Alice'


@pytest.mark.parametrize('value', ['ab', 'a b', 'a\tb'])
def test_invalid_login_is_rejected(value: str) -> None:
    """Слишком короткий логин или whitespace внутри не проходят."""
    with pytest.raises(ValueError):
        validate_login(value)


@pytest.mark.parametrize('value', ['short', 'x' * 73])
def test_invalid_password_is_rejected(
    value: str,
    password_policy: PasswordPolicy,
) -> None:
    """Пароль вне согласованных границ длины не проходит."""
    with pytest.raises(ValueError):
        password_policy.validate(value)


@pytest.mark.parametrize('value', ['password123!', 'qwertyuiop'])
def test_guessable_password_is_rejected(
    value: str,
    password_policy: PasswordPolicy,
) -> None:
    """Длинный, но легко угадываемый пароль не должен пройти policy."""
    with pytest.raises(CredentialValidationError, match='too easy to guess'):
        password_policy.validate(value)


def test_valid_password_is_returned_without_normalization(
    password_policy: PasswordPolicy,
) -> None:
    """Пароль нельзя незаметно обрезать или менять при валидации."""
    password = 'Plasma!Tundra7-Cobalt-River'

    assert password_policy.validate(password) == password


def test_password_policy_uses_configured_minimum_length() -> None:
    """Политика применяет заданную минимальную длину пароля."""
    policy = PasswordPolicy(min_length=20, min_score=0)
    with pytest.raises(CredentialValidationError, match='20'):
        policy.validate('Plasma!Tundra7')


def test_password_policy_uses_configured_minimum_score() -> None:
    """Политика применяет заданный минимальный балл zxcvbn."""
    weak_password = 'password123!'
    permissive = PasswordPolicy(min_length=8, min_score=0)
    strict = PasswordPolicy(min_length=8, min_score=3)

    assert permissive.validate(weak_password) == weak_password
    with pytest.raises(CredentialValidationError, match='too easy to guess'):
        strict.validate(weak_password)
