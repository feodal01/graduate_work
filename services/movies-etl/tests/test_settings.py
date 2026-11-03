from typing import Any

import pytest
from pydantic import ValidationError

from postgres_to_es import db
from postgres_to_es.settings import Settings


@pytest.mark.parametrize(
    "name", ["catalog_batch_size", "catalog_relay_batch_size", "catalog_legacy_batch_size"]
)
def test_batch_size_environment(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(name.upper(), "7")
    # BaseSettings загружает поля из окружения и принимает _env_file во время выполнения.
    value = Settings(_env_file=None)  # pyright: ignore[reportCallIssue]
    assert getattr(value, name) == 7


@pytest.mark.parametrize(
    "name", ["catalog_batch_size", "catalog_relay_batch_size", "catalog_legacy_batch_size"]
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5, 1.0])
def test_invalid_batch_sizes(name: str, value: Any) -> None:
    with pytest.raises(ValidationError, match=name):
        # Pyright не видит служебный аргумент BaseSettings._env_file.
        Settings(_env_file=None, **{name: value})  # pyright: ignore[reportCallIssue]


def test_settings_repr_and_missing_fields_hide_password(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "ELASTICSEARCH_URL",
        "ELASTICSEARCH_INDEX",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValidationError) as error:
        # Намеренно пропускаем обязательные поля для проверки ошибки валидации.
        Settings(_env_file=None, postgres_password="FAKE32")  # pyright: ignore[reportCallIssue]

    assert "FAKE32" not in str(error.value)
    assert "input_value" not in str(error.value)


def test_database_boundary_preserves_environment_password(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "fake@:/?#%"
    monkeypatch.setenv("POSTGRES_PASSWORD", secret)
    # Остальные поля BaseSettings поступают из окружения теста.
    value = Settings(_env_file=None)  # pyright: ignore[reportCallIssue]
    monkeypatch.setattr(db, "settings", value)

    assert secret not in repr(value)
    assert value.postgres_password.get_secret_value() == secret
    assert db.get_postgres_dsl()["password"] == secret


def test_invalid_nested_settings_hide_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "FAKE32")
    with pytest.raises(ValidationError) as error:
        # Обязательные поля BaseSettings поступают из окружения теста.
        Settings(_env_file=None, postgres_backoff={"max_tries": 0})  # pyright: ignore[reportCallIssue]

    assert "FAKE32" not in str(error.value)
    assert "input_value" not in str(error.value)
