"""Database-only settings for migrations, independent of application processes."""

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NOTIFICATIONS_", extra="forbid", hide_input_in_errors=True
    )

    database_url: SecretStr

    @field_validator("database_url")
    @classmethod
    def valid_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            make_url(value.get_secret_value())
        except (ArgumentError, ValueError):
            raise ValueError("database_url must be a valid SQLAlchemy database URL") from None
        return value
