"""Environment configuration without initializing Django or accessing its database."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class BootstrapSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix='SENTRY_',
        hide_input_in_errors=True,
    )

    admin_email: str = Field(min_length=1)
    admin_password: SecretStr = Field(min_length=1)
    dsn: SecretStr = SecretStr('')
