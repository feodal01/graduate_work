"""Configuration for the one-off Auth administrator bootstrap."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from validation.timezones import Timezone


class BootstrapSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix='AUTH_ADMIN_',
        hide_input_in_errors=True,
    )

    tz: Timezone
    login: str = Field(min_length=1)
    password: SecretStr = Field(min_length=1)
