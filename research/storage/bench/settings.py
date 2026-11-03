"""Настройки соединений исследовательского стенда из переменных окружения."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(hide_input_in_errors=True)

    mongo_uri: SecretStr = SecretStr(
        'mongodb://127.0.0.1:27117/?replicaSet=rs0&directConnection=true',
    )
    postgres_dsn: SecretStr = SecretStr('postgresql://research:research@127.0.0.1:55432/ugc')
