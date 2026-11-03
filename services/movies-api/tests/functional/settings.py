from pydantic_settings import BaseSettings

MOVIES_INDEX_DEFAULT = 'movies_test'


class TestSettings(BaseSettings):
    """Адреса всех участников теста. Задаются переменными окружения."""

    api_url: str = 'http://127.0.0.1:8000'
    api_health_path: str = '/api/v1/health/'

    es_url: str = 'http://127.0.0.1:9200'
    # Приложение читает эту же переменную, поэтому индекс тестов и индекс API совпадают
    elasticsearch_index: str = MOVIES_INDEX_DEFAULT

    redis_host: str = '127.0.0.1'
    redis_port: int = 6379

    auth_url: str = 'http://127.0.0.1:8002/health'

    wait_timeout: float = 90.0
    wait_interval: float = 1.0


test_settings = TestSettings()
