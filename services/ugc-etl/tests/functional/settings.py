from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    """Адреса участников теста. Задаются переменными окружения."""

    etl_url: str = 'http://127.0.0.1:18000'
    etl_health_path: str = '/api/v1/health/'
    gateway_url: str = ''
    gateway_health_path: str = '/ugc-etl/health'

    kafka_bootstrap_servers: str = '127.0.0.1:19094'
    kafka_topic: str = 'ugc.events'
    kafka_dlq_topic: str = 'ugc.events.dlq'
    kafka_group_id: str = 'ugc-etl'

    clickhouse_host: str = '127.0.0.1'
    clickhouse_port: int = 18123
    clickhouse_database: str = 'ugc'
    clickhouse_user: str = 'ugc'
    clickhouse_password: str = 'functional-clickhouse-password'

    kafka_container: str = 'ugc-etl-ft-kafka'
    clickhouse_container: str = 'ugc-etl-ft-clickhouse-node1'
    etl_container: str = 'ugc-etl-ft-etl'

    wait_timeout: float = 180.0
    wait_interval: float = 1.0
    offset_hold_seconds: float = 8.0

    def bootstrap_servers(self) -> list[str]:
        return [item.strip() for item in self.kafka_bootstrap_servers.split(',') if item.strip()]


test_settings = TestSettings()
