from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import AnyHttpUrl, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

_SERVICE_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = (
    _SERVICE_ROOT.parents[1] if len(_SERVICE_ROOT.parents) > 1 else _SERVICE_ROOT
)
_ENV_FILE = (
    (_REPO_ROOT / '.env')
    if (_REPO_ROOT / '.env').exists()
    else (_SERVICE_ROOT / '.env')
)


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='POSTGRES_',
        extra='ignore',
    )

    user: str = 'billing'
    password: SecretStr = SecretStr('local-billing-password')
    host: str = 'localhost'
    port: int = 5432
    db: str = 'billing'
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    pool_timeout_seconds: float = Field(default=10.0, gt=0)
    command_timeout_seconds: float = Field(default=15.0, gt=0)
    statement_timeout_ms: int = Field(default=15_000, gt=0)

    @property
    def dsn(self) -> str:
        return URL.create(
            drivername='postgresql+asyncpg',
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.db,
        ).render_as_string(hide_password=False)


class TokenSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    jwt_secret: Annotated[SecretStr, Field(min_length=32)]
    jwt_algorithm: Literal['HS256'] = 'HS256'


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='SERVICE_',
        extra='ignore',
    )

    api_key: Annotated[SecretStr, Field(min_length=32)]


class NotificationsClientSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='NOTIFICATIONS_',
        extra='ignore',
    )

    campaign_url: AnyHttpUrl = AnyHttpUrl('http://notifications-campaign-api:8000')
    # Без значения по умолчанию: общеизвестный ключ позволил бы любому
    # процессу сети Compose слать «транзакционные» письма от имени Биллинга
    # и отменять их. Длина — как у SERVICE_API_KEY.
    campaign_api_key: Annotated[SecretStr, Field(min_length=32)]
    timeout_seconds: float = Field(default=5.0, gt=0)


class AuthClientSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='AUTH_',
        extra='ignore',
    )

    service_url: AnyHttpUrl = AnyHttpUrl('http://auth:8000')
    timeout_seconds: float = Field(default=3.0, gt=0)
    # Общий лимит на весь выход, включая обходной refresh (BIL-NFR-SEC-05).
    logout_timeout_seconds: float = Field(default=1.5, gt=0, le=5.0)


class YooKassaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='YOOKASSA_',
        extra='ignore',
    )

    shop_id: str = Field(
        default='replace-with-test-yookassa-shop-id',
        min_length=1,
    )
    secret_key: SecretStr = Field(
        default=SecretStr('replace-with-test-yookassa-secret-key'),
        min_length=1,
    )
    api_base_url: AnyHttpUrl = AnyHttpUrl('https://api.yookassa.ru/v3')
    timeout_seconds: float = Field(default=5.0, gt=0, le=5.0)
    # Розыск возврата по `metadata` (инвариант 14) читает список возвратов
    # платежа страницами. ЮKassa отдаёт не больше 100 записей за запрос.
    refund_list_page_size: int = Field(default=100, ge=1, le=100)
    # Сколько страниц просматривать, прежде чем признать обход незавершённым.
    refund_list_max_pages: int = Field(default=10, ge=1, le=1000)


class PaymentProvidersSettings(BaseSettings):
    """Платёжные провайдеры: активный для новых платежей и реквизиты каждого.

    Реквизиты у провайдеров разные, поэтому у каждого свой вложенный класс
    со своим префиксом переменных окружения (`YOOKASSA_*`, ...). Новый провайдер
    добавляется полем здесь и адаптером в реестре (BIL-NFR-S-02).
    """

    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='PAYMENT_PROVIDER_',
        extra='ignore',
    )

    active: str = Field(default='yookassa', min_length=1, max_length=32)
    yookassa: YooKassaSettings = Field(default_factory=YooKassaSettings)


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    billing_public_base_url: AnyHttpUrl = AnyHttpUrl('http://localhost')
    billing_reconcile_interval_seconds: float = Field(default=30.0, gt=0)
    reconcile_min_age_seconds: float = Field(default=5.0, ge=0)
    billing_outbox_interval_seconds: float = Field(default=2.0, gt=0)
    billing_outbox_max_attempts: int = Field(default=20, ge=1)
    # На сколько задача outbox закрепляется за обработчиком, пока он ждёт
    # ответ Auth или Notifications. Должно быть больше тайм-аутов этих клиентов.
    billing_outbox_lease_seconds: float = Field(default=60.0, gt=0)
    billing_stuck_threshold_seconds: int = Field(default=600, ge=1)
    # Ожидающий платёж с номером ЮKassa ждёт пользователя на странице оплаты
    # (ЮKassa держит его около часа, 2.5.4), поэтому зависшим он считается
    # только после этого порога (6.4, US-A-02).
    billing_stuck_waiting_payment_threshold_seconds: int = Field(default=4200, ge=1)
    # Возврат без номера ЮKassa у границы 24-часового ключа (инв. 14):
    # за сколько секунд до истечения ключа POST /v3/refunds больше не шлётся
    # (ответ у самой границы мог бы прийти после розыска по списку);
    # сколько пустых розысков подряд нужно для отказа «не создан» и пауза
    # между ними (возврат может появиться в списке ЮKassa не сразу).
    billing_refund_post_stop_before_key_expiry_seconds: float = Field(
        default=3600.0, ge=0, lt=24 * 3600
    )
    billing_refund_lookup_not_found_attempts: int = Field(default=2, ge=1)
    billing_refund_lookup_interval_seconds: float = Field(default=300.0, gt=0)
    # Пробное чтение ЮKassa по вебхуку с незнакомым номером (BIL-API-05):
    # вебхук без подписи может прислать кто угодно, поэтому один номер читается
    # не чаще раза в окно, а всех таких чтений в окне — не больше лимита.
    billing_webhook_probe_window_seconds: float = Field(default=600.0, gt=0)
    billing_webhook_probe_max_per_window: int = Field(default=20, ge=1)

    @model_validator(mode='after')
    def _waiting_payment_threshold_not_shorter(self) -> Self:
        if (
            self.billing_stuck_waiting_payment_threshold_seconds
            < self.billing_stuck_threshold_seconds
        ):
            raise ValueError(
                'billing_stuck_waiting_payment_threshold_seconds must not be less '
                'than billing_stuck_threshold_seconds'
            )
        return self


class TracingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='TRACING_',
        extra='ignore',
    )

    enabled: bool = False
    service_name: str = Field(default='billing', min_length=1)
    otlp_endpoint: str = Field(
        default='http://127.0.0.1:4318/v1/traces',
        min_length=1,
    )
    sample_rate: float = Field(default=0.05, ge=0.0, le=1.0)


class SentrySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='SENTRY_',
        extra='ignore',
    )

    dsn: str = ''
    environment: str = 'local'
    traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0)


class LogstashSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='LOGSTASH_',
        extra='ignore',
    )

    host: str = ''
    port: int = Field(default=5044, ge=1, le=65535)


class PaginationSettings(BaseSettings):
    """Размер страницы в постраничных списках: внутреннее API и история платежей."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='PAGINATION_',
        extra='ignore',
    )

    default_page_size: int = Field(default=50, ge=1)
    max_page_size: int = Field(default=100, ge=1)

    @model_validator(mode='after')
    def _default_fits_max(self) -> Self:
        if self.default_page_size > self.max_page_size:
            raise ValueError('default_page_size must not exceed max_page_size')
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    project_name: str = 'billing service'

    postgres: PostgresSettings
    auth_client: AuthClientSettings
    payment_providers: PaymentProvidersSettings
    worker: WorkerSettings
    tracing: TracingSettings
    sentry: SentrySettings = Field(default_factory=SentrySettings)
    logstash: LogstashSettings = Field(default_factory=LogstashSettings)
    pagination: PaginationSettings = Field(default_factory=PaginationSettings)


settings = Settings(
    postgres=PostgresSettings(),
    auth_client=AuthClientSettings(),
    payment_providers=PaymentProvidersSettings(),
    worker=WorkerSettings(),
    tracing=TracingSettings(),
)
