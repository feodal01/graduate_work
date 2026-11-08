from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from repositories.admin_queries import AdminQueries, SQLAlchemyAdminQueries
from repositories.metrics import (
    OperationalMetricsRepository,
    SQLAlchemyOperationalMetricsRepository,
)
from repositories.outbox import (
    OutboxAdminRepository,
    OutboxRepository,
    SQLAlchemyOutboxRepository,
)
from repositories.payments import PaymentRepository, SQLAlchemyPaymentRepository
from repositories.refunds import RefundRepository, SQLAlchemyRefundRepository
from repositories.user_views import SQLAlchemyUserBillingViews, UserBillingViews


@dataclass(frozen=True, slots=True)
class BillingRepositories:
    """Набор репозиториев Биллинга с общей фабрикой сессий.

    Каждый сервис получает только нужные ему репозитории. Атомарные цепочки
    («оплата прошла → подписка, версия доступа, outbox») живут внутри одного
    метода репозитория, поэтому общая транзакция между репозиториями не нужна.
    """

    payments: PaymentRepository
    refunds: RefundRepository
    outbox: OutboxRepository
    outbox_admin: OutboxAdminRepository
    user_views: UserBillingViews
    admin_queries: AdminQueries
    metrics: OperationalMetricsRepository

    @classmethod
    def sqlalchemy(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> 'BillingRepositories':
        # pyright не применяет дескриптор `Mapped[T]` при сверке ORM-модели с
        # протоколом записи (видит `Mapped[UUID]` вместо `UUID`). Совместимость
        # проверяют mypy (он понимает дескриптор) и
        # test_orm_models_satisfy_record_protocols.
        outbox = SQLAlchemyOutboxRepository(session_factory)
        return cls(
            payments=SQLAlchemyPaymentRepository(session_factory),  # pyright: ignore[reportArgumentType]
            refunds=SQLAlchemyRefundRepository(session_factory),  # pyright: ignore[reportArgumentType]
            outbox=outbox,
            outbox_admin=outbox,
            user_views=SQLAlchemyUserBillingViews(session_factory),
            admin_queries=SQLAlchemyAdminQueries(session_factory),
            metrics=SQLAlchemyOperationalMetricsRepository(session_factory),
        )
