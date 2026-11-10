"""Архитектурные ограничения Биллинга: не дают вернуть уже найденные ошибки.

Каждый тест ловит конкретный класс ошибок, найденный на ревью:
- репозиторий и доменные сервисы зависели от веб-слоя (`api.errors`, HTTP-коды);
- сверка вызывала методы возвратов через `getattr`, и mypy их не проверял;
- протоколы, SQL-реализации и фейки расходились по сигнатурам;
- один репозиторий разросся до 2700 строк и 35 методов.
"""

import ast
import inspect
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Mapped

from api.errors import register_exception_handlers
from domain.errors import (
    BillingError,
    DependencyUnavailableError,
    EntityNotFoundError,
    RuleConflictError,
)
from models import OutboxTask, Payment, Refund
from repositories.admin_queries import AdminQueries, SQLAlchemyAdminQueries
from repositories.bundle import BillingRepositories
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
from repositories.records import (
    OutboxTaskRecordProtocol,
    PaymentRecordProtocol,
    RefundRecordProtocol,
)
from repositories.refunds import RefundRepository, SQLAlchemyRefundRepository
from repositories.user_views import SQLAlchemyUserBillingViews, UserBillingViews
from tests.fakes import InMemoryPurchaseRepository

SRC = Path(__file__).resolve().parents[1] / 'src'

# Слои ниже API не знают про HTTP: статус ответа назначает только api/.
LOWER_LAYERS = ('domain', 'repositories', 'services', 'clients')
WEB_MODULES = ('api', 'fastapi', 'starlette', 'dependencies', 'main')

PROTOCOL_IMPLEMENTATIONS: list[tuple[type, type]] = [
    (PaymentRepository, SQLAlchemyPaymentRepository),
    (RefundRepository, SQLAlchemyRefundRepository),
    (OutboxRepository, SQLAlchemyOutboxRepository),
    (OutboxAdminRepository, SQLAlchemyOutboxRepository),
    (UserBillingViews, SQLAlchemyUserBillingViews),
    (AdminQueries, SQLAlchemyAdminQueries),
    (OperationalMetricsRepository, SQLAlchemyOperationalMetricsRepository),
]

MAX_REPOSITORY_MODULE_LINES = 700
MAX_PROTOCOL_METHODS = 15


def _python_files(*packages: str) -> list[Path]:
    return sorted(path for pkg in packages for path in (SRC / pkg).rglob('*.py'))


def _imported_roots(path: Path) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split('.')[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split('.')[0])
    return roots


def _protocol_methods(protocol: type) -> dict[str, Any]:
    return {
        name: member
        for name, member in vars(protocol).items()
        if inspect.iscoroutinefunction(member) and not name.startswith('_')
    }


def _params(func: Any) -> list[tuple[str, Any, Any]]:
    return [
        (p.name, p.kind, p.default)
        for p in inspect.signature(func).parameters.values()
        if p.name != 'self'
    ]


@pytest.mark.parametrize(
    'path',
    _python_files(*LOWER_LAYERS),
    ids=lambda p: str(p.relative_to(SRC)),
)
def test_lower_layers_do_not_import_web_layer(path: Path) -> None:
    """Репозитории, домен, сервисы и клиенты не импортируют api/FastAPI."""
    leaked = _imported_roots(path) & set(WEB_MODULES)
    assert not leaked, f'{path.relative_to(SRC)} imports web layer: {leaked}'


@pytest.mark.parametrize(
    'path',
    _python_files('services'),
    ids=lambda p: str(p.relative_to(SRC)),
)
def test_services_do_not_reach_collaborators_via_getattr(path: Path) -> None:
    """`getattr(self._repo, 'method')` прячет вызов от mypy и протокола."""
    offenders = [
        node.lineno
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == 'getattr'
        and node.args
        and isinstance(node.args[0], ast.Attribute)
        and isinstance(node.args[0].value, ast.Name)
        and node.args[0].value.id == 'self'
    ]
    assert not offenders, f'{path.relative_to(SRC)}: getattr on self.* at {offenders}'


@pytest.mark.parametrize(
    ('protocol', 'implementation'),
    PROTOCOL_IMPLEMENTATIONS,
    ids=lambda t: t.__name__,
)
def test_sql_repositories_match_their_protocols(
    protocol: type,
    implementation: type,
) -> None:
    """SQL-реализация повторяет сигнатуры протокола: имена, виды и умолчания."""
    for name, method in _protocol_methods(protocol).items():
        impl = getattr(implementation, name, None)
        assert impl is not None, f'{implementation.__name__} lacks {name}'
        assert _params(impl) == _params(method), f'{implementation.__name__}.{name}'


@pytest.mark.parametrize(
    'protocol',
    [protocol for protocol, _ in PROTOCOL_IMPLEMENTATIONS],
    ids=lambda t: t.__name__,
)
def test_in_memory_fake_matches_repository_protocols(protocol: type) -> None:
    """Фейк для юнит-тестов не расходится с протоколами, которые подменяет."""
    for name, method in _protocol_methods(protocol).items():
        fake = getattr(InMemoryPurchaseRepository, name, None)
        assert fake is not None, f'fake lacks {protocol.__name__}.{name}'
        expected = [(n, k) for n, k, _ in _params(method)]
        actual = [(n, k) for n, k, _ in _params(fake)]
        assert actual == expected, f'fake {name}: {actual} != {expected}'


def test_bundle_wires_every_repository_role() -> None:
    """Каждая роль в BillingRepositories заполнена своей SQL-реализацией."""
    repos = BillingRepositories.sqlalchemy(session_factory=None)  # type: ignore[arg-type]
    expected = {
        'payments': SQLAlchemyPaymentRepository,
        'refunds': SQLAlchemyRefundRepository,
        'outbox': SQLAlchemyOutboxRepository,
        'outbox_admin': SQLAlchemyOutboxRepository,
        'user_views': SQLAlchemyUserBillingViews,
        'admin_queries': SQLAlchemyAdminQueries,
        'metrics': SQLAlchemyOperationalMetricsRepository,
    }
    assert {f: type(getattr(repos, f)) for f in expected} == expected


@pytest.mark.parametrize(
    'path',
    _python_files('repositories'),
    ids=lambda p: p.name,
)
def test_repository_modules_stay_small(path: Path) -> None:
    """Лимит не даёт снова собрать все запросы в один модуль на тысячи строк."""
    lines = len(path.read_text().splitlines())
    assert lines <= MAX_REPOSITORY_MODULE_LINES, (
        f'{path.name}: {lines} lines > {MAX_REPOSITORY_MODULE_LINES}; '
        'split it by use case'
    )


@pytest.mark.parametrize(
    'protocol',
    [protocol for protocol, _ in PROTOCOL_IMPLEMENTATIONS],
    ids=lambda t: t.__name__,
)
def test_repository_protocols_stay_narrow(protocol: type) -> None:
    """Протокол описывает одну роль, а не «все запросы Биллинга»."""
    count = len(_protocol_methods(protocol))
    assert count <= MAX_PROTOCOL_METHODS, f'{protocol.__name__}: {count} methods'


@pytest.mark.parametrize(
    ('error', 'status_code'),
    [
        (EntityNotFoundError('Платёж не найден', error_code='payment_not_found'), 404),
        (RuleConflictError('Подписка уже активна', error_code='sub_active'), 409),
        (DependencyUnavailableError('ЮKassa недоступна', error_code='psp_down'), 503),
    ],
    ids=lambda v: type(v).__name__ if isinstance(v, BillingError) else str(v),
)
async def test_domain_errors_map_to_http_only_in_api_layer(
    error: BillingError,
    status_code: int,
) -> None:
    """JSON API и HTML-экраны получают статус и код из доменной ошибки."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get('/api/v1/billing/probe')
    async def api_probe() -> None:
        raise error

    @app.get('/billing/probe')
    async def ui_probe() -> None:
        raise error

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url='http://t') as client:
        api = await client.get('/api/v1/billing/probe')
        ui = await client.get('/billing/probe')

    assert api.status_code == status_code
    assert api.json() == {'error_code': error.error_code, 'detail': error.detail}
    assert ui.status_code == status_code
    assert error.detail in ui.text


@pytest.mark.parametrize(
    ('model', 'protocol'),
    [
        (Payment, PaymentRecordProtocol),
        (Refund, RefundRecordProtocol),
        (OutboxTask, OutboxTaskRecordProtocol),
    ],
    ids=lambda t: t.__name__,
)
def test_orm_models_satisfy_record_protocols(model: type, protocol: type) -> None:
    """Колонки ORM-модели совпадают с протоколом записи по имени, типу и NULL.

    pyright не сверяет `Mapped[T]` с протоколом, поэтому в `bundle.py` и
    `outbox.py` его проверка отключена; этот тест закрывает пробел.
    """
    columns: Any = sa_inspect(model).columns
    model_hints = get_type_hints(model)
    for name, annotation in get_type_hints(protocol).items():
        assert name in columns, f'{model.__name__} lacks column {name}'
        mapped = model_hints[name]
        assert get_origin(mapped) is Mapped, f'{model.__name__}.{name}: {mapped}'
        (orm_type,) = get_args(mapped)
        assert orm_type == annotation, (
            f'{model.__name__}.{name}: Mapped[{orm_type}] != protocol {annotation}'
        )
        nullable = type(None) in get_args(annotation)
        assert columns[name].nullable == nullable, (
            f'{model.__name__}.{name}: nullable={columns[name].nullable}'
        )


# Значения тарифа живут только в таблице `tariffs` (сид — в миграции).
TARIFF_LITERALS = ('Премиум', '299', '29900', '29_900', '30 дней')
TARIFF_CONSTANTS = (
    'DEFAULT_TARIFF_TITLE',
    'DEFAULT_TARIFF_PRICE_KOPEKS',
    'DEFAULT_TARIFF_CURRENCY',
    'DEFAULT_TARIFF_DURATION_DAYS',
)


@pytest.mark.parametrize(
    'path',
    [
        p
        for p in sorted(SRC.rglob('*.py'))
        if 'migrations' not in p.parts
        and p != SRC / 'models' / 'entity.py'
        and p != SRC / 'models' / '__init__.py'
    ],
    ids=lambda p: str(p.relative_to(SRC)),
)
def test_tariff_values_are_not_hardcoded(path: Path) -> None:
    """Название, цена и срок тарифа не зашиты в код: смена строки в `tariffs`.

    Регрессия: «Премиум», «30 дней» и «299 ₽» стояли в описании платежа,
    письмах и на экране, и расходились с тарифом в БД.
    """
    tree = ast.parse(path.read_text())
    literals = [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and any(bad in node.value for bad in TARIFF_LITERALS)
    ]
    numbers = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, int)
        and not isinstance(node.value, bool)
        and node.value == 29_900
    ]
    names = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Name | ast.alias)
        and getattr(node, 'id', getattr(node, 'name', None)) in TARIFF_CONSTANTS
    ]
    assert not literals and not numbers and not names, (
        f'{path.relative_to(SRC)}: tariff hardcoded at {literals} {numbers} {names}'
    )
