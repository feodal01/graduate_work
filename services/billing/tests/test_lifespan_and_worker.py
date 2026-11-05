import asyncio
import signal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

import main
from core.config import PostgresSettings, WorkerSettings
from db.sql.postgres_health import PostgresHealthCheck
from security.service_credentials import ServiceCredentials
from worker import BillingWorkerRunner, install_signal_handlers, run_periodic_loop


def test_postgres_and_worker_settings_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Настройки БД и воркера имеют безопасные значения по умолчанию."""
    monkeypatch.setenv('POSTGRES_USER', 'billing')
    monkeypatch.setenv('POSTGRES_PASSWORD', 'local-billing-password')
    monkeypatch.setenv('POSTGRES_DB', 'billing')
    pg = PostgresSettings()
    worker_cfg = WorkerSettings()

    assert pg.dsn.startswith('postgresql+asyncpg://')
    assert worker_cfg.reconcile_min_age_seconds >= 0
    assert worker_cfg.billing_reconcile_interval_seconds > 0
    assert worker_cfg.billing_outbox_interval_seconds > 0


def test_resolve_env_file_inside_docker_container_root() -> None:
    """Внутри контейнера (/app/src/core/config.py) расчёт пути не даёт IndexError."""
    container_file = Path('/app/src/core/config.py')
    service_root = container_file.parents[2]
    repo_root = (
        service_root.parents[1] if len(service_root.parents) > 1 else service_root
    )
    assert service_root == Path('/app')
    assert repo_root == Path('/app')


@pytest.mark.asyncio
async def test_lifespan_registers_and_closes_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan регистрирует подключение к БД и закрывает его при остановке."""
    app = FastAPI()
    tracer_provider = MagicMock()
    app.state.tracer_provider = tracer_provider
    engine = MagicMock()
    database = MagicMock()
    database.engine = engine
    database.close = AsyncMock()
    database_type = MagicMock(return_value=database)

    token_settings = MagicMock()
    service_settings = MagicMock()
    service_settings.api_key.get_secret_value.return_value = (
        'test-internal-service-key-with-32-characters'
    )

    monkeypatch.setattr(main, 'PostgresDatabase', database_type)
    monkeypatch.setattr(main, 'TokenSettings', MagicMock(return_value=token_settings))
    monkeypatch.setattr(
        main,
        'ServiceSettings',
        MagicMock(return_value=service_settings),
    )

    async with main.lifespan(app):
        assert app.state.database is database
        assert isinstance(app.state.database_health, PostgresHealthCheck)
        assert isinstance(app.state.service_credentials, ServiceCredentials)
        assert app.state.service_credentials.verify(
            'test-internal-service-key-with-32-characters'
        )
        assert not app.state.service_credentials.verify('wrong-key')

    database.close.assert_awaited_once_with()
    tracer_provider.shutdown.assert_called_once_with()


@pytest.mark.asyncio
async def test_worker_runner_executes_initial_pass_and_stops_gracefully() -> None:
    """Фоновый воркер делает проход при старте и закрывает БД при остановке."""
    database = MagicMock()
    database.close = AsyncMock()
    tracer_provider = MagicMock()
    reconcile_calls = 0
    outbox_calls = 0

    runner_ref: list[BillingWorkerRunner] = []

    async def reconcile_step() -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1
        if outbox_calls >= 1 and runner_ref:
            runner_ref[0].request_stop()

    async def outbox_step() -> None:
        nonlocal outbox_calls
        outbox_calls += 1
        if reconcile_calls >= 1 and runner_ref:
            runner_ref[0].request_stop()

    runner = BillingWorkerRunner(
        database,
        reconcile_interval_seconds=60.0,
        outbox_interval_seconds=60.0,
        reconcile_step=reconcile_step,
        outbox_step=outbox_step,
        tracer_provider=tracer_provider,
    )
    runner_ref.append(runner)

    await asyncio.wait_for(runner.run(), timeout=2.0)

    assert reconcile_calls >= 1
    assert outbox_calls >= 1
    database.close.assert_awaited_once_with()
    tracer_provider.shutdown.assert_called_once_with()


@pytest.mark.asyncio
async def test_periodic_loop_survives_step_exception_and_signal_handlers() -> None:
    """run_periodic_loop не падает при ошибке в step() и повторяет цикл по таймауту."""
    stop_event = asyncio.Event()
    calls = 0

    async def flaky_step() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('transient failure in step')
        stop_event.set()

    await asyncio.wait_for(
        run_periodic_loop('flaky', flaky_step, 0.01, stop_event),
        timeout=1.0,
    )
    assert calls == 2

    # Проверяем регистрацию SIGINT и SIGTERM в install_signal_handlers
    database = MagicMock()
    database.close = AsyncMock()
    runner = BillingWorkerRunner(
        database,
        reconcile_interval_seconds=60.0,
        outbox_interval_seconds=60.0,
    )
    fake_loop = MagicMock()
    install_signal_handlers(fake_loop, runner)
    registered_signals = {
        call.args[0] for call in fake_loop.add_signal_handler.call_args_list
    }
    assert registered_signals == {signal.SIGINT, signal.SIGTERM}


def test_gateway_nginx_and_compose_configuration_match_tz_requirements() -> None:
    """Конфигурация nginx.conf, gateway.conf и compose.yaml покрывает требования GW."""
    repo_root = Path(__file__).resolve().parents[3]
    nginx_conf = (repo_root / 'infra' / 'nginx' / 'nginx.conf').read_text(
        encoding='utf-8'
    )
    gateway_conf = (
        repo_root / 'infra' / 'nginx' / 'conf.d' / 'gateway.conf'
    ).read_text(encoding='utf-8')
    compose_yaml = (repo_root / 'compose.yaml').read_text(encoding='utf-8')

    assert 'map $http_x_request_id $req_id' in nginx_conf
    assert 'proxy_set_header   X-Request-Id     $req_id;' in nginx_conf
    assert 'proxy_set_header   X-Forwarded-Proto $scheme;' in nginx_conf
    assert 'zone=billing_per_ip:10m rate=20r/s;' in nginx_conf

    assert 'set $billing_upstream billing-api:8000;' in gateway_conf
    assert 'location = /billing/health' in gateway_conf
    assert 'location = /billing {' in gateway_conf
    assert 'location ^~ /billing/' in gateway_conf
    assert 'location ^~ /api/v1/billing/' in gateway_conf
    assert 'location @billing_rate_limited' in gateway_conf
    assert '/api/v1/internal/billing' not in gateway_conf

    for service_name in (
        'billing-db:',
        'billing-migrate:',
        'billing-api:',
        'billing-worker:',
        'notifications-campaign-api:',
    ):
        assert service_name in compose_yaml
    assert 'healthcheck: {disable: true}' in compose_yaml
