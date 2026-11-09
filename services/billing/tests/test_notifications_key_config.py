"""Ключ Campaign API Notifications обязателен и не короче 32 символов."""

import pytest
from pydantic import ValidationError

from core.config import NotificationsClientSettings


def test_campaign_key_has_no_public_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Регрессия: по умолчанию стоял общеизвестный `local-demo-campaign-key`."""
    monkeypatch.delenv('NOTIFICATIONS_CAMPAIGN_API_KEY', raising=False)

    with pytest.raises(ValidationError):
        NotificationsClientSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_campaign_key_must_be_long(monkeypatch: pytest.MonkeyPatch) -> None:
    """Короткий ключ отклоняется так же, как короткий SERVICE_API_KEY."""
    monkeypatch.setenv('NOTIFICATIONS_CAMPAIGN_API_KEY', 'local-demo-campaign-key')

    with pytest.raises(ValidationError):
        NotificationsClientSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_migrator_imports_config_without_campaign_key(tmp_path: object) -> None:
    """billing-migrate получает только переменные Postgres.

    Регрессия: обязательный ключ Notifications в глобальном `settings` ронял
    импорт `core.config`, и `alembic upgrade head` в Docker падал на старте.
    """
    import os
    import subprocess
    import sys
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / 'src'
    env = {
        key: value
        for key, value in os.environ.items()
        if key != 'NOTIFICATIONS_CAMPAIGN_API_KEY'
    }
    # Пустая переменная перекрывает локальный .env: ключа у мигратора нет.
    env['NOTIFICATIONS_CAMPAIGN_API_KEY'] = ''
    env['PYTHONPATH'] = str(src)
    result = subprocess.run(  # noqa: S603
        [sys.executable, '-c', 'import core.config'],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
