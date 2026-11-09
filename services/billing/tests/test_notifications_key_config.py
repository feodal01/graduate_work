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
