"""Validate batch counts before starting relay or bootstrap."""

import pytest
from pydantic import ValidationError

from core.config import Settings


@pytest.mark.parametrize('name', ['relay_batch_size', 'bootstrap_batch_size'])
def test_batch_size_environment(monkeypatch, name):
    monkeypatch.setenv(name.upper(), '7')
    # Required mongo_uri is supplied by the test environment.
    settings = Settings()  # pyright: ignore[reportCallIssue]
    assert getattr(settings, name) == 7


@pytest.mark.parametrize('name', ['relay_batch_size', 'bootstrap_batch_size'])
@pytest.mark.parametrize('value', [0, -1, True, 1.5, 1.0])
def test_invalid_batch_sizes(name, value):
    with pytest.raises(ValidationError, match=name):
        Settings(**{name: value})
