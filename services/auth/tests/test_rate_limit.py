import pytest

from services.errors import RateLimitExceededError
from services.rate_limit import RateLimitPolicy


@pytest.mark.parametrize(
    ('field_name', 'invalid_value'),
    [
        ('rate', 0),
        ('window_seconds', -1),
        ('capacity', 0),
    ],
)
def test_policy_rejects_non_positive_numeric_values(
    field_name: str,
    invalid_value: int,
) -> None:
    """A bucket with a non-positive bound cannot make a valid decision."""
    values = {
        'name': 'login',
        'rate': 5,
        'window_seconds': 60,
        'capacity': 5,
    }
    values[field_name] = invalid_value

    with pytest.raises(ValueError):
        RateLimitPolicy(**values)


@pytest.mark.parametrize('name', ['', '   '])
def test_policy_rejects_blank_name(name: str) -> None:
    """A blank policy name cannot produce a stable Redis namespace."""
    with pytest.raises(ValueError):
        RateLimitPolicy(
            name=name,
            rate=5,
            window_seconds=60,
            capacity=5,
        )


def test_exceeded_error_keeps_positive_retry_after() -> None:
    """The API layer can preserve the bucket wait time in HTTP headers."""
    error = RateLimitExceededError(retry_after_seconds=12)

    assert error.retry_after_seconds == 12


@pytest.mark.parametrize('retry_after_seconds', [0, -1, True])
def test_exceeded_error_rejects_invalid_retry_after(
    retry_after_seconds: int,
) -> None:
    """A rejected request always reports a positive integer wait time."""
    with pytest.raises(ValueError):
        RateLimitExceededError(retry_after_seconds=retry_after_seconds)
