from datetime import date

import pytest

from services.film_access import can_read_during_auth_failure


@pytest.mark.parametrize(
    ('creation_date', 'expected'),
    (
        (None, False),
        (date(2023, 8, 16), False),
        (date(2023, 8, 15), True),
        (date(2020, 1, 1), True),
    ),
)
def test_auth_failure_policy_uses_three_calendar_year_boundary(
    creation_date: date | None,
    expected: bool,
) -> None:
    assert (
        can_read_during_auth_failure(
            creation_date,
            today=date(2026, 8, 15),
        )
        is expected
    )


def test_auth_failure_policy_handles_leap_day() -> None:
    assert (
        can_read_during_auth_failure(
            date(2021, 2, 28),
            today=date(2024, 2, 29),
        )
        is True
    )
    assert (
        can_read_during_auth_failure(
            date(2021, 3, 1),
            today=date(2024, 2, 29),
        )
        is False
    )
