from datetime import date


def can_read_during_auth_failure(
    creation_date: date | None,
    *,
    today: date,
) -> bool:
    """Разрешает только фильм с подтверждённым возрастом от трёх лет."""
    if creation_date is None:
        return False
    return creation_date <= _calendar_years_ago(today, 3)


def _calendar_years_ago(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)
