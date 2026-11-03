"""Pure recipient-local send-window calculation; all returned times are UTC."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from notifications.config import TimingSettings
from notifications.delivery.routing import SendClass


def next_send_at(
    send_class: SendClass,
    tz: str | None,
    not_before: datetime,
    *,
    timing: TimingSettings,
) -> datetime:
    """Keep urgent mail immediate and defer daytime mail without guessing its zone."""
    if not_before.utcoffset() is None:
        raise ValueError("not_before must be timezone-aware")
    moment = not_before.astimezone(UTC)
    if send_class == "transactional":
        return moment
    try:
        if not tz:
            raise ValueError("missing recipient timezone")
        zone = ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("invalid_timezone") from None
    local = moment.astimezone(zone)
    start, end = timing.send_window_start, timing.send_window_end
    if start <= local.time() < end:
        return moment

    # Try both folds. Round-trip each boundary through UTC: a nonexistent local
    # start can map outside the allowed window, in which case that day is skipped.
    day = local.date()
    while True:
        candidates: list[datetime] = []
        for fold in (0, 1):
            boundary = datetime.combine(day, start, tzinfo=zone).replace(fold=fold)
            candidate = boundary.astimezone(UTC)
            actual = candidate.astimezone(zone)
            if candidate >= moment and start <= actual.time() < end:
                candidates.append(candidate)
        if candidates:
            due = min(candidates)
            break
        day += timedelta(days=1)

    # On an offset transition a gap can open the window later than its nominal
    # start, or a repeated hour can reopen it before tomorrow's boundary. Scan
    # real UTC minutes only on these rare days; ordinary days take constant time.
    if local.utcoffset() != due.astimezone(zone).utcoffset():
        cursor = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        while cursor < due:
            if start <= cursor.astimezone(zone).time() < end:
                return cursor
            cursor += timedelta(minutes=1)
    return due
