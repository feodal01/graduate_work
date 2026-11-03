"""Recipient-local daytime boundaries, independent of the wall clock."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from notifications.config import TimingSettings
from notifications.scheduling.windows import next_send_at


@pytest.mark.parametrize("send_class", ["service", "bulk"])
@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        ("2026-01-01T00:00:00+00:00", "2026-01-01T07:00:00+00:00"),
        ("2026-01-01T06:59:59+00:00", "2026-01-01T07:00:00+00:00"),
        ("2026-01-01T07:00:00+00:00", "2026-01-01T07:00:00+00:00"),
        ("2026-01-01T16:59:59+00:00", "2026-01-01T16:59:59+00:00"),
        ("2026-01-01T17:00:00+00:00", "2026-01-02T07:00:00+00:00"),
        ("2026-01-01T22:00:00+00:00", "2026-01-02T07:00:00+00:00"),
    ],
)
def test_daytime_boundaries(send_class, moment, expected):
    due = next_send_at(
        send_class,
        "Europe/Moscow",
        datetime.fromisoformat(moment),
        timing=TimingSettings(),
    )
    assert due == datetime.fromisoformat(expected)
    assert due.tzinfo is UTC


@pytest.mark.parametrize("tz", [None, "", "not-a-zone", "/Europe/Moscow", "../UTC"])
@pytest.mark.parametrize("send_class", ["service", "bulk"])
def test_invalid_timezone_defers_without_fallback(tz, send_class):
    now = datetime(2026, 1, 1, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="invalid_timezone"):
        next_send_at(send_class, tz, now, timing=TimingSettings())


@pytest.mark.parametrize("tz", [None, "invalid", "Europe/Moscow"])
def test_transactional_is_immediate_and_normalizes_utc(tz):
    now = datetime(2026, 1, 1, 22, tzinfo=ZoneInfo("Asia/Tokyo"))
    due = next_send_at("transactional", tz, now, timing=TimingSettings())
    assert due == now
    assert due.tzinfo is UTC


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        ("2026-03-08T01:00:00+00:00", "2026-03-08T14:00:00+00:00"),
        ("2026-11-01T00:00:00+00:00", "2026-11-01T15:00:00+00:00"),
        ("2026-11-01T05:30:00+00:00", "2026-11-01T15:00:00+00:00"),
        ("2026-11-01T06:30:00+00:00", "2026-11-01T15:00:00+00:00"),
    ],
)
def test_dst_uses_next_local_date(moment, expected):
    assert next_send_at(
        "bulk",
        "America/New_York",
        datetime.fromisoformat(moment),
        timing=TimingSettings(),
    ) == datetime.fromisoformat(expected)


@pytest.mark.parametrize("send_class", ["transactional", "service", "bulk"])
def test_naive_timestamp_is_rejected(send_class):
    with pytest.raises(ValueError, match="aware"):
        next_send_at(
            send_class,
            "UTC",
            datetime(2026, 1, 1),
            timing=TimingSettings(),
        )
