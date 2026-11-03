"""Configurable cadence and recipient-local windows, including clock transitions."""

from datetime import UTC, datetime, time

import pytest
from pydantic import ValidationError

from notifications.config import TimingSettings
from notifications.scheduling.scheduler import period_start
from notifications.scheduling.windows import next_send_at


def test_timing_reads_environment(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_LIKES_SCHEDULE_ANCHOR_UTC", "1970-01-01T09:30:00Z")
    monkeypatch.setenv("NOTIFICATIONS_FILMS_SCHEDULE_ANCHOR_UTC", "1970-01-05T09:00:00Z")
    monkeypatch.setenv("NOTIFICATIONS_SEND_WINDOW_START", "08:30")
    monkeypatch.setenv("NOTIFICATIONS_SEND_WINDOW_END", "18:45")
    settings = TimingSettings()
    assert settings.likes_schedule_anchor_utc == datetime(1970, 1, 1, 9, 30, tzinfo=UTC)
    assert settings.films_schedule_anchor_utc == datetime(1970, 1, 5, 9, tzinfo=UTC)
    assert settings.send_window_start == time(8, 30)
    assert settings.send_window_end == time(18, 45)


@pytest.mark.parametrize(
    "anchor", ["2026-01-01", "2026-01-01T09:00:00", "2026-01-01T09:00:00+03:00", 0, True]
)
def test_anchors_require_explicit_utc(anchor):
    with pytest.raises(ValidationError):
        TimingSettings(likes_schedule_anchor_utc=anchor)


@pytest.mark.parametrize(
    "start,end",
    [
        ("20:00", "10:00"),
        ("10:00", "10:00"),
        ("24:00", "20:00"),
        ("10:00:01", "20:00"),
        ("10:00Z", "20:00"),
        (3600, "20:00"),
    ],
)
def test_window_rejects_invalid_or_overnight_configuration(start, end):
    with pytest.raises(ValidationError):
        TimingSettings(send_window_start=start, send_window_end=end)


@pytest.mark.parametrize(
    "now,expected",
    [
        ("2026-10-12T08:59:59Z", "2026-10-05T09:00:00Z"),
        ("2026-10-12T09:00:00Z", "2026-10-12T09:00:00Z"),
        ("2026-10-18T23:59:59Z", "2026-10-12T09:00:00Z"),
    ],
)
def test_weekly_monday_at_nine_is_stable_across_restarts(now, expected):
    anchor = datetime(1970, 1, 5, 9, tzinfo=UTC)
    assert period_start(datetime.fromisoformat(now), 604800, anchor) == datetime.fromisoformat(
        expected
    )


def test_daily_anchor_and_future_start():
    anchor = datetime(2026, 10, 12, 9, 30, tzinfo=UTC)
    assert period_start(datetime(2026, 10, 12, 9, tzinfo=UTC), 86400, anchor) is None
    assert period_start(datetime(2026, 10, 13, 9, tzinfo=UTC), 86400, anchor) == anchor


@pytest.mark.parametrize(
    "moment,expected",
    [
        ("2026-01-01T05:29:59Z", "2026-01-01T05:30:00Z"),
        ("2026-01-01T05:30:00Z", "2026-01-01T05:30:00Z"),
        ("2026-01-01T15:44:59Z", "2026-01-01T15:44:59Z"),
        ("2026-01-01T15:45:00Z", "2026-01-02T05:30:00Z"),
    ],
)
def test_custom_window_boundaries(moment, expected):
    timing = TimingSettings(send_window_start=time(8, 30), send_window_end=time(18, 45))
    assert next_send_at(
        "bulk",
        "Europe/Moscow",
        datetime.fromisoformat(moment),
        timing=timing,
    ) == datetime.fromisoformat(expected)


@pytest.mark.parametrize(
    "start,end,moment,expected",
    [
        ("02:30", "04:00", "2026-03-08T06:00:00Z", "2026-03-08T07:00:00Z"),
        ("02:10", "02:40", "2026-03-08T06:00:00Z", "2026-03-09T06:10:00Z"),
        ("01:15", "01:45", "2026-11-01T05:50:00Z", "2026-11-01T06:15:00Z"),
        ("00:30", "01:45", "2026-11-01T05:50:00Z", "2026-11-01T06:00:00Z"),
    ],
)
def test_custom_window_respects_dst_gaps_and_repeated_hours(start, end, moment, expected):
    timing = TimingSettings(send_window_start=start, send_window_end=end)
    assert next_send_at(
        "service",
        "America/New_York",
        datetime.fromisoformat(moment),
        timing=timing,
    ) == datetime.fromisoformat(expected)


def test_timing_json_round_trip_preserves_valid_configuration():
    settings = TimingSettings(send_window_start=time(8, 30), send_window_end=time(18, 45))
    assert TimingSettings.model_validate_json(settings.model_dump_json()) == settings
