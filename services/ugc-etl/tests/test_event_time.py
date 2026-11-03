from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.config import Settings
from services.contracts import ConsumedRecord
from services.pipeline import run_once
from services.transform import parse_envelope
from tests.conftest import FILM_ID, MemoryConsumer, MemoryWarehouse
from tests.test_transform import envelope_bytes

RECEIVED_AT = '2026-09-16T12:00:01Z'
PAYLOAD = {'film_id': str(FILM_ID)}


@pytest.mark.parametrize(
    'occurred_at',
    [
        '2026-09-09T12:00:01Z',
        '2026-09-16T12:05:01Z',
        '2026-09-09T15:00:01+03:00',
        '2026-09-16T08:05:01-04:00',
    ],
)
@pytest.mark.parametrize('received_at', [RECEIVED_AT, '2026-09-16T15:00:01+03:00'])
def test_etl_accepts_exact_boundaries_across_timezones(occurred_at: str, received_at: str) -> None:
    raw = envelope_bytes(
        event_type='watch_completed',
        payload=PAYLOAD,
        occurred_at=occurred_at,
        received_at=received_at,
    )

    row = parse_envelope(raw)

    assert row.occurred_at == datetime.fromisoformat(occurred_at)
    assert row.received_at == datetime.fromisoformat(RECEIVED_AT)


@pytest.mark.parametrize(
    'occurred_at',
    [
        '2026-09-09T12:00:00.999999Z',
        '2026-09-16T12:05:01.000001Z',
        '2026-09-09T15:00:00.999999+03:00',
        '2026-09-16T08:05:01.000001-04:00',
        '2500-01-01T00:00:00Z',
        '2026-01-01T00:00:00Z',
    ],
)
async def test_etl_outside_time_window_goes_to_dlq(occurred_at: str) -> None:
    raw = envelope_bytes(event_type='watch_completed', payload=PAYLOAD, occurred_at=occurred_at)
    consumer = MemoryConsumer([ConsumedRecord(value=raw, key=b'user-1')])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    assert warehouse.insert_calls == 0
    assert consumer.dlq == [(raw, b'user-1')]
    assert consumer.events == ['dlq', 'commit']
    assert consumer.committed == 1


@pytest.mark.parametrize('processing_year', [2026, 2030])
async def test_delayed_and_replayed_event_uses_saved_received_at(
    monkeypatch: pytest.MonkeyPatch,
    processing_year: int,
) -> None:
    processing_time = datetime(processing_year, 12, 31, tzinfo=UTC)

    class ProcessingDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return processing_time

    monkeypatch.setattr('services.transform.datetime', ProcessingDatetime)
    raw = envelope_bytes(
        event_type='watch_completed', payload=PAYLOAD, occurred_at='2026-09-09T12:00:01Z'
    )
    consumer = MemoryConsumer([ConsumedRecord(value=raw, key=b'user-1')])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    assert consumer.dlq == []
    assert consumer.committed == 1
    assert warehouse.inserted[0][0].ingested_at == processing_time
    assert warehouse.inserted[0][0].received_at == datetime.fromisoformat(RECEIVED_AT)


@pytest.mark.parametrize('field', ['occurred_at', 'received_at'])
def test_etl_requires_timezone_on_both_timestamps(field: str) -> None:
    timestamps = {field: '2026-09-16T12:00:01'}
    raw = envelope_bytes(event_type='watch_completed', payload=PAYLOAD, **timestamps)

    with pytest.raises(ValidationError):
        parse_envelope(raw)


@pytest.mark.parametrize(
    ('occurred_at', 'accepted'),
    [
        ('2026-09-16T11:59:01Z', True),
        ('2026-09-16T11:59:00Z', False),
        ('2026-09-16T12:00:01Z', True),
        ('2026-09-16T12:00:02Z', False),
    ],
)
def test_etl_uses_configured_time_window(
    monkeypatch: pytest.MonkeyPatch,
    occurred_at: str,
    accepted: bool,
) -> None:
    monkeypatch.setenv('EVENTS_MAX_AGE_SECONDS', '60')
    monkeypatch.setenv('EVENTS_MAX_FUTURE_SKEW_SECONDS', '0')
    monkeypatch.setattr('services.transform.settings', Settings(_env_file=None))
    raw = envelope_bytes(event_type='watch_completed', payload=PAYLOAD, occurred_at=occurred_at)

    if accepted:
        assert parse_envelope(raw).event_type == 'watch_completed'
    else:
        with pytest.raises(ValidationError):
            parse_envelope(raw)


@pytest.mark.parametrize(
    ('name', 'value'),
    [
        ('EVENTS_MAX_AGE_SECONDS', '0'),
        ('EVENTS_MAX_AGE_SECONDS', '-1'),
        ('EVENTS_MAX_FUTURE_SKEW_SECONDS', '-1'),
    ],
)
def test_invalid_time_settings_fail_at_startup(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
