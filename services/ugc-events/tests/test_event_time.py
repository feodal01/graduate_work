import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from core.config import Settings
from tests.conftest import RECEIVED_AT, MemoryEventBroker, auth_header, click_event, make_token


@pytest.mark.parametrize(
    'occurred_at',
    [
        '2026-09-08T12:00:00Z',
        '2026-09-15T12:05:00Z',
        '2026-09-08T15:00:00+03:00',
        '2026-09-15T08:05:00-04:00',
    ],
)
def test_exact_time_boundaries_are_accepted_and_preserved(
    client: TestClient,
    broker: MemoryEventBroker,
    occurred_at: str,
) -> None:
    event = click_event()
    event['occurred_at'] = occurred_at

    response = client.post('/api/v1/ugc/events', headers=auth_header(), json={'events': [event]})

    assert response.status_code == 202
    envelope = json.loads(broker.records[0].value)
    assert envelope['received_at'] == RECEIVED_AT.isoformat()
    assert envelope['occurred_at'] == datetime.fromisoformat(occurred_at).isoformat()


@pytest.mark.parametrize(
    'occurred_at',
    [
        '2026-09-08T11:59:59.999999Z',
        '2026-09-15T12:05:00.000001Z',
        '2026-09-08T14:59:59.999999+03:00',
        '2026-09-15T08:05:00.000001-04:00',
        '2500-01-01T00:00:00Z',
        '2026-01-01T00:00:00Z',
    ],
)
def test_mixed_batch_outside_time_window_is_rejected_atomically(
    client: TestClient,
    broker: MemoryEventBroker,
    occurred_at: str,
) -> None:
    invalid = click_event()
    invalid['occurred_at'] = occurred_at

    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={'events': [click_event(), invalid, click_event()]},
    )

    assert response.status_code == 422
    assert response.json()['detail'][0]['loc'] == ['body', 'events', 1, 'occurred_at']
    assert broker.records == []


def test_all_invalid_event_indices_are_reported(client: TestClient) -> None:
    old, future = click_event(), click_event()
    old['occurred_at'] = '2026-09-01T12:00:00Z'
    future['occurred_at'] = '2500-01-01T00:00:00Z'

    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={'events': [old, click_event(), future]},
    )

    assert response.status_code == 422
    assert [error['loc'] for error in response.json()['detail']] == [
        ['body', 'events', 0, 'occurred_at'],
        ['body', 'events', 2, 'occurred_at'],
    ]


def test_invalid_time_does_not_consume_rate_limit(
    client: TestClient,
    broker: MemoryEventBroker,
) -> None:
    headers = auth_header(make_token(user_id=str(uuid4())))
    invalid = click_event()
    invalid['occurred_at'] = '2500-01-01T00:00:00Z'

    for _ in range(11):
        assert (
            client.post(
                '/api/v1/ugc/events',
                headers=headers,
                json={'events': [invalid]},
            ).status_code
            == 422
        )
    for _ in range(10):
        assert (
            client.post(
                '/api/v1/ugc/events',
                headers=headers,
                json={'events': [click_event()]},
            ).status_code
            == 202
        )

    assert (
        client.post(
            '/api/v1/ugc/events',
            headers=headers,
            json={'events': [click_event()]},
        ).status_code
        == 429
    )
    assert (
        client.post(
            '/api/v1/ugc/events',
            headers=headers,
            json={'events': [invalid]},
        ).status_code
        == 422
    )
    assert len(broker.records) == 10


def test_batch_uses_one_clock_read_for_validation_and_publication(
    client: TestClient,
    broker: MemoryEventBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    class AdvancingDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            calls.append(tz)
            return RECEIVED_AT + timedelta(days=30 * (len(calls) - 1))

    monkeypatch.setattr('api.v1.events.datetime', AdvancingDatetime)
    old, future = click_event(), click_event()
    old['occurred_at'] = '2026-09-08T12:00:00Z'
    future['occurred_at'] = '2026-09-15T12:05:00Z'
    response = client.post(
        '/api/v1/ugc/events',
        headers=auth_header(),
        json={'events': [old, future]},
    )

    assert response.status_code == 202
    assert calls == [UTC]
    assert [json.loads(record.value)['received_at'] for record in broker.records] == [
        RECEIVED_AT.isoformat(),
        RECEIVED_AT.isoformat(),
    ]


@pytest.mark.parametrize(
    ('occurred_at', 'status_code'),
    [
        ('2026-09-15T11:59:00Z', 202),
        ('2026-09-15T11:58:59Z', 422),
        ('2026-09-15T12:00:00Z', 202),
        ('2026-09-15T12:00:01Z', 422),
    ],
)
def test_api_uses_configured_time_window(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    occurred_at: str,
    status_code: int,
) -> None:
    monkeypatch.setenv('EVENTS_MAX_AGE_SECONDS', '60')
    monkeypatch.setenv('EVENTS_MAX_FUTURE_SKEW_SECONDS', '0')
    monkeypatch.setattr('services.ingest.settings', Settings(_env_file=None))
    event = click_event()
    event['occurred_at'] = occurred_at

    response = client.post('/api/v1/ugc/events', headers=auth_header(), json={'events': [event]})

    assert response.status_code == status_code


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
