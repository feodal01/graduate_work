"""Проверка лимитов UGC Events через HTTP и фактических записей в Kafka.

Запуск из корня: docker compose exec -T ugc-events python < tests/load/events_rate_limit.py
Создаёт тестовые события; использует JWT_SECRET контейнера, не выводит токены.
"""

import asyncio
import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from time import monotonic
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import jwt
from aiokafka import AIOKafkaConsumer, TopicPartition

from core.config import settings


def send(
    base_url: str,
    user_id: str,
    request_id: str,
    *,
    valid: bool = True,
) -> tuple[int, str | None, str]:
    now = datetime.now(UTC)
    token = (
        jwt.encode(
            {
                'sub': user_id,
                'jti': str(uuid4()),
                'type': 'access',
                'iat': now,
                'exp': now + timedelta(minutes=5),
            },
            settings.auth_jwt_secret.get_secret_value(),
            algorithm=settings.auth_jwt_algorithm,
        )
        if valid
        else 'invalid-token'
    )
    event_id = str(uuid4())
    request = Request(
        f'{base_url}/api/v1/ugc/events',
        data=json.dumps(
            {
                'events': [
                    {
                        'event_id': event_id,
                        'occurred_at': now.isoformat(),
                        'event_type': 'click',
                        'payload': {
                            'element_type': 'load-test',
                            'element_id': 'rate-limit',
                            'page': '/',
                        },
                    }
                ]
            }
        ).encode(),
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}',
            'X-Request-Id': request_id,
        },
        method='POST',
    )
    try:
        response = urlopen(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        response.read()
        return response.status, response.headers.get('Retry-After'), event_id


async def main() -> None:
    logging.basicConfig(level=logging.ERROR)
    run_id = f'events-rate-limit-{uuid4()}'
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(','),
        group_id=None,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(settings.kafka_topic)
        assert partitions, 'Kafka topic is missing'
        assigned = [TopicPartition(settings.kafka_topic, partition) for partition in partitions]
        consumer.unsubscribe()
        consumer.assign(assigned)
        await consumer.seek_to_end(*assigned)

        user_id = str(uuid4())
        capacity = settings.events_rate_limit_capacity
        started_at = monotonic()
        results = [send('http://127.0.0.1:8000', user_id, run_id) for _ in range(capacity + 10)]
        duration = monotonic() - started_at
        # Exact burst expectations are valid only before one token can refill.
        assert (
            duration < settings.events_rate_limit_window_seconds / settings.events_rate_limit_rate
        ), 'Scenario took longer than the refill interval; capacity evidence is inconclusive'
        statuses = dict(Counter(status for status, _, _ in results))
        assert statuses == {202: capacity, 429: 10}, statuses
        assert all(retry and int(retry) > 0 for status, retry, _ in results if status == 429)
        other_user = send('http://127.0.0.1:8000', str(uuid4()), run_id)
        assert other_user[0] == 202, other_user[0]

        expected_ids = {event_id for status, _, event_id in [*results, other_user] if status == 202}
        seen_ids = set()
        deadline = monotonic() + 10
        while monotonic() < deadline:
            batches = await consumer.getmany(timeout_ms=500)
            for messages in batches.values():
                for message in messages:
                    envelope = json.loads(message.value)
                    if envelope.get('request_id') == run_id:
                        seen_ids.add(envelope['event_id'])
            if seen_ids == expected_ids and not batches:
                break
        assert seen_ids == expected_ids, 'Kafka records do not match accepted batches'

        # Invalid JWT isolates the IP limiter: API can return only 401 here.
        with ThreadPoolExecutor(max_workers=40) as executor:
            futures = [
                executor.submit(send, 'http://gateway', str(uuid4()), run_id, valid=False)
                for _ in range(100)
            ]
            ip_results = [future.result() for future in futures]
        ip_statuses = dict(Counter(status for status, _, _ in ip_results))
        assert set(ip_statuses) == {401, 429}, ip_statuses
        assert all(retry == '1' for status, retry, _ in ip_results if status == 429)
        print(
            json.dumps(
                {
                    'result': 'PASS',
                    'user_limit_statuses': statuses,
                    'user_limit_duration_seconds': round(duration, 3),
                    'other_user_status': other_user[0],
                    'kafka_records': len(seen_ids),
                    'ip_limit_statuses': ip_statuses,
                    'retry_after_verified': True,
                },
                indent=2,
            )
        )
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(main())
