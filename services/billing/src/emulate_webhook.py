"""Локальный эмулятор входящих вебхуков ЮKassa для разработки (T-18, BIL-FR-07)."""

import argparse
import asyncio
import re
from typing import Any
from uuid import uuid4

import httpx

_SAFE_PROVIDER_ID_RE = re.compile(r'^[A-Za-z0-9._-]{1,128}$')


def build_yookassa_webhook_payload(
    *,
    provider_object_id: str,
    event: str = 'payment.succeeded',
) -> dict[str, Any]:
    """Формирует JSON-уведомление ЮKassa (Billing берёт из него только object.id)."""
    cleaned_id = provider_object_id.strip()
    if not cleaned_id or not _SAFE_PROVIDER_ID_RE.fullmatch(cleaned_id):
        raise ValueError('provider_object_id must be a safe non-empty provider ID')
    status = event.split('.', 1)[-1] if '.' in event else 'succeeded'
    return {
        'type': 'notification',
        'event': event,
        'object': {
            'id': cleaned_id,
            'status': status,
        },
    }


async def send_emulated_webhook(
    *,
    provider_object_id: str,
    event: str = 'payment.succeeded',
    base_url: str = 'http://localhost',
    request_id: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[int, dict[str, Any]]:
    """Отправляет эмулированный вебхук ЮKassa в сервис Billing."""
    payload = build_yookassa_webhook_payload(
        provider_object_id=provider_object_id,
        event=event,
    )
    req_id = request_id or f'emulated-webhook-{uuid4().hex[:12]}'
    url = f'{base_url.rstrip("/")}/api/v1/billing/webhooks/yookassa'
    async with httpx.AsyncClient(timeout=10.0, transport=transport) as client:
        response = await client.post(
            url,
            json=payload,
            headers={'X-Request-Id': req_id},
        )
        try:
            data = response.json()
            if isinstance(data, dict):
                return response.status_code, data
        except ValueError:
            pass
        return response.status_code, {'raw_body': response.text}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Разбирает аргументы командной строки локального эмулятора вебхука."""
    parser = argparse.ArgumentParser(
        description='Send an emulated YooKassa webhook to Billing.'
    )
    parser.add_argument(
        '--id',
        dest='provider_object_id',
        required=True,
        help='Provider payment or refund ID (e.g. 2e001001-000f-5000-8000-...)',
    )
    parser.add_argument(
        '--event',
        default='payment.succeeded',
        help='YooKassa webhook event name (default: payment.succeeded)',
    )
    parser.add_argument(
        '--base-url',
        default='http://localhost',
        help='Gateway or Billing API base URL (default: http://localhost)',
    )
    return parser.parse_args(argv)


def main() -> None:  # pragma: no cover
    args = parse_args()
    status_code, body = asyncio.run(
        send_emulated_webhook(
            provider_object_id=args.provider_object_id,
            event=args.event,
            base_url=args.base_url,
        )
    )
    print(f'HTTP {status_code}: {body}')


if __name__ == '__main__':  # pragma: no cover
    main()
