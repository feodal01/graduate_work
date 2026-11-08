"""Конфигурация шлюза nginx для маршрутов Биллинга (GW-NFR-R-01, матрица 6.3).

Регрессия: после остановки `billing-api` шлюз слал SYN на старый адрес
контейнера и ждал стандартные 60 с `proxy_connect_timeout`, поэтому клиент
получал зависание вместо `502`/`503` (найдено e2e-прогоном T-37).
"""

import re
from pathlib import Path

import pytest

GATEWAY_CONF = (
    Path(__file__).resolve().parents[3] / 'infra' / 'nginx' / 'conf.d' / 'gateway.conf'
)
MAX_CONNECT_TIMEOUT_SECONDS = 5


def _billing_locations() -> list[tuple[str, str]]:
    text = GATEWAY_CONF.read_text()
    blocks = re.findall(r'location\s+([^{]+?)\s*\{(.*?)\n    \}', text, re.S)
    return [
        (name.strip(), body) for name, body in blocks if '$billing_upstream' in body
    ]


@pytest.mark.skipif(not GATEWAY_CONF.exists(), reason='gateway config not present')
def test_every_billing_route_fails_fast_when_api_is_down() -> None:
    """Каждый маршрут на billing-api ограничивает подключение и отдаёт 503."""
    locations = _billing_locations()
    assert len(locations) >= 5, locations

    for name, body in locations:
        timeout = re.search(r'proxy_connect_timeout\s+(\d+)s;', body)
        assert timeout, f'{name}: no proxy_connect_timeout (default is 60s)'
        assert int(timeout[1]) <= MAX_CONNECT_TIMEOUT_SECONDS, name
        assert re.search(r'error_page\s+504\s+=\s*@billing_unavailable;', body), (
            f'{name}: connect timeout must answer 503, not 504'
        )


@pytest.mark.skipif(not GATEWAY_CONF.exists(), reason='gateway config not present')
def test_billing_unavailable_location_answers_503_json() -> None:
    """Запасной ответ шлюза — 503 с кодом ошибки в формате API Биллинга."""
    text = GATEWAY_CONF.read_text()
    block = re.search(r'location @billing_unavailable \{(.*?)\n    \}', text, re.S)
    assert block, 'missing @billing_unavailable'
    assert re.search(r"return 503 '\{\"error_code\":\"billing_unavailable\"", block[1])
