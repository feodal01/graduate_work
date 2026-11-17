import pytest
from pydantic import ValidationError

from core.config import PaginationSettings, settings
from main import create_app


def _pagination_parameters() -> dict[str, dict[str, dict[str, object]]]:
    """Параметры `limit`/`offset` каждого GET-обработчика из схемы OpenAPI."""
    found: dict[str, dict[str, dict[str, object]]] = {}
    for path, operations in create_app(app_lifespan=None).openapi()['paths'].items():
        params = {
            p['name']: p
            for p in operations.get('get', {}).get('parameters', [])
            if p['in'] == 'query' and p['name'] in ('limit', 'offset')
        }
        if params:
            found[path] = params
    return found


def test_all_paginated_lists_share_one_definition() -> None:
    """Все постраничные списки берут `limit` и `offset` из общей зависимости."""
    by_path = _pagination_parameters()

    # Четыре списка внутреннего API, JSON-сводка и экран «Моя подписка».
    assert len(by_path) == 6
    for path, params in by_path.items():
        assert params['limit']['schema'] == {
            'type': 'integer',
            'minimum': 1,
            'maximum': settings.pagination.max_page_size,
            'default': settings.pagination.default_page_size,
            'title': 'Limit',
            'description': 'Сколько записей вернуть на странице',
        }, path
        assert params['offset']['schema'] == {
            'type': 'integer',
            'minimum': 0,
            'default': 0,
            'title': 'Offset',
            'description': 'Сколько записей пропустить от начала списка',
        }, path


def test_default_page_size_must_fit_max_page_size() -> None:
    """Размер страницы по умолчанию больше максимального — ошибка настроек."""
    with pytest.raises(ValidationError):
        PaginationSettings(default_page_size=200, max_page_size=100)
