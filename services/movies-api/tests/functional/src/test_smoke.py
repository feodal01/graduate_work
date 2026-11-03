"""Smoke-проверка инфраструктуры: сервис отвечает, фикстуры доносят данные до API."""

import aiohttp

from tests.functional.utils.helpers import film_document


async def test_health(http_client: aiohttp.ClientSession) -> None:
    async with http_client.get('/api/v1/health/') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body == {
        'status': 'ok',
        'elasticsearch': 'ok',
        'redis': 'ok',
    }


async def test_loaded_film_is_visible_through_api(http_client, es_write_data) -> None:
    film = film_document(title='Infrastructure Smoke')
    await es_write_data([film])

    async with http_client.get(f'/api/v1/films/{film["id"]}/') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body['uuid'] == film['id']
    assert body['title'] == film['title']


async def test_index_is_empty_for_next_test(http_client, es_index) -> None:
    """Индекс пересоздаётся на каждый тест, поэтому данных предыдущего теста нет."""
    async with http_client.get('/api/v1/films') as response:
        status = response.status
        body = await response.json()

    assert status == 200
    assert body == []
