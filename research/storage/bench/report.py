"""Таблицы Markdown из results/*.json: uv run python -m bench.report"""

import json
from pathlib import Path
from typing import Any

from bench.dataset import RESULTS_DIR

TITLES = {
    'user_likes': 'Список лайков пользователя',
    'user_bookmarks': 'Список закладок пользователя',
    'film_summary': 'Лайки, дизлайки и средняя оценка фильма',
    'film_summary_popular': 'То же для популярного фильма',
    'set_rating': 'Поставить или изменить оценку',
}
STORES = ('mongo', 'postgres')
NAMES = {'mongo': 'MongoDB', 'postgres': 'PostgreSQL'}


def _read(name: str) -> dict[str, Any] | None:
    path = RESULTS_DIR / name
    return json.loads(path.read_text()) if path.exists() else None


def _ms(value: float) -> str:
    return f'{value:.2f}'


def _mb(value: int) -> str:
    return f'{value / 2**20:,.0f}'.replace(',', ' ')


def _int(value: float) -> str:
    return f'{value:,.0f}'.replace(',', ' ')


def _table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = ['| ' + ' | '.join(header) + ' |', '|' + ' --- |' * len(header)]
    lines += ['| ' + ' | '.join(row) + ' |' for row in rows]
    return lines


def render() -> str:
    loads = {store: _read(f'load-{store}.json') for store in STORES}
    benches = {store: _read(f'bench-{store}.json') for store in STORES}
    present = [store for store in STORES if benches[store] and loads[store]]
    lines: list[str] = []

    lines += ['### Загрузка', '']
    rows = []
    for store in present:
        load = loads[store]
        assert load is not None
        sizes = load['sizes']
        rows.append(
            [
                NAMES[store],
                f'{load["ratings_seconds"]:.0f}',
                f'{load["bookmarks_seconds"]:.0f}',
                f'{load["index_seconds"]:.0f}',
                _mb(sizes['ratings']['data_bytes']),
                _mb(sizes['ratings']['index_bytes']),
                _mb(sizes['bookmarks']['data_bytes'] + sizes['bookmarks']['index_bytes']),
            ],
        )
    lines += _table(
        [
            'Хранилище',
            'Оценки, с',
            'Закладки, с',
            'Индексы, с',
            'Оценки: данные, МБ',
            'Оценки: индексы, МБ',
            'Закладки всего, МБ',
        ],
        rows,
    )

    lines += ['', '### Один клиент, мс', '']
    rows = []
    for scenario, title in TITLES.items():
        row = [title]
        for store in present:
            stats = benches[store]['sequential'][scenario]  # type: ignore[index]
            row += [_ms(stats['p50_ms']), _ms(stats['p99_ms'])]
        rows.append(row)
    header = ['Сценарий']
    for store in present:
        header += [f'{NAMES[store]} p50', f'{NAMES[store]} p99']
    lines += _table(header, rows)

    clients = next(
        (benches[s]['concurrent']['user_likes']['clients'] for s in present),  # type: ignore[index]
        0,
    )
    lines += ['', f'### {clients} клиентов одновременно', '']
    rows = []
    for scenario, title in TITLES.items():
        row = [title]
        for store in present:
            stats = benches[store]['concurrent'][scenario]  # type: ignore[index]
            row += [_int(stats['rps']), _ms(stats['p99_ms'])]
        rows.append(row)
    header = ['Сценарий']
    for store in present:
        header += [f'{NAMES[store]} запросов/с', f'{NAMES[store]} p99, мс']
    lines += _table(header, rows)

    lines += ['', '### Лайк в реальном времени, мс', '']
    rows = []
    for store in present:
        realtime = benches[store]['realtime']  # type: ignore[index]
        rows.append(
            [
                NAMES[store],
                str(realtime['background_clients']),
                _ms(realtime['write']['p50_ms']),
                _ms(realtime['write']['p99_ms']),
                _ms(realtime['visible']['p50_ms']),
                _ms(realtime['visible']['p99_ms']),
                str(realtime['max_polls']),
            ],
        )
    lines += _table(
        [
            'Хранилище',
            'Фоновых читателей',
            'Запись p50',
            'Запись p99',
            'Виден p50',
            'Виден p99',
            'Попыток чтения, max',
        ],
        rows,
    )
    return '\n'.join(lines) + '\n'


def write(path: Path = RESULTS_DIR / 'report.md') -> None:
    path.write_text(render())
    print(f'Отчёт: {path}')


if __name__ == '__main__':
    write()
