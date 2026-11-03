"""Генерация набора и загрузка его в хранилища.

    uv run python -m bench.generate                  # оба хранилища
    uv run python -m bench.generate --store mongo

Индексы строятся после загрузки: так массовая вставка быстрее в обоих
хранилищах. Время загрузки и построения индексов пишется в results/load-*.json.
"""

import argparse
import json
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import numpy.typing as npt

from bench import dataset
from bench.stores import STORES, MongoStore, PostgresStore, open_store

CHUNK = 50_000

_data: dict[str, npt.NDArray[np.generic]] = {}
_store: MongoStore | PostgresStore | None = None


def _init_worker(store_name: str) -> None:
    global _store
    _data.update(dataset.load())
    _store = open_store(store_name)


def _load_chunk(kind: str, start: int, stop: int) -> int:
    assert _store is not None
    if kind == 'ratings':
        _store.load_ratings(_data, start, stop)
    else:
        _store.load_bookmarks(_data, start, stop)
    return stop - start


def _parallel_load(store_name: str, kind: str, total: int, workers: int) -> float:
    ranges = [(start, min(start + CHUNK, total)) for start in range(0, total, CHUNK)]
    started = time.perf_counter()
    with ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(store_name,)) as pool:
        loaded = 0
        futures = [pool.submit(_load_chunk, kind, start, stop) for start, stop in ranges]
        for future in futures:
            loaded += future.result()
    elapsed = time.perf_counter() - started
    print(f'  {kind}: {loaded:,} за {elapsed:.1f} с ({loaded / elapsed:,.0f} строк/с)')
    return elapsed


def load_store(store_name: str, workers: int) -> None:
    print(f'{store_name}:')
    store = open_store(store_name)
    try:
        store.reset()
        ratings_seconds = _parallel_load(store_name, 'ratings', dataset.N_RATINGS, workers)
        bookmarks_seconds = _parallel_load(store_name, 'bookmarks', dataset.N_BOOKMARKS, workers)
        started = time.perf_counter()
        store.create_indexes()
        index_seconds = time.perf_counter() - started
        print(f'  индексы: {index_seconds:.1f} с')
        report = {
            'store': store_name,
            'workers': workers,
            'ratings_seconds': ratings_seconds,
            'bookmarks_seconds': bookmarks_seconds,
            'index_seconds': index_seconds,
            'sizes': store.sizes(),
        }
    finally:
        store.close()
    dataset.RESULTS_DIR.mkdir(exist_ok=True)
    path = dataset.RESULTS_DIR / f'load-{store_name}.json'
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--store', choices=[*STORES, 'all'], default='all')
    parser.add_argument('--workers', type=int, default=6, help='процессов загрузки')
    parser.add_argument('--rebuild', action='store_true', help='пересоздать data/dataset.npz')
    args = parser.parse_args()

    if args.rebuild or not dataset.DATA_FILE.exists():
        started = time.perf_counter()
        dataset.build()
        print(f'Набор данных: {dataset.DATA_FILE} за {time.perf_counter() - started:.1f} с')
        dataset.RESULTS_DIR.mkdir(exist_ok=True)
        stats = dataset.describe(dataset.load())
        (dataset.RESULTS_DIR / 'dataset.json').write_text(
            json.dumps(stats, indent=2, ensure_ascii=False) + '\n',
        )

    for store_name in STORES if args.store == 'all' else [args.store]:
        load_store(store_name, args.workers)


if __name__ == '__main__':
    main()
