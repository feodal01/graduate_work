"""Нагрузочное тестирование чтения и записи.

    uv run python -m bench.run                       # оба хранилища
    uv run python -m bench.run --store postgres --clients 32

Для каждого сценария три замера:
- задержка одного клиента: запросы идут последовательно;
- пропускная способность: --clients процессов выполняют сценарий --duration секунд;
- чтение в реальном времени: пока фоновые клиенты читают, новый лайк
  записывается и сразу читается в списке лайков и в сводке фильма.
Результаты пишутся в results/bench-*.json, отчёт — в results/report.md.
"""

import argparse
import json
import multiprocessing as mp
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from typing import Any
from uuid import UUID, uuid4

import numpy as np

from bench import dataset, report
from bench.stores import STORES, Store, open_store

Operation = Callable[[Store, dataset.Ids, random.Random], object]


def _user(ids: dataset.Ids, rnd: random.Random) -> UUID:
    return ids.users[rnd.randrange(len(ids.users))]


def _film(ids: dataset.Ids, rnd: random.Random) -> UUID:
    return ids.films[rnd.randrange(len(ids.films))]


def _popular_film(ids: dataset.Ids, rnd: random.Random) -> UUID:
    return ids.films[rnd.randrange(dataset.POPULAR_FILMS)]


def _set_rating(store: Store, ids: dataset.Ids, rnd: random.Random) -> None:
    store.set_rating(_user(ids, rnd), _film(ids, rnd), rnd.randint(0, 10), datetime.now(UTC))


READS: dict[str, Operation] = {
    'user_likes': lambda store, ids, rnd: store.user_likes(_user(ids, rnd)),
    'user_bookmarks': lambda store, ids, rnd: store.user_bookmarks(_user(ids, rnd)),
    'film_summary': lambda store, ids, rnd: store.film_summary(_film(ids, rnd)),
    'film_summary_popular': lambda store, ids, rnd: store.film_summary(_popular_film(ids, rnd)),
}
WRITES: dict[str, Operation] = {'set_rating': _set_rating}
SCENARIOS: dict[str, Operation] = {**READS, **WRITES}


def _mixed_read(store: Store, ids: dataset.Ids, rnd: random.Random) -> object:
    return READS[rnd.choice(list(READS))](store, ids, rnd)


WARMUP = 50


def _stats(latencies_ns: list[int] | np.ndarray[Any, Any]) -> dict[str, float]:
    values = np.asarray(latencies_ns, dtype=np.float64) / 1e6
    return {
        'requests': int(values.size),
        'mean_ms': float(values.mean()),
        'p50_ms': float(np.percentile(values, 50)),
        'p95_ms': float(np.percentile(values, 95)),
        'p99_ms': float(np.percentile(values, 99)),
    }


def _timed(operation: Callable[[], object]) -> int:
    started = time.perf_counter_ns()
    operation()
    return time.perf_counter_ns() - started


def sequential(store: Store, ids: dataset.Ids, scenario: str, requests: int) -> dict[str, float]:
    operation = SCENARIOS[scenario]
    rnd = random.Random(1)
    for _ in range(WARMUP):
        operation(store, ids, rnd)
    return _stats([_timed(lambda: operation(store, ids, rnd)) for _ in range(requests)])


def _load_worker(
    store_name: str,
    scenario: str,
    start_at: float,
    duration: float,
    seed: int,
    results: 'Queue[list[int]]',
) -> None:
    ids = dataset.load_ids()
    store = open_store(store_name)
    operation = SCENARIOS[scenario]
    rnd = random.Random(seed)
    for _ in range(WARMUP):
        operation(store, ids, rnd)
    time.sleep(max(0.0, start_at - time.time()))
    deadline = start_at + duration
    latencies = []
    while time.time() < deadline:
        latencies.append(_timed(lambda: operation(store, ids, rnd)))
    store.close()
    results.put(latencies)


def concurrent(store_name: str, scenario: str, clients: int, duration: float) -> dict[str, float]:
    context = mp.get_context('spawn')
    results: Queue[list[int]] = context.Queue()
    # Процессы стартуют и прогреваются заранее, замер начинается одновременно.
    start_at = time.time() + 3 + clients * 0.1
    processes = [
        context.Process(
            target=_load_worker,
            args=(store_name, scenario, start_at, duration, seed, results),
        )
        for seed in range(clients)
    ]
    for process in processes:
        process.start()
    # Упавший процесс не пришлёт результат: timeout вместо вечного ожидания.
    timeout = start_at - time.time() + duration + 60
    latencies = [value for _ in processes for value in results.get(timeout=timeout)]
    for process in processes:
        process.join()
    stats = _stats(latencies)
    stats['clients'] = clients
    stats['rps'] = len(latencies) / duration
    return stats


def _background_reader(store_name: str, seed: int, stop: Event, results: 'Queue[int]') -> None:
    ids = dataset.load_ids()
    store = open_store(store_name)
    rnd = random.Random(seed)
    count = 0
    while not stop.is_set():
        _mixed_read(store, ids, rnd)
        count += 1
    store.close()
    results.put(count)


def realtime(store_name: str, ids: dataset.Ids, iterations: int, clients: int) -> dict[str, Any]:
    """Лайк нового пользователя и время, через которое он виден в обоих чтениях."""
    context = mp.get_context('spawn')
    stop = context.Event()
    results: Queue[int] = context.Queue()
    readers = [
        context.Process(target=_background_reader, args=(store_name, 1000 + seed, stop, results))
        for seed in range(clients)
    ]
    for reader in readers:
        reader.start()
    time.sleep(3)

    store = open_store(store_name)
    rnd = random.Random(2)
    writes, visible, polls = [], [], []
    started_at = time.time()
    for _ in range(iterations):
        user_id, film_id = uuid4(), _film(ids, rnd)
        likes_before = store.film_summary(film_id).likes
        write_started = time.perf_counter_ns()
        store.set_rating(user_id, film_id, dataset.LIKE_SCORE, datetime.now(UTC))
        written = time.perf_counter_ns()
        attempts = 0
        while True:
            attempts += 1
            in_list = film_id in store.user_likes(user_id)
            if in_list and store.film_summary(film_id).likes == likes_before + 1:
                break
            if time.perf_counter_ns() - written > 5e9:
                raise RuntimeError(f'{store_name}: лайк не появился за 5 с')
        writes.append(written - write_started)
        visible.append(time.perf_counter_ns() - write_started)
        polls.append(attempts)
    elapsed = time.time() - started_at
    store.close()

    stop.set()
    background = sum(results.get(timeout=60) for _ in readers)
    for reader in readers:
        reader.join()
    return {
        'background_clients': clients,
        'background_rps': background / (elapsed + 3),
        'write': _stats(writes),
        'visible': _stats(visible),
        'max_polls': max(polls),
    }


def run_store(store_name: str, args: argparse.Namespace) -> None:
    print(f'{store_name}:')
    ids = dataset.load_ids()
    result: dict[str, Any] = {'store': store_name, 'sequential': {}, 'concurrent': {}}
    store = open_store(store_name)
    try:
        for scenario in SCENARIOS:
            stats = sequential(store, ids, scenario, args.requests)
            result['sequential'][scenario] = stats
            print(
                f'  {scenario}: 1 клиент p50 {stats["p50_ms"]:.2f} мс,'
                f' p99 {stats["p99_ms"]:.2f} мс',
            )
    finally:
        store.close()
    for scenario in SCENARIOS:
        stats = concurrent(store_name, scenario, args.clients, args.duration)
        result['concurrent'][scenario] = stats
        print(
            f'  {scenario}: {args.clients} клиентов {stats["rps"]:,.0f} запросов/с,'
            f' p99 {stats["p99_ms"]:.2f} мс',
        )
    result['realtime'] = realtime(store_name, ids, args.realtime, args.background_clients)
    print(f'  лайк виден через p99 {result["realtime"]["visible"]["p99_ms"]:.2f} мс')

    path = dataset.RESULTS_DIR / f'bench-{store_name}.json'
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--store', choices=[*STORES, 'all'], default='all')
    parser.add_argument('--requests', type=int, default=2000, help='запросов одного клиента')
    parser.add_argument('--clients', type=int, default=16, help='клиентов под нагрузкой')
    parser.add_argument('--duration', type=float, default=15, help='секунд под нагрузкой')
    parser.add_argument('--realtime', type=int, default=500, help='лайков в реальном времени')
    parser.add_argument('--background-clients', type=int, default=8, help='фоновых читателей')
    args = parser.parse_args()

    dataset.RESULTS_DIR.mkdir(exist_ok=True)
    for store_name in STORES if args.store == 'all' else [args.store]:
        run_store(store_name, args)
    report.write()


if __name__ == '__main__':
    main()
