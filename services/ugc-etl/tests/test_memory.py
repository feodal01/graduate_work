from services.contracts import ConsumedRecord
from services.memory import read_memory
from services.pipeline import run_once, stats
from tests.conftest import FILM_ID, MemoryConsumer, MemoryWarehouse
from tests.test_transform import envelope_bytes


async def test_read_memory_returns_positive_rss_and_flush_clears_batch() -> None:
    raw = envelope_bytes(
        event_type='watch_progress',
        payload={
            'film_id': str(FILM_ID),
            'position_seconds': 120,
            'duration_seconds': 7200,
        },
    )
    consumer = MemoryConsumer([ConsumedRecord(value=raw)])
    warehouse = MemoryWarehouse()

    await run_once(consumer, warehouse)

    memory = read_memory()
    assert stats.batch_size == 0
    assert memory.rss_bytes > 0
