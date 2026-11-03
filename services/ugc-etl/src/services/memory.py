import tracemalloc
from dataclasses import dataclass

import psutil


@dataclass(frozen=True)
class MemorySnapshot:
    rss_bytes: int
    tracemalloc_current: int
    tracemalloc_peak: int


def read_memory() -> MemorySnapshot:
    rss_bytes = psutil.Process().memory_info().rss
    current, peak = tracemalloc.get_traced_memory()
    return MemorySnapshot(
        rss_bytes=rss_bytes,
        tracemalloc_current=current,
        tracemalloc_peak=peak,
    )
