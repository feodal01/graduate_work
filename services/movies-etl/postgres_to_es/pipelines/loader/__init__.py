from postgres_to_es.pipelines.loader.client import bulk_index, ensure_index_exists
from postgres_to_es.pipelines.loader.mapping import MOVIES_INDEX_MAPPING
from postgres_to_es.pipelines.loader.pipeline import load_documents

__all__ = [
    "MOVIES_INDEX_MAPPING",
    "bulk_index",
    "ensure_index_exists",
    "load_documents",
]
