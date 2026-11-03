from postgres_to_es.pipelines.loader.client import bulk_index
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import settings


def load_documents(documents: list[MovieDocument]) -> None:
    """Загрузить документы в Elasticsearch (индекс из settings). Чистый шаг:
    не знает про job — как и extract_job_rows/transform_rows, вызывается
    внутри try/except worker-а.
    """
    bulk_index(documents, settings.elasticsearch_index)
