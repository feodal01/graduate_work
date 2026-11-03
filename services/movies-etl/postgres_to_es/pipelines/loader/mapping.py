"""Маппинг индекса movies для Elasticsearch.

Схема индекса, используемая loader-ом в рантайме
"""

from typing import Any

_RU_EN_TEXT: dict[str, Any] = {"type": "text", "analyzer": "ru_en"}

_NESTED_ID_NAME: dict[str, Any] = {
    "type": "nested",
    "dynamic": "strict",
    "properties": {
        "id": {"type": "keyword"},
        "name": {"type": "text", "analyzer": "ru_en"},
    },
}

MOVIES_INDEX_MAPPING: dict[str, Any] = {
    "settings": {
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english",
                },
                "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                "russian_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                    ],
                }
            },
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "film_version": {"type": "long"},
            "catalog_published_at": {"type": "date_nanos"},
            "catalog_updated_at": {"type": "date_nanos"},
            "imdb_rating": {"type": "float"},
            "creation_date": {"type": "date"},
            "genres": _NESTED_ID_NAME,
            "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {"raw": {"type": "keyword"}},
            },
            "description": _RU_EN_TEXT,
            "directors_names": _RU_EN_TEXT,
            "actors_names": _RU_EN_TEXT,
            "writers_names": _RU_EN_TEXT,
            "directors": _NESTED_ID_NAME,
            "actors": _NESTED_ID_NAME,
            "writers": _NESTED_ID_NAME,
        },
    },
}
