from postgres_to_es.pipelines.loader.mapping import MOVIES_INDEX_MAPPING


def test_creation_date_mapping_supports_film_access_policy() -> None:
    properties = MOVIES_INDEX_MAPPING["mappings"]["properties"]

    assert properties["creation_date"] == {"type": "date"}


def test_genres_mapping_preserves_id_and_name_as_nested_objects() -> None:
    genres_mapping = MOVIES_INDEX_MAPPING["mappings"]["properties"]["genres"]

    assert genres_mapping == {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "ru_en"},
        },
    }
