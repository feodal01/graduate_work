"""Схема индекса продублирована намеренно: тесты не импортируют приложение.

genres обязан быть nested: фильтр по жанру приложение строит как nested-запрос.
"""

PERSON = {
    'type': 'object',
    'properties': {
        'id': {'type': 'keyword'},
        'name': {'type': 'text'},
    },
}

MOVIES_INDEX = {
    'settings': {
        'refresh_interval': '1s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
    },
    'mappings': {
        'dynamic': 'strict',
        'properties': {
            'id': {'type': 'keyword'},
            'creation_date': {'type': 'date'},
            'imdb_rating': {'type': 'float'},
            'title': {'type': 'text'},
            'description': {'type': 'text'},
            'genres': {
                'type': 'nested',
                'properties': {
                    'id': {'type': 'keyword'},
                    'name': {'type': 'text'},
                },
            },
            'directors_names': {'type': 'text'},
            'actors_names': {'type': 'text'},
            'writers_names': {'type': 'text'},
            'directors': PERSON,
            'actors': PERSON,
            'writers': PERSON,
        },
    },
}
