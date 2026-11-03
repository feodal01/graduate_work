"""Метаданные OpenAPI / Swagger — отдельно от сборки приложения."""

TAGS = [
    {'name': 'films', 'description': 'Оценки, закладки и рецензии конкретного фильма.'},
    {'name': 'reviews', 'description': 'Рецензия, её изменение и голосование за неё.'},
    {'name': 'users', 'description': 'Личные списки: оценки и закладки текущего пользователя.'},
    {'name': 'health', 'description': 'Проверка готовности: MongoDB обязательна.'},
]

DESCRIPTION = (
    'Закладки, лайки и рецензии онлайн-кинотеатра в MongoDB.\n\n'
    'Интерактивная документация: [/ugc-api/docs](/ugc-api/docs) (Swagger UI).'
)
