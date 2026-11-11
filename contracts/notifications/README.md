# Контракты событий Notifications

Эти файлы описывают события, которые другие сервисы присылают в
[Notifications](../../services/notifications). Сервис появился в спринте
уведомлений, а не в дипломном Биллинге: Биллинг шлёт письма через Campaign API
и эти схемы не использует.

Папка лежит в корне, а не внутри одного сервиса, потому что это граница между
сервисами. Отправитель и получатель проверяют себя по одному и тому же файлу,
поэтому, если одна сторона изменит формат события, а другая нет, упадут тесты.

| Схема | Кто отправляет | События |
| --- | --- | --- |
| [`auth.schema.json`](auth.schema.json) | Auth | `user.registered`, `user.profile_updated`, `user.deleted` |
| [`ugc.schema.json`](ugc.schema.json) | UGC API | `review.likes_updated` |
| [`movies.schema.json`](movies.schema.json) | Movies ETL | `film.published`, `film.updated` |
| [`bootstrap.schema.json`](bootstrap.schema.json) | UGC API | Манифест начальной загрузки: `load_id`, `expected_count`, `manifest_hash` |

В [`fixtures/`](fixtures) лежат примеры правильных сообщений для каждой схемы.

## Кто проверяет

| Тест | Что проверяет |
| --- | --- |
| [`services/notifications/tests/test_contracts.py`](../../services/notifications/tests/test_contracts.py) | Notifications принимает события в таком формате |
| [`services/ugc-api/tests/test_likes_relay.py`](../../services/ugc-api/tests/test_likes_relay.py), [`test_likes_bootstrap.py`](../../services/ugc-api/tests/test_likes_bootstrap.py) | UGC API отправляет события и манифест в таком формате |
| [`services/movies-etl/tests/test_catalog_contract.py`](../../services/movies-etl/tests/test_catalog_contract.py) | Movies ETL отправляет события в таком формате |
| [`tests/notifications/test_contracts.py`](../../tests/notifications/test_contracts.py) | Сами схемы и примеры к ним; запускается в CI |

Тесты находят папку по относительному пути, поэтому, если её переместить,
пути придётся поменять во всех этих файлах.
