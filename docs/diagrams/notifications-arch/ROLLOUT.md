# Запуск и восстановление Notifications

Этот runbook описывает первоначальный запуск задач 1–17 и дополнение 18–22: три класса доставки, общий SMTP limiter и дневное окно дайджестов. Результаты проверок этапов приведены отдельно ниже.

Outbox — запись события в базе источника вместе с изменением данных. Relay — отдельный процесс, который передаёт outbox в Kafka и отмечает публикацию после подтверждения. Bootstrap — первичная передача старых данных с постоянным `load_id`. Manifest — количество и SHA-256 отсортированных пар идентификаторов снимка UGC; Notifications сверяет его со своими постоянными receipts.

## Настройки и подготовка

1. Заполнить `.env` по `.env.example`. Значения начальных флагов, порога, периодов, SMTP и языка — явные решения оператора. Начальные согласованные значения включают likes/new_films, выключают marketing, задают порог 3, периоды 86400/604800 секунд. `NOTIFICATIONS_INVALID_TZ_RETRY_SECONDS=300` в примере — изменяемый эксплуатационный выбор интервала повторной проверки часового пояса. Код не подставляет эти продуктовые значения. Пароли в URL PostgreSQL/RabbitMQ должны быть URL-encoded. Для локального запуска Compose уже содержит Mailpit (`notifications-mailpit:1025`, интерфейс `http://localhost:8025`); credentials не нужны. Для внешнего SMTP скопировать `infra/notifications/smtp.env.example` в игнорируемый Git файл `smtp.env` рядом и заполнить `NOTIFICATIONS_SMTP_USERNAME` и `NOTIFICATIONS_SMTP_PASSWORD` вместе.
2. Проверить `docker compose --env-file .env config --quiet`. Сначала сохранить резервные копии баз источников, состояния публикации Movies, Notifications и очередей. Не удалять старый ES-индекс.
3. Каждый узел каждого Mongo shard запускается с `transactionLifetimeLimitSeconds=${UGC_TRANSACTION_LIFETIME_SECONDS}`. Проверить `getParameter` непосредственно на всех шести `mongors{1,2}n{1,2,3}`, включая secondary. Mongos не возвращает этот параметр. Relay требует `batch_timeout + commit_timeout/1000 + 5 < lifetime`: текущие технические значения 20 + 5 + 5 < 60. Проверку повторять после замены узла.
4. Применить миграции `auth-migrate`, `admin-migrate`, `notifications-migrate`. Notifications должна иметь revision `004_campaigns`; предыдущая миграция `002_receipt_event_id` необходима до получения первых receipts и отвергает старые непустые строки без проверяемых event_id. Уточнить фактический revision через `alembic current`. Source Movies применяет только добавляющие SQL-миграции командой `python -m postgres_to_es.catalog_events migrate`.
5. `kafka-init` явно создаёт `auth.users.v1`, `ugc.reviews.v1`, `movies.films.v1` с 6 партициями/3 репликами; auto-create выключен. Старые `ugc.events`/DLQ сохраняются. Не запускать проверки на этих топиках.

## Первичная загрузка

Сервисы scheduler и detector имеют отдельные opt-in профили `notifications-scheduling` и `notifications-detection`. Bootstrap-команды входят в профиль `notifications-bootstrap`; явный `docker compose run` включает целевой сервис. На первом запуске не задавать эти два daemon-профиля через `COMPOSE_PROFILES`.

1. Развернуть код обычной записи outbox в Auth/UGC и состояние публикации Movies. Обычные события продолжают записываться на всём протяжении переноса, кроме явно описанных коротких capture-fences. Старый email `NULL` не восстанавливать из login и не угадывать.
2. Запустить зависимости, API, `notifications-consumer`, три relay, `notifications-publisher` и три группы `notifications-worker-transactional`, `notifications-worker-service`, `notifications-worker-bulk`. Перед worker выполнить отдельный `docker compose run --rm notifications-limiter-init`; этот сервис не является зависимостью worker. Scheduler пока выключен. У consumer одна группа на одну PostgreSQL-проекцию. Publisher направляет каждый класс в свою очередь. Пустой outbox не считается признаком завершённой загрузки.
3. Импортировать Auth: `docker compose run --rm auth-notifications-bootstrap --load-id initial-users-v1`. Команда передаёт текущие профили как `user.profile_updated`, без welcome. При повторе использовать тот же load_id; массовая запись атомарна.
4. Зафиксировать UGC: `docker compose run --rm ugc-notifications-bootstrap capture --load-id initial-likes-v1`. Capture блокирует 256 writer-gate buckets одной короткой транзакцией, затем материализует снимок пачками и снимает блокировку только после завершения. API временно возвращает 503 на конфликтующие записи. Relay использует отдельную блокировку и не удерживает writer-gate.
5. Отправить UGC: `docker compose run --rm ugc-notifications-bootstrap enqueue --load-id initial-likes-v1`. Команда проходит пачки до конца. Сохранить manifest: `docker compose run --rm -T ugc-notifications-bootstrap status --load-id initial-likes-v1 > infra/notifications/ugc-manifest.json`. JSON и load_id хранить вместе с резервной копией; повторная генерация status читает тот же постоянный снимок.
6. Подготовить Movies. Если рабочий индекс использует старую внутреннюю ES-версию, сначала выполнить описанный в README Movies ETL перенос в чистый индекс. Историческое множество берётся только из замороженного рабочего ES; строки, существующие только в PostgreSQL, не становятся историческими фильмами. Сохранить старый индекс. Координированно перевести API, ETL и detector на один новый индекс/alias. Использовать первоначальный UUID legacy load как bootstrap load_id.
7. Остановить запуск новых ETL jobs на время capture Movies. Выполнить `docker compose run --rm movies-notifications-bootstrap bootstrap capture --load-id initial-movies-v1`. Команда фиксирует UUID конкретного индекса, блокирует запись специальным ES add-block API с подтверждением drain, сохраняет неизменяемый снимок и только затем снимает собственную блокировку. После `captured_at` и `released_at` возобновить ETL.
8. Выполнять `docker compose run --rm movies-notifications-bootstrap bootstrap enqueue --load-id initial-movies-v1 --limit 500` до ненулевого `completed_at` в `bootstrap status`. Одна команда передаёт одну пачку. Общее количество known_films и постоянных enqueued-маркеров должно соответствовать captured_count. Только после этого включить `docker compose --profile notifications-detection up -d movies-detector`. Первая полная сверка обнаруживает фильмы и изменения, появившиеся после исходной границы. `catalog_published_at` означает сохранённое время первой попытки публикации, а событие новинки возникает после реальной видимости фильма в рабочем ES.
9. В Notifications зарегистрировать manifest: `docker compose run --rm notifications-bootstrap register --manifest /bootstrap/ugc-manifest.json`. Затем `docker compose run --rm notifications-bootstrap finalize --load-id initial-likes-v1`. Неуспех (exit 1) означает незавершённые/несовпадающие receipts, в том числе ожидающие профиля; дождаться Auth/UGC relay, устранить причину и повторить. Нельзя исправлять manifest под фактически полученное меньшее число.
10. Проверить контакты старых пользователей и агрегат `profiles_missing_email`. Проверить отсутствие исторических welcome/film_announcements и правильный baseline лайков. Затем включить `docker compose --profile notifications-scheduling up -d notifications-scheduler`. Периоды определяются по UTC; флаги проверяются при выборе аудитории и перед подготовкой письма.

Пустая установка проходит те же этапы: явно выполнить Auth import, UGC capture/enqueue/status/register/finalize с нулевым manifest и Movies capture/enqueue до completed_at. Пустая таблица не даёт права обойти readiness.

## Восстановление

- Хранить первоначальные load_id, manifest, границу времени, конкретный ES index UUID и постоянные маркеры. Повторять enqueue безопасно с тем же ID, в том числе после очистки опубликованного outbox. Не удалять Auth load markers, UGC bootstrap rows/receipts, Movies snapshot/enqueued markers, known_films или publication ledger вместе с outbox.
- После обрыва UGC capture сначала проверить, что writer-gate остаётся закрытым; возобновить тот же capture. Другой load_id отвергается. После завершённого capture повторять enqueue, а не создавать новый снимок.
- После обрыва Movies capture сохранить остановку записи. Повторить ту же команду при том же index UUID. Чужой ранее существовавший блок без доказанного drain отвергается и сохраняется; его владелец должен согласованно остановить ETL и разрешить снять/повторно установить блок. При неизвестном подтверждении собственного первоначального блока re-arm допускается только до `fenced_at` и до первой строки снимка. Частично сохранённый снимок нельзя открыть для записи. Не менять alias/индекс/fence вручную между попытками.
- Relay отмечает только подтверждённые сообщения. Обрыв после Kafka ACK до commit повторяет прежний event_id/payload. Consumer сохраняет событие, проекцию, задания и следующий offset одним commit. Crash/rebalance до commit повторяет запись; после commit не повторяет прикладной эффект. Неверный payload приостанавливает только свою партицию; ERROR-лог содержит topic, partition, offset и `manual repair required`. Независимые партиции продолжают работу. После исправления причины перезапустить consumer: он повторно прочитает сохранённую позицию. Новое корректное событие после некорректного не снимает блокировку. Offset вне retention по-прежнему останавливает процесс. Не делать auto-reset: восстановить нужный журнал/согласованную проекцию отдельной операцией.
- Просроченные scheduler/publisher/worker leases подхватывает новый процесс. Worker восстанавливает задания из PostgreSQL независимо от RabbitMQ. Frozen payload и Message-ID сохраняются на retry. После неопределённого SMTP-результата возможен дубликат; учитывать `duplicate_risk`, не объявлять доставку exactly-once. Успех сохраняет именно отправленный likes_count.
- При откате приложения остановить scheduler и worker, сохранить БД, очередь и pending события. Не выполнять downgrade, удаляющий outbox/проекцию, и не очищать очередь. Обычная запись событий источников продолжается; после исправления запустить процессы с прежним состоянием.

## Выпуски и дневное окно

Остановить старые scheduler и worker, применить миграцию `009_film_digest_selection`,
затем запустить обновлённые процессы. Миграция сохраняет состав старых выпусков.
Настройка `NOTIFICATIONS_FILMS_PER_DIGEST=20` сохраняется в каждом новом выпуске:
последующие изменения лимита не меняют его состав. Выбираются только самые свежие
фильмы по `catalog_published_at` (при равенстве — по `film_id`), остальные кандидаты
считаются обработанными без переноса. Для старых неподготовленных писем без сохранённого
лимита применяется текущая настройка worker; готовые письма остаются неизменными для повторов.
Откат миграции после исключения новинок запрещён, чтобы старый код не включил их
в существующие выпуски; для такого отката нужна резервная копия до миграции.

Выпуск — сохранённый состав рассылки и её аудитория за период. Начальные параметры: три новых лайка **на одном комментарии**, суточный период лайков и семисуточный период новинок. Прирост считается от последней успешной отправки, ежедневная проверка его не обнуляет. UTC-периоды привязаны к настраиваемым точкам `NOTIFICATIONS_{LIKES,FILMS}_SCHEDULE_ANCHOR_UTC`
(начальное значение `1970-01-01T00:00:00Z`): лайки готовятся ежедневно в 00:00, новинки по четвергам в 00:00. Перезапуски не сдвигают границы.

Scheduler одной транзакцией создаёт уникальный `(kind, period_start)`, сохраняет выбранные фильмы либо комментарии со счётчиками и фиксирует аудиторию. Новые подписчики и включение флага после отбора не дополняют выпуск. Курсор — сохранённая позиция в этой аудитории; задания создаются ограниченными пачками. Delivery, items, outbox и продвижение курсора пачки сохраняются вместе. Сбой продолжает существующий выпуск; завершение создания заданий не означает отправку всех писем. Карточки выбранных фильмов читаются при первой подготовке письма, затем snapshot остаётся неизменным.

Первичные отправки и retry классов `service`/`bulk` начинают передачу SMTP только в настроенном окне `NOTIFICATIONS_SEND_WINDOW_START` / `NOTIFICATIONS_SEND_WINDOW_END`
(по умолчанию **10:00 включительно — 20:00 исключительно**) по актуальному IANA `tz` профиля. Scheduler назначает ближайшее разрешённое время в UTC; worker повторяет проверку перед SMTP, включая задержку в RabbitMQ, ожидание Redis и смену `tz`. До начала окна используется его начало текущего местного дня, после конца — следующего. Welcome (`transactional`) отправляется сразу при выполнении общих условий.

При отсутствующем/неверном `tz` задание ждёт повторной проверки через `NOTIFICATIONS_INVALID_TZ_RETRY_SECONDS`; fallback не подтверждает местное время пользователя. Для scheduler и каждой группы worker этот параметр задан явно. Ожидание окна сохраняет outbox и освобождает lease одной транзакцией до ACK, без SMTP-попытки. Retry сначала учитывает обычную задержку, затем ближайшее дневное окно; он сохраняет ID и snapshot и не ждёт нового выпуска. Проверять `future_outbox` отдельно от готового backlog. E2E выбирает действующий IANA tz с текущим местным временем внутри окна, production-правило при проверке не меняется.

## Переход с одной очереди на три класса

1. Зафиксировать согласованные с SMTP-провайдером общий rate/burst, три доли и безопасный cooldown. Значения `.env.example` демонстрационные. Все экземпляры одного SMTP-аккаунта должны иметь один account_id и одинаковые настройки. Проверить итоговый Compose; concurrency и размеры пачек заданы отдельно для каждого класса.
2. Остановить **старые** publisher и worker старой версией Compose до замены конфигурации: `docker compose stop notifications-publisher notifications-worker`. Сохранить резервные копии PostgreSQL Notifications и RabbitMQ. Старую очередь `notifications.delivery` и её exchange не purge/delete: там могут остаться опубликованные задания. Запись событий источников продолжается.
3. Применить `docker compose run --rm notifications-migrate` до `003_delivery_send_class`. Миграция вычисляет send_class из kind, не изменяет ID, snapshot, Message-ID и сроки. Таблиц SMTP-лимитов в PostgreSQL нет.
4. Запустить `notifications-redis`; он хранит AOF на постоянном volume с `appendfsync always`, `no-appendfsync-on-rewrite no` и `noeviction`. Для внешнего Redis обязательны те же настройки: подтверждённое списание должно пройти fsync до ответа. Цена — задержка записи; `everysec` здесь не обеспечивает сохранность квоты. Выполнить отдельный `docker compose run --rm notifications-limiter-init`. При новом account hash tokens равны нулю; refill начнётся только после `NOTIFICATIONS_LIMITER_COOLDOWN_SECONDS`. При старом совместимом hash команда ничего не сбрасывает. Init не входит в автоматические зависимости worker.
5. Запустить новый publisher и все три группы worker. Каждая группа после истечения прежнего lease и handoff grace (по умолчанию 120 секунд, выбрать не меньше прежнего значения) восстанавливает свои незавершённые задания из PostgreSQL и публикует прежние delivery_id в новую очередь. `available_at`/`next_attempt_at` в будущем не приближаются, существующий неопубликованный retry не заменяется немедленным. Snapshot и Message-ID остаются прежними. Если перед остановкой результат SMTP был неизвестен, сохраняется риск дубликата; смотреть `duplicate_risk`.
6. Проверить прогресс всех классов и отсутствие попыток раньше срока. Сохранить старую очередь до отдельного согласованного завершения перехода: её удаление не является шагом этого runbook. Повторный запуск старого worker недопустим. Для отката остановить новые группы и publisher, сохранить БД, обе топологии и Redis, затем подготовить отдельный совместимый план отката.

## Восстановление общего лимитера

При недоступном Redis, отсутствии hash или неверном fingerprint worker не отправляет письмо и не увеличивает attempt_count: сохраняет deferred outbox и освобождает lease одной транзакцией до ACK. SQL-мониторинг различает готовые и будущие записи по классам. Масштабирование worker не увеличивает лимит аккаунта.

После потери или повреждения состояния остановить **все** группы этого SMTP-аккаунта, включая экземпляры на других узлах. Сохранить повреждённый hash для разбора. Копию можно использовать только при доказанном отсутствии потери подтверждённых списаний. Старая копия или отставшая реплика может вернуть уже потраченный запас при корректном fingerprint; её нельзя сразу подключать к worker. Если сохранность списаний не подтверждена, явно удалить **только** `notifications:smtp:{<account_id>}:limiter`, затем выполнить init с паузой, выбранной по ранее потраченному лимиту провайдера. Общие `FLUSHDB`/`FLUSHALL` запрещены. Изменение rate/capacity также меняет fingerprint и требует такого операторского перехода; увеличение лимита не должно мгновенно создавать полный запас.

Проверка `docker compose run --rm notifications-limiter-init status` возвращает 0 для исправного совместимого состояния, 1 для отсутствующего, повреждённого или недоступного; 2 означает неверные аргументы/настройки. Init возвращает 0 при создании или безопасном no-op, 1 при отказе состояния/Redis. После init дождаться cooldown, запустить группы и проверить class backlog, accepted attempts и ошибки лимитера. Не добавлять автоматический reset при рестарте контейнера.

## Наблюдение

`infra/notifications/metrics.sql` выводит pending outbox/возраст и ready/future по классам, offsets по topic/partition, pending без профиля, полученные/применённые receipts, задержку незавершённых расписаний, статусы доставок, SMTP attempts/outcome/duplicate_risk и число профилей без email. Пример: `docker compose exec -T notifications-db psql -U notifications -d notifications < infra/notifications/metrics.sql`.

Для consumer lag сравнивать Kafka log-end offset по каждой партиции с `kafka_offsets.next_offset`, а не с Kafka group committed offset: Kafka auto-commit выключен. Доступный в Kafka образе `kafka-get-offsets.sh --bootstrap-server kafka-0:9092 --topic auth.users.v1 --time -1` аналогично применяется к UGC/Movies.

В Auth контролировать `notification_outbox WHERE published_at IS NULL` и возраст `created_at`; в Movies — `notifications_source.outbox`; в UGC — `ugc_outbox`. Ошибки relay видны в stderr отдельных процессов; публикация с ошибкой оставляет строки pending. Для bootstrap использовать source status и локальную manifest-сверку. Агрегаты можно собирать существующим мониторингом без чтения адресов или тел писем. В диагностике сохранять event_id/delivery_id и outcome, не email/имя/JWT/SMTP credentials. Наличие HTTP API не означает, что consumer/worker готовы: следить за прогрессом offsets и попытками отправки отдельно.

## Проверено 10 октября 2026

Проверки выполнены на отдельном Compose-проекте `notifications-integration`; PostgreSQL/Mongo базы, Kafka topics, ES index и Rabbit queue имеют UUID. Production-хранилища и default source topics не изменялись. Изолированный Redis теперь входит в test Compose; прежний вспомогательный контейнер не останавливался.

- Реальный E2E с отдельными OS-процессами Auth/UGC/Notifications HTTP, consumer, relay, detector, scheduler, publisher и SMTP worker: `uv run --directory tests/notifications pytest test_contracts.py test_end_to_end.py -q -s` → **58 passed, 26.19s**, `old_data_emails=0`, `new_data_emails=5`. Проверены изменения во время переноса, новое письмо, новый email в следующих доставках, порог лайков, новинка/обновление фильма, отключение флагов и soft delete.
- `uv run --directory tests/notifications python run_service_checks.py <service>`: **Auth 350**, **UGC 55**, **Movies ETL 77**, **Notifications 171** passed; все выбранные integration cases выполнены. Включены реальные crash/restart/ACK-before-commit/rebalance/lease/retry проверки задач 4, 6, 8, 11, 15, 16. UGC concurrent enqueue дополнительно отдельно на свежей UUID-базе: **1 passed**, минимум три собственные рецензии.
- `ruff check .`, `ruff format --check .`, `mypy` всех четырёх сервисов прошли (Movies ETL с `--group lint`). Compose config проверен с `.env.example`, с bootstrap/scheduling/detection профилями и без них. Новые E2E/helper файлы прошли Ruff.
- Собраны runtime-образы Auth, UGC, Movies ETL, Notifications и Admin. Независимая проверка Admin восстановила dump в UUID PostgreSQL, выполнила настоящий Django migrate до movies0003, проверила пять вариантов trigger, reverse/reapply и повторную идемпотентную migrate; тестовая база удалена и отсутствие проверено. Миграция в образе совпала по SHA-256 с исходником. Это проверка образа/миграции, а не развёртывание production Compose.
- `docker compose -f tests/notifications/compose.yaml --profile sharded up -d --wait shard-router`, затем `uv run --directory tests/notifications pytest test_sharded.py -q -s` → **1 passed, 1.02s**. Два отдельных shard replica sets и mongos: значение lifetime **60** проверено на каждом участнике; одна транзакция записала в две UUID-базы с разными primary shards, следующая откатила обе записи. Production-кластер не менялся.
- После проверки логов добавлены регрессии с контрольным email в SQL-параметрах и цепочке исключений. Consumer/worker сохраняют класс ошибки и безопасные идентификаторы, не печатают исключение с payload; standalone consumer завершает процесс кодом 1 без traceback.
- Docker **29.1.3**, Compose **5.0.1**, uv **0.9.18**, PostgreSQL **16.15**, MongoDB **8.0.32**, Kafka **3.4.1**, ES **8.6.2**, RabbitMQ **4.1.4**, Mailpit **1.27.8**. Host Python: Notifications/UGC/E2E **3.12.12**, Auth/Movies ETL **3.14.2**; Admin image Python **3.10.22**, Django **5.0.4**. Production runtime Notifications закреплён на **3.12.12**.

Итоговое ревью всей реализации выявило несоответствие международных адресов между Auth и SMTP. Исправление `7af144d` нормализует IDN-домены через IDNA2008 и использует SMTPUTF8 для Unicode localpart. Если сервер его не поддерживает, попытка завершается известным постоянным отказом до DATA, без `duplicate_risk`. Исходный snapshot сохраняется. Повторное ревью исправления завершено без замечаний.

После исправления: `uv run --directory services/notifications pytest tests/test_delivery.py -q` — **34 passed**; Ruff, mypy и lock-check прошли. Заключительный `uv run --directory tests/notifications pytest test_end_to_end.py -q -s` — **1 passed, 26.41s**, прежние **0 исторических / 5 новых писем**. Образ Notifications пересобран успешно (manifest `c0a03783da50c5d534f26c2b9b700897c3e61bb26cefab0e9bccbe0018c1960f`). Полные наборы выше выполнялись до этого исправления; после него повторены затронутые проверки и E2E.

Существующие предупреждения Starlette/httpx и backoff/Python 3.14 сохранены; ошибок проверок не скрывают. CI-конфигурация добавлена, удалённый GitHub run в этом локальном запуске не выполнялся. Проверка всего production Compose с реальными данными намеренно не проводилась.

## Проверка дополнения 18–22, 10 октября 2026

Использован уже работающий отдельный `notifications-integration`. E2E создаёт собственные UUID-базы, топики, очереди, индексы и Redis account hash; cleanup удаляет только эти ресурсы. Отказ Redis проверен реальным недоступным TCP endpoint, затем отсутствующим и удалённым UUID hash. Общий Redis не останавливался и не очищался. Source-service suites не повторялись: код источников в дополнении не менялся.

- `uv run --directory tests/notifications pytest test_contracts.py test_end_to_end.py test_delivery_classes.py -q -s` → **59 passed, 51.13s**. Исходный процессный сценарий сохранил **0 исторических / 5 новых писем**. Новый сценарий проверил прогресс трёх классов при bulk backlog, повтор service после настоящего отказа SMTP, отсутствие SMTP/попыток при недоступном Redis и потере hash, восстановление после явного init/cooldown, будущий срок и неизменный snapshot. Перед миграцией `003` созданы legacy published handoff и delayed retry; после миграции доставка восстановлена через PostgreSQL, три сообщения старой UUID-очереди остались на месте.
- `uv run --directory tests/notifications python run_service_checks.py notifications` → **346 passed, 143.54s**, без пропусков. Одно предупреждение Alembic в проверке совпадения ORM/миграции: `Computed default on deliveries.send_class cannot be modified`; тест завершился успешно.
- Ruff по сервису и обоим E2E прошёл; `ruff format --check` — **58 файлов**, mypy — **38 source files**, Compose config с `.env.example` и профилем `notifications-bootstrap` прошёл.
- Собран Notifications runtime image `sha256:488a0859fdeb33a45626ff70efef70225c7afa05285331b430b09e586a7fd161`. Внутри образа проверены IANA `Europe/Moscow` и `America/New_York`, наличие упакованного `rate_limit.lua` и настоящий вызов Lua к тестовому Redis: отсутствующее состояние блокирует, init создаёт нулевой запас, cooldown блокирует, затем разрешение получается. Единственный UUID account hash удалён с проверкой отсутствия.

Это проверка реализации и собранного образа, а не развёртывание production. Лимиты, burst и cooldown в `.env.example` остаются демонстрационными и требуют согласования с SMTP-провайдером; invalid-tz retry 300 секунд — отдельный эксплуатационный выбор. Production-данные, старая production-очередь и учётная запись SMTP не изменялись. Удалённый GitHub CI run не выполнялся.

### Корректирующая проверка финального review, 10 октября 2026

Два эксплуатационных исправления проверены отдельно: Redis теперь подтверждает списание только после fsync, worker обрабатывает настоящий SIGTERM/SIGINT. При остановке worker прекращает intake/recovery, даёт активным callbacks до 45 секунд (`--shutdown-timeout`), затем отменяет оставшиеся, ждёт освобождения ownership и закрывает ресурсы. Compose `stop_grace_period: 60s` оставляет запас на cleanup. Прерванная SMTP-попытка остаётся неопределённой; после lease recovery выставляет `uncertain` и `duplicate_risk`.

- `uv run --directory services/notifications pytest tests/test_delivery.py tests/test_delivery_limits.py tests/test_delivery_routing.py tests/test_delivery_windows.py tests/test_worker_shutdown.py tests/test_schema.py -q` → **103 passed, 65.17s**, без предупреждений. Известное предупреждение Computed теперь узко проверяется в тесте; сравнение metadata по-прежнему требует пустой diff, проверка реальной миграции сохранена.
- После усиления проверки освобождения advisory lock отдельным PG-соединением до закрытия worker DB: `uv run --directory services/notifications pytest tests/test_worker_shutdown.py -q` → **3 passed, 4.88s**. Subprocess проходит production `main/run/consume/handle/send_delivery`; управляемые broker/SMTP/limiter adapters делают момент сигнала воспроизводимым. Проверены SIGTERM/SIGINT с завершением активной отправки и SIGTERM с истечением deadline, отсутствие ACK после отмены и последующая uncertain recovery. PostgreSQL и advisory ownership настоящие. Отдельный процессный E2E ниже использует настоящие RabbitMQ/Redis/SMTP.
- `uv run --directory tests/notifications pytest test_delivery_classes.py -q -s` → **1 passed, 27.78s**. Worker готовы до init; отсутствие ранних попыток/отправок проверяется по сохранённым timestamps относительно фактического Redis `ready_at`, без требования запустить процессы за две секунды. Проверка последующего прогресса сохранена.
- `uv run --directory tests/notifications pytest test_redis_durability.py -q -s` → **1 passed, 2.22s**. Отдельный UUID-контейнер запущен с командой production-сервиса Redis; эффективные настройки проверены через `CONFIG GET`. Настоящее списание Lua сохранено после SIGKILL и запуска того же AOF, повторное списание запрещено, повторной инициализации нет. Удалены только собственные UUID-контейнер/volume. Этот тест проверяет падение процесса; гарантия при потере питания опирается на [контракт Redis `appendfsync always`](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) и исправное хранилище, выполняющее fsync.
- Ruff/format по пяти затронутым Python-файлам, mypy по worker и Compose config прошли. Runtime image пересобран: `sha256:bfafba2b6cccef56504f63d645df97988c754b73d69980040c3805804e69fe8b`; CLI внутри образа содержит `--shutdown-timeout`.

Команды и результаты корректирующих проверок приведены выше. Полные неизменённые Auth/UGC/Movies/Notifications suites повторно не запускались; production и удалённый CI не менялись. Общий тестовый стек и Redis не останавливались, `FLUSHDB`/`FLUSHALL` не применялись.

Последующая проверка reconnect выявила и устранила зависание после штатного завершения чтения очереди: worker теперь ждёт завершения intake или сигнала остановки, затем корректно возвращается к переподключению. После исправления `pytest tests/test_worker_shutdown.py -q` — **4 passed, 5.81s**, включая новый subprocess сценарий normal iterator end → reconnect → SIGTERM; исходный cancellation test — **1 passed, 1.82s**. Ruff/format/mypy прошли. Финальный образ пересобран: `sha256:59c793b286700a25ea059f6283186b5059bce1417e07d1f7d1a58191f90aab5a`; in-image CLI smoke прошёл.

Итоговое широкое ревью дополнения и повторные проверки исправлений завершены. Все пять исходных замечаний и обнаруженная при повторном ревью регрессия reconnect закрыты; открытых замечаний нет.

## Внешние рассылки: запуск дополнения 23–27

Campaign API — отдельный внутренний HTTP-процесс для приёма батчей персональных писем. Перед запуском применить `alembic upgrade head` в Notifications: новая миграция расширяет существующие deliveries и добавляет campaigns, campaign_batches, campaign_recipients. Сохранённые задания, outbox и снимки старых писем должны оставаться неизменными. Точное имя текущей revision можно проверить через `alembic current`.

Запустить `docker compose --profile notifications-campaigns up -d notifications-campaign-api`. Сервис использует общую базу Notifications, явный `NOTIFICATIONS_INVALID_TZ_RETRY_SECONDS` и обязательный непустой `NOTIFICATIONS_CAMPAIGN_API_KEY`; слушает внутренний 8000 без публикации порта на хост и без маршрута nginx. В обычном API endpoints рассылок отсутствуют. Backend передаёт `X-API-Key` при приёме, повторе, закрытии и чтении статуса; без действующего ключа получает `401`. Подключение backend должно быть защищено TLS, ключ не передаётся браузеру. Compose profile сам по себе не обеспечивает защиту соединения.

Перед переходом согласовать ключ с вызывающим backend и применить миграцию `005_campaign_author`. Новые кампании требуют UUID `created_by_user_id`; автор неизменен и входит в хеш батча. Исторические кампании сохраняют `null` и прежние хеши: доступен точный повтор старого батча без автора, close и GET, но новый батч отклоняется `409 campaign_author_conflict`. Не назначать истории вымышленного автора. Для отзыва ключа заменить env и пересоздать все экземпляры API; обычный `docker compose restart` не обновляет env контейнера. Старый ключ перестаёт приниматься после завершения замены всех экземпляров, уже принятые доставки не отменяются.

Publisher и `notifications-worker-bulk` должны работать, а limiter — быть инициализирован оператором по процедуре выше. `personal_offer` требует активный профиль с email и `marketing=true`; default marketing=false из `.env.example` не даёт согласия на рассылку. Профили поступают существующим Kafka consumer. Если профиль ещё не дошёл, API возвращает `409 profile_not_ready`, полностью откатывает батч и позволяет повторить его позже с прежними идентификаторами.

При сбое ответа повторять исходный батч с теми же UUID и содержимым. Точное подтверждение повтора не создаёт новых заданий и не меняет сохранённое время, в том числе после close, отзыва подписки или запрета/удаления шаблона из реестра. После подтверждения всех батчей вызвать close с expected_batches и опрашивать GET; открытая загрузка всегда остаётся in_progress. `smtp_uncertain` входит в failed, но не доказывает отсутствие письма у SMTP-провайдера. Ручная повторная кампания может продублировать такое письмо. Поля запросов, пределы и примеры ответов: [контракт Campaign API](../../../services/notifications/docs/CAMPAIGNS.md).

Для запрета новых заявок установить у шаблона `external=False`; запись реестра и HTML-файл сохранять до завершения принятых заданий. Первая подготовка читает текущий шаблон, отдельного хранения его версий нет. После подготовки повторы SMTP используют сохранённый снимок.

Откат приложения не требует downgrade базы. Явный downgrade `004_campaigns` → `003_delivery_send_class` удаляет таблицы и историю рассылок, даже если все получатели были пропущены. Старые внутренние deliveries сохраняются; при external deliveries downgrade блокируется. Перед любым удалением схемы сохранить резервную копию истории.

HTTP-контракт тела описан в README. Потоковое чтение с ограничением размера не публикует request body schema в OpenAPI; автоматическая генерация клиентских тел в этот этап не входит. При откате ниже `005` информация об авторе будет утрачена: перед downgrade сохранить её, а старое приложение без проверки ключа не открывать вызывающим сервисам.

Тестовый Kafka требует достаточного heap для LogCleaner. При запуске этого этапа изолированный broker с `-Xmx256m` завершился с `OutOfMemoryError` в `SkimpyOffsetMap`; `tests/notifications/compose.yaml` использует `-Xms256m -Xmx512m`. После изменения пересоздан только Kafka отдельного `notifications-integration`, и healthcheck прошёл. Это размер для тестового стенда, не рекомендация для production.

### Проверка дополнения 23–27, 11 октября 2026

Проверки выполнены на отдельном `notifications-integration` после завершения исходников API и worker. Новый E2E использует самостоятельные процессы campaign API, обычного API, publisher и bulk worker; PostgreSQL, RabbitMQ, Redis и Mailpit настоящие. Профили здесь создаются в локальной базе: получение событий источников проверяется сохранённым исходным E2E. Все изменяемые ресурсы имеют UUID; общие Redis и production-данные не очищались.

- `uv run --directory tests/notifications python run_service_checks.py notifications` → **490 passed, 194.10s**, без пропусков и предупреждений. Включены новые API/transaction/migration/worker-проверки и прежние Notifications-регрессии.
- `uv run --directory tests/notifications pytest test_contracts.py test_end_to_end.py test_delivery_classes.py test_campaigns_end_to_end.py test_redis_durability.py -q -s` → **61 passed, 71.58s**, без пропусков. Исходный процессный сценарий: **0 исторических / 5 новых писем**. Campaign-сценарий: 6 получателей, 3 sent и 3 skipped с отдельными причинами; после настоящего отказа SMTP ещё 1 письмо принято Mailpit с сохранённым snapshot. Всего для этого сценария Mailpit подтвердил 4 письма. Проверены повтор батча после close/opt-out, отсутствие дублей, missing-profile retry, согласованные суммы GET, неизменный исходный available_at, отсутствие ранних публикаций/попыток и экранированный HTML/Jinja-текст с переносами.
- Неопределённый SMTP проверен реальным TCP: локальный тестовый сервер читает DATA и разрывает соединение до подтверждения. Worker-процесс выполнил 3 попытки с одинаковыми байтами; API вернул `failed_reasons={"smtp_uncertain":1}`. Получатель, выключивший marketing после приёма батча, дал `skipped_reasons={"subscription_disabled":1}` без SMTP-попытки. Этот fault-сценарий проверяет неопределённость ответа и не доказывает доставку или недоставку в почтовый ящик; успешный SMTP отдельно проверен Mailpit.
- Ruff по сервису и процессному harness прошёл; `ruff format --check` → **75 файлов**, `mypy src migrations` → **46 файлов без ошибок**, `uv lock --check` → прошёл. Compose config с `.env.example` и профилями `notifications-campaigns`, `notifications-bootstrap` прошёл; дополнительно проверено отсутствие host ports, JWT и gateway dependency у campaign API.
- `docker build -t notifications-campaign-task27:local services/notifications` → образ **sha256:28538aa29ec5e10047cf78ee19c18847cf202d557dd6ee7104f013e55274475f**. Внутри образа загружены все 4 HTML-шаблона и 3 внутренних subject-файла, проверены literal/escaped рендер personal_offer, наличие Lua, IANA Europe/Moscow и America/New_York, отдельные маршруты двух приложений.

Source-service suites и неизменённые sharded source-проверки не повторялись. Production-развёртывание и удалённый GitHub CI run не выполнялись. Service authentication остаётся отдельным открытым решением.

### Проверка исправлений финального ревью, 11 октября 2026

После исходного прогона исправлены три границы: HTML получает сохранённый literal `subject`; текущий допуск шаблона проверяется после поиска точного повтора батча; переполнение календаря возвращает очищенную ошибку с `items[index].available_at`. Схема входа остаётся явно связана с `personal_offer`, новые неподдерживаемые шаблоны и каналы отклоняются до записи батча.

- Новые проверки сначала воспроизвели дефекты: **6 failed, 7 passed**; после исправлений — **13 passed**. PostgreSQL-проверки подтверждают неизменность всех сохранённых строк при повторе после закрытия, удаления профиля и изменения реестра, а также полный откат ошибочного батча.
- `uv run --directory services/notifications pytest tests/test_campaigns_api.py tests/test_campaigns_transactions.py tests/test_templates.py tests/test_campaign_delivery.py tests/test_delivery.py -q` → **153 passed, 61.10s**. Включён настоящий рендер шаблона с `subject` и прежние проверки внутренних писем.
- `ruff check .` прошёл; `ruff format --check .` → **70 файлов**; `mypy src migrations` → **46 файлов без ошибок** (все команды через `uv run --directory services/notifications`).
- `uv run --directory tests/notifications pytest test_campaigns_end_to_end.py -q -s` → **1 passed, 21.07s**: 6 получателей, 3 sent, 3 skipped; отдельный retry sent=1; 4 письма Mailpit; uncertain=1; поздний отказ от подписки skipped=1.
- `docker build -t notifications-campaign-final-fix:local services/notifications` и прежний `task-27-validation/image_smoke.py` на новом образе прошли. Image ID: **sha256:4c07e3fe217b3656887dd9615da9bd4c47368bb5ec9ff27474a5ce8869c474fd**.

Полный прежний прогон 490 тестов не повторялся; приведённые 153 проверки и процессный E2E выполнены после исправлений. Исходные ограничения production/CI и service authentication остаются прежними.

Итоговое независимое ревью этапа 23–27 и отдельная проверка исправлений `687f66d..50fabe5` завершены: все три замечания закрыты, новых дефектов не найдено. Реализация сохранена локальными коммитами в `main`; production-развёртывание и запуск удалённого CI в этот этап не входили.

## Единые настройки через Pydantic Settings

Задачи 28–30 заменяют прямое чтение эксплуатационных env в worker Notifications, Django Admin, bootstrap Auth/GlitchTip и клиентах исследовательского стенда. Остальные Python-сервисы уже используют BaseSettings. Сохраняются имена env и совместимые defaults; явные CLI-параметры worker имеют приоритет. Значения worker проверяются до открытия клиентов; пароли и новые секретные строки скрыты в представлении модели и тексте ошибок. Техническая установка DJANGO_SETTINGS_MODULE и подстановка переменных самим Compose/uWSGI сохраняются.

Проверки на окончательном коде этого этапа:

- Notifications: `pytest tests/test_config.py tests/test_worker_config.py tests/test_worker_shutdown.py tests/test_delivery.py tests/test_delivery_limits.py tests/test_delivery_routing.py tests/test_delivery_windows.py tests/test_rate_limit.py -q` — **238 passed, 53.01s**. После добавления двух тестов без изменения production-кода отдельно `pytest tests/test_config.py tests/test_worker_config.py -q` — **105 passed, 0.41s** (перекрывается с предыдущим набором). Ruff/format — 71 файл; mypy — 46 файлов, без ошибок.
- `uv run --directory tests/notifications pytest test_end_to_end.py test_delivery_classes.py test_campaigns_end_to_end.py -q -s` — **3 passed, 82.60s**. Реальные API/consumer/publisher/worker, PostgreSQL/Kafka/RabbitMQ/Redis/SMTP; welcome, три класса, повторы, пропуски и неопределённый SMTP-результат сохранили поведение.
- Admin: собран `notifications-task29-admin:local` из обычного Dockerfile с новой зависимостью. `python manage.py test users example --noinput` в этом образе на отдельном временном PostgreSQL — **63 passed**, включая 8 новых config-тестов. `manage.py check`, импорты settings/WSGI/ASGI и config-тесты без сети прошли. Тестовые PostgreSQL и сеть удалены. В сборке остаются прежние предупреждения синтаксиса ENV; ожидаемый тест исчерпания backoff пишет сообщение в лог.
- Auth: `uv run --directory services/auth pytest tests/test_bootstrap.py tests/test_config.py tests/test_cli.py -q` — **25 passed**; scoped Ruff/mypy и проверка lock прошли. Mypy сообщает прежние заметки о неиспользуемых overrides.
- Research: `uv run --directory research/storage python -m unittest discover -s tests -v` — **4 passed**; Ruff, mypy и lock прошли. Benchmark не запускался, исследовательские данные не менялись.
- GlitchTip: в реальном `glitchtip/glitchtip:6.2.6` уже есть pydantic-settings 2.13.1. Изолированные config-тесты в образе — **3 passed**; импорт нового settings-модуля и компиляция bootstrap прошли без сети и без запуска операций создания пользователей/проектов. Compose монтирует оба Python-файла.
- `docker compose --env-file .env.example --profile notifications-campaigns --profile notifications-bootstrap config --quiet` прошёл без предупреждений. Первоначальный поиск прямого чтения env ошибочно исключил migrations/ и поэтому пропустил два чтения NOTIFICATIONS_DATABASE_URL в Alembic; окончательная проверка и исправление приведены ниже. Тестовые fixtures исключены.

Три части прошли отдельные независимые ревью. Полные наборы незатронутых сервисов и production-развёртывание не выполнялись. Новые функциональные требования APPROVED об авторе кампании, проверке сервисного ключа и сроке срочной доставки этот рефакторинг не реализует.


### Исправление настроек миграций после финального ревью, 11 октября 2026

Online и offline Alembic теперь получают `NOTIFICATIONS_DATABASE_URL` через отдельный `DatabaseSettings` на основе BaseSettings. Модель требует только URL базы; `NOTIFICATIONS_PROCESS`, JWT, SMTP и другие настройки приложения не нужны. SQLAlchemy проверяет структуру URL без соединения; драйвер, query-параметры и исходная запись URL сохраняются. Значение хранится как SecretStr до передачи Alembic/SQLAlchemy, входные данные скрыты в тексте ошибок валидации.

- RED: новый `pytest tests/test_migration_config.py -q` — **9 failed, 1 passed**: отсутствовала отдельная модель, оба реальных режима Alembic обходили Pydantic-валидацию. GREEN после исправления — **10 passed, 0.84s**; проверены обязательность/ошибочная структура URL, скрытие секретов, независимость от настроек приложения и обе точки входа Alembic.
- Настоящий `alembic upgrade 001_initial --sql` выполнился без соединения и создал **256 строк SQL**. Offline `upgrade head --sql` по-прежнему не поддерживается исходной миграцией `002_receipt_event_id.py:13`: проверка существующих bootstrap_receipts вызывает `MockConnection.scalar`, которого нет в offline-режиме. Та же ошибка воспроизведена с исходным `HEAD` env.py во временной копии миграций; старые миграции не изменялись.
- Ruff/format всего Notifications — **73 файла**, mypy `src migrations` — **47 файлов без ошибок**. Первые scoped-проверки выявили форматирование одного теста и статический `call-arg` для env-загрузки BaseSettings; форматирование исправлено, загрузка помечена узким пояснённым ignore по существующему соглашению сервиса.
- Повторный поиск по всему принадлежащему проекту Python-коду **включает migrations/**: `rg -n 'os\.getenv|os\.environ|\bgetenv\s*\(|\benviron\s*\[' --glob '*.py' --glob '!**/tests/**' --glob '!**/test_*.py' --glob '!**/conftest.py' .` нашёл только **4 разрешённые записи DJANGO_SETTINGS_MODULE**. Runtime-чтений env вне BaseSettings не осталось.

`uv run --directory services/notifications pytest -q` — **593 passed in 213.00s (0:03:33)**, без пропусков и предупреждений. Настоящие online `upgrade head` выполнялись fixture на отдельных UUID-базах PostgreSQL; schema-тест также прошёл downgrade до base и повторный upgrade head. Production-миграции и развёртывание не выполнялись.

Итоговое независимое ревью настроек и scoped-проверка `51e75d0..cf4f61b` завершены: пропуск конфигурации миграций устранён, новых дефектов не найдено. Изменения сохранены локальными коммитами в `main`.

## Исправления полного аудита, 11 октября 2026

Задачи 31–34 исправляют все пять пунктов аудита от 11 октября 2026 года. Ключ и автор описаны в разделе запуска выше. Пароли PostgreSQL Auth/Movies теперь `SecretStr` до передачи DB-клиенту; обычные ошибки конфигурации не выводят входные значения. При явной сериализации Pydantic `ValidationError.errors()`/`.json()` вызывающий код по-прежнему должен задавать `include_input=False`.

Свежие проверки после исправлений:

| Область | Результат |
| --- | --- |
| Auth / Movies ETL, полные наборы с хранилищами | 361 / 80 passed |
| Notifications, полный набор на финальном production-коде | 606 passed; после коллекции добавлены ещё пять тестовых случаев, проверены адресно |
| Финальные кампании: API/транзакции, security, миграции | 87 / 17 / 48 passed соответственно, наборы пересекаются |
| Контракты + прежние end-to-end / классы доставки | 59 passed |
| Кампании, реальные процессы и SMTP | 1 passed |
| Полный путь срочных писем + отрицательные проверки измерения | 2 passed, 71,35 секунды |
| Ruff/format/mypy/lock в изменённых сервисах и модели | PASS |
| Новый Docker-образ Notifications | Сборка, импорт, обязательность ключа, app factory и наличие миграции PASS без сети/lifespan |

Нагрузочный профиль: 300 bulk через API, 12 регистраций Auth с интервалом около секунды и одна немедленная внешняя transactional заявка. По одному worker каждого класса, concurrency=2; rates/capacities total/transactional/service/bulk = 20/10/5/5. SMTP — локальный Mailpit; отдельный service-поток не подавался. Bulk backlog сохранялся при всех 13 срочных поступлениях, число отправленных bulk за это время выросло с 4 до 67; в конце отправлены все 300.

Максимальная консервативная верхняя граница для срочного письма — **2,100554 секунды**. Начало измерения — до HTTP-запроса источнику, конец — после видимости письма в SMTP и commit статуса sent после SMTP acknowledgement. Это включает COMMIT→SMTP и не выдаёт точное время COMMIT за измеренное. Проверяется каждое письмо, а отсутствие получателя и превышение 180 секунд приводят к падению. [Полные измерения](validation/transactional-latency-2026-10-11.json). Нагрузочный тест и сохранение JSON включены в CI; hosted CI в этой сессии не запускался. Профиль подтверждает локальную проверку, не произвольную производственную нагрузку/квоту SMTP.

Независимые ревью каждой задачи одобрили требования и качество; замечание о старом комментарии Compose исправлено. В Auth остаётся прежний шум завершения OTEL exporter, в Movies — предупреждения backoff на Python 3.14; тесты завершились с exit 0. Никаких подавлений этих сообщений не добавлено. Полная матрица Python, gateway и TLS-развёртывание остаются за границами этого прогона.

Общее независимое ревью `d1a1467..4432cef` одобрило все пять исправлений и их соответствие актуальному APPROVED. Замечание об откате схемы в README исправлено в `d4653c7` и прошло отдельное повторное ревью; открытых замечаний этапа нет.

### Настройки размеров батчей

Размеры Notifications перечислены в README сервиса и корневом `.env.example`.
Размеры UGC relay/bootstrap задаются `UGC_RELAY_BATCH_SIZE` и `UGC_BOOTSTRAP_BATCH_SIZE`;
Compose передаёт их как `RELAY_BATCH_SIZE` и `BOOTSTRAP_BATCH_SIZE`.
Movies использует `CATALOG_BATCH_SIZE`, `CATALOG_RELAY_BATCH_SIZE` и
`CATALOG_LEGACY_BATCH_SIZE` для обнаружения/первичной загрузки, relay и переноса старого каталога.
После изменения `.env` пересоздать соответствующие контейнеры.

Перед увеличением `NOTIFICATIONS_CAMPAIGN_BATCH_SIZE` применить `alembic upgrade head`
(revision `006_configurable_batch_size`): верхняя граница приёма переходит из CHECK БД
в настройки API, положительный размер остаётся ограничением БД. Отдельный лимит HTTP-тела
в 1 МиБ сохраняется. Downgrade на `005` отклоняется, если уже сохранены батчи больше 100;
для отката приложения сохранять схему и историю без downgrade.
Auth bootstrap по согласованию остаётся одной транзакцией для всех пользователей.

### Линтеры и типизация нового кода, 11 октября 2026

Область проверки — Python-код, добавленный или изменённый после `c3863a9`:
Notifications, Auth, UGC API, Movies ETL, Admin, GlitchTip, research и интеграционные тесты.
Ruff check и format проходят для 335 файлов; настроенные mypy-проверки — для 207 файлов.
Pyright проходит для Notifications целиком, UGC API, Movies ETL и всего Auth (126 файлов).
Notifications использует strict для `src`/`migrations`, standard для тестов с pytest-фикстурами.
Обязательные env-настройки сохранены; точечные исключения поясняют ограничения аннотаций библиотек.
По дополнительному запросу исправлены также 16 прежних замечаний в семи тестовых файлах
Auth: уточнены типы, проверки nullable-значений и подмена операций миграций.
Все 50 затронутых тестов, включая сценарии с реальным Redis, прошли;
полные Ruff/format/mypy/Pyright для Auth повторно прошли.

Проверки после исправлений: Notifications **660 passed**, Movies ETL **99 passed**,
контракты **57 passed**; целевые Auth **86 + 1 + 23 passed** (наборы пересекаются),
UGC **12 + 10 passed**, Admin **8**, GlitchTip **3**, storage **4**.
Полные Notifications/Movies и вспомогательные тесты запускались в отдельных окружениях
Python 3.13: локальный Python 3.12 зависал до выполнения кода; Auth и DES проверены на 3.14.
Зависимости основных сервисов установлены из lock-файлов; вспомогательные процессы тестов
использовали совместимые harness-зависимости в тех же временных окружениях.
Compose config и независимое ревью проходят. CI теперь линтит всю `tests/notifications`;
для Admin/GlitchTip добавлены локальные правила Ruff только для новых файлов.

### Самодостаточный SMTP для ревью, 11 октября 2026

Основной Compose содержит `notifications-mailpit` (SMTP 1025 внутри сети, UI на
127.0.0.1:8025, отдельный том писем). `.env.example` направляет worker в этот контейнер.
`smtp.env` нужен только для credentials внешнего провайдера; добавлены шаблон и Git ignore.
Команды запуска и `review_smoke.py` приведены в README Notifications.
Проверены сборка и запуск выбранных сервисов основного Compose в отдельном проекте
`notifications-review`, миграции, первичная и повторная инициализация лимитера,
две регистрации через Auth и два welcome-письма в Mailpit через Kafka/RabbitMQ/worker.
Compose config, Ruff/format и mypy smoke-скрипта проходят. Внешние письма не отправлялись;
дайджесты и полный набор остальных сервисов в этой SMTP-проверке не запускались.

## Переход на контакты в Auth и обязательный tz

Контакты — `email`, `first_name`, `last_name`, `locale`. После перехода они остаются
в Auth; Notifications хранит идентификатор, версию, часовой пояс, активность и флаги.
Готовое письмо временно содержит адрес и персонализированный текст для повторов.
Миграция `007_contact_privacy` удаляет четыре контактные колонки `profiles`, добавляет
время завершения задания `terminal_at` и допускает очистку содержимого завершённого письма.
Удалённые контакты не восстанавливаются откатом схемы; откат после начала очистки требует
отдельного восстановления данных, а не запуска старого worker на новой схеме.

### Новая установка

Использовать актуальные образы Auth и Notifications вместе. В `.env.example` заданы
`AUTH_ADMIN_TZ`, лимиты пачек API контактов, таймаут/пауза обращения к Auth и срок
хранения готовых писем. API контактов использует общий `SERVICE_API_KEY` через
`X-Service-Key`; внешний gateway не маршрутизирует `/api/v1/internal/`.
Worker запускает `notifications-maintenance` как зависимость. Это отдельный процесс,
который завершает старые дневные задания без `tz` и очищает содержимое завершённых писем.
Пример хранения — сутки; это эксплуатационная настройка, которую можно изменить.

### Установка с существующими данными

Образы этого изменения выкладываются согласованно: промежуточный старый worker
после удаления контактных колонок не работает. Для простой локальной установки:

1. Остановить Auth API/relay, Notifications API/consumer/scheduler/publisher/workers и
   Campaign API. Дать работающим worker завершиться штатно; брокеры и базы сохранить.
2. Собрать новые образы, сохранить резервную копию перед удалением колонок и выполнить
   миграцию Notifications до `007_contact_privacy`. Новые reader принимают старые полные
   Auth-события, но не сохраняют их контакты. Сохранённые задания и письма не теряются.
3. Запустить новый Auth API с обязательным `tz` и API контактов, затем Auth relay и
   процессы Notifications. Старые незавершённые OAuth-входы начать заново: состояние
   авторизации теперь должно содержать пояс для создания пользователя.
4. Запустить `notifications-maintenance --once` либо дождаться его первого прохода.
   Он завершит service/bulk без корректного `tz`, даже если срок отправки в будущем.
   Активный lease и действующее владение worker не прерываются; повторный проход подберёт
   освободившиеся записи. Повторный запуск безопасен, baseline лайков не изменяется.
5. Проверить регистрацию с `tz`, письмо через Auth HTTP → SMTP, отсутствие нового события
   при изменении только email и событие при изменении `tz`. Исторический `NULL` не заменять
   на UTC; новые регистрации без пояса должны возвращать 422.

Это порядок остановки и согласованного обновления, а не утверждение о возможности
rolling deployment одного финального образа через несовместимые состояния схемы.
События старого outbox не переписываются под прежним `event_id`; relay доставляет их
как сохранено. Повторная загрузка пользователей не увеличивает версии и не создаёт welcome.

### Где остаются исторические персональные данные

| Место | Срок и способ обработки |
| --- | --- |
| `profiles` Notifications | Контактные колонки удаляет миграция 007 |
| `deliveries.payload` | Maintenance очищает только завершённые письма после заданного срока; активные повторы сохраняют снимок |
| Опубликованный Auth outbox | Автоматического удаления в Auth пока нет. После проверки доставки опубликованные строки удаляются отдельной эксплуатационной операцией; не удалять pending и постоянные `notification_load_markers` |
| Kafka `auth.users.v1` | В локальном review-стенде проверено: broker `log.retention.hours=168`, проверка очистки раз в 300000 ms, topic overrides отсутствуют. Очистка идёт сегментами и не гарантирует удаления ровно через 7 суток; в другой установке проверить её настройки |
| Mailpit | В Compose ограничение `MP_MAX_MESSAGES=1000`, постоянный volume; срока по времени нет. Исторические письма удаляются отдельно через UI Mailpit, очистка Notifications их не затрагивает |
| Внешний SMTP и резервные копии | Собственные правила хранения; этот проект ими не управляет |
| Текст заявки кампании | `subject`/`text` могут содержать произвольные персональные данные; они сохраняются в исходной заявке и не исчезают при очистке `deliveries.payload` |

Контакты, тела запросов/ответов Auth и письма не выводятся в логи нового пути подготовки.
Изменение формата новых событий сокращает дальнейшее копирование, но не является
мгновенной очисткой исторических журналов, почтовых ящиков и резервных копий.

### Проверки до внедрения

- Auth: полный набор с настоящими PostgreSQL/Redis/Kafka — **401 passed**; дополнительная
  проверка физического удаления и исключения чужих контактов — **9 passed** в наборе API контактов.
- Notifications: полный набор — **737 passed**. Затем добавлена проверка URL контактов
  при запуске: **120 passed** в затронутых тестах конфигурации; полный набор после этой
  узкой проверки не повторялся. Ruff/format, mypy и Pyright обоих сервисов проходят.
- Кампании: **1 passed** через реальные процессы Auth HTTP, Notifications, PostgreSQL,
  RabbitMQ, Redis и Mailpit. Проверены поздний `no_email`, окончательный `invalid_timezone`,
  повтор сохранённого письма и восстановление подготовки после недоступности Auth.
- Источники/доставка/срок срочного письма: **4 passed** в `test_end_to_end.py`,
  `test_delivery_classes.py`, `test_transactional_latency.py`. Проверены отсутствие welcome
  при bootstrap, изменение контактов без события, актуальный адрес из Auth, очереди классов,
  повторы и существующий профиль проверки задержки на фоне 300 bulk-писем.
- Обслуживание и откат: **15 passed** с PostgreSQL; откат несовместимых заполненных данных
  отклоняется до DDL с понятным требованием восстановления из резервной копии.
- Дополнительный функциональный стенд Auth: **4 passed**, включая bootstrap и регистрацию
  с обязательным `tz`. Старые integration/load-клиенты обновлены, шесть форм запросов
  проверены схемой Auth; целиком эти два сценария не запускались. Их прежнее расхождение
  с форматтером сохранено, Ruff для изменённых файлов проходит.
- Образы `auth` и `notifications-api` собраны через Docker; Compose конфигурация валидна.
  Это проверка сборки и изолированных процессов, а не обновление работающего review-стенда.

Независимое итоговое ревью выявило пропущенные старые клиенты регистрации и примеры OAuth.
Исправления прошли отдельное ревью; последняя формулировка о `tz` при OAuth приведена
к согласованному правилу: при повторном входе пояс существующего пользователя сохраняется.
Открытых замечаний этого этапа нет.

Для изолированных локальных окружений process harness принимает `SERVICE_TEST_ENV_AUTH`,
`SERVICE_TEST_ENV_NOTIFICATIONS`, `SERVICE_TEST_ENV_UGC_API`, `SERVICE_TEST_ENV_MOVIES_ETL` —
пути к подготовленным виртуальным окружениям соответствующих сервисов. Без них использует
обычный `uv run --directory services/<service>`. Во время параллельных проверок нельзя
пересоздавать одну `.venv` под разными версиями Python. Никакие общие базы или очереди
проверки не очищают: тестовые ресурсы имеют собственные UUID.
