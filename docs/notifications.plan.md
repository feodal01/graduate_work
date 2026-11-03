# Реализовать нотификации на RabbitMQ с двумя типами воркеров

Это самостоятельный план исполнения для репозитория `notifications_sprint_1`. Разделы `Progress`, `Surprises & Discoveries`, `Decision Log` и `Outcomes & Retrospective` обновляются при выполнении. План описывает будущую реализацию; отмеченные выполненными пункты относятся только к подготовке документов. Подробный дизайн и изображения процессов находятся в [notifications.design.md](notifications.design.md).

## Цель и наблюдаемый результат

После реализации регистрация пользователя создаёт персональное приветственное письмо. Сервисный клиент может запустить одинаковую или персонализированную рассылку по всем пользователям либо указанным UUID. По пятницам система формирует общую подборку десяти фильмов по рейтингу. Пользователь может отключить уведомления или отдельную категорию через API. Остановка процессов не удаляет уже сохранённую работу; после запуска обработка продолжается.

Реализуются только пункты 2 и 3 `task.md` с требованиями `considerations.md`. Админка рассылок, WebSocket, сокращатель ссылок, SMS/push, личный список уведомлений, произвольный HTML, CRUD шаблонов и пользовательские расписания не входят в работу. Генератор одного фиксированного расписания включён для проверки восстановления периодических событий. Лайки, серии и напоминания о закладках не являются выбранными сценариями первой версии.

Для демонстрации используется Mailpit: письмо считается отправленным, когда SMTP принял его и оно появилось в тестовом почтовом ящике. Реальное получение во входящие не гарантируется. Система устраняет повторное создание логических доставок, но SMTP-повтор возможен при падении после принятия письма и до сохранения результата. Приоритет такой неопределённости — повторить отправку, сохранив возможность дубля.

## Progress

- [x] (2026-10-08) Изучены `task.md`, `considerations.md`, интеграционные границы Auth и каталога.
- [x] (2026-10-08) Записаны требования, ограничения и целевой дизайн RabbitMQ с двумя типами воркеров.
- [x] (2026-10-08) Подготовлены исходники и отрисованные диаграммы отдельных процессов.
- [x] (2026-10-09) Добавлена общая схема трёх сценариев; упрощена интеграция Auth; согласованы контракт и локальная схема разовой отсрочки.
- [ ] Проверить инфраструктурные свойства выбранного RabbitMQ и зафиксировать digest.
- [ ] Создать пакет, модели, миграции, контракты и реестр шаблонов.
- [ ] Добавить основной email, источник событий, сервисные методы и relay Auth.
- [ ] Реализовать устойчивый приём, ожидание `send_at`, relay и организующий воркер.
- [ ] Реализовать отправителя, пользовательские настройки и операторский повтор.
- [ ] Реализовать еженедельный генератор и восстановление периодов.
- [ ] Подключить Compose, gateway, CI и эксплуатационные команды.
- [ ] Проверить сквозные сценарии, сбои и задержку под нагрузкой.

## Surprises & Discoveries

У `User` в `services/auth/src/models/entity.py` нет основного email; email внешнего аккаунта хранится отдельно. `services/auth/src/schemas/authentication.py` принимает только login/password/имя/фамилию. Это интеграционная работа, а не готовая зависимость.

`SQLAlchemyUserRepository.create` и `SQLAlchemySocialAccountRepository._try_create` коммитят создание внутри репозитория. Outbox только на стороне нотификаций не закроет потерю между этим коммитом и HTTP-запросом. Исходящее событие должно коммититься внутри Auth вместе с пользователем.

`require_internal_service` в `services/auth/src/dependencies.py` всегда возвращает `movies-api`. Нужны отдельные права и лимит для доступа к контактам, без изменения существующего маршрута Movies API.

В `services/movies-api/src/api/v1/films.py` есть сортировка по `imdb_rating`, но нет контракта новинок за неделю. Выбранная подборка — десять фильмов по рейтингу; она не называется новинками. У UGC нет данных последнего посещения закладок; их сценарий не добавляется скрыто в этот план.

Publisher confirm допускает unroutable сообщение; ordinary DLX не обеспечивает безопасную пересылку при недоступной целевой очереди. Поэтому обязательны обработка `mandatory` returns и отдельная проверка безопасного dead lettering на выбранном образе.

## Decision Log

2026-10-08, автор плана: RabbitMQ используется только для нотификаций; действующая Kafka и её процессы остаются на месте. Это соответствует выбранному пользователем варианту и сохраняет существующую систему.

2026-10-08, автор плана: PostgreSQL является источником состояния, RabbitMQ — транспортом ID заданий. Organiser разворачивает аудиторию; sender проверяет контакт/настройки, формирует и отправляет. Relay и scheduler — отдельные команды того же пакета. Такое разделение позволяет независимо ограничить массовую работу и доставку.

2026-10-08, автор плана: выбраны welcome, сервисная кампания и еженедельная общая подборка. Email необязателен для совместимости Auth; отсутствие адреса даёт видимую блокировку доставки. OAuth создаёт welcome только при первом создании пользователя.

2026-10-08, автор плана: аудитория `all` — снимок ID в Auth при первой успешной операции создания снимка. Один SQL-снимок ограничен 10 000 пользователями; превышение блокирует кампанию без частичной рассылки. Это устраняет нестабильный обход изменяемого списка.

2026-10-08, автор плана: пять автоматических повторов с задержкой, затем устойчивое состояние `blocked` и операторский повтор. Перезапуск не объявляет такую доставку успешной. Общий выключатель применяется ко всем категориям перед SMTP.

2026-10-08, автор плана: модель отказов — перезапуск процессов и временная недоступность при сохранных томах. Потеря диска/узла и непрерывная доступность не входят в учебный Compose. Максимум 180 с проверяется при штатных зависимостях и оговорённой нагрузке. Эти пределы должны присутствовать в приёмке наставником.

2026-10-09, автор плана, по уточнению пользователя: общая sequence-диаграмма показывает мгновенный вход, отложенный вход и персонализированную рассылку с единым путём через RabbitMQ и два типа воркеров. Существующая логика регистрации/OAuth не повторяется в новой схеме Auth; показано только сохранение и HTTP-передача события. API/БД/relay в обзоре сгруппированы, локальные схемы разделяют их.

2026-10-09, автор плана: отложенный сценарий включает уже выбранный регулярный выпуск и разовую кампанию с необязательным `send_at`. Время ожидания хранится в PostgreSQL, публикацию выполняет существующий relay; это не третий тип бизнес-воркера. Персонализация совместима с обоими режимами времени. Для отсрочки срок ограничен 72 часами после `send_at`, аудитория all фиксируется после наступления времени; точная секунда SMTP не обещается.

## Outcomes & Retrospective

На этапе подготовки созданы дизайн, план и двенадцать целевых sequence-диаграмм с исходниками: общий механизм и одиннадцать локальных процессов. Прикладной код, миграции и новые контейнеры ещё не реализованы. Проверка документации и рендеринга не является доказательством надёжности будущего сервиса. Этот раздел должен получить реальные результаты каждого этапа, а в конце — ссылки на тесты, измерения и оставшиеся ограничения.

2026-10-09: PlantUML syntax check и PNG render завершились с кодом 0. Проверены 12 пар исходник/PNG, локальные ссылки обоих документов и покрытие 14 проектируемых HTTP-контрактов. Изменённые изображения просмотрены; непонятная подпись «захватить исходящую запись» заменена конкретными действиями. Проверки исполнения сервиса на этом этапе не проводились.

## Контекст и устройство

Рабочий каталог — `/Users/user/Documents/repos/notifications_sprint_1`. Корневой `compose.yaml` запускает существующие сервисы. Зависимости каждого Python-сервиса находятся в своём `pyproject.toml` и `uv.lock`; проверки — в `.github/workflows/ci.yml` и `python-service.yml`. Новое приложение размещается в `services/notifications`; код — `src/notifications`, тесты — `tests`, SQL-миграции — `src/notifications/db/migrations`.

Событие — один фиксированный факт с UUID. Кампания — общие содержимое, шаблон и аудитория. Доставка — один пользователь и один канал кампании. Попытка — один цикл обработки доставки, который может завершиться до SMTP. Outbox — запись о передаче работы, которая коммитится вместе с породившим её изменением. Relay повторяет передачу; ack подтверждает получение результата Rabbit-потребителем; confirm подтверждает приём публикации брокером. Эти подтверждения имеют разные обязанности.

Lane — класс очереди instant (мгновенный) или bulk (массовый). Lease — временный захват, token — идентификатор его владельца, generation — поколение задания. SMTP — протокол отправки почтовому серверу. Задания захватываются на 60 с с продлением каждые 15 с. Таймаут обычного HTTP — 5 с, снимка Auth — 10 с, SMTP — 15 с, confirm — 5 с, health — 2 с. Ожидание квоты продлевает lease и не удерживает SQL-транзакцию.

Создать один образ нотификаций с командами `api`, `organiser`, `sender`, `relay`, `scheduler`, `inspect`, `retry-campaign` и `process-health` через `python -m notifications`. Organiser и sender принимают `--lane instant|bulk`. Relay также принимает lane. Inspect принимает необязательный `--campaign-id`. Retry-campaign принимает campaign-id и operation-id, проверяет срок/blocked, идемпотентно создаёт продолжение в транзакции. База хранит состояние и повторные попытки; брокер содержит только `{schema_version, job_id, job_type, lane, generation}`. Схема версии 1 отклоняет неизвестные поля и неизвестный канал. Email/ФИО получают только через Auth; их нет в кампании, событии, брокере и логах.

Источник welcome — транзакция Auth, а не браузер. Схема потока: `User + source outbox → HTTP API → event + outbox → RabbitMQ → organiser → delivery + outbox → RabbitMQ → sender → SMTP → result → ack`. Массовая кампания проходит тот же путь от API. Будущий `send_at` удерживает её в БД до назначенного времени; затем начинается обычная обработка bulk. Мгновенные события идут по instant, сервисные кампании и периодические выпуски — по bulk независимо от наличия персонализации. Scheduler сохраняет выпуск и HTTP-outbox в БД нотификаций, а затем relay вызывает собственный API с правами scheduler. Благодаря этому общий приём и дедупликация используются всеми источниками.

## Контракты, которые реализуются

API нотификаций создаёт `POST /internal/v1/events`, `POST /internal/v1/campaigns`, `GET /internal/v1/deliveries/{delivery_id}`, `POST /internal/v1/deliveries/{delivery_id}/retry`, `GET/PATCH /api/v1/notifications/preferences`, `GET /health/live` и `GET /health/ready`. Все внутренние пути закрыты от gateway. Swagger и OpenAPI в первой версии отключены; контракт проверяется тестами и описывается README. Пользовательские пути используют local access JWT: HS256, подпись, обязательные `sub/jti/type/iat/exp`, тип access и срок; отзыв токена до истечения, как у существующего UGC, не проверяется.

Сервисные ключи имеют независимые роли `auth`, `scheduler`, `campaign-client`, `operator`. По ключу API определяет источник. Auth может передать только `user.registered`, scheduler — только `digest.weekly`. Campaign-client создаёт кампании, operator читает доставку и повторяет её. Неавторизованный запрос даёт 401, недостаточные права — 403, ошибка схемы — 422, недоступная БД — 503. Размер тела до 64 КиБ, явная аудитория до 100 UUID. Admission limit: 50 000 незавершённых bulk-доставок, 500 активных bulk-кампаний, отдельно 5 000 активных instant-событий. Счётчики в `admission_budgets` изменяются атомарно; повтор принятого ключа не занимает новое место; завершение/парковка освобождает его. Fan-out ждёт освобождения бюджета, а не создаёт бесконечный backlog. Ответ 429 включает `Retry-After`, исходящий источник сохраняет обязанность повторить.

Событие содержит UUID `event_id`, `schema_version=1`, `event_type`, UTC `occurred_at`, `expires_at`, `user_id` для welcome и типизированный payload. Welcome определяется сервером как `welcome/v1`, `welcome`, `email`, `personalised`; срок семь суток. Digest содержит стабильный `schedule_id`, границы периода и до десяти `{film_id,title}`; срок 72 часа. Кампания содержит `campaign_id`, версию схемы, `audience=all|user_ids`, явные ID при необходимости, `template_id`, `template_version`, `render_mode=generic|personalised`, `payload`, необязательный `send_at`, `expires_at`; категория newsletter, канал email. `announcement/v1` принимает subject/text и необязательный UUID фильма; `weekly_digest/v1` — подборку. Пользовательский HTML и произвольные URL не принимаются.

Контракт разовой отсрочки: отсутствие `send_at` и `null` означают немедленную обработку. Ненулевой timestamp обязан иметь часовой пояс, нормализуется в UTC и при первом принятии должен быть в будущем. Требуется `send_at < expires_at <= send_at + 72 часа`; без отсрочки — `accepted_at < expires_at <= accepted_at + 72 часа`. Нарушение даёт 422. Сначала проверяется уже принятый ключ и хеш: повтор возвращает прежний 202, даже если время уже наступило; изменение `send_at` с тем же ключом — 409. Хеш включает нормализованный `send_at` (отсутствие равнозначно null), но не вычисленный accepted_at. Перенос и отмена кампании отдельным API не реализуются.

Кампания с будущим временем создаётся в `scheduled`; её outbox имеет `available_at=send_at`. Она занимает одно из 500 мест активных bulk-кампаний. Relay по времени PostgreSQL UTC каждые 5 с выбирает готовые записи, переводит scheduled в pending и выполняет обычную подтверждаемую публикацию. После простоя обрабатывает наступившую актуальную кампанию; просроченную переводит в expired без публикации. Reconciliation, organiser и ручной повтор соблюдают send_at и expires_at. Преждевременный конверт восстанавливает ожидание до send_at, не создаёт доставок и подтверждается после COMMIT. Снимок all фиксируется при первой успешной операции Auth после наступления времени, явные ID — при приёме. Контакты и настройки актуальны на момент отправки. Точность отправки в заданную секунду не гарантируется; предел 180 с относится к instant. Регулярный выпуск по-прежнему создаёт scheduler при наступлении периода, а персонализация по имени работает в любом временном режиме.

Успешный приём возвращает 202 с исходным ключом и внутренним job_id. Уникальность — `(source,event_id)` и `(source,campaign_id)`, сравнение нормализованного хеша. Внутренний ID рассчитывается UUIDv5 из фиксированного namespace, источника, вида запроса и входного ID; ключ клиента хранится отдельно. Одинаковый UUID разных источников не сталкивается. Внутренний campaign ID используется для Auth снимка и доставок. Другой payload с тем же ключом даёт 409. Нотификации атомарно сохраняют входную запись и outbox. API не вызывает Auth, Movies API или SMTP при приёме. Rabbit outage не мешает HTTP 202, если работает БД и не исчерпан admission limit.

Namespace UUIDv5 — `76b3f290-574a-4f8a-9b0e-1cd103d2e6ad`, строка — `source:kind:external_uuid`. Kind: event, event_campaign, direct_campaign. Event ingress атомарно создаёт событие, связанную кампанию и organise outbox; у welcome один получатель, у digest — all. Direct campaign ingress создаёт только кампанию и organise outbox. Активная версия шаблона фиксируется при первом принятии и не меняется при повторе.

GET preferences возвращает `enabled`, `welcome`, `newsletter`; по умолчанию все true. PATCH изменяет только явно переданные булевы поля для UUID из JWT. Read-only ID из тела игнорировать нельзя: схема его запрещает. Настройки читаются перед каждой реальной попыткой SMTP. Отсутствие ФИО даёт нейтральное обращение; общий выключатель подавляет все категории. Generic использует одинаковое содержимое, персональные контакты остаются в отдельных SMTP-конвертах.

GET delivery возвращает ID, статус, lane, попытки, код причины, время следующей попытки и срок, без контакта/текста. Retry принимает `retry_request_id`: одна транзакция сохраняет request ID, новую generation и outbox. Повтор request ID возвращает прежний ответ. Retry запрещён для принятой SMTP, suppressed, expired или активной sending доставки; разрешён для исправленной blocked до срока. Автоматическое оживление suppressed не допускается.

Auth добавляет основной nullable email к User, регистрации, профилю и ответам. Email не используется для входа/связывания и не обязательно уникален. При профильном PATCH отсутствие email не меняет адрес, явный null удаляет. OAuth первый раз копирует email провайдера, если он есть, и создаёт source outbox; повторный вход не перезаписывает основной адрес и не создаёт welcome. Старые аккаунты остаются без адреса. Подтверждение владения адресом в эту версию не входит.

Новые Auth пути: `POST /internal/v1/recipients/lookup` с максимум 100 UUID и результатом для каждого `{user_id,found,email,first_name,last_name}`; `POST /internal/v1/audiences` с `audience_id=campaign_id`, возвращающий постоянный снимок; `GET /internal/v1/audiences/{audience_id}/members?after=&limit=100`, возвращающий ID, курсор и complete. Курсор — Base64URL последнего UUID без padding, отсутствие after начинает обход; неверный курсор даёт 422. Снимок создаётся одной транзакцией `INSERT SELECT` всех закоммиченных пользователей; проверка предела 10 000 выполняется в той же согласованной транзакции, при превышении всё откатывается. ID в снимке сортируются стабильно, следующая страница выбирает строго большие UUID. Повтор создания возвращает уже сохранённый снимок, даже если появились новые пользователи. Контакт читается отдельно при отправке. У новых путей отдельный ключ/identity `notifications` и лимит; действующий Movies API guard сохраняется.

База нотификаций содержит `notification_events`, `campaigns`, `deliveries`, `delivery_attempts`, `notification_preferences`, `schedule_runs`, `outbox`, `operation_requests`, `smtp_rate_windows`, `admission_budgets`. Campaign хранит неизменяемые общие данные и версию шаблона, аудиторию, курсор, lane, accepted_at, неизменяемый send_at, срок и lease. Delivery уникальна по `(campaign_id,user_id,channel)`, содержит статус, generation, `next_attempt_at`, lease token и счётчик. Attempt не содержит контакта или текста. Schedule уникален по `(schedule_id,period_start)`. Outbox уникален по `(destination,job_id,generation)` и хранит indexed available_at для следующей допустимой публикации. SMTP windows атомарно разрешают до 50 отправок/UTC-секунду, bulk не больше 40; настройки проверяются после ожидания бюджета. Бюджет используется всеми экземплярами, без дополнительного Redis. Все время UTC, операции пагинации индексированы. Автоматическое удаление записей дедупликации и снимков не добавляется; их рост — известное эксплуатационное ограничение.

Состояния доставки: pending, sending, retry_wait, uncertain, accepted_by_smtp, suppressed, expired, blocked. Attempt хранит фазы preparing, smtp_started, finished; smtp_started коммитится до вызова SMTP. Падение в preparing даёт безопасный повтор; smtp_started без результата — uncertain, даже если вызов ещё не успел начаться. Campaign: scheduled, pending, expanding, expanded, blocked, expired; expanded — все доставки созданы, а не все отправлены. HTTP/SQL-операции захвата используют 60-секундный lease и token; результат сохраняет только актуальный владелец. Внешние HTTP/SMTP выполняются без открытой SQL-транзакции. Watchdog повторяет работу с истёкшим lease и создаёт новую generation; подтверждённая публикация не означает окончание бизнес-задачи.

## План работ

### Этап 1. Проверить брокер и создать каркас

Создать `services/notifications/pyproject.toml`, `.python-version` (3.12), `.env.example`, `Dockerfile`, `.dockerignore`, `README.md`, `alembic.ini`, `src/notifications/__init__.py`, `__main__.py`, `cli.py`, `core/config.py`, `core/logger.py`, `main.py`. Установить FastAPI, Pydantic/settings, SQLAlchemy 2, asyncpg, Alembic, aio-pika, httpx, Jinja2, aiosmtplib; lint/test groups — Ruff, mypy, pytest, pytest-asyncio. Проверить текущие документы библиотек через ctx7 перед использованием конкретного API. Создать `uv.lock`; не копировать lock другого сервиса. CLI исполняет только объявленные команды, опечатка завершает процесс с ошибкой.

Создать минимальный `services/notifications/tests/integration/compose.yaml` с PostgreSQL 16, `rabbitmq:4.2-management` и Mailpit; тома и имена изолированы от корневого проекта. Проверить доступность образа линии 4.2, зафиксировать digest. Если образ не поддерживает оговорённую policy, работа останавливается на этом этапе: исправить выбор версии в дизайне и плане, а не ослаблять гарантии молча. Добавить `tests/integration/test_broker_contract.py`: publish/confirm/mandatory return, manual ack/redelivery, safe DLX при недоступной целевой очереди. Для проверки DLX использовать воспроизводимый разрыв/overflow целевой очереди без удаления исходных данных.

Создать `infra/rabbitmq/bootstrap.py` и файл политик `infra/rabbitmq/policies.json`. Инициализатор повторяемо создаёт durable direct exchanges `notifications.organise`, `notifications.deliver`, `notifications.dead`, рабочие quorum queues `organise.instant`, `organise.bulk`, `deliver.instant`, `deliver.bulk` с префиксом `notifications`, и четыре dead queues. Policy задаёт at-least-once dead lettering, reject-publish overflow, отдельный dead routing key и delivery-limit 20. Никаких transient очередей, циклических dead bindings и drop-head. AMQP publications persistent, mandatory, с обработкой returns и confirms. Повторы с задержкой в базе; TTL/delayed-retry плагины не используются.

Результат этапа — запускаемый пакет и тест, доказывающий свойства выбранного брокера; прикладных писем пока нет. Нельзя отмечать этап готовым по одной успешной публикации без проверки unroutable и недоступной DLQ.

### Этап 2. Создать модели, миграции и шаблоны

Создать `db/models.py`, `db/session.py`, `db/migrations/env.py`, `db/migrations/versions/20261008_0001_notifications.py`, `schemas/events.py`, `schemas/campaigns.py`, `schemas/preferences.py`, `schemas/jobs.py`, `schemas/operations.py`, `repositories/events.py`, `campaigns.py`, `deliveries.py`, `outbox.py`, `preferences.py`, `schedules.py` внутри `src/notifications`. Уникальность и обязательные поля реализовать SQL-ограничениями, а не только проверкой Python. Для generation, lease и курсора предусмотреть условные обновления; все commit boundaries соответствуют контрактам выше. Добавить nullable send_at, accepted_at, scheduled и индекс outbox.available_at; данные кампании и отложенный outbox создаются вместе.

Создать `templates/welcome/v1/{subject.txt,body.txt,body.html}`, аналогичные файлы announcement и weekly_digest, `templates/registry.py`, `services/rendering.py`. Реестр разрешает только описанные шаблоны/версии и поля. HTML экранируется, отсутствующая обязательная переменная даёт ошибку, generic не использует имя. Фильм получает ссылку из доверенного `PUBLIC_FILM_BASE_URL` и UUID. Готовый текст не сохранять в БД.

Добавить `tests/unit/test_schemas.py`, `test_templates.py`, `tests/integration/test_atomicity.py`, `test_idempotency.py`. Проверить повтор одинакового ключа и конфликт данных, откат кампании вместе с outbox, уникальность доставки, generation, нейтральное обращение, экранирование и неизменяемость версии. Для send_at проверить отсутствие/null, нормализацию часового пояса, прошлое время, границы expires_at, совпадающий повтор после назначенного времени и конфликт при изменении send_at. Результат — согласованный контракт, который сохраняется и проверяется до подключения сетевых процессов.

### Этап 3. Дополнить Auth

В `services/auth/src/models/entity.py` добавить email и модели source outbox/снимков. Создать миграцию `services/auth/src/db/migrations/versions/20261008_0005_notifications.py` после существующего head; предварительно проверить фактический revision ID `alembic heads`, так как исходная история могла измениться. Колонка nullable, без переноса login в email и без смены существующих PK/ролей. Обновить схемы, протокол `UserRepository`, `AuthenticationService.signup`, `SQLAlchemyUserRepository.create/update`, профильный validator и маршруты передачи email.

В транзакции `UserRepository.create` записывать source outbox с UUID, типом user.registered и user_id до commit. В `social_accounts.py` сделать то же в `_try_create`; конфликт вставки откатывает и пользователя, и событие. Существующий OAuth пользователь событие не создаёт, основной email не переписывается при повторном login. Добавить `repositories/notification_outbox.py`, `repositories/notification_audiences.py`, `schemas/notification_recipients.py`, `api/v1/notification_recipients.py`, `services/notification_audiences.py`, `notification_relay.py` и команду `python -m services.notification_relay` из `services/auth/src`.

Добавить отдельную зависимость service guard для нового ключа и отдельную политику recipients; существующий `require_internal_service` Movies API не менять без необходимости. Подключить новые `/internal/v1/...` маршруты в `services/auth/src/main.py`. Relay вызывает API нотификаций с source key, сохраняет исходящий ID, повторяет HTTP 429/503/timeout; ответ 202 завершает передачу. 409 при ином payload и 422 дают видимую блокировку исходящего события. Redis/Auth outage не превращается в потерю регистрации.

Роль notifications использует две сервисные идентичности/ключа: notifications-instant (600 запросов/мин, burst 20) и notifications-bulk (1 200/мин, burst 50). Новые лимиты не общие с Movies API и не общие между классами. Снимки разрешены только bulk. В bulk sender lookup объединяет до десяти ID с ожиданием пакета до 100 мс, instant отправляет lookup без ожидания. Настройки и SMTP при этом остаются индивидуальными. Использовать `AUTH_NOTIFICATIONS_SERVICE_KEY_INSTANT/BULK` в соответствующих процессах и сохранить отдельный source key Auth для передачи событий в нотификации.

Добавить Auth тесты `test_notifications_email.py`, `test_notifications_source_outbox.py`, `test_notifications_audiences.py`, `test_notifications_service_identity.py` в существующую структуру tests. Проверить неизменность login/password и социальных связей, регистрацию без email, email-only PATCH/null, rollback signup, две конкурентные OAuth регистрации, повтор снимка после добавления пользователя и пагинацию неизменяемого снимка. Результат этапа — реальный источник и реальные методы получения контактов, без необходимости запускать весь кинотеатр.

### Этап 4. Реализовать приём, relay и organiser

Создать `api/events.py`, `campaigns.py`, `dependencies.py`, `security/jwt.py`, `security/service_keys.py`, `services/acceptance.py`, `services/organisation.py`, `services/reconciliation.py`, `clients/auth.py`, `brokers/rabbitmq.py`, `workers/relay.py`, `workers/organiser.py`. API выполняет только проверку/SQL, возвращает 202 после commit. Relay маршрутизирует instant/bulk и не отмечает передачу при return/nack/timeout. В одном AMQP соединении отслеживается соответствие confirm конкретной публикации. Relay ждёт send_at через outbox.available_at; не держит HTTP/SQL-транзакцию или сообщение RabbitMQ во время отсрочки. Актуальную наступившую кампанию публикует с прежним ID, просроченную завершает без отправки.

Organiser захватывает кампанию, получает/переиспользует Auth снимок либо сохранённые user_ids и создаёт до 100 доставок за шаг. Курсор, deliveries, их outbox и continuation коммитятся вместе. Ack текущего конверта происходит только после этого. Два воркера не двигают одну кампанию одновременно; устаревший token не записывает прогресс. HTTP timeout повторяет тот же audience_id, не создаёт другую аудиторию. Snapshot overflow сохраняет blocked без отправки части аудитории.

Reconciliation каждые 30 с сканирует готовую незахваченную или потерявшую lease работу. Подтверждённую, но не захваченную за 60 с передачу повторяет с той же generation через прежний outbox: ожидание очереди не должно инвалидировать конверт. Новая generation возникает при продолжении порции, повторной попытке или восстановлении истёкшего lease. Копия с активным владельцем подтверждается без нового эффекта, рабочий lease не сбрасывается. Relay опрашивает каждые 5 с. Reconciliation и organiser проверяют send_at; ранний конверт восстанавливает ожидание и подтверждается после COMMIT без развёртывания аудитории. Нет открытой SQL-транзакции во время HTTP или AMQP ожидания. Добавить `tests/integration/test_ingress.py`, `test_relay.py`, `test_fanout.py`, `test_reconciliation.py` с двумя потребителями, остановкой между страницами, повторными конвертами и потерей confirm. Добавить `test_delayed_campaigns.py`: до send_at нет публикации, после времени кампания возобновляется с тем же ID; проверить перезапуск до/после времени, истечение срока при простое, ранний конверт, отсутствие ранней публикации при reconciliation, учёт scheduled в бюджете и снимок аудитории после send_at. Результат — из одной кампании создаётся ровно по одной логической доставке на UUID снимка, даже после перезапуска.

### Этап 5. Реализовать sender, настройки и операторские операции

Создать `workers/sender.py`, `services/delivery.py`, `services/retry.py`, `services/send_budget.py`, `clients/smtp.py`, `api/preferences.py`, `api/operations.py`, `api/health.py`. Sender использует ограниченные таймауты Auth/SMTP, prefetch не выше размера параллельной обработки, lease с token и контроль generation. Последовательность: срок → контакт Auth → получить общий SMTP-бюджет → настройки непосредственно перед SMTP → рендеринг → SMTP → устойчивый результат → ack. Транзакция получения квоты завершена до сетевого вызова; недостаток квоты ждёт следующего окна, не увеличивая число SMTP-ошибок. При недоступной БД ack запрещён; процесс перестаёт брать новую работу.

SMTP4xx/сетевой сбой/Auth429/503 дают retry_wait; задержки 10 с, 60 с, 5 мин, 30 мин, 2 ч с jitter и нижней границей Retry-After. После пяти повторов blocked и dead outbox. SMTP5xx, отсутствующий email, неверный шаблон сразу blocked. Неизвестный/повреждённый транспортный конверт идёт через safe DLX. Ожидаемая бизнес-ошибка паркуется через DB+dead outbox, затем ack. Timeout после передачи SMTP может быть uncertain; истёкшая sending тоже становится uncertain и повторяется, не объявляется успехом.

Preferences хранит только собственный user_id токена и bool поля. Отсутствующая запись возвращает все true. Retry endpoint выполняет проверку статуса/срока, idempotency retry_request_id и generation в одной транзакции. Inspect выдаёт агрегаты pending/retry/blocked/uncertain и возраст instant, без персональных данных. Liveness 200 без зависимости, readiness требует БД и шаблоны, не SMTP/RabbitMQ; процессные проверки relay/worker требуют БД и брокер.

Добавить `tests/unit/test_retry_policy.py`, `test_render_modes.py`, `tests/integration/test_sender.py`, `test_preferences.py`, `test_operator_retry.py`, `test_dead_letter.py`. Результат — видимые письма в Mailpit, разные статусы пропуска/ошибки, воспроизводимое поведение неопределённой отправки, возможность исправить контакт и повторить blocked до срока. Успешную доставку повторять нельзя.

### Этап 6. Реализовать генератор

Создать `workers/scheduler.py`, `clients/movies.py`, `services/scheduling.py`. Расписание `weekly_catalog`: пятница 12:00 Europe/Moscow, вычисление границ сохраняется в UTC. Цикл 60 с выбирает последний наступивший период. В БД уникальная запись schedule_id+period_start, постоянный event_id и lease подготовки. Два генератора не создают два выпуска. При простое создаётся только последний выпуск, если он ещё актуален (72 ч); пропущенные старые периоды фиксируются skipped_stale. Незавершённый актуальный run возобновляется.

Генератор вызывает существующий Movies API `GET /api/v1/films?sort=-imdb_rating&page_number=1&page_size=10`. Содержимое одного выпуска фиксируется один раз. Пустой список — skipped_empty; недоступность — ожидание и повтор до срока. Подборка и HTTP-outbox digest.weekly коммитятся вместе; relay вызывает API с тем же ID, потерянный ответ не дублирует кампанию. Пересчёт содержимого после принятого выпуска запрещён. При первом включении старт — последний наступивший период; история до внедрения не создаётся. Последующий catch-up использует сохранённую историю, фиксированные расписание/часовой пояс и срок 72 часа от времени выпуска.

Добавить `tests/unit/test_schedule_periods.py`, `tests/integration/test_scheduler.py`, `test_scheduler_recovery.py`: пятница/границы UTC, простой в одну и несколько недель, падение до и после content commit, два генератора, пустой/недоступный каталог. Результат — один общий неизменяемый выпуск, максимум одна кампания периода и отсутствие серии устаревших писем после запуска.

### Этап 7. Подключить запуск, наблюдение и CI

Дополнить корневые `compose.yaml` и `.env.example` новыми сервисами: notifications-db, rabbitmq, notifications-migrate, notifications-api, organiser/sender/relay с instant и bulk, notifications-scheduler, auth-notifications-relay. Один образ нотификаций, отдельные команды. Mailpit — профиль notifications-test. Миграции выполняются отдельными one-shot контейнерами до API/воркеров. Существующие сервисы, volumes и Kafka не переименовываются.

Переменные: `NOTIFICATIONS_POSTGRES_*`, `NOTIFICATIONS_AMQP_URL`, отдельные `NOTIFICATIONS_SERVICE_KEYS_*`, `AUTH_NOTIFICATIONS_SERVICE_KEY_INSTANT/BULK`, `AUTH_NOTIFICATIONS_BASE_URL`, `AUTH_JWT_SECRET` и `AUTH_JWT_ALGORITHM=HS256` из общей конфигурации Auth, `MOVIES_API_BASE_URL`, `SMTP_HOST/PORT/USERNAME/PASSWORD/STARTTLS`, `SMTP_FROM`, `PUBLIC_FILM_BASE_URL`, lane/lease/batch/retry настройки. Передавать ключи соответствующим процессам; не выдавать campaign-client/operator ключ Auth. В Compose указать отдельный notifications volume и постоянный rabbitmq volume. Management UI/SMTP UI только localhost. В integration Compose фиксировать Mailpit digest после проверки образа.

В `infra/nginx/conf.d/gateway.conf` добавить upstream notifications, только public preferences proxy и явный запрет новых internal путей. Для Auth новые `/internal/` пути также не проксировать. Разделить rate limits новых пользовательских и внутренних операций. Структурированные логи используют Logstash/Sentry по действующим шаблонам, без контактов/текста/секретов. Не добавлять отдельный Grafana/Prometheus стек.

Создать `services/notifications/tests/integration/run.sh`, `tests/functional/run.sh`, `tests/functional/compose.yaml`, `tests/functional/fixtures.py`. Скрипты поднимают изолированные Auth/PostgreSQL/Redis/RabbitMQ/Mailpit и нотификации, строят образы, ждут health, выполняют проверки и удаляют только собственные test volumes после результата. Fixtures создаёт UUID/тестовые email в зоне example.test и пишет временные идентификаторы в файл внутри test volume; ни один секрет не печатает. Тестовые ключи не используются в корневом .env.

В `.github/workflows/ci.yml` добавить unit/lint задачу notifications с Python 3.12/3.13 и отдельный notifications integration job, который запускает изолированный Compose; не расширять reusable workflow БД нового сервиса без необходимости. Обновить перечень зависимостей notify, чтобы новый job влиял на статус существующего уведомления CI. Корневые README и docs/project-structure.md описывают целевой запуск только после реальной реализации. Результат — единая точка запуска и проверяемая изоляция тестовых данных.

### Этап 8. Доказать сквозное поведение и устойчивость

Создать `tests/functional/test_welcome.py`, `test_campaigns.py`, `test_delayed_campaigns.py`, `test_schedule.py`, `test_fault_recovery.py`, `test_security.py`, `tests/load/test_instant_latency.py`. Добавить управляемые точки остановки только в тестовый harness: после source commit, до/после HTTP response, после confirm, до commit страницы, после SMTP acceptance, до ack. Production код не должен позволять включить остановку HTTP-заголовком извне.

Проверить, что отсутствует потерянная учтённая работа, количество логических deliveries совпадает с аудиторией, каждая имеет объяснимое состояние. Отдельный тест неопределённого SMTP допускает повторное письмо и доказывает соответствующий статус/попытку; его нельзя подменять обещанием exactly-once. Недоступность DLQ не должна стирать исходное задание. Load harness фиксирует 5 000 пользователей, 100 instant событий за 60 с и общий лимит SMTP 50/с с резервом 10/с; все instant acceptance укладываются в 180 с без искусственных outage. Проверять также blocked admission, общий Auth лимит и backlog bulk.

Результаты записать в Outcomes & Retrospective: конкретные команды, exit codes, измеренный максимум задержки и границы теста. Только после этого утверждать работоспособность сервиса. Актуализировать диаграммы в соответствии с фактическими контрактами; реализованный маршрут меняет lifecycle view с TO-BE на AS-IS при следующем обновлении документации.

## Конкретные команды

Команды ниже — контракт будущей реализации: папки/скрипты из плана нужно создать на соответствующем этапе. Сейчас они не существуют. Все скрипты обязаны завершаться ненулевым кодом при провале или недоступных обязательных зависимостях; нельзя выдавать skipped integration tests за успех.

Из корня репозитория проверить исходное состояние и сохранить пользовательские изменения:

    git branch --show-current
    git status --short

Работа в main уже разрешена в этой задаче. Ничего не откатывать из перенесённого проекта. Новые правки ограничить указанными путями. Commit/push требуют отдельного явного разрешения пользователя; сам план не является таким разрешением.

В `services/notifications` установить и проверить новый пакет:

    uv sync --all-groups
    uv lock --check
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy
    uv run pytest tests/unit -q
    ./tests/integration/run.sh
    ./tests/functional/run.sh

Ожидание: проверки проходят, integration/functional не пропускаются из-за недоступной БД/брокера, Mailpit содержит предусмотренные письма. Число тестов не фиксируется заранее; итоговое фактическое число записывается в результат этапа.

В `services/auth` проверить миграцию и регрессии:

    uv sync --locked --all-groups
    uv run alembic heads
    uv run alembic upgrade head --sql
    uv run ruff check src tests
    uv run mypy
    TRACING_ENABLED=false uv run pytest tests -q
    ./functional_tests/run.sh

Auth pytest требует настоящих PostgreSQL/Redis по сервисной конфигурации; при отсутствии выполнить проверки в действующем изолированном functional Compose, не направлять fixtures в пользовательские данные. Существующие login, profile password-change, refresh, OAuth и Movies API permission tests должны сохранить поведение.

Из корня после создания Compose-конфигурации:

    docker compose config --quiet
    docker compose --profile notifications-test up -d --build notifications-api notifications-organiser-instant notifications-organiser-bulk notifications-sender-instant notifications-sender-bulk notifications-relay-instant notifications-relay-bulk notifications-scheduler auth-notifications-relay mailpit
    docker compose exec notifications-api python -m notifications inspect
    docker compose exec notifications-sender-instant python -m notifications process-health --role sender --lane instant

SMTP UI учебного профиля — `http://127.0.0.1:8025`. Порты integration harness назначаются отдельно. Сервисная демонстрация выполняется fixtures из test Compose: не вставлять реальные JWT/ключи в текст команд, файлы плана или журналы.

Для сквозной демонстрации из `services/notifications`:

    ./tests/functional/run.sh --scenario welcome
    ./tests/functional/run.sh --scenario generic-campaign
    ./tests/functional/run.sh --scenario personalised-campaign
    ./tests/functional/run.sh --scenario delayed-campaign
    ./tests/functional/run.sh --scenario weekly-digest
    ./tests/functional/run.sh --scenario recovery
    ./tests/functional/run.sh --scenario instant-load

Delayed проверяет generic и personalised: до send_at нет письма и публикации, после наступления времени SMTP принимает письмо; после простоя за expires_at отправки нет. Welcome показывает одно письмо с нейтральным или персональным обращением. Generic показывает одинаковые тему/текст у разных адресатов. Personalised различает имя при общих данных. Weekly показывает один выпуск периода. Recovery сохраняет ID и прогресс. Instant-load печатает максимальную задержку и завершается ошибкой выше 180 с.

Диаграммы перерисовываются из корня локальным PlantUML:

    plantuml --check-syntax docs/diagrams/notifications
    plantuml -tpng docs/diagrams/notifications/*.puml

## Валидация и критерии приёмки

R01–R06 доказываются реальным signup Auth, HTTP-приёмом, снимком аудитории и письмами Mailpit. Входящий конверт не содержит email/ФИО, sender действительно вызывает Auth. Повтор события с тем же payload возвращает прежний ID; другой payload с тем же ID даёт 409. API продолжает принимать в outbox при простое RabbitMQ до своего явного предела.

R07/R12 проверяются реестром разрешённых каналов и одним renderer всех трёх шаблонов. Generic письма имеют одинаковое содержимое. HTML экранирует пользовательский текст. Старые версии шаблона присутствуют, пока существует незавершённая доставка этой версии.

R08/R13 проверяются нагрузочным профилем и параллельными worker instances. Все instant acceptance не позже 180 с; прогресс не откатывается и не пропускает получателей. Outage тесты отдельно не сравниваются со штатной задержкой.

R09/R14 требуют остановок Auth после commit, API после commit до ответа, relay после publish до отметки, organiser до/после страницы, sender до/после SMTP, а также недоступных Auth/БД/broker/DLQ. Последовательная сверка PostgreSQL, очередей и Mailpit показывает устойчивое восстановление. SMTP uncertain — явно принятая возможность повторной отправки, не потеря неучтённой работы.

R10 требует двух генераторов, нескольких пропущенных недель и восстановления незавершённого выпуска. Одна period key создаёт не более одной кампании; старые периоды отмечены skipped_stale. R11 требует отключения до первой попытки и во время retry_wait; чужой JWT не изменяет настройки другого пользователя; blocked без email можно повторить после исправления, suppressed нельзя оживить автоматически.

R15 проверяется реальными PostgreSQL/RabbitMQ/Mailpit: отложенная кампания переживает рестарт, не публикуется и не отправляется раньше send_at; при восстановлении до expires_at проходит обычный pipeline, после expires_at учитывается как expired. Проверить оба render_mode, ранний конверт, два relay и неизменность логических ID. Регулярные выпуски покрывает R10.

Безопасность проверяет ключи разной роли, недоступность internal путей через gateway, неверные/истёкшие JWT, отсутствующий user, лимиты payload/аудитории и отсутствие секретов/контактов в логах. Успешный SMTP не называется доказанной доставкой во входящие.

## Повторяемость, миграции и восстановление

Bootstrap брокера повторяем при неизменяемом типе очереди; смена типа существующей очереди требует отдельной миграции и не выполняется удалением очереди. Миграции Auth/notifications добавляют таблицы и nullable email; старые клиенты сохраняют формат обязательных полей. Перед применением к рабочим данным сделать резервную копию обеих баз и проверить её восстановление на изолированном экземпляре. Контакты вручную не backfill из login.

Порядок rollout: Auth миграция и внутренние методы → инфраструктура/реестр/notifications миграция → API и relay/воркеры → source relay Auth → scheduler. Auth сохраняет source outbox, даже если нотификации ещё недоступны. При rollback сначала остановить scheduler/source relay, затем organiser/sender/relay; сохранённые записи и volumes оставить. Откат application без удаления nullable схемы допустим; destructive downgrade с незавершёнными заданиями запрещён.

Не использовать `docker compose down -v` для корневого проекта. Test runner удаляет только свой namespace после завершения и сохраняет короткий отчёт без секретов при провале. DLQ разбирается через operator API после исправления причины; старое сообщение нельзя публиковать вручную без проверки бизнес-состояния. Дедупликационные ключи и snapshots пока не очищаются автоматически.

Ручные commits на границах этапов выполняются только при явном разрешении пользователя. Обновить Progress/Decision Log/Outcomes до остановки работы; сохранять реальные ограничения и результат проверки независимо от публикации Git.

## Интерфейсы и зависимости

В `services/acceptance.py` нужны операции принятия типизированного события/кампании с результатом ID и признаками принятого повтора; конфликт выделен отдельной ошибкой. В `services/organisation.py` — обработка одной порции campaign ID/generation с устойчивым результатом продолжения. В `services/delivery.py` — одна попытка delivery ID/generation. В `clients/auth.py` — `lookup_recipients(user_ids)`, `ensure_audience(audience_id)`, `get_audience_page(audience_id,cursor,limit)`. В `clients/smtp.py` — интерфейс отправителя `send(recipient, rendered_message, delivery_id)`, возвращающий SMTP acceptance либо типизированную временную/постоянную/неопределённую ошибку.

`brokers/rabbitmq.py` скрывает AMQP от бизнес-сервисов, связывает routing/confirm/return и поддерживает manual ack. `repositories/outbox.py` умеет атомарно сохранять обязанность в переданной SQL-сессии и выдавать записи по lane/времени с lease. Ни один repository метод не должен незаметно коммитить часть операции нотификаций; commit принадлежит сервисной транзакции. В Auth существующие commit boundaries расширяются source outbox адресно, без общего переписывания слоя.

Использовать Python 3.12, FastAPI для HTTP, Pydantic для схем/конфига, SQLAlchemy/asyncpg/Alembic для PostgreSQL, aio-pika для AMQP, httpx для межсервисных вызовов, Jinja2 для шаблонов и aiosmtplib для SMTP. Прямое использование чужих баз, Celery, новый Redis и второй источник состояния задач не нужны. Lockfile фиксирует проверенные версии; стек наблюдения переиспользует существующие Logstash/Sentry.

## Артефакты для ревью

`docs/notifications.design.md` содержит матрицу требований, полный целевой контракт, ограничения и изображения. `docs/diagrams/notifications/00`–`11` с суффиксом `-sequence-to-be` содержат пары `.puml`/`.png`: общий механизм трёх сценариев, Auth источник, приём, Auth контакты/аудитория, organiser, sender, retries/DLX, scheduler, preferences, operations, health, разовая отсрочка. Ревью начинать с [общей схемы](diagrams/notifications/00-overview-sequence-to-be.png), затем переходить к локальному процессу. Каждый новый HTTP маршрут покрыт диаграммой; новые служебные команды покрыты процессными диаграммами.

Изменение плана 2026-10-08: исходная версия фиксирует выбранную пользователем архитектуру RabbitMQ с двумя типами воркеров и включает замечания независимого ревью Astra о транзакционных границах, DLX, SMTP, аудитории и восстановлении.

Изменение плана 2026-10-09: по запросу пользователя добавлен обзор трёх сценариев, убрано дублирование внутренней логики Auth, уточнён смысл чтения исходящих событий. Разовая отсрочка send_at согласована с API, состояниями, бюджетом, восстановлением, этапами реализации и приёмкой; регулярный выпуск сохранён как отдельный способ отложенной отправки.
