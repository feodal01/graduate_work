# UGC ETL

UGC ETL читает топик Kafka `ugc.events` и пишет строки в распределённую таблицу ClickHouse `ugc.events` на кластере `company_cluster`. Клиент ClickHouse — `aiochclient` (HTTP). Materialized view наполняют `ugc.film_watches` и `ugc.page_events`. Состояние курсора хранит Kafka: commit офсета выполняется после успешного INSERT.

## Время события

`occurred_at` — время события на клиенте. `received_at` — сохранённое время UTC,
которое API зафиксировал при приёме пакета. Оба поля должны содержать часовой пояс.
ETL сравнивает моменты времени с учётом смещений и проверяет включённые границы:

```text
received_at − 7 суток ≤ occurred_at ≤ received_at + 5 минут
```

Текущее время обработки записывается в `ingested_at` и не участвует в проверке окна.
Поэтому задержка в Kafka и повторная обработка не делают корректное событие слишком старым.
ETL не исправляет `occurred_at`. Нарушившее окно сообщение попадает в очередь ошибочных
сообщений (DLQ) `ugc.events.dlq`, без INSERT в ClickHouse; офсет подтверждается после
успешной отправки в DLQ.

Корневой Compose задаёт API и ETL одинаковые значения из `.env`:
`UGC_EVENTS_MAX_AGE_SECONDS=604800`, `UGC_EVENTS_MAX_FUTURE_SKEW_SECONDS=300`.
Для отдельного процесса используются `EVENTS_MAX_AGE_SECONDS` и
`EVENTS_MAX_FUTURE_SKEW_SECONDS` из сервисного `.env`. Максимальный возраст должен быть
положительным, отклонение в будущее — неотрицательным. Настройки API и ETL должны совпадать.
При сужении окна ранее принятые сообщения могут попасть в DLQ: ETL проверяет их
по своим настройкам на момент обработки.

## Запуск в едином проекте

Выполните команды из корня репозитория:

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f ugc-etl
```

Корневой Compose запускает ETL после готовности Kafka, `kafka-init`, ZooKeeper и четырёх узлов ClickHouse. ETL подключается к `clickhouse-node1:8123`. Gateway не зависит от состояния ETL. Health снаружи: `http://localhost/ugc-etl/health`.

Описание кластера, шардов и проверки цепочки — в корневом [README](../../README.md#события-зрителя).

## Локальный запуск

Для локального процесса подготовьте Kafka и ClickHouse, которые доступны с хоста. Затем выполните команды из `services/ugc-etl`:

```bash
cp .env.example .env
uv sync --group test
uv run uvicorn main:app --app-dir src --host 127.0.0.1 --port 8000
```

## Проверка здоровья

Неподтверждённый пакет — события, для которых ещё не завершены INSERT, отправка
ошибочных сообщений в DLQ и `commit` смещений Kafka. ETL хранит в памяти процесса
время получения этого пакета, время последнего успешного `commit` и число ошибок подряд.
Длительности считает через `time.monotonic()`, независимо от перевода системных часов.

Health возвращает `503` и `pipeline: unavailable`, если задача завершилась либо
возраст текущего пакета достиг `ETL_STALL_TIMEOUT_SECONDS` (по умолчанию 360 секунд).
Ошибки и повторные попытки не сбрасывают возраст пакета. Зависшая операция тоже
достигает порога, даже если ещё не вернула ошибку. Проверки Kafka, ClickHouse и RSS сохраняются.

После успешного `commit` пакет очищается, число ошибок обнуляется, фиксируется успех;
health восстанавливается при исправных зависимостях. Простой без текущего пакета
не считается остановкой прогресса, даже если последняя обработка была давно.
После перезапуска процесса показатели начинаются заново; неподтверждённые события
повторно читаются из Kafka.

В ответе health есть `pending_age_seconds` (null без пакета), `last_success_age_seconds`
(null до первого успешного `commit`) и `consecutive_errors`. Счётчик учитывает ошибки,
дошедшие до `run_forever`, после внутренних повторных попыток зависимостей.
Порог должен быть положительным и конечным. Для общего Compose настройка в корневом
`.env` — `UGC_ETL_STALL_TIMEOUT_SECONDS=360`; для отдельного процесса —
`ETL_STALL_TIMEOUT_SECONDS=360` в сервисном `.env`.

## Проверка

Модульные тесты не требуют запущенных Kafka и ClickHouse:

```bash
uv lock --check
uv run pytest tests -q
```

Живые проверки гоняют изолированные Kafka, ClickHouse и образ ETL.
`--abort-on-container-exit` здесь нельзя: тесты сами останавливают ClickHouse и Kafka.

```bash
cd tests/functional
docker compose up -d --build kafka zookeeper clickhouse-node1 clickhouse-node2 clickhouse-node3 clickhouse-node4 kafka-init etl
docker compose run --rm --no-deps tests
docker compose down -v
```

Функциональные тесты используют порты из `tests/functional/.env`. Они не используют корневой Compose.
