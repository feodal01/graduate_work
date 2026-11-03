# Isolated Functional API Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить один воспроизводимый запуск функциональных тестов Auth API по реальному HTTP с отдельными PostgreSQL и Redis.

**Architecture:** Все артефакты контура находятся в `services/auth/functional_tests`. `run.sh` поднимает отдельный Compose-проект, Auth-контейнер применяет миграции и создаёт суперпользователя, pytest проверяет API через опубликованный порт, после чего стек всегда удаляется.

**Tech Stack:** Docker Compose, FastAPI, PostgreSQL 16, Redis 7, pytest, pytest-asyncio, httpx, uv.

## Global Constraints

- Работать в текущей ветке `main` без отдельного worktree.
- Не менять бизнес-логику, публичные маршруты или миграции.
- Не использовать основной локальный PostgreSQL и Redis для функциональных тестов.
- Все функциональные артефакты хранить в `services/auth/functional_tests`.
- Локальный `services/auth/.env` не коммитить и не выводить значения секретов.
- После любого результата pytest удалять функциональные контейнеры и данные.

---

### Task 1: Изолированный Docker Compose

**Files:**
- Create: `services/auth/functional_tests/functional.env`
- Create: `services/auth/functional_tests/docker-compose.yaml`
- Create: `services/auth/functional_tests/run.sh`

**Interfaces:**
- Consumes: `services/auth/Dockerfile`, `services/auth/alembic.ini`, `_create_superuser(login: str, password: str) -> str`.
- Produces: Auth API по `AUTH_FUNCTIONAL_BASE_URL` и переменные окружения для pytest.

- [ ] **Step 1: Создать неизменяемые настройки тестового контура**

`functional.env` содержит отдельные учётные данные PostgreSQL, тестовые
`JWT_SECRET` и `SERVICE_API_KEY`, порт `18000`, URL API и учётные данные
суперпользователя. Все значения помечены как тестовые и не используются
основным Compose.

- [ ] **Step 2: Создать Compose-файл**

Определить сервисы `auth`, `auth-db` и `redis`. PostgreSQL и Redis используют
`tmpfs`, Auth собирается из `..`, применяет миграции, вызывает
`_create_superuser` со значениями окружения и запускает Uvicorn. Публикуется
только порт Auth API.

- [ ] **Step 3: Создать единый runner**

`run.sh` должен:

```sh
docker compose --env-file functional.env -f docker-compose.yaml up \
  -d --build --wait --wait-timeout 90
uv run pytest functional_tests -q
```

Очистка `docker compose down -v --remove-orphans` регистрируется через
`trap ... EXIT`, поэтому выполняется и при падении pytest.

- [ ] **Step 4: Проверить конфигурацию до запуска**

Run:

```bash
docker compose \
  --env-file functional_tests/functional.env \
  -f functional_tests/docker-compose.yaml \
  config -q
```

Expected: exit code `0`.

---

### Task 2: Функциональный тест аутентификации

**Files:**
- Create: `services/auth/functional_tests/conftest.py`
- Create: `services/auth/functional_tests/test_auth_api.py`

**Interfaces:**
- Consumes: `AUTH_FUNCTIONAL_BASE_URL` из runner-а.
- Produces: fixture `http_client() -> AsyncGenerator[httpx.AsyncClient, None]`.

- [ ] **Step 1: Создать настоящий HTTP-клиент**

`conftest.py` читает обязательный `AUTH_FUNCTIONAL_BASE_URL` и создаёт
function-scoped `httpx.AsyncClient`. При отсутствии переменной pytest
останавливается с явным `UsageError`.

- [ ] **Step 2: Добавить жизненный цикл пользователя**

Тест использует уникальный login и последовательно проверяет:

```text
signup 201 -> duplicate signup 409 -> login 200 -> login-history 200
-> profile 200 -> refresh 200 -> old refresh 401 -> logout 204
-> revoked access 401 -> revoked refresh 401
```

Ответ signup и profile дополнительно проверяется на отсутствие `password` и
`password_hash`.

- [ ] **Step 3: Запустить тест через изолированный стек**

Run: `functional_tests/run.sh functional_tests/test_auth_api.py`

Expected: authentication functional test passes; Compose project is absent
after command completion.

---

### Task 3: Функциональные тесты авторизации

**Files:**
- Create: `services/auth/functional_tests/test_permissions_api.py`

**Interfaces:**
- Consumes: fixtures `http_client`, `functional_settings`; сервисный ключ и учётные данные суперпользователя.
- Produces: проверки политики фильмов и полного цикла RBAC.

- [ ] **Step 1: Проверить политику фильмов**

Первый тест проверяет неверный `X-Service-Key` (`401`), публичный старый фильм
(`allowed=true`), новый фильм для анонима (`allowed=false`) и отсутствие
`creation_date` (`422`). Для стабильности используются даты `2000-01-01` и
`9999-01-01`.

- [ ] **Step 2: Проверить управление ролями**

Второй тест входит суперпользователем, создаёт обычного пользователя и роль с
`films.early_access:read`, обновляет и находит роль в списке, назначает её,
получает `allowed=true`, отзывает роль, получает `allowed=false` и удаляет
роль. Имя роли и login создаются через `uuid4()`.

- [ ] **Step 3: Запустить полный функциональный набор**

Run: `functional_tests/run.sh`

Expected: все функциональные тесты проходят, после завершения отсутствуют
контейнеры Compose-проекта `auth-functional`.

---

### Task 4: Локальные секреты и документация

**Files:**
- Modify: `services/auth/.env` (не коммитится)
- Create: `services/auth/functional_tests/README.md`
- Modify: `services/auth/README.md`

**Interfaces:**
- Consumes: `openssl rand -hex 32`, runner из Task 1.
- Produces: запускаемый основной Compose и документированная команда functional-тестов.

- [ ] **Step 1: Добавить локальные секреты без вывода значений**

Сгенерировать два независимых значения `openssl rand -hex 32`, добавить их как
`JWT_SECRET` и `SERVICE_API_KEY` только если переменные отсутствуют. Проверить
наличие имён и длину значений, не печатая сами значения.

- [ ] **Step 2: Описать функциональный контур через процесс**

`functional_tests/README.md` сначала определяет функциональный контур,
`functional.env` и runner, затем пошагово описывает запуск, проверяемые
сценарии и очистку.

- [ ] **Step 3: Добавить команду в основной README**

В раздел `Проверка` добавить:

```bash
./functional_tests/run.sh
```

Указать, что команда использует отдельные контейнеры и сама удаляет данные.

---

### Task 5: Финальная проверка

**Files:**
- Verify: `services/auth/functional_tests/**`
- Verify: `services/auth/README.md`
- Verify: `services/auth/pyproject.toml`

**Interfaces:**
- Consumes: все результаты Tasks 1-4.
- Produces: подтверждённый запуск функциональных и обычных тестов.

- [ ] **Step 1: Запустить functional-тесты**

Run: `./functional_tests/run.sh`

Expected: все functional-тесты проходят; `docker compose ... ps -a` не
показывает оставшихся контейнеров.

- [ ] **Step 2: Запустить обычные проверки**

Run:

```bash
uv run ruff check src tests functional_tests
uv run pytest -q
uv lock --check
uv run python -m compileall -q src tests functional_tests
git diff --check
```

Expected: все команды завершаются с exit code `0`.

- [ ] **Step 3: Проверить основной Compose**

Run:

```bash
docker compose up -d --build --wait --wait-timeout 90
curl -sS http://127.0.0.1:8000/api/v1/health/
docker compose down
```

Expected: health возвращает `status=ok`; локальные секреты не выводятся.
