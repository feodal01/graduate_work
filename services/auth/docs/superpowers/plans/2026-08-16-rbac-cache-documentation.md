# RBAC Cache Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Объяснить общую модель RBAC-кэша в README, PlantUML и module docstring без изменения поведения сервиса.

**Architecture:** PostgreSQL документируется как единственный источник истины для пользователей, ролей, permissions и назначений. Redis документируется как производный cache-aside слой: пользовательские снимки, общая UUID-ревизия ролевых правил и временные marker-ключи мутаций. README задаёт термины, sequence показывает процесс во времени, C4 показывает владельцев данных и компоненты, Python-модули получают краткие локальные карты.

**Tech Stack:** Markdown, PlantUML, C4-PlantUML, Python module docstring, pytest.

## Global Constraints

- Работать в текущей ветке `main`.
- Менять только README, PlantUML и комментарии/docstring.
- Не менять Python-инструкции, интерфейсы, Redis-ключи, SQL или настройки.
- Не утверждать, что Redis хранит справочник ролей.
- Использовать одинаковые термины во всех документах: роль, назначение роли, эффективные права, снимок пользователя, ревизия ролей, marker мутации.
- Все объяснения писать простым русским языком.

---

### Task 1: Общая модель в README

**Files:**
- Modify: `services/auth/README.md`

**Interfaces:**
- Produces: canonical section `Как работает RBAC-кэш` used as the terminology source for diagrams and module docstrings.

- [x] **Step 1: Заменить плотные абзацы самостоятельным разделом**

После описания токенов добавить заголовок `### Как работает RBAC-кэш` и вводный
абзац: PostgreSQL — источник истины, Redis — производный кэш уже вычисленных
прав, а отдельного Redis-кэша ролей нет.

- [x] **Step 2: Добавить термины и владельцев данных**

Добавить таблицу:

```markdown
| Хранилище | Что хранится |
| --- | --- |
| PostgreSQL | users, roles, permissions, role_permissions, user_roles |
| Redis | снимки пользователей, ревизия ролей, marker-ключи |
```

Перед таблицей определить роль, назначение роли, эффективные права, снимок,
ревизию и marker.

- [x] **Step 3: Добавить таблицу Redis-ключей**

Описать точные ключи `{authz}:roles-revision`, `{authz}:roles-changing`,
`{authz}:user-changing:<user_id>` и `{authz}:user:<user_id>` с назначением и
временем использования.

- [x] **Step 4: Описать четыре процесса**

Пошагово описать:

1. cache hit/miss при проверке permissions;
2. условное заполнение снимка после чтения PostgreSQL;
3. локальную инвалидацию одного пользователя;
4. глобальную инвалидацию сменой ревизии.

Завершить раздел поведением при отказе Redis: чтение прав идёт в PostgreSQL,
но SQL-мутация прав не начинается без marker-а и отвечает `503`.

### Task 2: Обзорные карты Python-модулей

**Files:**
- Modify: `services/auth/src/db/cache/authorization.py`
- Modify: `services/auth/src/db/cache/authorization_snapshot.py`
- Modify: `services/auth/src/db/cache/authorization_mutations.py`
- Modify: `services/auth/src/services/authorization.py`

**Interfaces:**
- Consumes: terminology from Task 1.
- Produces: Russian module docstrings only; runtime code remains byte-for-byte unchanged below the docstring insertion points.

- [x] **Step 1: Добавить карту фасада**

В `db/cache/authorization.py` объяснить, что `AuthorizationCache` скрывает три
Redis-компонента и переводит `RedisError` в `CacheUnavailableError`.

- [x] **Step 2: Добавить карту снимков**

В `db/cache/authorization_snapshot.py` перечислить четыре значения одного
`MGET`, условия cache miss и проверку ревизии при условной записи Lua.

- [x] **Step 3: Добавить карту мутаций**

В `db/cache/authorization_mutations.py` сопоставить user mutation с удалением
одного снимка, role mutation со сменой общей ревизии и объяснить token/TTL
marker-а.

- [x] **Step 4: Добавить карту cache-aside сервиса**

В `services/authorization.py` описать путь Redis hit -> ответ и Redis miss/error
-> PostgreSQL -> условное заполнение Redis. Подчеркнуть, что PostgreSQL остаётся
источником истины.

### Task 3: Подробный cache-aside сценарий в sequence

**Files:**
- Modify: `services/auth/c4/sequence-roles.puml`

**Interfaces:**
- Consumes: terminology from Task 1 and component names from production code.
- Produces: first sequence section `Проверка permissions через RBAC-кэш`.

- [x] **Step 1: Добавить участников кэша**

Внутри системы показать `AuthorizationService` и `RedisAuthorizationCache` как
отдельных участников; PostgreSQL и Redis оставить базами данных.

- [x] **Step 2: Добавить ветки cache hit и cache miss**

Показать `MGET` четырёх значений. `alt` должен различать активный marker,
отсутствующий снимок, несовпадающую ревизию и валидный снимок. Первые три ветки
возвращают miss, последняя — готовые permissions без PostgreSQL.

- [x] **Step 3: Показать fallback и условную запись**

На miss показать чтение пользователя и эффективных permissions из PostgreSQL,
затем Lua-запись только при прежней ревизии и отсутствии marker-ов. Отдельная
ветка ошибки Redis показывает чтение PostgreSQL без заполнения кэша.

- [x] **Step 4: Синхронизировать подписи мутаций**

В существующих сценариях использовать термины `user mutation marker`,
`roles mutation marker`, `удалить снимок пользователя` и `сменить общую
ревизию`, не меняя HTTP-контракт.

### Task 4: Владельцы данных и компоненты на C4

**Files:**
- Modify: `services/auth/c4/c4-container.puml`
- Modify: `services/auth/c4/c4-component.puml`

**Interfaces:**
- Consumes: storage ownership and cache flow from Tasks 1 and 3.
- Produces: valid C4-PlantUML level 2 and level 3 diagrams.

- [x] **Step 1: Уточнить container-level модель**

Подписать PostgreSQL как источник истины RBAC, Redis как производный
cache-aside слой без справочника ролей. Отношения Auth API с базами разделить
на чтение/изменение исходных RBAC-данных и чтение/инвалидацию снимков.

- [x] **Step 2: Декомпозировать component-level кэш**

Заменить общий компонент `db` на `PostgreSQL repositories`,
`RedisAuthorizationCache`, `RedisAuthorizationSnapshotStore`,
`RedisUserAuthorizationMutation` и `RedisRoleAuthorizationMutation`.

- [x] **Step 3: Добавить явные связи**

Показать: `AuthorizationService -> repositories`,
`AuthorizationService -> RedisAuthorizationCache`, фасад -> snapshot store и
два mutation component, repositories -> PostgreSQL, cache components -> Redis.

### Task 5: Проверка документации

**Files:**
- Verify: `services/auth/README.md`
- Verify: `services/auth/c4/*.puml`
- Verify: four Python modules from Task 2.

**Interfaces:**
- Produces: documentation consistent with the running service.

- [x] **Step 1: Проверить PlantUML**

```bash
plantuml -checkonly c4/*.puml
```

Expected: exit code 0.

- [x] **Step 2: Проверить Python и поведение**

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run python -m compileall -q src tests
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest -q
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run --with pyright pyright
```

Expected: compileall exits 0, all tests pass, Pyright reports 0 errors.

- [x] **Step 3: Проверить качество diff**

```bash
git diff --check
rg -n "источник истины|не хранит справочник ролей|снимок|ревизи|marker" \
  README.md c4/*.puml src/db/cache/authorization*.py src/services/authorization.py
```

Expected: no whitespace errors; each documentation layer contains the agreed
storage ownership and cache terminology.

### Task 6: Исправить обзорные объяснения по замечаниям ревью

**Files:**
- Modify: `services/auth/src/db/cache/authorization.py`
- Modify: `services/auth/src/db/cache/authorization_snapshot.py`
- Modify: `services/auth/src/db/cache/authorization_mutations.py`
- Modify: `services/auth/src/services/authorization.py`

**Interfaces:**
- Consumes: фактические классы, методы и процессы RBAC-кэша.
- Produces: только новые docstring и комментарии; исполняемые инструкции Python
  не меняются.

- [x] **Step 1: Переписать каждый обзорный docstring от объектов к процессу**

В начале каждого файла сначала кратко определить все используемые дальше
объекты. Затем отдельными нумерованными списками описать процессы файла.
`authorization_mutations.py` обязан содержать два независимых списка: изменение
прав одного пользователя и изменение общих ролевых правил.

- [x] **Step 2: Убрать смешение языков из пояснений**

В docstring и локальных комментариях использовать «права», «маркер
блокировки», «идентификатор владельца», «срок жизни», «данные найдены в кэше»
и «данных в кэше нет или им нельзя доверять». Точные имена кода, Redis-команд
и полей оставлять в обратных кавычках после русского определения.

- [x] **Step 3: Проверить объяснения вручную**

Прочитать все четыре docstring сверху вниз и проверить два условия:

1. каждый объект определён до первого использования;
2. каждый процесс можно понять без перехода в другой файл.

- [x] **Step 4: Проверить отсутствие поведенческих изменений**

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run python -m compileall -q src tests
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest -q
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run --with pyright pyright
git diff --check
```

Expected: compileall exits 0, all tests pass, Pyright reports 0 errors, diff has
no whitespace errors.
