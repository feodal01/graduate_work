# Sequence Diagram Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Разделить слишком длинную RBAC sequence-диаграмму на три читаемых файла и дать диаграмме аутентификации конкретное имя.

**Architecture:** `sequence-roles` остаётся каноническим описанием механизма RBAC-кэша. Сценарии управления ролями и проверки фильма переносятся в отдельные диаграммы и ссылаются на механизм кэша без его копирования. Существующая диаграмма регистрации и токенов только переименовывается.

**Tech Stack:** PlantUML, PNG.

## Global Constraints

- Не менять смысл и порядок существующих сценариев.
- Сначала определять используемые сущности, затем показывать процесс по шагам.
- Использовать русский текст; точные имена методов, полей, таблиц и команд можно оставлять.
- Каждый `.puml` должен создавать одноимённый PNG высотой меньше 4096 пикселей.
- Работать в текущей ветке `main`, не затрагивая несвязанные изменения.

---

### Task 1: Переименовать диаграмму аутентификации

**Files:**
- Rename: `docs/c4/sequence.puml` → `docs/c4/sequence-authentication.puml`
- Rename: `docs/c4/sequence.png` → `docs/c4/sequence-authentication.png`

**Interfaces:**
- Consumes: существующие сценарии регистрации, входа, обновления токенов, профиля, истории входов и выхода.
- Produces: `sequence-authentication.puml` и `sequence-authentication.png`.

- [x] **Step 1: Переименовать исходник и уточнить его техническое имя**

Переименовать файл, заменить `@startuml sequence` на
`@startuml sequence-authentication` и заменить заголовок на
`Последовательность: аутентификация и личный кабинет`.

- [x] **Step 2: Перерисовать PNG**

```bash
plantuml docs/c4/sequence-authentication.puml
```

Ожидается `docs/c4/sequence-authentication.png`; старые `sequence.puml` и
`sequence.png` отсутствуют.

### Task 2: Оставить в `sequence-roles` только механизм RBAC-кэша

**Files:**
- Modify: `docs/c4/sequence-roles.puml`

**Interfaces:**
- Consumes: первые четыре секции текущего файла, от «Проверка права через RBAC-кэш» до «Следующая проверка прав после изменения».
- Produces: каноническое описание чтения, блокировки, инвалидации и повторного заполнения снимка.

- [x] **Step 1: Удалить пользовательские API-сценарии из файла**

Оставить участников сервиса, PostgreSQL и Redis, а также секции:

1. `Проверка права через RBAC-кэш`;
2. `Изменение прав одного пользователя`;
3. `Изменение общих ролевых правил`;
4. `Следующая проверка прав после изменения`.

После четвёртой секции завершить файл директивой `@enduml`.

- [x] **Step 2: Перерисовать PNG**

```bash
plantuml docs/c4/sequence-roles.puml
```

Ожидается полная диаграмма без обрезания нижней части.

### Task 3: Создать диаграмму управления ролями

**Files:**
- Create: `docs/c4/sequence-role-management.puml`
- Create: `docs/c4/sequence-role-management.png`

**Interfaces:**
- Consumes: секции текущего `sequence-roles.puml` от «Просмотр ролей» до «Отобрать роль у пользователя».
- Produces: самостоятельную диаграмму HTTP-сценариев управления ролями.

- [x] **Step 1: Перенести шесть сценариев без изменения поведения**

Создать файл с участниками `Администратор`, `Админка`, `api`,
`AuthenticationService / AuthorizationService`, `RedisAuthorizationCache`,
PostgreSQL и Redis. Перенести просмотр, создание, изменение и удаление роли,
назначение и отзыв роли.

В местах проверки административных прав использовать ссылку:

```plantuml
ref over svc, redis
  Чтение и обновление снимка описаны в sequence-roles
end ref
```

- [x] **Step 2: Перерисовать PNG**

```bash
plantuml docs/c4/sequence-role-management.puml
```

### Task 4: Создать диаграмму проверки доступа к фильму

**Files:**
- Create: `docs/c4/sequence-film-permission.puml`
- Create: `docs/c4/sequence-film-permission.png`

**Interfaces:**
- Consumes: секцию `Проверка прав у пользователя` из текущего `sequence-roles.puml`.
- Produces: самостоятельную диаграмму `POST /permissions/check`.

- [x] **Step 1: Перенести сценарий проверки фильма**

Создать файл с участниками `Зритель`, `Бэкенд кинотеатра`, `api`,
`AuthenticationService / AuthorizationService` и Redis. Перенести проверку
служебного ключа, политики трёх лет, анонимного доступа, токена,
суперпользователя и права `films.early_access:read`.

Для проверки права использовать ссылку на `sequence-roles` без копирования
механизма кэша.

- [x] **Step 2: Перерисовать PNG**

```bash
plantuml docs/c4/sequence-film-permission.puml
```

### Task 5: Проверить полный набор диаграмм

**Files:**
- Verify: `docs/c4/*.puml`
- Verify: `docs/c4/*.png`

**Interfaces:**
- Consumes: четыре итоговые sequence-диаграммы.
- Produces: синтаксически корректный и полностью отрисованный набор документации.

- [x] **Step 1: Проверить синтаксис**

```bash
plantuml -checkonly docs/c4/*.puml
```

- [x] **Step 2: Проверить размеры и список файлов**

Проверить, что существуют четыре пары `.puml`/`.png`, старых `sequence.*` нет,
а высота каждого PNG меньше 4096 пикселей.

- [x] **Step 3: Проверить diff**

```bash
git diff --check
```

Ожидается отсутствие ошибок оформления.
