"""Защита снимков кэша во время изменения прав.

Основные данные механизма:

- Снимок содержит уже вычисленные права одного пользователя.
- Маркер блокировки — временный ключ Redis. Он сообщает чтению кэша, что права
  сейчас меняются и сохранённому снимку доверять нельзя.
- Идентификатор владельца — случайное значение внутри маркера. Удалить маркер
  может только тот процесс, который записал в него свой идентификатор.
- Ревизия ролей обозначает текущее состояние общих ролевых правил. Снимки со
  старой ревизией не используются.
- Срок жизни маркера снимает блокировку, если процесс завершился до очистки.

Изменение прав одного пользователя происходит так:

1. ``RedisUserAuthorizationMutation`` устанавливает маркер этого пользователя.
2. ``yield`` передаёт управление сервису, который меняет назначение роли в
   PostgreSQL.
3. После возвращения управления Lua-скрипт увеличивает поколение
   пользователя, проверяет идентификатор владельца, удаляет снимок
   пользователя и его маркер. Поколение не даёт запросу, прочитавшему
   PostgreSQL до коммита, записать устаревший снимок после снятия маркера.
4. При следующей проверке прав снимок будет построен заново из PostgreSQL.

Изменение общих ролевых правил происходит так:

1. ``RedisRoleAuthorizationMutation`` устанавливает общий маркер изменения
   ролей.
2. ``yield`` передаёт управление сервису, который меняет роль в PostgreSQL.
3. После успешного изменения Lua-скрипт публикует новую ревизию и удаляет
   общий маркер. Если изменение не выполнено, ревизия остаётся прежней.
4. Снимки со старой ревизией перестают использоваться без удаления каждого
   ключа по отдельности.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.backoff_policy import on_redis_exception
from db.cache.authorization_snapshot import AuthorizationKeyspace

logger = logging.getLogger(__name__)

# Поколение должно пережить любой запрос, который успел прочитать его до
# изменения: иначе после истечения ключа счётчик начнётся заново и может
# совпасть с прочитанным значением. Неделя заведомо больше любого запроса.
USER_GENERATION_TTL_SECONDS = 7 * 24 * 60 * 60

# Скрипт одной операцией проверяет владельца маркера, затем удаляет снимок
# пользователя и принадлежащий этому процессу маркер.
_FINISH_USER_MUTATION_SCRIPT = """
-- KEYS: маркер пользователя, его снимок, поколение пользователя.
-- ARGV: идентификатор владельца маркера, срок жизни поколения.
-- Поколение растёт всегда: права в PostgreSQL могли измениться, и снимок,
-- вычисленный по данным до этого момента, сохранять уже нельзя.
redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], ARGV[2])

if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[2], KEYS[1])
    return 1
end

-- Маркер уже не наш: истёк (мутация шла дольше его TTL) или его взял другой
-- процесс. Снимок, записанный без маркера, мог прочитать PostgreSQL до
-- нашего коммита, поэтому удаляем его в любом случае.
redis.call('DEL', KEYS[2])
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 1
end

return 0
"""

# После успешного изменения в PostgreSQL скрипт одной операцией публикует новую
# ревизию ролей и удаляет маркер, принадлежащий этому процессу.
_FINISH_ROLES_MUTATION_SCRIPT = """
-- KEYS: общий маркер изменения ролей, общая ревизия ролей.
-- ARGV: идентификатор владельца, новая ревизия,
-- признак успешного изменения в PostgreSQL.
if redis.call('GET', KEYS[1]) == ARGV[1] then
    if ARGV[3] == '1' then
        redis.call('SET', KEYS[2], ARGV[2])
    end
    redis.call('DEL', KEYS[1])
    return 1
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    if ARGV[3] == '0' or redis.call('GET', KEYS[2]) == ARGV[2] then
        return 1
    end
end

return 0
"""


class AuthorizationMutationInProgressError(Exception):
    """Маркер блокировки уже принадлежит другому процессу изменения."""


class _RedisAuthorizationMutation:
    """Общая логика безопасного получения маркера блокировки в Redis."""

    def __init__(
        self,
        redis: Redis,
        *,
        marker_ttl_seconds: int,
        keys: AuthorizationKeyspace,
    ) -> None:
        self._redis = redis
        self._marker_ttl_seconds = marker_ttl_seconds
        self._keys = keys

    async def _acquire_marker(self, marker_key: str, token: str) -> None:
        # Идентификатор обозначает владельца маркера. Без этой проверки один
        # процесс мог бы удалить маркер другого, более нового процесса.
        acquired = await self._set_marker(marker_key, token)
        if not acquired:
            raise AuthorizationMutationInProgressError

    @on_redis_exception()
    async def _set_marker(self, marker_key: str, token: str) -> bool:
        return bool(
            await self._redis.set(
                marker_key,
                token,
                # Срок жизни защищает от вечной блокировки, если процесс
                # завершится раньше, чем успеет очистить маркер.
                ex=self._marker_ttl_seconds,
                # Опция ``NX`` разрешает создать маркер только одному процессу.
                nx=True,
            )
        )


class RedisUserAuthorizationMutation(_RedisAuthorizationMutation):
    """Защищает изменение прав одного пользователя."""

    def __init__(
        self,
        redis: Redis,
        *,
        marker_ttl_seconds: int,
        keys: AuthorizationKeyspace,
    ) -> None:
        super().__init__(
            redis,
            marker_ttl_seconds=marker_ttl_seconds,
            keys=keys,
        )
        self._finish_script = redis.register_script(_FINISH_USER_MUTATION_SCRIPT)

    @asynccontextmanager
    async def mutate(self, user_id: UUID) -> AsyncGenerator[None, None]:
        marker_key = self._keys.user_mutation_marker(user_id)
        token = str(uuid4())
        await self._acquire_marker(marker_key, token)
        try:
            # Маркер уже установлен. ``yield`` передаёт управление операции в
            # PostgreSQL внутри внешнего ``async with user_mutation(...)``.
            yield
        finally:
            # Очистка выполняется и при успехе, и при ошибке операции в
            # PostgreSQL: снимок мог устареть, поэтому его безопаснее удалить.
            try:
                finalized = await self._finish(
                    marker_key,
                    self._keys.user_snapshot(user_id),
                    self._keys.user_generation(user_id),
                    token,
                )
                if not finalized:
                    logger.error(
                        'Authorization user mutation marker ownership lost user_id=%s',
                        user_id,
                    )
            except RedisError:
                # Изменение в PostgreSQL могло уже сохраниться. Не подменяем
                # его результат ошибкой очистки: маркер останется до истечения
                # своего срока жизни.
                logger.exception(
                    'Could not finalize authorization cache invalidation user_id=%s',
                    user_id,
                )

    @on_redis_exception()
    async def _finish(
        self,
        marker_key: str,
        snapshot_key: str,
        generation_key: str,
        token: str,
    ) -> bool:
        result = await self._finish_script(
            keys=(marker_key, snapshot_key, generation_key),
            args=(token, USER_GENERATION_TTL_SECONDS),
        )
        return bool(result)


class RedisRoleAuthorizationMutation(_RedisAuthorizationMutation):
    """Защищает изменение прав, влияющее на снимки многих пользователей."""

    def __init__(
        self,
        redis: Redis,
        *,
        marker_ttl_seconds: int,
        keys: AuthorizationKeyspace,
    ) -> None:
        super().__init__(
            redis,
            marker_ttl_seconds=marker_ttl_seconds,
            keys=keys,
        )
        self._finish_script = redis.register_script(_FINISH_ROLES_MUTATION_SCRIPT)

    @asynccontextmanager
    async def mutate(self) -> AsyncGenerator[None, None]:
        token = str(uuid4())
        new_revision = str(uuid4())
        await self._acquire_marker(
            self._keys.roles_mutation_marker,
            token,
        )
        succeeded = False
        try:
            # Пока выполнение находится внутри ``yield``, чтение видит общий
            # маркер изменения ролей и не доверяет сохранённым снимкам.
            yield
            succeeded = True
        finally:
            # Новая ревизия публикуется только после успешного изменения в
            # PostgreSQL.
            # Все снимки со старой ревизией сразу становятся недействительными.
            try:
                finalized = await self._finish(
                    token,
                    new_revision,
                    succeeded=succeeded,
                )
                if not finalized:
                    logger.error('Authorization roles mutation marker ownership lost')
            except RedisError:
                # При сбое маркер остаётся до конца своего срока жизни и
                # запрещает читать старые снимки после сохранения изменений в
                # PostgreSQL.
                logger.exception('Could not finalize authorization role revision')

    @on_redis_exception()
    async def _finish(
        self,
        token: str,
        new_revision: str,
        *,
        succeeded: bool,
    ) -> bool:
        result = await self._finish_script(
            keys=(
                self._keys.roles_mutation_marker,
                self._keys.roles_revision,
            ),
            args=(token, new_revision, int(succeeded)),
        )
        return bool(result)
