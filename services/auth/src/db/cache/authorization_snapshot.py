"""Чтение и сохранение вычисленных прав пользователя в Redis.

Основные данные механизма:

- Снимок ``AuthorizationSnapshot`` содержит признак суперпользователя и набор
  его действующих прав. Роли и назначения в снимок не входят: они хранятся в
  PostgreSQL.
- Ревизия ролей — случайный идентификатор текущего состояния ролевых правил.
  Такая же ревизия записывается в каждый снимок.
- Маркер изменения пользователя — временный ключ Redis. Пока он существует,
  роли этого пользователя меняются и доверять его снимку нельзя.
- Маркер изменения ролей — временный ключ Redis. Пока он существует, меняются
  общие ролевые правила и доверять нельзя ни одному снимку.
- Поколение пользователя — счётчик, который растёт при каждом завершении
  изменения его прав. Маркер снимается сразу после изменения, поэтому только
  поколение отличает «права не менялись» от «изменились, маркер уже снят».

Чтение снимка происходит так:

1. Команда ``MGET`` одновременно читает ревизию ролей, оба маркера, снимок и
   поколение пользователя.
2. Если установлен хотя бы один маркер, метод возвращает результат без снимка.
3. Если снимка нет, его JSON повреждён или его ревизия не совпадает с текущей,
   метод тоже возвращает результат без снимка.
4. Только корректный снимок с текущей ревизией возвращается сервису.

Сохранение снимка происходит так:

1. После результата без снимка сервис вычисляет права по данным PostgreSQL.
2. Вместе с новым снимком сервис передаёт ревизию и поколение, полученные в
   начале чтения.
3. Lua-скрипт повторно проверяет ревизию, поколение и отсутствие маркеров.
4. Если за это время права начали меняться, снимок не сохраняется. Иначе он
   записывается в Redis с ограниченным сроком жизни.
"""

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, ValidationError
from redis.asyncio import Redis

from db.backoff_policy import on_redis_exception

logger = logging.getLogger(__name__)

# Скрипт одной операцией проверяет состояние прав и сохраняет снимок. Между
# проверкой и записью другой запрос не может изменить проверяемые ключи.
_SET_SNAPSHOT_SCRIPT = """
-- KEYS: текущая ревизия, маркер изменения ролей,
-- маркер изменения пользователя, снимок, поколение пользователя.
-- ARGV: ожидаемая ревизия, JSON снимка, срок жизни снимка,
-- ожидаемое поколение ('' — ключа поколения не было).
if redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end

-- Права пользователя изменились после чтения PostgreSQL: маркер уже снят,
-- но поколение выросло. Такой снимок устарел ещё до записи.
if (redis.call('GET', KEYS[5]) or '') ~= ARGV[4] then
    return 0
end

redis.call('SET', KEYS[4], ARGV[2], 'EX', ARGV[3])
return 1
"""


@dataclass(frozen=True, slots=True)
class AuthorizationSnapshot:
    """Готовый результат вычисления прав одного пользователя."""

    is_superuser: bool
    permissions: frozenset[str]
    nearest_expires_at: datetime | None = None


def compute_effective_snapshot_ttl(
    base_ttl_seconds: int,
    nearest_expires_at: datetime | None,
    *,
    now_utc: datetime | None = None,
) -> int:
    """Ограничивает TTL снимка прав ближайшим сроком истечения роли (AUTH-FR-03)."""
    if nearest_expires_at is None:
        return base_ttl_seconds
    current = now_utc if now_utc is not None else datetime.now(UTC)
    target = (
        nearest_expires_at
        if nearest_expires_at.tzinfo is not None
        else nearest_expires_at.replace(tzinfo=UTC)
    )
    remaining_seconds = math.ceil((target - current).total_seconds())
    return min(base_ttl_seconds, max(1, remaining_seconds))


@dataclass(frozen=True, slots=True)
class AuthorizationSnapshotReadResult:
    """Результат чтения снимка и ревизия, при которой он был прочитан.

    Если ``cached_snapshot`` равен ``None``, сервис читает права из PostgreSQL.
    ``roles_revision_at_read`` затем защищает запись нового снимка: Redis
    примет его, только если роли за время запроса не изменились.
    """

    cached_snapshot: AuthorizationSnapshot | None
    roles_revision_at_read: str
    user_generation_at_read: str = ''


@dataclass(frozen=True, slots=True)
class AuthorizationKeyspace:
    """Все Redis-ключи, используемые механизмом RBAC-кэша.

    Фигурные скобки в начале ключа заставляют Redis Cluster размещать эти ключи
    в одной секции. Благодаря этому один Lua-скрипт может работать со всеми
    нужными ключами.
    """

    prefix: str

    @classmethod
    def create(cls, namespace: str) -> 'AuthorizationKeyspace':
        return cls(prefix=f'{{{namespace}}}')

    @property
    def roles_revision(self) -> str:
        return f'{self.prefix}:roles-revision'

    @property
    def roles_mutation_marker(self) -> str:
        return f'{self.prefix}:roles-changing'

    def user_mutation_marker(self, user_id: UUID) -> str:
        return f'{self.prefix}:user-changing:{user_id}'

    def user_snapshot(self, user_id: UUID) -> str:
        return f'{self.prefix}:user:{user_id}'

    def user_generation(self, user_id: UUID) -> str:
        return f'{self.prefix}:user-generation:{user_id}'


@dataclass(frozen=True, slots=True)
class _RedisAuthorizationState:
    """Пять связанных значений, прочитанных командой Redis ``MGET``."""

    roles_revision: bytes | str | None
    roles_mutation_marker: bytes | str | None
    user_mutation_marker: bytes | str | None
    snapshot_payload: bytes | str | None
    user_generation: bytes | str | None

    @property
    def mutation_in_progress(self) -> bool:
        """Показывает, что снимок нельзя читать или обновлять."""

        return (
            self.roles_mutation_marker is not None
            or self.user_mutation_marker is not None
        )


class AuthorizationRevisionUnavailableError(Exception):
    """Общую ревизию ролей не удалось прочитать или создать в Redis."""


class _StoredAuthorizationSnapshot(BaseModel):
    """JSON-снимок в Redis вместе с ревизией использованных ролей."""

    model_config = ConfigDict(strict=True)

    roles_revision: str
    is_superuser: bool
    permissions: list[str]
    nearest_expires_at: str | None = None


class RedisAuthorizationSnapshotStore:
    """Читает и условно сохраняет итоговые права пользователей в Redis."""

    def __init__(
        self,
        redis: Redis,
        *,
        ttl_seconds: int,
        keys: AuthorizationKeyspace,
    ) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds
        self._keys = keys
        self._set_snapshot_script = redis.register_script(_SET_SNAPSHOT_SCRIPT)

    async def read_snapshot(
        self,
        user_id: UUID,
    ) -> AuthorizationSnapshotReadResult:
        redis_state = await self._read_redis_state(
            user_id,
            revision_candidate=str(uuid4()),
        )
        current_roles_revision = self._decode_redis_text(redis_state.roles_revision)
        if current_roles_revision is None:
            raise AuthorizationRevisionUnavailableError

        user_generation = self._decode_redis_text(redis_state.user_generation) or ''
        cache_miss = AuthorizationSnapshotReadResult(
            cached_snapshot=None,
            roles_revision_at_read=current_roles_revision,
            user_generation_at_read=user_generation,
        )

        # Пока права меняются, снимок может уже не соответствовать PostgreSQL.
        # Поэтому возвращаем результат без снимка.
        if redis_state.mutation_in_progress:
            return cache_miss

        snapshot_json = self._decode_redis_text(redis_state.snapshot_payload)
        if snapshot_json is None:
            return cache_miss
        try:
            stored_snapshot = _StoredAuthorizationSnapshot.model_validate_json(
                snapshot_json
            )
        except ValidationError:
            logger.warning('Ignoring malformed authorization cache entry')
            return cache_miss

        # Смена общей ревизии означает, что права какой-то роли изменились.
        # Все снимки со старой ревизией после этого считаются устаревшими.
        if stored_snapshot.roles_revision != current_roles_revision:
            return cache_miss

        nearest_expires_at: datetime | None = None
        if stored_snapshot.nearest_expires_at is not None:
            try:
                parsed = datetime.fromisoformat(stored_snapshot.nearest_expires_at)
                nearest_expires_at = (
                    parsed.astimezone(UTC)
                    if parsed.tzinfo is not None
                    else parsed.replace(tzinfo=UTC)
                )
            except ValueError:
                return cache_miss
            if nearest_expires_at <= datetime.now(UTC):
                return cache_miss

        return AuthorizationSnapshotReadResult(
            cached_snapshot=AuthorizationSnapshot(
                is_superuser=stored_snapshot.is_superuser,
                permissions=frozenset(stored_snapshot.permissions),
                nearest_expires_at=nearest_expires_at,
            ),
            roles_revision_at_read=current_roles_revision,
            user_generation_at_read=user_generation,
        )

    @on_redis_exception()
    async def _read_redis_state(
        self,
        user_id: UUID,
        revision_candidate: str,
    ) -> _RedisAuthorizationState:
        keys = (
            self._keys.roles_revision,
            self._keys.roles_mutation_marker,
            self._keys.user_mutation_marker(user_id),
            self._keys.user_snapshot(user_id),
            self._keys.user_generation(user_id),
        )
        redis_state = self._unpack_redis_state(await self._redis.mget(keys))
        if redis_state.roles_revision is None:
            # Первый запрос создаёт общую ревизию. Опция ``NX`` записывает
            # значение только при отсутствии ключа, поэтому параллельные
            # запросы не могут перезаписать выбранную ревизию.
            await self._redis.set(
                self._keys.roles_revision,
                revision_candidate,
                nx=True,
            )
            redis_state = self._unpack_redis_state(await self._redis.mget(keys))
        return redis_state

    @staticmethod
    def _unpack_redis_state(
        values: list[bytes | str | None],
    ) -> _RedisAuthorizationState:
        # MGET возвращает значения в порядке переданных ключей. Здесь этот
        # позиционный контракт один раз превращается в именованные поля.
        (
            roles_revision,
            roles_mutation_marker,
            user_mutation_marker,
            snapshot_payload,
            user_generation,
        ) = values
        return _RedisAuthorizationState(
            roles_revision=roles_revision,
            roles_mutation_marker=roles_mutation_marker,
            user_mutation_marker=user_mutation_marker,
            snapshot_payload=snapshot_payload,
            user_generation=user_generation,
        )

    async def store_snapshot(
        self,
        user_id: UUID,
        snapshot: AuthorizationSnapshot,
        expected_roles_revision: str,
        *,
        expected_user_generation: str,
    ) -> None:
        now_utc = datetime.now(UTC)
        normalized_expiry: datetime | None = None
        if snapshot.nearest_expires_at is not None:
            normalized_expiry = (
                snapshot.nearest_expires_at.astimezone(UTC)
                if snapshot.nearest_expires_at.tzinfo is not None
                else snapshot.nearest_expires_at.replace(tzinfo=UTC)
            )
            if normalized_expiry <= now_utc:
                return

        snapshot_json = _StoredAuthorizationSnapshot(
            roles_revision=expected_roles_revision,
            is_superuser=snapshot.is_superuser,
            permissions=sorted(snapshot.permissions),
            nearest_expires_at=(
                normalized_expiry.isoformat() if normalized_expiry is not None else None
            ),
        ).model_dump_json()
        effective_ttl = compute_effective_snapshot_ttl(
            self._ttl_seconds,
            normalized_expiry,
            now_utc=now_utc,
        )
        await self._store_snapshot(
            user_id,
            expected_roles_revision,
            expected_user_generation,
            snapshot_json,
            effective_ttl,
        )

    @on_redis_exception()
    async def _store_snapshot(
        self,
        user_id: UUID,
        expected_roles_revision: str,
        expected_user_generation: str,
        snapshot_json: str,
        effective_ttl: int,
    ) -> None:
        await self._set_snapshot_script(
            keys=(
                self._keys.roles_revision,
                self._keys.roles_mutation_marker,
                self._keys.user_mutation_marker(user_id),
                self._keys.user_snapshot(user_id),
                self._keys.user_generation(user_id),
            ),
            args=(
                expected_roles_revision,
                snapshot_json,
                effective_ttl,
                expected_user_generation,
            ),
        )

    @staticmethod
    def _decode_redis_text(value: bytes | str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return value
