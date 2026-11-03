from uuid import UUID

from redis.asyncio.client import Pipeline

from db.cache.session_state import SessionKeyspace


class RedisAccessTokenDenylist:
    def __init__(self, *, keys: SessionKeyspace) -> None:
        self._keys = keys

    def queue_add(
        self,
        pipeline: Pipeline,
        user_id: UUID,
        token_id: UUID,
        ttl: int,
    ) -> None:
        pipeline.set(
            self._keys.revoked_access(user_id, token_id),
            '1',
            ex=ttl,
        )
