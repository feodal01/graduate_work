from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    name: str
    rate: int
    window_seconds: int
    capacity: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError('Rate-limit policy name must not be blank')
        for field_name in ('rate', 'window_seconds', 'capacity'):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(
                    f'Rate-limit policy {field_name} must be a positive integer'
                )


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: int


class RateLimiter(Protocol):
    async def check(
        self,
        *,
        policy: RateLimitPolicy,
        identity: str,
    ) -> RateLimitDecision: ...
