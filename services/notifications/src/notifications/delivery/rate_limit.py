"""Fail-closed shared Redis permits and explicit operator initialization."""

import argparse
import asyncio
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files
from typing import Protocol, cast

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from notifications.delivery.limiter_config import BUCKETS, LimiterSettings
from notifications.delivery.routing import SEND_CLASSES, SendClass


@dataclass(frozen=True)
class PermitDecision:
    allowed: bool
    retry_after_seconds: float
    reason: str


class Limiter(Protocol):
    async def acquire(self, send_class: SendClass) -> PermitDecision: ...


class RedisRateLimiter:
    def __init__(self, settings: LimiterSettings) -> None:
        self.settings = settings
        # redis leaves forwarded connection keyword arguments untyped.
        self._redis = Redis.from_url(  # pyright: ignore[reportUnknownMemberType]
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            socket_connect_timeout=settings.redis_timeout_seconds,
            socket_timeout=settings.redis_timeout_seconds,
            retry=Retry(NoBackoff(), 0),
        )
        self._script = self._redis.register_script(
            files("notifications.delivery").joinpath("rate_limit.lua").read_text()
        )

    async def _execute(
        self,
        mode: str,
        send_class: str = "",
        cooldown: float = 0,
    ) -> PermitDecision:
        args: list[str | float | int] = [
            mode,
            self.settings.fingerprint,
            send_class,
            self.settings.unavailable_retry_seconds,
        ]
        for name in BUCKETS:
            args.extend(
                [getattr(self.settings, f"{name}_rate"), getattr(self.settings, f"{name}_capacity")]
            )
        args.append(cooldown)
        try:
            # This Lua script returns [allowed, delay, reason]; redis types Lua results loosely.
            result = cast(
                Sequence[int | float | str],
                await self._script(keys=[self.settings.key], args=args),
            )
            allowed, delay, reason = result
            delay = float(delay)
            if allowed not in (0, 1) or not math.isfinite(delay) or delay < 0:
                raise ValueError("invalid limiter response")
            return PermitDecision(bool(allowed), delay, str(reason))
        except (RedisError, OSError, ValueError, TypeError):
            # An unknown response may have consumed a permit. Never refund or retry it.
            return PermitDecision(False, self.settings.unavailable_retry_seconds, "unavailable")

    async def acquire(self, send_class: SendClass) -> PermitDecision:
        if send_class not in SEND_CLASSES:
            raise ValueError("unknown send class")
        return await self._execute("acquire", send_class)

    async def initialize(self, *, cooldown_seconds: float) -> PermitDecision:
        """Create an absent limiter state after validating the operator's cooldown."""
        return await self._execute("initialize", cooldown=_cooldown(cooldown_seconds))

    async def status(self) -> PermitDecision:
        """Validate state without refilling or changing it; allowed means healthy."""
        return await self._execute("status")

    async def aclose(self) -> None:
        await self._redis.aclose()


def _cooldown(value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError("cooldown_seconds must be finite and positive")
    return value


async def initialize_limiter(settings: LimiterSettings, *, cooldown_seconds: float) -> bool:
    """Create only an absent hash with zero tokens and a future refill start."""
    cooldown = _cooldown(cooldown_seconds)
    limiter = RedisRateLimiter(settings)
    try:
        return (await limiter.initialize(cooldown_seconds=cooldown)).allowed
    finally:
        await limiter.aclose()


async def _run_cli(args: argparse.Namespace, settings: LimiterSettings) -> int:
    limiter = RedisRateLimiter(settings)
    try:
        if args.operation == "initialize":
            decision = await limiter.initialize(cooldown_seconds=args.cooldown_seconds)
            if decision.reason == "already_exists":
                status = await limiter.status()
                decision = PermitDecision(
                    status.allowed,
                    status.retry_after_seconds,
                    "already_exists" if status.allowed else status.reason,
                )
        else:
            decision = await limiter.status()
        print(
            json.dumps(
                {
                    "healthy": decision.allowed,
                    "reason": decision.reason,
                    "retry_after_seconds": decision.retry_after_seconds,
                }
            )
        )
        return 0 if decision.allowed else 1
    finally:
        await limiter.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_subparsers(dest="operation", required=True)
    initialize = operations.add_parser("initialize")
    initialize.add_argument("--cooldown-seconds", type=float, required=True)
    operations.add_parser("status")
    args = parser.parse_args()
    try:
        settings = LimiterSettings()  # type: ignore[call-arg]
        result = asyncio.run(_run_cli(args, settings))
    except (ValidationError, ValueError):
        # Pydantic exception rendering can include sensitive input URLs.
        parser.exit(2, "Invalid limiter settings or cooldown; check explicit configuration.\n")
    raise SystemExit(result)


if __name__ == "__main__":
    main()
