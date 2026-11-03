"""Real Redis tests mutate only a UUID account hash, never shared data."""

import asyncio
import os
import socket
import subprocess
import sys
from collections.abc import Awaitable
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from pydantic import SecretStr, ValidationError

from notifications.delivery.limiter_config import LimiterSettings
from notifications.delivery.rate_limit import RedisRateLimiter, initialize_limiter

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def settings():
    port = subprocess.check_output(
        [
            "docker",
            "compose",
            "-p",
            "notifications-integration",
            "-f",
            str(ROOT / "tests/notifications/compose.yaml"),
            "port",
            "redis",
            "6379",
        ],
        text=True,
    ).strip()
    assert port.startswith("127.0.0.1:")
    return LimiterSettings(
        redis_url=SecretStr(f"redis://{port}/0"),
        account_id=uuid4().hex,
        total_rate=0.003,
        total_capacity=3,
        transactional_rate=0.001,
        transactional_capacity=1,
        service_rate=0.001,
        service_capacity=1,
        bulk_rate=0.001,
        bulk_capacity=1,
        unavailable_retry_seconds=7,
        redis_timeout_seconds=0.1,
    )


@pytest.fixture
async def state(settings):
    from redis.asyncio import Redis

    client = Redis.from_url(settings.redis_url.get_secret_value(), decode_responses=True)
    limiter = RedisRateLimiter(settings)
    try:
        yield client, limiter
    finally:
        await client.delete(settings.key)
        await limiter.aclose()
        await client.aclose()


async def seed(settings, client, **overrides):
    assert await initialize_limiter(settings, cooldown_seconds=0.01)
    seconds, micros = await client.time()
    now = seconds + micros / 1_000_000
    fields = {"ready_at": 0}
    for bucket in ("total", "transactional", "service", "bulk"):
        fields[f"{bucket}_tokens"] = getattr(settings, f"{bucket}_capacity")
        fields[f"{bucket}_updated_at"] = now
    fields.update(overrides)
    await client.hset(settings.key, mapping=fields)


async def test_two_clients_cannot_spend_one_token_twice(settings, state):
    client, limiter = state
    await seed(settings, client, total_tokens=1)
    other = RedisRateLimiter(settings)
    try:
        results = await asyncio.gather(
            *[(limiter if i % 2 else other).acquire("transactional") for i in range(20)]
        )
        assert sum(result.allowed for result in results) == 1
    finally:
        await other.aclose()


async def test_bulk_cannot_borrow_transactional_share(settings, state):
    client, limiter = state
    await seed(settings, client)
    assert (await limiter.acquire("bulk")).allowed
    denied = await limiter.acquire("bulk")
    assert not denied.allowed and denied.retry_after_seconds > 0
    assert (await limiter.acquire("transactional")).allowed


@pytest.mark.parametrize("empty_bucket", ["total", "bulk"])
async def test_denial_never_debits_other_bucket(settings, state, empty_bucket):
    client, limiter = state
    await seed(settings, client, **{f"{empty_bucket}_tokens": 0})
    before = await client.hgetall(settings.key)
    assert not (await limiter.acquire("bulk")).allowed
    assert await client.hgetall(settings.key) == before


async def test_missing_state_fails_closed_without_initializing(settings, state):
    client, limiter = state
    missing_state = await limiter.acquire("bulk")
    assert missing_state.allowed is False
    assert missing_state.retry_after_seconds == 7
    assert not await client.exists(settings.key)


async def test_initialize_zero_cooldown_and_repeat_do_not_reset(settings, state):
    client, limiter = state
    assert await initialize_limiter(settings, cooldown_seconds=10)
    before = await client.hgetall(settings.key)
    assert all(
        float(before[f"{name}_tokens"]) == 0
        for name in ("total", "transactional", "service", "bulk")
    )
    assert await client.ttl(settings.key) == -1
    denied = await limiter.acquire("bulk")
    assert not denied.allowed and denied.retry_after_seconds >= 1000
    assert not await initialize_limiter(settings, cooldown_seconds=20)
    assert await client.hgetall(settings.key) == before


async def test_refill_starts_after_cooldown(settings, state):
    client, _ = state
    fast = LimiterSettings(
        **{
            **settings.model_dump(),
            "total_rate": 300,
            "transactional_rate": 100,
            "service_rate": 100,
            "bulk_rate": 100,
        }
    )
    limiter = RedisRateLimiter(fast)
    try:
        assert await initialize_limiter(fast, cooldown_seconds=0.15)
        denied = await limiter.acquire("bulk")
        assert not denied.allowed and denied.retry_after_seconds > 0.1
        await asyncio.sleep(0.2)
        assert (await limiter.acquire("bulk")).allowed
    finally:
        await limiter.aclose()


@pytest.mark.parametrize(
    "damage",
    [
        "deleted",
        "field",
        "fingerprint",
        "nan",
        "infinity",
        "negative",
        "over_capacity",
        "future",
        "wrong_type",
        "ttl",
    ],
)
async def test_corruption_never_mutates_or_grants(settings, state, damage):
    client, limiter = state
    await seed(settings, client)
    if damage == "deleted":
        await client.delete(settings.key)
    elif damage == "field":
        await client.hdel(settings.key, "service_updated_at")
    elif damage == "wrong_type":
        await client.delete(settings.key)
        await client.set(settings.key, "invalid")
    elif damage == "ttl":
        await client.expire(settings.key, 100)
    else:
        updates = {
            "fingerprint": ("fingerprint", "wrong"),
            "nan": ("service_tokens", "nan"),
            "infinity": ("service_tokens", "inf"),
            "negative": ("service_tokens", -1),
            "over_capacity": ("service_tokens", 2),
            "future": ("service_updated_at", 99999999999),
        }
        field, value = updates[damage]
        await client.hset(settings.key, field, value)
    before = await client.dump(settings.key)
    denied = await limiter.acquire("bulk")
    assert not denied.allowed and denied.retry_after_seconds > 0
    assert await client.dump(settings.key) == before
    if damage != "deleted":
        assert not await initialize_limiter(settings, cooldown_seconds=10)
        assert await client.dump(settings.key) == before


async def test_different_config_and_account_are_isolated(settings, state):
    client, limiter = state
    await seed(settings, client)
    mismatch = RedisRateLimiter(LimiterSettings(**{**settings.model_dump(), "total_rate": 0.004}))
    other = RedisRateLimiter(
        LimiterSettings(**{**settings.model_dump(), "account_id": uuid4().hex})
    )
    try:
        assert not (await mismatch.acquire("bulk")).allowed
        assert not (await other.acquire("bulk")).allowed
        assert (await limiter.acquire("bulk")).allowed
    finally:
        await mismatch.aclose()
        await other.aclose()


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_rate", 0),
        ("bulk_rate", -1),
        ("service_rate", float("nan")),
        ("total_rate", float("inf")),
        ("bulk_rate", True),
        ("total_capacity", True),
        ("bulk_capacity", 1.5),
        ("bulk_capacity", 0),
        ("total_capacity", 2),
        ("total_rate", 0.002),
        ("redis_timeout_seconds", 0),
        ("unavailable_retry_seconds", False),
        ("unavailable_retry_seconds", float("inf")),
    ],
)
def test_settings_reject_unsafe_values(settings, field, value):
    with pytest.raises(ValidationError):
        LimiterSettings(**{**settings.model_dump(), field: value})


@pytest.mark.parametrize("cooldown", [0, -1, True, float("nan"), float("inf")])
async def test_initialize_requires_explicit_positive_finite_cooldown(settings, cooldown):
    with pytest.raises(ValueError):
        await initialize_limiter(settings, cooldown_seconds=cooldown)


async def test_connection_refused_returns_configured_pause(settings):
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        limiter = RedisRateLimiter(
            LimiterSettings(
                **{
                    **settings.model_dump(),
                    "redis_url": f"redis://127.0.0.1:{port}/0",
                }
            )
        )
        try:
            decision = await limiter.acquire("bulk")
            assert not decision.allowed and decision.retry_after_seconds == 7
        finally:
            await limiter.aclose()


async def test_socket_timeout_fails_closed(settings):
    connections = []

    async def blackhole(reader, writer):
        connections.append(writer)
        await reader.read()

    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    limiter = RedisRateLimiter(
        LimiterSettings(
            **{
                **settings.model_dump(),
                "redis_url": f"redis://127.0.0.1:{port}/0",
            }
        )
    )
    try:
        decision = await asyncio.wait_for(limiter.acquire("bulk"), 2)
        assert not decision.allowed and decision.retry_after_seconds == 7
    finally:
        await limiter.aclose()
        server.close()
        for writer in connections:
            writer.close()
            await writer.wait_closed()
        await server.wait_closed()


async def test_status_cli_is_read_only_and_exit_codes(settings, state):
    client, _ = state
    env = {
        **os.environ,
        **{
            "NOTIFICATIONS_LIMITER_" + key.upper(): str(
                value.get_secret_value() if key == "redis_url" else value
            )
            for key, value in settings.model_dump().items()
        },
    }

    async def cli(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "notifications.delivery.rate_limit",
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert settings.redis_url.get_secret_value().encode() not in stdout + stderr
        return process.returncode, stdout

    assert (await cli("status"))[0] == 1
    assert (await cli("initialize", "--cooldown-seconds", "10"))[0] == 0
    before = await client.hgetall(settings.key)
    assert (await cli("status"))[0] == 0
    assert (await cli("initialize", "--cooldown-seconds", "20"))[0] == 0
    assert await client.hgetall(settings.key) == before
    assert (await cli("initialize", "--cooldown-seconds", "0"))[0] == 2
    await client.hdel(settings.key, "service_tokens")
    damaged = await client.hgetall(settings.key)
    assert (await cli("status"))[0] == 1
    assert (await cli("initialize", "--cooldown-seconds", "10"))[0] == 1
    assert await client.hgetall(settings.key) == damaged


def test_decimal_rate_shares_are_compared_without_binary_rounding(settings):
    changed = {
        **settings.model_dump(),
        "total_rate": 0.3,
        "transactional_rate": 0.1,
        "service_rate": 0.1,
        "bulk_rate": 0.1,
    }
    assert LimiterSettings(**changed).total_rate == 0.3


def test_large_finite_rate_sum_is_validation_error(settings):
    with pytest.raises(ValidationError):
        LimiterSettings(
            **{
                **settings.model_dump(),
                "total_rate": 1e308,
                "transactional_rate": 1e308,
                "service_rate": 1e308,
            }
        )


@pytest.mark.parametrize(
    "field",
    [
        "schema",
        "ready_at",
        "total_tokens",
        "total_updated_at",
        "transactional_tokens",
        "transactional_updated_at",
        "service_tokens",
        "service_updated_at",
        "bulk_tokens",
        "bulk_updated_at",
    ],
)
async def test_every_persisted_field_is_validated_before_writes(settings, state, field):
    client, limiter = state
    await seed(settings, client)
    await client.hset(settings.key, field, "invalid")
    before = await client.hgetall(settings.key)
    assert not (await limiter.acquire("transactional")).allowed
    assert await client.hgetall(settings.key) == before


async def test_script_cache_flush_recovers_without_reset(settings):
    from redis.asyncio import Redis
    from redis.exceptions import ConnectionError

    name = "notifications-limiter-test-" + uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            "127.0.0.1::6379",
            "redis:7-alpine",
        ],
        check=True,
        capture_output=True,
    )
    client = limiter = None
    try:
        port = subprocess.check_output(["docker", "port", name, "6379"], text=True).strip()
        isolated = LimiterSettings(**{**settings.model_dump(), "redis_url": f"redis://{port}/0"})
        client = Redis.from_url(isolated.redis_url.get_secret_value(), decode_responses=True)
        for attempt in range(100):
            try:
                await client.ping()
                break
            except ConnectionError:
                if attempt == 99:
                    raise
                await asyncio.sleep(0.05)
        limiter = RedisRateLimiter(isolated)
        await seed(isolated, client)
        assert (await limiter.acquire("bulk")).allowed
        # Redis shares sync/async command stubs; this client always returns awaitables.
        before = await cast(Awaitable[dict[str, str]], client.hgetall(isolated.key))
        await client.script_flush()
        assert not (await limiter.acquire("bulk")).allowed
        after = await cast(Awaitable[dict[str, str]], client.hgetall(isolated.key))
        assert after == before
        assert (await limiter.acquire("transactional")).allowed
    finally:
        if limiter is not None:
            await limiter.aclose()
        if client is not None:
            await client.aclose()
        subprocess.run(["docker", "rm", "--force", name], check=True, capture_output=True)


async def test_unknown_result_does_not_refund_consumed_token(settings, state, monkeypatch):
    from redis.exceptions import TimeoutError

    client, limiter = state
    await seed(settings, client, total_tokens=1)
    script = limiter._script

    async def lost_reply(**kwargs):
        await script(**kwargs)
        raise TimeoutError("reply lost after Redis committed")

    monkeypatch.setattr(limiter, "_script", lost_reply)
    decision = await limiter.acquire("bulk")
    assert not decision.allowed and decision.retry_after_seconds == 7
    monkeypatch.setattr(limiter, "_script", script)
    assert not (await limiter.acquire("transactional")).allowed
    assert float(await client.hget(settings.key, "total_tokens")) < 1


async def test_concurrent_initialize_only_creates_state_once(settings, state):
    client, _ = state
    results = await asyncio.gather(
        *[initialize_limiter(settings, cooldown_seconds=10) for _ in range(10)]
    )
    assert sum(results) == 1
    assert float(await client.hget(settings.key, "bulk_tokens")) == 0


async def test_large_capacities_preserve_each_debit(settings, state):
    client, _ = state
    large = LimiterSettings(
        **{
            **settings.model_dump(),
            "total_capacity": 9007199254740991,
            "transactional_capacity": 9007199254740989,
        }
    )
    limiter = RedisRateLimiter(large)
    try:
        await seed(large, client)
        assert (await limiter.acquire("transactional")).allowed
        assert float(await client.hget(large.key, "total_tokens")) == 9007199254740990
        assert float(await client.hget(large.key, "transactional_tokens")) == 9007199254740988
    finally:
        await limiter.aclose()
