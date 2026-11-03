"""Crash only a disposable Redis launched with the production service command."""

import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]


def docker(*args):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, timeout=45
    ).stdout.strip()


def test_acknowledged_limiter_debit_survives_process_kill():
    config = json.loads(
        docker(
            "compose",
            "--project-directory",
            str(ROOT),
            "--env-file",
            str(ROOT / ".env.example"),
            "config",
            "--format",
            "json",
        )
    )["services"]["notifications-redis"]
    name = "notifications-durability-" + uuid4().hex
    volume = name + "-data"
    key = "notifications:smtp:{" + uuid4().hex + "}:limiter"
    script = (ROOT / "services/notifications/src/notifications/delivery/rate_limit.lua").read_text()
    # Capacity one with a 1,000-second refill exposes any restored old balance.
    parameters = [
        "durability-probe",
        "transactional",
        "1",
        "0.003",
        "3",
        "0.001",
        "1",
        "0.001",
        "1",
        "0.001",
        "1",
        "0.01",
    ]

    def redis(*args):
        return json.loads(docker("exec", name, "redis-cli", "--json", *map(str, args)))

    def ready():
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if redis("PING") == "PONG":
                    return
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                pass
            time.sleep(0.05)
        raise AssertionError("Disposable Redis did not become ready")

    docker("volume", "create", volume)
    try:
        docker(
            "run",
            "-d",
            "--name",
            name,
            "--mount",
            f"type=volume,src={volume},dst=/data",
            config["image"],
            *config["command"],
        )
        ready()
        effective = redis(
            "CONFIG",
            "GET",
            "appendonly",
            "appendfsync",
            "no-appendfsync-on-rewrite",
            "maxmemory-policy",
        )
        assert effective == {
            "appendonly": "yes",
            "appendfsync": "always",
            "no-appendfsync-on-rewrite": "no",
            "maxmemory-policy": "noeviction",
        }
        assert redis("EVAL", script, 1, key, "initialize", *parameters)[0] == 1
        seconds, micros = redis("TIME")
        now = int(seconds) + int(micros) / 1_000_000
        fields = ["ready_at", "0"]
        for bucket, capacity in (("total", 3), ("transactional", 1), ("service", 1), ("bulk", 1)):
            fields += [bucket + "_tokens", capacity, bucket + "_updated_at", now]
        redis("HSET", key, *fields)
        assert redis("EVAL", script, 1, key, "acquire", *parameters)[0] == 1
        debited = redis("HGETALL", key)
        assert float(debited["transactional_tokens"]) == 0
        docker("kill", "--signal", "KILL", name)
        docker("start", name)
        ready()
        assert redis("HGETALL", key) == debited
        denied = redis("EVAL", script, 1, key, "acquire", *parameters)
        assert denied[0] == 0 and float(denied[1]) > 900
        assert redis("HGETALL", key) == debited
        print(
            "production_redis_config=always/noeviction; "
            "acknowledged_debit_survived_SIGKILL=true; no_reinitialize=true"
        )
    finally:
        # Exact UUID-owned container and volume only; never touch shared services.
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
        docker("volume", "rm", volume)
