"""Real signals through main/run/consume with PG ownership and controlled SMTP."""

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from test_delivery import delivery_limiter as delivery_limiter
from test_delivery import endpoints as endpoints
from test_delivery import expire
from test_delivery import limiter_settings as limiter_settings
from test_delivery import setup as setup

from notifications.db.models import Delivery, DeliveryAttempt


@pytest.mark.parametrize(
    "signum,forced,reconnect",
    [
        (signal.SIGTERM, False, False),
        (signal.SIGINT, False, False),
        (signal.SIGTERM, True, False),
        pytest.param(signal.SIGTERM, False, True, id="iterator-end-reconnect"),
    ],
)
async def test_production_signal_drains_and_releases_ownership(
    setup, tmp_path, signum, forced, reconnect
):
    db, worker, did, _, _ = setup
    # A welcome avoids coupling process lifecycle coverage to local daytime.
    async with db.transaction() as session:
        await session.execute(
            update(Delivery).where(Delivery.delivery_id == did).values(kind="welcome")
        )
    worker.send_class = "transactional"
    assert await worker.prepare_delivery(did) is not None
    (tmp_path / "settings.json").write_text(worker.settings.model_dump_json())
    log = (tmp_path / "child.log").open("w")
    child = subprocess.Popen(
        [sys.executable, __file__, str(tmp_path), str(did), str(int(forced)), str(int(reconnect))],
        stdout=log,
        stderr=subprocess.STDOUT,
    )

    async def marker(name):
        async with asyncio.timeout(10):
            while not (tmp_path / name).exists():
                assert child.poll() is None, (tmp_path / "child.log").read_text()
                await asyncio.sleep(0.02)

    try:
        await marker("smtp-started")
        if reconnect:
            assert (tmp_path / "reconnected").exists()
        async with worker.ownership(did) as connection:
            assert connection is None
        os.kill(child.pid, signum)
        await marker("intake-closed")
        if not forced:
            assert child.poll() is None
            (tmp_path / "release").touch()
        await asyncio.to_thread(child.wait, timeout=10)
        assert child.returncode == 0, (tmp_path / "child.log").read_text()
        for name in (
            "smtp-closed",
            "broker-closed",
            "limiter-closed",
            "ownership-released",
            "database-closed",
        ):
            assert (tmp_path / name).exists(), name
        assert (tmp_path / "acknowledged").exists() is (not forced)
        async with worker.ownership(did) as connection:
            assert connection is not None
        async with db.transaction() as session:
            attempt = await session.scalar(select(DeliveryAttempt))
            assert attempt.outcome == ("started" if forced else "accepted")
        if forced:
            await expire(db, did)
            worker.send_class = "transactional"
            await worker.recover()
            async with db.transaction() as session:
                attempt = await session.scalar(select(DeliveryAttempt))
                assert attempt.outcome == "uncertain" and attempt.duplicate_risk
    finally:
        if child.poll() is None:
            child.kill()
            await asyncio.to_thread(child.wait, timeout=5)
        log.close()


def child_main():
    import notifications.delivery.worker as module
    from notifications.config import Settings, WorkerSettings
    from notifications.delivery.rate_limit import PermitDecision

    directory, did, forced, reconnect = sys.argv[1:]
    directory = Path(directory)

    def mark(name):
        (directory / name).touch()

    original_database = module.Database

    class Database(original_database):
        async def close(self):
            # Check release while this process and its original DB pool are alive.
            key = int.from_bytes(UUID(did).bytes[:8], "big", signed=True)
            probe = original_database(
                Settings.model_validate_json((directory / "settings.json").read_text()).database_url
            )
            try:
                async with probe.engine.connect() as connection:
                    assert await connection.scalar(
                        text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
                    )
                    await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                    await connection.commit()
            finally:
                await probe.close()
            mark("ownership-released")
            await super().close()
            mark("database-closed")

    class Limiter:
        def __init__(self, settings):
            pass

        async def acquire(self, send_class):
            return PermitDecision(True, 0, "probe")

        async def aclose(self):
            mark("limiter-closed")

    class Messages:
        def __init__(self):
            self.sent = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            mark("intake-closed")

        def __aiter__(self):
            return self

        async def __anext__(self):
            if reconnect == "1" and not (directory / "iterator-ended").exists():
                # Robust queue restoration can end iteration normally. A fresh
                # Broker/consume must start without waiting for a stop signal.
                mark("iterator-ended")
                raise StopAsyncIteration
            if self.sent:
                await asyncio.Event().wait()
            self.sent = True

            async def acknowledged(*args, **kwargs):
                mark("acknowledged")

            return SimpleNamespace(
                body=did.encode(), ack=acknowledged, nack=acknowledged, reject=acknowledged
            )

    class Broker:
        def __init__(self, *args):
            self.queues = {"transactional": SimpleNamespace(iterator=Messages)}
            self.channel = self

        async def set_qos(self, **kwargs):
            pass

        async def __aenter__(self):
            if (directory / "iterator-ended").exists():
                mark("reconnected")
            return self

        async def __aexit__(self, *args):
            mark("broker-closed")

    async def transmit(*args):
        mark("smtp-started")
        try:
            while not (directory / "release").exists():
                await asyncio.sleep(0.02)
        finally:
            mark("smtp-closed")

    module.WorkerSettings = lambda **values: WorkerSettings(
        **{
            **Settings.model_validate_json((directory / "settings.json").read_text()).model_dump(),
            **values,
        }
    )
    module.LimiterSettings = lambda: SimpleNamespace(unavailable_retry_seconds=1)
    module.RedisRateLimiter = Limiter
    module.Database = Database
    module.Broker = Broker
    module.transmit = transmit
    sys.argv = [
        "worker",
        "--send-class",
        "transactional",
        "--max-concurrency",
        "1",
        "--retry-seconds",
        "1",
        "--max-attempts",
        "3",
    ]
    if forced == "1":
        sys.argv += ["--shutdown-timeout", "0.2"]
    module.main()


if __name__ == "__main__":
    child_main()
