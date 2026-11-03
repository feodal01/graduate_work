"""Register immutable source manifests and verify permanent local receipts."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.db.models import BootstrapReceipt, LoadState
from notifications.db.session import get_database, transaction


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    load_id: str
    expected_count: int = Field(ge=0)
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


async def lock_load(session: AsyncSession, load_id: str) -> None:
    # Also works before the load is registered or its first receipt exists.
    await session.execute(
        select(
            func.pg_advisory_xact_lock(
                func.hashtextextended("notifications:bootstrap:" + load_id, 0)
            )
        )
    )


async def register_load(load_id: str, expected_count: int, manifest_hash: str) -> None:
    manifest = Manifest(load_id=load_id, expected_count=expected_count, manifest_hash=manifest_hash)
    async with transaction() as session:
        await lock_load(session, load_id)
        saved = await session.get(LoadState, load_id)
        if saved is not None:
            if (saved.expected_count, saved.manifest_hash) != (expected_count, manifest_hash):
                raise ValueError("load_id already registered with a different manifest")
            return
        session.add(LoadState(**manifest.model_dump()))


async def finalize_load(load_id: str) -> bool:
    async with transaction() as session:
        await lock_load(session, load_id)
        state = await session.get(LoadState, load_id)
        if state is None:
            raise ValueError("load_id must be registered before finalization")
        digest = hashlib.sha256()
        count = 0
        applied = True
        rows = await session.stream(
            select(
                BootstrapReceipt.entity_id, BootstrapReceipt.event_id, BootstrapReceipt.applied_at
            )
            .where(BootstrapReceipt.load_id == load_id)
            .order_by(BootstrapReceipt.entity_id)
            .execution_options(yield_per=1000)
        )
        async for entity_id, event_id, applied_at in rows:
            count += 1
            applied = applied and applied_at is not None
            digest.update(f"{entity_id},{event_id}\n".encode())
        if (
            not applied
            or count != state.expected_count
            or digest.hexdigest() != state.manifest_hash
        ):
            return False
        if state.completed_at is None:
            state.completed_at = await session.scalar(select(func.now()))
        return True


async def _run(args: argparse.Namespace) -> int:
    try:
        if args.command == "register":
            manifest = Manifest.model_validate_json(Path(args.manifest).read_text())
            await register_load(manifest.load_id, manifest.expected_count, manifest.manifest_hash)
            print(json.dumps({"load_id": manifest.load_id, "registered": True}))
            return 0
        completed = await finalize_load(args.load_id)
        print(json.dumps({"load_id": args.load_id, "completed": completed}))
        return 0 if completed else 1
    finally:
        if get_database.cache_info().currsize:
            await get_database().close()
            get_database.cache_clear()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("register")
    register.add_argument("--manifest", required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--load-id", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(_run(args))
    except (ValueError, OSError) as error:
        parser.error(str(error))
    else:
        raise SystemExit(result)


if __name__ == "__main__":
    main()
