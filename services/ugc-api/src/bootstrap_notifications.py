"""CLI: capture → enqueue → status (JSON manifest для Notifications)."""

import argparse
import asyncio
import json

from core.config import settings
from events.bootstrap_store import capture_reviews, enqueue_reviews, load_status
from events.runtime import mongo_runtime


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['capture', 'enqueue', 'status'])
    parser.add_argument('--load-id', required=True)
    parser.add_argument('--limit', type=int, default=settings.bootstrap_batch_size)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error('--limit must be positive')
    async with mongo_runtime():
        if args.phase == 'capture':
            print(
                json.dumps(
                    {'load_id': args.load_id, 'captured': await capture_reviews(args.load_id)}
                )
            )
        elif args.phase == 'enqueue':
            count = 0
            while batch := await enqueue_reviews(args.load_id, args.limit):
                count += batch
            print(json.dumps({'load_id': args.load_id, 'enqueued': count}))
        else:
            print(json.dumps(await load_status(args.load_id), sort_keys=True))


if __name__ == '__main__':
    asyncio.run(main())
