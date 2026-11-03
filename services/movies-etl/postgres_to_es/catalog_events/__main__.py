"""Separate detector/relay processes. No HTTP server is started."""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

from postgres_to_es.catalog_events import bootstrap, store
from postgres_to_es.catalog_events.detector import scan_catalog
from postgres_to_es.catalog_events.relay import publish_batch
from postgres_to_es.publication import migrate_publication_state
from postgres_to_es.settings import settings

logger = logging.getLogger(__name__)


def detect() -> None:
    interval = settings.catalog_scan_interval_seconds
    reconciliation = settings.catalog_reconcile_interval_seconds
    if interval is None or reconciliation is None or settings.catalog_overlap_seconds is None:
        raise ValueError(
            "Detector requires CATALOG_SCAN_INTERVAL_SECONDS, "
            "CATALOG_OVERLAP_SECONDS and CATALOG_RECONCILE_INTERVAL_SECONDS"
        )
    if not store.readiness()["bootstrap_completed_at"]:
        raise RuntimeError("Historical bootstrap must complete before detector startup")
    while True:
        try:
            now = datetime.now(timezone.utc)
            last = store.readiness()["last_reconciled_at"]
            scan_catalog(
                now, reconcile=last is None or (now - last).total_seconds() >= reconciliation
            )
        except Exception:
            logger.exception("Catalog scan failed; retrying durable window")
        time.sleep(interval)


def relay() -> None:
    if not settings.kafka_bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required for relay")
    while True:
        try:
            if publish_batch():
                continue
        except Exception:
            logger.exception("Catalog relay failed; unmarked messages remain pending")
        time.sleep(settings.catalog_relay_pause_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("detect", "relay", "migrate"):
        commands.add_parser(command)
    initial = commands.add_parser("bootstrap")
    actions = initial.add_subparsers(dest="action", required=True)
    for action in ("capture", "enqueue", "status"):
        operation = actions.add_parser(action)
        operation.add_argument("--load-id", required=True)
        if action == "enqueue":
            operation.add_argument("--limit", type=int, default=settings.catalog_batch_size)
    args = parser.parse_args()
    logging.basicConfig(level=settings.log_level)
    migrate_publication_state()
    if args.command == "detect":
        detect()
    elif args.command == "relay":
        relay()
    elif args.command == "bootstrap":
        if args.action == "capture":
            bootstrap.capture_catalog(args.load_id)
        elif args.action == "enqueue":
            bootstrap.enqueue_catalog(args.load_id, args.limit)
        print(json.dumps(bootstrap.catalog_status(args.load_id), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
