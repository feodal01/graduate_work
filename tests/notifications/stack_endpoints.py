"""Discover integration stack endpoints using only the standard library."""

import json
import os
import subprocess
from pathlib import Path

COMPOSE_FILE = Path(__file__).with_name("compose.yaml")
COMPOSE_PROJECT = "notifications-integration"


def compose(*arguments):
    return subprocess.run(
        ["docker", "compose", "-p", COMPOSE_PROJECT, "-f", str(COMPOSE_FILE), *arguments],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()


def published_port(service, container_port):
    """Resolve only a loopback binding in the explicitly named test project."""
    address = compose("port", service, str(container_port))
    host, port = address.rsplit(":", 1)
    if host != "127.0.0.1":
        raise RuntimeError(f"Unsafe test endpoint: {address}")
    return int(port)


def stack_endpoints():
    """Public helper: discover ports without creating or deleting any data."""
    return {
        "REDIS_HOST": "127.0.0.1",
        "REDIS_PORT": str(published_port("redis", 6379)),
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": str(published_port("postgres", 5432)),
        "POSTGRES_USER": "notifications_test",
        "POSTGRES_PASSWORD": "notifications_test",
        "MONGO_URI": (
            f"mongodb://127.0.0.1:{published_port('mongo', 27017)}/"
            "?replicaSet=notifications-test-rs&directConnection=true"
        ),
        "KAFKA_BOOTSTRAP_SERVERS": (
            "127.0.0.1:"
            f"{published_port('kafka', int(os.environ.get('NOTIFICATIONS_KAFKA_PORT', '19094')))}"
        ),
        "ELASTICSEARCH_URL": f"http://127.0.0.1:{published_port('elasticsearch', 9200)}",
        "RABBITMQ_URL": (
            "amqp://notifications_test:notifications_test@127.0.0.1:"
            f"{published_port('rabbitmq', 5672)}/notifications_test"
        ),
        "SMTP_HOST": "127.0.0.1",
        "SMTP_PORT": str(published_port("smtp", 1025)),
        "SMTP_API_URL": f"http://127.0.0.1:{published_port('smtp', 8025)}",
    }


if __name__ == "__main__":
    print(json.dumps(stack_endpoints(), indent=2))
