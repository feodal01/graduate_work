"""Host connections for the standalone test Compose project.

Start with docker compose -f tests/notifications/compose.yaml up -d --wait.
Each fixture run creates UUID-named databases and only drops those exact names.
notification_service_env exposes the same mapping for subprocess environments.
"""

import json
import os
import smtplib
from urllib.request import urlopen
from uuid import uuid4

import pika
import psycopg
import pytest
from psycopg import sql
from pymongo import MongoClient

from stack_endpoints import compose as compose
from stack_endpoints import published_port as published_port
from stack_endpoints import stack_endpoints


def drop_test_postgres_database(connection, name):
    if (
        not name.startswith("notifications_test_")
        or len(name.removeprefix("notifications_test_")) != 32
    ):
        raise ValueError(f"Refusing to drop a database outside the test namespace: {name}")
    connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture(scope="session")
def notification_stack():
    endpoints = stack_endpoints()
    name = f"notifications_test_{uuid4().hex}"
    endpoints.update(POSTGRES_DB=name, MONGO_DB=name)
    postgres = psycopg.connect(
        host=endpoints["POSTGRES_HOST"],
        port=endpoints["POSTGRES_PORT"],
        user=endpoints["POSTGRES_USER"],
        password=endpoints["POSTGRES_PASSWORD"],
        dbname="notifications_test_admin",
        autocommit=True,
        connect_timeout=5,
    )
    mongo = MongoClient(endpoints["MONGO_URI"], serverSelectionTimeoutMS=5000)
    created = False
    try:
        postgres.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        endpoints["DATABASE_URL"] = (
            f"postgresql://notifications_test:notifications_test@127.0.0.1:{endpoints['POSTGRES_PORT']}/{name}"
        )
        with psycopg.connect(endpoints["DATABASE_URL"]) as database:
            assert database.execute("SELECT current_database()").fetchone()[0] == name
        collection = mongo[name].transaction_probe
        collection.insert_one({"_id": "initial"})
        with mongo.start_session() as session:
            with session.start_transaction():
                collection.insert_one({"_id": "committed"}, session=session)
            assert collection.find_one({"_id": "committed"}) is not None
            session.start_transaction()
            collection.insert_one({"_id": "aborted"}, session=session)
            session.abort_transaction()
            assert collection.find_one({"_id": "aborted"}) is None
        collection.drop()
        compose(
            "exec", "-T", "kafka", "kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"
        )
        with urlopen(f"{endpoints['ELASTICSEARCH_URL']}/_cluster/health", timeout=5) as response:
            assert json.load(response)["status"] in {"green", "yellow"}
        rabbit = pika.BlockingConnection(pika.URLParameters(endpoints["RABBITMQ_URL"]))
        try:
            channel = rabbit.channel()
            queue = channel.queue_declare(queue="", exclusive=True).method.queue
            channel.basic_publish(exchange="", routing_key=queue, body=b"notifications-stack-probe")
            method, _, body = channel.basic_get(queue, auto_ack=True)
            assert method is not None and body == b"notifications-stack-probe"
        finally:
            rabbit.close()
        with smtplib.SMTP(endpoints["SMTP_HOST"], int(endpoints["SMTP_PORT"]), timeout=5) as smtp:
            assert smtp.noop()[0] == 250
        yield endpoints
    finally:
        try:
            mongo.drop_database(name)
        finally:
            mongo.close()
            try:
                if created:
                    drop_test_postgres_database(postgres, name)
            finally:
                postgres.close()


@pytest.fixture
def notification_service_env(notification_stack):
    """Pass to subprocess.run(..., env=notification_service_env)."""
    return {**os.environ, **notification_stack}


if __name__ == "__main__":
    print(json.dumps(stack_endpoints(), indent=2))
