"""Operational transaction prerequisite on every shard, through real mongos."""

import json
from uuid import uuid4

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

from conftest import compose, published_port


def test_every_shard_lifetime_and_router_transaction():
    for shard in ("shard-one", "shard-two"):
        actual = json.loads(
            compose(
                "exec",
                "-T",
                shard,
                "mongosh",
                "--quiet",
                "--eval",
                "print(JSON.stringify(db.adminCommand({getParameter:1, "
                "transactionLifetimeLimitSeconds:1})))",
            )
        )
        assert actual["transactionLifetimeLimitSeconds"] >= 60
    with MongoClient(f"mongodb://127.0.0.1:{published_port('shard-router', 27017)}") as client:
        assert client.admin.command("hello")["msg"] == "isdbgrid"
        names = ["notifications_test_" + uuid4().hex for _ in range(2)]
        try:
            for name, shard in zip(names, ("test-shard-one", "test-shard-two"), strict=True):
                client.admin.command("enableSharding", name, primaryShard=shard)
                client[name].create_collection("probe")
                actual = client.config.databases.find_one({"_id": name})
                assert actual["primary"] == shard
            with client.start_session() as session:
                with session.start_transaction(write_concern=WriteConcern("majority")):
                    for name in names:
                        client[name].probe.insert_one({"_id": "committed"}, session=session)
                session.start_transaction()
                for name in names:
                    client[name].probe.insert_one({"_id": "aborted"}, session=session)
                session.abort_transaction()
            for name in names:
                assert list(client[name].probe.find()) == [{"_id": "committed"}]
        finally:
            for name in names:
                client.drop_database(name)
