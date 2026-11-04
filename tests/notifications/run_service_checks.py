"""Run a complete service suite against UUID-owned real integration stores."""

import argparse
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from stack_endpoints import stack_endpoints


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "service",
        choices=["auth", "billing", "ugc-api", "movies-etl", "notifications"],
    )
    parser.add_argument("pytest_args", nargs="*")
    args = parser.parse_args()
    e = stack_endpoints()
    name = "notifications_test_" + uuid4().hex
    env = {**os.environ, **e, "POSTGRES_DB": name, "MOVIES_PUBLICATION_INTEGRATION": "1"}
    if os.environ.get("SERVICE_TEST_PYTHON"):
        env["UV_PYTHON"] = os.environ["SERVICE_TEST_PYTHON"]
    env.pop("VIRTUAL_ENV", None)
    root = Path(__file__).resolve().parents[2]
    with psycopg.connect(
        host=e["POSTGRES_HOST"],
        port=e["POSTGRES_PORT"],
        user=e["POSTGRES_USER"],
        password=e["POSTGRES_PASSWORD"],
        dbname="notifications_test_admin",
        autocommit=True,
    ) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "--directory",
                    str(root / "services" / args.service),
                    "--all-groups",
                    "pytest",
                    "-o",
                    "addopts=",
                    *args.pytest_args,
                ],
                env=env,
            )
            raise SystemExit(result.returncode)
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


if __name__ == "__main__":
    main()
