"""Register a new user through Auth and wait for their welcome email in local Mailpit."""

import json
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def main() -> None:
    """Exercise Auth → Kafka → Notifications → RabbitMQ → worker → SMTP."""
    run_id = uuid4().hex
    recipient = f"review-{run_id}@example.org"
    health = Request("http://auth:8000/api/v1/health/", headers={"X-Request-Id": run_id})
    ready_deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(health, timeout=5):
                break
        except (URLError, TimeoutError):
            if time.monotonic() >= ready_deadline:
                raise RuntimeError("Auth did not become ready within 60 seconds") from None
            time.sleep(1)
    request = Request(
        "http://auth:8000/api/v1/auth/signup",
        data=json.dumps(
            {
                "login": f"review-{run_id}",
                "password": f"Review-Cinema-{run_id}!",
                "email": recipient,
                "first_name": "Reviewer",
                "locale": "ru",
                "tz": "UTC",
            }
        ).encode(),
        headers={"Content-Type": "application/json", "X-Request-Id": run_id},
        method="POST",
    )
    with urlopen(request, timeout=15) as response:
        user = json.load(response)
    print(f"Registered user_id={user['id']}; recipient={recipient}", flush=True)

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        with urlopen("http://notifications-mailpit:8025/api/v1/messages", timeout=5) as response:
            messages = json.load(response)["messages"]
        for message in messages:
            if any(address["Address"] == recipient for address in message["To"]):
                print(f"PASS: welcome accepted by Mailpit; subject={message['Subject']}")
                return
        time.sleep(1)
    raise RuntimeError(f"Welcome email for {recipient} did not arrive within 180 seconds")


if __name__ == "__main__":
    main()
