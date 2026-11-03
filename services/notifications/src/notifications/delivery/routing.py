"""Stable delivery-kind routing and optionally isolated RabbitMQ names."""

from dataclasses import dataclass
from typing import Literal

SendClass = Literal["transactional", "service", "bulk"]
SEND_CLASSES: tuple[SendClass, ...] = ("transactional", "service", "bulk")


def class_for_kind(kind: str) -> SendClass:
    match kind:
        case "welcome":
            return "transactional"
        case "likes":
            return "service"
        case "new_films":
            return "bulk"
        case _:
            raise ValueError(f"Unknown delivery kind: {kind}")


@dataclass(frozen=True)
class Topology:
    namespace: str = ""

    def name(self, production_name: str) -> str:
        return f"{self.namespace}.{production_name}" if self.namespace else production_name

    @property
    def exchange(self) -> str:
        return self.name("notifications.email")

    def queue(self, send_class: SendClass) -> str:
        if send_class not in SEND_CLASSES:
            raise ValueError(f"Unknown send class: {send_class}")
        return self.name(f"email.{send_class}")

    @property
    def dead_exchange(self) -> str:
        return self.name("notifications.delivery.dlx")

    @property
    def dead_queue(self) -> str:
        return self.name("notifications.delivery.dead")

    @property
    def dead_routing_key(self) -> str:
        return "notifications.delivery"
