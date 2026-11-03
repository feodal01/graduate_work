"""SMTP acceptance is DATA's positive response, not successful connection or QUIT."""

import asyncio
from email.errors import NonASCIILocalPartDefect
from email.headerregistry import Address, AddressHeader, HeaderRegistry
from email.message import EmailMessage
from email.policy import SMTP as SMTP_POLICY
from typing import Any

import idna
from aiosmtplib import SMTP
from aiosmtplib.errors import SMTPException, SMTPResponseException

from notifications.config import Settings


class DeliveryError(Exception):
    def __init__(self, message: str, *, permanent: bool = False, uncertain: bool = False):
        super().__init__(message)
        self.permanent = permanent
        self.uncertain = uncertain


def mailbox(value: object) -> Address:
    if not isinstance(value, str) or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Invalid mailbox characters")
    header = HeaderRegistry()("To", value)
    assert isinstance(header, AddressHeader)
    if len(header.addresses) != 1 or any(
        not isinstance(defect, NonASCIILocalPartDefect) for defect in header.defects
    ):
        raise ValueError("Expected one valid mailbox")
    address = header.addresses[0]
    if not address.username or not address.domain:
        raise ValueError("Mailbox requires local part and domain")
    return Address(
        display_name=address.display_name,
        username=address.username,
        domain=idna.encode(address.domain, uts46=True).decode("ascii"),
    )


async def transmit(
    settings: Settings, payload: dict[str, Any], message_id: str, timeout: float
) -> None:
    try:
        sender, recipient = mailbox(payload["from"]), mailbox(payload["to"])
        utf8_required = not (sender.addr_spec + recipient.addr_spec).isascii()
        encoding = "utf-8" if utf8_required else "ascii"
        # Validate and serialize before DATA: local failures cannot imply acceptance.
        sender.addr_spec.encode(encoding)
        recipient.addr_spec.encode(encoding)
        message = EmailMessage(policy=SMTP_POLICY.clone(utf8=utf8_required))
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = payload["subject"]
        message["Message-ID"] = message_id
        # A 7-bit body works even when the server does not advertise 8BITMIME.
        message.set_content(
            payload["html"], subtype="html", charset="utf-8", cte="quoted-printable"
        )
        wire_message = message.as_bytes()
    except (ValueError, TypeError) as exc:
        raise DeliveryError("Invalid mail envelope or headers", permanent=True) from exc
    client = SMTP(
        hostname=settings.smtp_host,
        port=settings.smtp_port,
        start_tls=settings.smtp_starttls,
        timeout=timeout,
    )
    data_started = False
    try:
        async with asyncio.timeout(timeout):
            await client.connect()
            if settings.smtp_username and settings.smtp_password:
                await client.login(
                    settings.smtp_username, settings.smtp_password.get_secret_value()
                )
            await client.ehlo()
            if utf8_required and not client.supports_extension("SMTPUTF8"):
                raise DeliveryError("SMTP server does not support SMTPUTF8", permanent=True)
            await client.mail(
                sender.addr_spec, options=["SMTPUTF8"] if utf8_required else [], encoding=encoding
            )
            await client.rcpt(recipient.addr_spec, encoding=encoding)
            data_started = True
            await client.data(wire_message)
    except SMTPResponseException as exc:
        # An explicit negative reply proves rejection, even after DATA.
        raise DeliveryError(
            f"SMTP rejected command: {exc.code}", permanent=500 <= exc.code < 600
        ) from exc
    except (SMTPException, OSError, TimeoutError) as exc:
        raise DeliveryError(type(exc).__name__, uncertain=data_started) from exc
    finally:
        # QUIT failure must never turn accepted DATA into a retry.
        client.close()
