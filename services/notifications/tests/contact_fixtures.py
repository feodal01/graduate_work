"""Auth-owned contact data for existing database/SMTP scenarios."""

from uuid import uuid4

from notifications.clients.auth_contacts import ActiveContact, UnavailableContact
from notifications.db.models import Profile

CONTACTS = {}


def contact_profile(**values):
    user_id = values.setdefault("user_id", uuid4())
    contacts = {
        name: values.pop(name, None) for name in ("email", "first_name", "last_name", "locale")
    }
    CONTACTS[user_id] = ActiveContact(user_id=user_id, status="active", **contacts)
    return Profile(**values)


def contact(user_id):
    return CONTACTS.get(user_id, UnavailableContact(user_id=user_id, status="unavailable"))
