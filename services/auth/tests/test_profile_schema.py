"""Contact input distinguishes omitted fields from explicit clearing."""

import pytest
from pydantic import ValidationError

from schemas.authentication import ProfileUpdateRequest, SignupRequest


def test_profile_explicit_null_clears_contacts_only() -> None:
    """Omitted contacts stay absent and null credentials cannot clear login/password."""
    assert ProfileUpdateRequest(email=None).model_dump(exclude_unset=True) == {
        'email': None
    }
    for value in (
        {},
        {'new_login': None},
        {'new_password': None},
        {'email': None, 'new_login': None},
    ):
        with pytest.raises(ValidationError):
            ProfileUpdateRequest.model_validate(value)


@pytest.mark.parametrize(
    'values',
    [
        {'email': 'invalid'},
        {'email': 'a b@example.org'},
        {'locale': ''},
        {'locale': 'not a language'},
        {'tz': 'Unknown/Nowhere'},
        {'tz': ''},
    ],
)
def test_contact_validation(values: dict[str, str]) -> None:
    """Both endpoints reject malformed contact values before persistence."""
    with pytest.raises(ValidationError):
        ProfileUpdateRequest.model_validate(values)
    with pytest.raises(ValidationError):
        SignupRequest.model_validate(dict(login='valid', password='valid', **values))
