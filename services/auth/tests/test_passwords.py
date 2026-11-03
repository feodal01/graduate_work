from security.passwords import PasswordHasher


def test_password_hasher_never_returns_plaintext() -> None:
    """Замена хеширования на хранение plaintext должна быть обнаружена."""
    password_hasher = PasswordHasher()

    password_hash = password_hasher.hash('correct horse')

    assert password_hash != 'correct horse'
    assert password_hasher.verify('correct horse', password_hash) is True
    assert password_hasher.verify('wrong horse', password_hash) is False
