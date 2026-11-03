from werkzeug.security import check_password_hash, generate_password_hash


class PasswordHasher:
    """Хеширование и проверка пользовательских паролей."""

    def hash(self, password: str) -> str:
        return generate_password_hash(password)

    def verify(self, password: str, password_hash: str) -> bool:
        return check_password_hash(password_hash, password)
