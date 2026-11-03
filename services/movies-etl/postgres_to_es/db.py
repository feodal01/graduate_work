from postgres_to_es.schemas import PostgresDSL
from postgres_to_es.settings import settings


def get_postgres_dsl() -> PostgresDSL:
    return {
        "dbname": settings.postgres_db,
        "user": settings.postgres_user,
        "password": settings.postgres_password.get_secret_value(),
        "host": settings.postgres_host,
        "port": settings.postgres_port,
        "options": "-c search_path=content,public",
    }
