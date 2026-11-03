"""Настройка логирования сервиса.

Пишем в stdout простым текстовым форматом без сторонних зависимостей
"""

import logging
import sys

from postgres_to_es.settings import settings


def configure_logging() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
