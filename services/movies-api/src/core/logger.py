from typing import Any

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DEFAULT_HANDLERS = [
    'console',
]

# В логгере настраивается логгирование uvicorn-сервера.
# Про логирование в Python можно прочитать в документации
# https://docs.python.org/3/howto/logging.html
# https://docs.python.org/3/howto/logging-cookbook.html

LOGGING: dict[str, Any] = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {'format': LOG_FORMAT},
        'default': {
            '()': 'uvicorn.logging.DefaultFormatter',
            'fmt': '%(levelprefix)s %(message)s',
            'use_colors': None,
        },
        'access': {
            '()': 'uvicorn.logging.AccessFormatter',
            'fmt': ("%(levelprefix)s %(client_addr)s - '%(request_line)s' %(status_code)s"),
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'default': {
            'formatter': 'default',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'access': {
            'formatter': 'access',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {
        '': {
            'handlers': LOG_DEFAULT_HANDLERS,
            'level': 'INFO',
        },
        'uvicorn.error': {
            'level': 'INFO',
        },
        'uvicorn.access': {
            'handlers': ['access'],
            'level': 'INFO',
            'propagate': False,
        },
    },
    'root': {
        'level': 'INFO',
        'formatter': 'verbose',
        'handlers': LOG_DEFAULT_HANDLERS,
    },
}


def add_logstash_handler(host: str, port: int, service: str) -> None:
    """Дублирует логи приложения в logstash по UDP."""
    LOGGING['formatters']['logstash'] = {
        '()': 'core.observability.RequestIdLogstashFormatter',
        'message_type': service,
        'tags': ['app', service],
    }
    LOGGING['handlers']['logstash'] = {
        'class': 'core.observability.QuietLogstashHandler',
        'formatter': 'logstash',
        'host': host,
        'port': port,
        'version': 1,
    }
    handlers = [*LOG_DEFAULT_HANDLERS, 'logstash']
    LOGGING['loggers']['']['handlers'] = handlers
    LOGGING['root']['handlers'] = handlers
