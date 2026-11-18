"""Правила задач outbox, общие для SQL-репозиториев, экранов и фейка (US-S-02).

Чистые функции без БД: когда задачу повторять, когда звать администратора и
что ему разрешено с ней сделать.
"""

from typing import Literal

from core.config import settings

DeliveryOutcome = Literal['completed', 'transient_error', 'permanent_error']


# Задачи, которые нельзя бросать по лимиту попыток. Доставка доступа в Auth
# идемпотентна и монотонна по версии: повтор безопасен, а остановка оставляет
# возвращённую подписку открытой (или оплаченную закрытой) до ручного
# вмешательства. Отмена письма об окончании после возврата обязана дойти до
# момента T, иначе пользователь с возвращёнными деньгами получит письмо
# «срок подписки закончился» (ТЗ 6.3: «повторяется до восстановления»).
RETRY_UNTIL_DELIVERED_TASK_TYPES = frozenset({'sync_access', 'cancel_expiration_email'})


def admin_can_retry_or_close(status: str, task_type: str, attempts: int) -> bool:
    """Доступны ли «Повторить» и «Закрыть с причиной» (US-A-02, BIL-API-12/13).

    Обычная задача — только в `requires_intervention`. Бессрочная задача
    (`RETRY_UNTIL_DELIVERED_TASK_TYPES`) туда по лимиту не попадает и после
    лимита остаётся `pending` с повторами: без этого администратор не смог бы
    ни ускорить повтор, ни закрыть задачу, которую получатель отвергает всегда.
    """
    if status == 'requires_intervention':
        return True
    return (
        status == 'pending'
        and task_type in RETRY_UNTIL_DELIVERED_TASK_TYPES
        and attempts >= settings.worker.billing_outbox_max_attempts
    )


def transient_failure_transition(
    task_type: str,
    attempts: int,
    max_attempts: int,
) -> tuple[Literal['retry', 'requires_intervention'], bool]:
    """Решает судьбу задачи после временной ошибки: повтор или вмешательство.

    `attempts` — число попыток уже с учётом текущей. Второй элемент — нужен ли
    алерт: для бессрочных задач он поднимается один раз, когда лимит пройден,
    а задача продолжает повторяться с потолком backoff.
    """
    if task_type in RETRY_UNTIL_DELIVERED_TASK_TYPES:
        return 'retry', attempts == max_attempts
    if attempts >= max_attempts:
        return 'requires_intervention', True
    return 'retry', False


def compute_outbox_backoff_seconds(attempts: int) -> int:
    """Вычисляет паузу экспоненциального backoff (2^attempt с потолком 300 с)."""
    normalized = max(1, attempts)
    return int(min(300, 2 ** min(normalized, 9)))


LATEST_ACCESS_TASK_CLOSE_MESSAGE = (
    'Закрыть нельзя: задача несёт последнюю версию доступа пользователя, и без '
    'неё Auth не узнает о текущем состоянии подписки. Используйте «Повторить»; '
    'закрыть можно только устаревшую задачу, когда выдана более новая версия'
)


def admin_can_close(
    status: str,
    task_type: str,
    attempts: int,
    *,
    access_version: int | None,
    latest_access_version: int | None,
) -> bool:
    """Можно ли «Закрыть с причиной» (BIL-API-13).

    Задача доступа с последней версией пользователя — единственный способ
    довести до Auth его текущее состояние: закрыв её, администратор оставил бы
    оплаченную подписку без доступа (или возвращённую — с доступом) до конца
    периода. Такую задачу можно только повторить; устаревшую — закрыть.
    """
    if not admin_can_retry_or_close(status, task_type, attempts):
        return False
    return not is_latest_access_task(
        task_type,
        access_version=access_version,
        latest_access_version=latest_access_version,
    )


def is_latest_access_task(
    task_type: str,
    *,
    access_version: int | None,
    latest_access_version: int | None,
) -> bool:
    return (
        task_type == 'sync_access'
        and access_version is not None
        and latest_access_version is not None
        and access_version >= latest_access_version
    )
