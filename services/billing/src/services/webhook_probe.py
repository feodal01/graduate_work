"""Ограничение пробных чтений ЮKassa по вебхукам с незнакомым номером (BIL-API-05).

Вебхук без подписи может прислать кто угодно (2.5.4, п. 5). Пробное чтение
объекта по незнакомому номеру нужно только для редкого случая — потерянного
ответа на POST создания, — поэтому оно ограничено дважды:

- один и тот же номер читается не чаще раза в окно: ЮKassa повторяет
  уведомление, а ответ за несколько минут не изменится;
- всех пробных чтений в окне — не больше лимита, чтобы поток поддельных
  вебхуков не съедал квоту API ЮKassa, нужную настоящим платежам и возвратам.

Состояние живёт в памяти процесса `billing-api`: таблица ради этого не нужна,
а после перезапуска лимит просто начинается заново. Помним только номера,
которые действительно читали, поэтому память ограничена тем же лимитом.
"""

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from core.config import WorkerSettings


@dataclass(frozen=True, slots=True)
class WebhookProbePolicy:
    """Умолчания совпадают с настройками `BILLING_WEBHOOK_PROBE_*`."""

    window: timedelta = timedelta(minutes=10)
    max_per_window: int = 20

    @classmethod
    def from_settings(cls, worker: WorkerSettings) -> 'WebhookProbePolicy':
        return cls(
            window=timedelta(seconds=worker.billing_webhook_probe_window_seconds),
            max_per_window=worker.billing_webhook_probe_max_per_window,
        )


class WebhookProbeLimiter:
    """Решает, можно ли сейчас пробно прочитать объект с этим номером."""

    def __init__(self, policy: WebhookProbePolicy | None = None) -> None:
        self._policy = policy or WebhookProbePolicy()
        self._recent: deque[tuple[datetime, tuple[str, str]]] = deque()
        self._recent_keys: set[tuple[str, str]] = set()

    @property
    def policy(self) -> WebhookProbePolicy:
        return self._policy

    def allow(
        self,
        provider_code: str,
        provider_operation_id: str,
        *,
        now_utc: datetime | None = None,
    ) -> bool:
        now = now_utc or datetime.now(UTC)
        cutoff = now - self._policy.window
        while self._recent and self._recent[0][0] <= cutoff:
            _, old_key = self._recent.popleft()
            self._recent_keys.discard(old_key)
        key = (provider_code, provider_operation_id)
        if key in self._recent_keys:
            return False
        if len(self._recent) >= self._policy.max_per_window:
            return False
        self._recent.append((now, key))
        self._recent_keys.add(key)
        return True
