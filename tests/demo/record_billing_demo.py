"""Записывает видео живого демо биллинга (раздел 9.2 ТЗ) с комментариями в кадре.

Сценарий 1 — шаги 1–2: исходное состояние пользователя без подписки и покупка
подписки «Премиум» на тестовом магазине ЮKassa. Нужен поднятый стенд
`docker compose up -d` и тестовые ключи ЮKassa в `.env`.

Запуск из корня репозитория (аргумент — номер сценария, по умолчанию 1):
    uv run --with playwright --with httpx python tests/demo/record_billing_demo.py 2

Сценарий 2 — шаг 3: возврат средств администратором в Django Admin. Пользователь
с оплаченной подпиской готовится вне записи.

Результат в `tests/demo/output/`: видео `.webm`, субтитры `.vtt` и текст `.md`.
Каждый прогон регистрирует в Auth нового пользователя без подписки; чтобы взять
существующего, задайте DEMO_LOGIN и DEMO_PASSWORD.

Как устроена запись: кадры снимаются CDP-скринкастом вместе с временем снятия и
собираются в видео ffmpeg из Playwright. Поэтому ожидание внешней системы можно
ускорить (в кадре виден значок ⏩ ×N), а не заставлять зрителя ждать.
"""

from __future__ import annotations

import asyncio
import base64
import bisect
import json
import os
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from playwright.async_api import CDPSession, Locator, Page, async_playwright

BASE = os.environ.get('DEMO_BASE_URL', 'http://localhost')
MAILPIT = os.environ.get('DEMO_MAILPIT_URL', 'http://localhost:8025')
# Фильм без даты выхода: Auth требует для него право films.early_access:read («новинка»).
NEW_RELEASE_FILM = os.environ.get('DEMO_FILM_ID', '0312ed51-8833-413f-bff5-0e139c11264a')
# Mastercard без 3-D Secure из списка тестовых карт ЮKassa.
TEST_CARD = ('5555555555554444', '12', '28', '000')
OUT_DIR = Path(__file__).resolve().parent / 'output'
REPO_ROOT = Path(__file__).resolve().parents[2]
FPS = 25
WIDTH, HEIGHT = 1280, 800
FFMPEG = Path(
    os.environ.get(
        'DEMO_FFMPEG', Path.home() / 'Library/Caches/ms-playwright/ffmpeg-1013/ffmpeg-mac'
    )
)

OVERLAY_JS = """
([title, text, card, badge]) => {
  const ensure = (id, css) => {
    let el = document.getElementById(id);
    if (!el) {
      el = document.createElement('div'); el.id = id;
      document.documentElement.appendChild(el);
    }
    Object.assign(el.style, css);
    return el;
  };
  if (document.body && !document.getElementById('demo-spacer')) {
    const sp = document.createElement('div'); sp.id = 'demo-spacer';
    sp.style.height = '360px';
    (document.getElementById('content') || document.body).appendChild(sp);
  }
  const cap = ensure('demo-caption', {
    position: 'fixed', left: '40px', right: '40px', bottom: '96px', zIndex: 2147483646,
    background: 'rgba(15,23,42,0.94)', color: '#f8fafc', borderRadius: '12px',
    padding: '14px 20px', font: '19px/1.45 -apple-system, system-ui, sans-serif',
    boxShadow: '0 8px 24px rgba(0,0,0,.35)', pointerEvents: 'none',
    display: title ? 'block' : 'none',
  });
  cap.innerHTML = '';
  const t = document.createElement('div');
  Object.assign(t.style, {color: '#93c5fd', fontWeight: '700', marginBottom: '4px'});
  t.textContent = title; cap.appendChild(t);
  const b = document.createElement('div'); b.textContent = text; cap.appendChild(b);
  const c = ensure('demo-card', {
    position: 'fixed', right: '40px', bottom: '230px', width: '560px', zIndex: 2147483646,
    background: '#0b1220', color: '#e2e8f0', borderRadius: '10px', padding: '12px 14px',
    font: '14px/1.5 ui-monospace, Menlo, monospace', whiteSpace: 'pre-wrap',
    boxShadow: '0 8px 24px rgba(0,0,0,.35)', pointerEvents: 'none',
    display: card ? 'block' : 'none', border: '1px solid #334155',
  });
  c.textContent = card || '';
  const ff = ensure('demo-ff', {
    position: 'fixed', left: '24px', top: '20px', zIndex: 2147483646,
    background: '#f59e0b', color: '#111827', borderRadius: '999px', padding: '8px 16px',
    font: '700 22px/1 -apple-system, system-ui, sans-serif', pointerEvents: 'none',
    display: badge ? 'block' : 'none', boxShadow: '0 4px 14px rgba(0,0,0,.3)',
  });
  ff.textContent = badge || '';
}
"""

CURSOR_JS = """
([x, y, ripple]) => {
  let cur = document.getElementById('demo-cursor');
  if (!cur) {
    cur = document.createElement('div'); cur.id = 'demo-cursor';
    Object.assign(cur.style, {
      position: 'fixed', width: '22px', height: '22px', marginLeft: '-11px',
      marginTop: '-11px', borderRadius: '50%', background: 'rgba(239,68,68,0.85)',
      border: '3px solid #fff', boxShadow: '0 0 0 2px rgba(239,68,68,.6)',
      zIndex: 2147483647, pointerEvents: 'none',
      transition: 'left .45s ease-in-out, top .45s ease-in-out',
      left: (window.__demoCursor || [x, y])[0] + 'px',
      top: (window.__demoCursor || [x, y])[1] + 'px',
    });
    document.documentElement.appendChild(cur);
  }
  void cur.offsetWidth;
  cur.style.left = x + 'px'; cur.style.top = y + 'px';
  window.__demoCursor = [x, y];
  if (ripple) {
    const r = document.createElement('div');
    Object.assign(r.style, {
      position: 'fixed', left: x + 'px', top: y + 'px', width: '16px', height: '16px',
      marginLeft: '-8px', marginTop: '-8px', borderRadius: '50%',
      border: '4px solid rgba(239,68,68,.9)', zIndex: 2147483647, pointerEvents: 'none',
      transition: 'transform .6s ease-out, opacity .6s ease-out',
    });
    document.documentElement.appendChild(r);
    requestAnimationFrame(() => { r.style.transform = 'scale(5)'; r.style.opacity = '0'; });
    setTimeout(() => r.remove(), 700);
  }
}
"""


@dataclass
class Recorder:
    """Снимает кадры CDP-скринкаста и собирает из них видео с ускоренными участками."""

    cdp: CDPSession
    frames: list[tuple[float, bytes]] = field(default_factory=list)
    # Участки реального времени, которые в видео проигрываются ускоренно: (начало, конец, ×N)
    fast: list[tuple[float, float, float]] = field(default_factory=list)

    async def start(self) -> None:
        self.cdp.on('Page.screencastFrame', self._on_frame)
        await self.cdp.send(
            'Page.startScreencast',
            {'format': 'jpeg', 'quality': 85, 'maxWidth': WIDTH, 'maxHeight': HEIGHT},
        )

    def _on_frame(self, ev: dict) -> None:
        self.frames.append((time.monotonic(), base64.b64decode(ev['data'])))
        asyncio.ensure_future(
            self.cdp.send('Page.screencastFrameAck', {'sessionId': ev['sessionId']})
        )

    async def stop(self) -> None:
        try:
            await self.cdp.send('Page.stopScreencast')
        except Exception:
            pass

    def to_video_time(self, wall: float) -> float:
        """Переводит реальное время записи в время видео с учётом ускорений."""
        t, prev = 0.0, 0.0
        for a, b, k in self.fast:
            if wall <= a:
                break
            t += a - prev
            t += (min(wall, b) - a) / k
            prev = min(wall, b)
            if wall <= b:
                return t
        return t + (wall - prev)

    def encode(self, t0: float, t_end: float, out: Path) -> None:
        if not self.frames:
            raise RuntimeError('Скринкаст не прислал ни одного кадра')
        stamps = [ts - t0 for ts, _ in self.frames]
        duration = self.to_video_time(t_end - t0)
        # Обратное отображение: для каждого кадра видео ищем момент реального времени.
        wall_points = [i / 100 for i in range(int((t_end - t0) * 100) + 1)]
        video_points = [self.to_video_time(w) for w in wall_points]
        proc = subprocess.Popen(
            [
                str(FFMPEG),
                '-y',
                '-loglevel',
                'error',
                '-f',
                'image2pipe',
                '-framerate',
                str(FPS),
                '-c:v',
                'mjpeg',
                '-i',
                'pipe:0',
                '-vf',
                f'scale={WIDTH}:{HEIGHT}',
                '-c:v',
                'libvpx-vp9',
                '-b:v',
                '1200k',
                '-deadline',
                'realtime',
                '-cpu-used',
                '8',
                '-row-mt',
                '1',
                str(out),
            ],
            stdin=subprocess.PIPE,
        )
        assert proc.stdin is not None
        for i in range(int(duration * FPS) + 1):
            vt = i / FPS
            wall = wall_points[min(bisect.bisect_left(video_points, vt), len(wall_points) - 1)]
            idx = max(bisect.bisect_right(stamps, wall) - 1, 0)
            proc.stdin.write(self.frames[idx][1])
        proc.stdin.close()
        if proc.wait() != 0:
            raise RuntimeError('ffmpeg не смог собрать видео')


@dataclass
class Narrator:
    """Держит текущую подпись, курсор и значок ускорения; пишет тайминги субтитров."""

    page: Page
    rec: Recorder
    started: float = field(default_factory=time.monotonic)
    cues: list[tuple[float, float, str, str]] = field(default_factory=list)
    title: str = ''
    text: str = ''
    card: str = ''
    badge: str = ''
    _since: float = 0.0

    def _now(self) -> float:
        return time.monotonic() - self.started

    def _close_cue(self) -> None:
        if self.title:
            self.cues.append((self._since, self._now(), self.title, self.text))

    async def render(self) -> None:
        try:
            await self.page.evaluate(OVERLAY_JS, [self.title, self.text, self.card, self.badge])
            pos = await self.page.evaluate('window.__demoCursor || null')
            if pos is None and hasattr(self, '_cursor'):
                await self.page.evaluate(CURSOR_JS, [*self._cursor, False])
        except Exception:  # страница в процессе навигации — нарисуем после загрузки
            pass

    async def say(
        self, title: str, text: str, *, card: str = '', pause: float | None = None
    ) -> None:
        """Показывает подпись и держит её столько, сколько нужно, чтобы прочитать."""
        self._close_cue()
        self.title, self.text, self.card = title, text, card
        self._since = self._now()
        await self.render()
        words = len((text + ' ' + card).split())
        await self.page.wait_for_timeout(int(1000 * (pause or min(max(2.5, words * 0.32), 9))))

    async def click(self, target: Locator, *, double: bool = False) -> None:
        """Подводит видимый курсор к элементу, показывает «волну» и кликает."""
        await target.evaluate("el => el.scrollIntoView({block: 'center'})")
        await self.page.wait_for_timeout(250)
        box = await target.bounding_box()
        if box:
            x, y = box['x'] + box['width'] / 2, box['y'] + box['height'] / 2
            self._cursor = (x, y)
            await self.page.evaluate(CURSOR_JS, [x, y, False])
            await self.page.wait_for_timeout(550)
            await self.page.evaluate(CURSOR_JS, [x, y, True])
            await self.page.wait_for_timeout(250)
        if double:
            await target.dblclick()
        else:
            await target.click()

    async def type(self, target: Locator, value: str, *, delay: int = 45) -> None:
        await self.click(target)
        await target.press_sequentially(value, delay=delay)

    async def fast_forward(self, factor: float, until, *, timeout: float = 120) -> bool:
        """Ускоряет участок ожидания в видео: ждёт until(), показывая значок ⏩ ×N."""
        self.badge = f'⏩ ×{int(factor)}'
        await self.render()
        a = time.monotonic()
        ok = False
        while time.monotonic() - a < timeout:
            if await until():
                ok = True
                break
            await asyncio.sleep(1)
        self.badge = ''
        await self.render()
        b = time.monotonic()
        self.rec.fast.append((a - self.started, b - self.started, factor))
        return ok

    def finish(self) -> None:
        self._close_cue()
        self.title = ''


def _ts(sec: float) -> str:
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f'{int(h):02d}:{int(m):02d}:{s:06.3f}'


def write_subtitles(n: Narrator, vtt: Path, md: Path, title: str) -> None:
    cues = [(n.rec.to_video_time(a), n.rec.to_video_time(b), h, t) for a, b, h, t in n.cues]
    lines = ['WEBVTT', '']
    for i, (a, b, head, body) in enumerate(cues, 1):
        lines += [str(i), f'{_ts(a)} --> {_ts(b)}', head, body, '']
    vtt.write_text('\n'.join(lines), encoding='utf-8')
    md_lines = [f'# {title}', '', '| Время | Что происходит |', '| --- | --- |']
    for a, _b, head, body in cues:
        md_lines.append(f'| {_ts(a)[3:8]} | **{head}.** {body} |')
    md.write_text('\n'.join(md_lines) + '\n', encoding='utf-8')


def psql(service: str, user: str, db: str, sql: str) -> str:
    res = subprocess.run(
        ['docker', 'compose', 'exec', '-T', service, 'psql', '-U', user, '-d', db, '-c', sql],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return (res.stdout or res.stderr).strip()


def billing_sql(sql: str, *, rows: bool = False) -> str:
    """Запрос в billing-db: таблица для карточки в кадре или голое значение (rows=True)."""
    if rows:
        res = subprocess.run(
            [
                'docker',
                'compose',
                'exec',
                '-T',
                'billing-db',
                'psql',
                '-U',
                'billing',
                '-d',
                'billing',
                '-tAc',
                sql,
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return res.stdout.strip()
    return psql('billing-db', 'billing', 'billing', sql)


async def demo_user(http: httpx.AsyncClient) -> tuple[str, str]:
    """Берёт пользователя из DEMO_LOGIN/DEMO_PASSWORD или регистрирует нового в Auth."""
    if os.environ.get('DEMO_LOGIN'):
        return os.environ['DEMO_LOGIN'], os.environ['DEMO_PASSWORD']
    suffix = secrets.token_hex(3)
    login, password = f'viewer_{suffix}', f'Demo-{secrets.token_hex(8)}!'
    r = await http.post(
        f'{BASE}/api/v1/auth/signup',
        json={
            'login': login,
            'password': password,
            'email': f'viewer.{suffix}@cinema-demo.ru',
            'locale': 'ru',
            'tz': 'Europe/Moscow',
        },
    )
    r.raise_for_status()
    return login, password


async def api_login(http: httpx.AsyncClient, login: str, password: str) -> str:
    r = await http.post(f'{BASE}/api/v1/auth/login', json={'login': login, 'password': password})
    r.raise_for_status()
    return r.json()['access_token']


async def film_status(http: httpx.AsyncClient, token: str) -> int:
    r = await http.get(
        f'{BASE}/api/v1/films/{NEW_RELEASE_FILM}/', headers={'Authorization': f'Bearer {token}'}
    )
    return r.status_code


def order_id(url: str) -> str:
    m = re.search(r'orderId=([\w-]+)', url)
    return m.group(1) if m else '—'


def film_card(code: int) -> str:
    reason = {200: ' OK', 403: ' Forbidden'}.get(code, '')
    return (
        f'GET /api/v1/films/{NEW_RELEASE_FILM}/\n'
        f'Authorization: Bearer <токен пользователя>\n\n→ HTTP {code}{reason}'
    )


async def go_to_checkout(page: Page, n: Narrator, click) -> None:
    """Кликает и ждёт страницу ЮKassa; медленный ответ ЮKassa показывает ускоренно.

    Если ЮKassa не ответила за таймаут Billing (5 с), пользователь видит честную ошибку
    «попробуйте ещё раз» — ничего не списано, и он просто повторяет попытку.
    """
    on_checkout = re.compile(r'yoomoney\.ru')
    for attempt in range(3):
        await click()
        try:
            await page.wait_for_url(on_checkout, timeout=3000)
        except Exception:

            async def settled() -> bool:
                return bool(on_checkout.search(page.url)) or 'Произошла ошибка' in (
                    await page.content()
                )

            await n.fast_forward(4, settled, timeout=60)
        if on_checkout.search(page.url):
            await page.wait_for_load_state('networkidle')
            return
        await n.say(
            'ЮKassa не ответила вовремя',
            'Billing не дождался ответа ЮKassa за 5 с и честно просит повторить: '
            'деньги не списаны, платёж в БД не потерян. Пользователь повторяет попытку.',
        )
        await n.click(page.get_by_text(re.compile('Вернуться в «Мою подписку»')))
        await page.wait_for_load_state('networkidle')

        async def click():  # noqa: F811 — повтор идёт через «Продолжить оплату»
            await n.click(page.get_by_text(re.compile('Продолжить оплату')).first)

    raise RuntimeError('ЮKassa не ответила за три попытки')


async def scenario_purchase(page: Page, n: Narrator, http: httpx.AsyncClient) -> None:
    login, password = await demo_user(http)
    page.on('load', lambda _: asyncio.ensure_future(n.render()))

    await page.set_content(
        '<body style="margin:0;background:#0f172a;color:#f8fafc;font-family:system-ui;'
        'display:flex;align-items:center;justify-content:center;height:100vh">'
        '<div style="text-align:center"><div style="font-size:42px;font-weight:700">'
        'Биллинг онлайн-кинотеатра</div><div style="font-size:26px;margin-top:14px;'
        'color:#93c5fd">Сценарий 1. Покупка подписки «Премиум»</div>'
        '<div style="font-size:18px;margin-top:10px;color:#94a3b8">ТЗ 9.2, шаги 1–2 · '
        'тестовый магазин ЮKassa</div></div></body>'
    )
    await n.say(
        'Сценарий 1',
        'Пользователь без подписки покупает «Премиум» за 299 ₽ и сразу получает доступ '
        'к фильмам-новинкам. Проверяем TASK-1, TASK-5 и TASK-6.',
    )

    # --- Шаг 1. Исходное состояние ---
    await page.goto(f'{BASE}/billing/')
    await n.say(
        'Шаг 1.1 · Экран входа (Э1)',
        'Неавторизованного пользователя /billing/ перенаправляет на вход. '
        'Логин и пароль проверяет сервис Auth.',
    )
    await n.type(page.locator('#login'), login)
    await n.type(page.locator('#password'), password, delay=25)
    await n.click(page.get_by_role('button', name='Войти'))
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 1.2 · «Моя подписка» (Э2)',
        'Статус «Нет подписки», платежей пока нет, доступна кнопка «Оформить за 299 ₽».',
    )

    token = await api_login(http, login, password)
    await n.say(
        'Шаг 1.3 · Новинка недоступна',
        'Просим у movies-api фильм-новинку с токеном пользователя. '
        'Auth не находит роль subscriber и запрещает просмотр.',
        card=film_card(await film_status(http, token)),
    )

    # --- Шаг 2. Покупка и защита от двойного списания ---
    mails_before = (await http.get(f'{MAILPIT}/api/v1/messages')).json().get('total', 0)
    await n.say(
        'Шаг 2.1 · Двойное нажатие «Оформить»',
        'Пользователь нетерпеливо нажимает кнопку дважды подряд — '
        'браузер отправляет на сервер два запроса покупки.',
        pause=2.5,
    )
    await go_to_checkout(
        page,
        n,
        lambda: n.click(page.get_by_role('button', name=re.compile('Оформить')), double=True),
    )
    first_order = order_id(page.url)
    summary = (
        await http.get(
            f'{BASE}/api/v1/billing/me/summary', headers={'Authorization': f'Bearer {token}'}
        )
    ).json()
    payment_id = summary['payments'][0]['payment_id']
    payments = billing_sql(
        'SELECT id, status, created_at::time(0) AS created FROM payments WHERE user_id = '
        f"(SELECT user_id FROM payments WHERE id = '{payment_id}')"
    )
    await n.say(
        'Шаг 2.2 · Редирект на ЮKassa, платёж один',
        'Оба запроса получили один и тот же платёж: в БД его охраняет уникальный индекс '
        '«один pending на пользователя», а в ЮKassa ушёл один Idempotence-Key.',
        card='billing-db › payments этого пользователя\n\n'
        + payments
        + f'\n\nЗаказ в ЮKassa: {first_order}',
    )

    # Пользователь закрывает вкладку оплаты и возвращается в личный кабинет
    await n.say(
        'Шаг 2.3 · Пользователь передумал и закрыл вкладку',
        'Не оплатив, он закрывает страницу ЮKassa и позже снова открывает личный кабинет.',
        pause=3,
    )
    await page.goto(f'{BASE}/billing/')
    await n.say(
        'Шаг 2.4 · Незавершённая покупка в кабинете',
        'Статус «Ожидает оплаты», в истории — платёж в ожидании. '
        'Кнопка «Продолжить оплату» продолжит этот же платёж, а не создаст новый.',
    )
    await go_to_checkout(
        page, n, lambda: n.click(page.get_by_text(re.compile('Продолжить оплату')).first)
    )
    second_order = order_id(page.url)
    await n.say(
        'Шаг 2.5 · Та же страница оплаты',
        'Пользователь снова на ЮKassa, и это тот же заказ — повторного списания не будет. '
        'Данные карты вводятся только у ЮKassa, Billing их не видит.',
        card=f'Первый заход:  {first_order}\nПовторный заход: {second_order}\n\n'
        f'Совпадают: {"ДА" if first_order == second_order else "НЕТ"}',
    )
    await n.click(page.get_by_text('Банковская карта'))
    await page.locator('input[name=card-number]').wait_for()
    await n.say(
        'Шаг 2.6 · Тестовая карта',
        'Mastercard 5555 5555 5555 4444 без 3-D Secure из списка тестовых карт ЮKassa; '
        'срок — любая будущая дата, CVC — любой.',
        pause=3,
    )
    number, month, year, cvc = TEST_CARD
    await n.type(page.locator('input[name=card-number]'), number, delay=35)
    await n.type(page.locator('input[name=expiry-month]'), month, delay=60)
    await n.type(page.locator('input[name=expiry-year]'), year, delay=60)
    await n.type(page.locator('input[name=security-code]'), cvc, delay=60)
    await n.click(page.get_by_role('button', name=re.compile('Заплатить')))
    await page.wait_for_url(re.compile(r'/success'), timeout=60000)
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 2.7 · ЮKassa приняла оплату',
        'Деньги списаны в тестовом магазине. Теперь Billing должен узнать об этом '
        'и провести платёж у себя.',
    )

    # Возврат на сайт и проведение платежа по сигналу вебхука
    await page.goto(f'{BASE}/billing/payments/{payment_id}')
    await n.say(
        'Шаг 2.8 · «Результат оплаты» (Э3)',
        'Пользователь вернулся на сайт. Экран читает статус только из нашей БД и '
        'обновляется сам, пока идёт проверка.',
    )
    hook = await http.post(
        f'{BASE}/api/v1/billing/webhooks/yookassa',
        json={
            'type': 'notification',
            'event': 'payment.succeeded',
            'object': {'id': second_order},
        },
        headers={'X-Request-Id': f'demo-webhook-{secrets.token_hex(4)}'},
    )
    await n.say(
        'Шаг 2.9 · Сигнал вебхука',
        'В продакшене ЮKassa сама присылает вебхук. До локального стенда он из интернета '
        'не доходит, поэтому отправляем его скриптом. Телу вебхука Billing не верит: '
        'берёт только id и перепроверяет платёж через GET в ЮKassa.',
        card='POST /api/v1/billing/webhooks/yookassa\n'
        f'{{"event": "payment.succeeded", "object": {{"id": "{second_order}"}}}}\n\n'
        f'→ HTTP {hook.status_code}',
        pause=5,
    )

    async def paid() -> bool:
        try:
            await page.reload()
            return 'Оплата прошла' in await page.inner_text('body')
        except Exception:
            return False

    async def paid_soon() -> bool:
        for _ in range(4):
            if await paid():
                return True
            await asyncio.sleep(1.5)
        return False

    if not await paid_soon():
        # Платёж ещё не проведён: его проведёт следующая фоновая сверка billing-worker.
        # Зритель это ожидание видит ускоренным.
        await n.say(
            'Шаг 2.9 · Ждём подтверждения',
            'Платёж ещё не проведён — Billing не получил от ЮKassa итог при этой проверке. '
            'Он ничего не теряет: платёж проведёт фоновая сверка billing-worker '
            '(раз в 30 с). Ожидание показано ускоренно.',
        )
        if not await n.fast_forward(8, paid, timeout=150):
            raise RuntimeError('Платёж не проведён за 150 с')
    events = billing_sql(
        'SELECT from_status, to_status, source FROM operation_events '
        f"WHERE operation_id = '{payment_id}' ORDER BY created_at"
    )
    await n.say(
        'Шаг 2.10 · Платёж проведён',
        'Billing сверил сумму, валюту и наш UUID в metadata, создал подписку и в той же '
        'транзакции поставил задачи: выдать доступ в Auth и отправить письма.',
        card='billing-db › operation_events\n\n' + events,
    )

    await n.say(
        'Шаг 2.11 · Новинка доступна',
        'Тот же запрос фильма теперь проходит: Auth получил роль subscriber '
        'со сроком до конца подписки.',
        card=film_card(await film_status(http, token)),
    )

    await n.click(page.get_by_text(re.compile('Вернуться в «Мою подписку»')))
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 2.12 · «Моя подписка» после оплаты',
        'Статус «Активна до …» в часовом поясе пользователя, платёж 299 ₽ — «Оплачен».',
    )

    for _ in range(20):
        r = await http.get(f'{MAILPIT}/api/v1/messages')
        if r.json().get('total', 0) > mails_before:
            break
        await asyncio.sleep(1.5)
    await page.goto(MAILPIT)
    await page.wait_for_load_state('networkidle')
    letter = page.get_by_text(re.compile('Премиум.*оплачен', re.I)).first
    await n.click(letter)
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 2.13 · Письмо об оплате',
        'Notifications отправил письмо «Подписка „Премиум“ оплачена» (Mailpit перехватывает '
        'почту стенда). Адрес и имя Billing не хранит — письмо собрано по user_id.',
    )

    user_id = billing_sql(f"SELECT user_id FROM payments WHERE id = '{payment_id}'", rows=True)
    scheduled = psql(
        'notifications-db',
        'notifications',
        'notifications',
        "SELECT kind, status, to_char(next_attempt_at AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD HH24:MI') AS send_at_utc FROM deliveries "
        f"WHERE user_id = '{user_id}' ORDER BY next_attempt_at",
    )
    await n.say(
        'Шаг 2.14 · Письмо об окончании запланировано',
        'Последняя строка — отложенное письмо об окончании подписки на дату ends_at. '
        'При возврате оно будет отменено (сценарий 2).',
        card='notifications-db › deliveries\n\n' + scheduled,
    )

    await page.goto(f'{BASE}/billing/')
    r3 = await http.post(
        f'{BASE}/api/v1/billing/payments', headers={'Authorization': f'Bearer {token}'}
    )
    await n.say(
        'Шаг 2.15 · Повторная покупка запрещена',
        'Кнопки покупки в кабинете больше нет, а прямой запрос к API при активной подписке '
        'получает понятный отказ — второго списания не будет.',
        card=f'POST /api/v1/billing/payments\n\n→ HTTP {r3.status_code}\n'
        f'{json.dumps(r3.json(), ensure_ascii=False, indent=2)[:300]}',
    )
    await n.say(
        'Итог сценария 1',
        'Оплата через настоящий тестовый магазин ЮKassa, двойного списания нет, доступ '
        'к новинкам открыт, письмо пришло, письмо об окончании запланировано.',
    )
    n.finish()


async def show_section(page: Page, heading: str) -> None:
    """Прокручивает страницу так, чтобы раздел с заголовком heading начинался сверху."""
    await page.get_by_text(heading).first.evaluate(
        "el => { el.scrollIntoView({block: 'start'}); window.scrollBy(0, -12); }"
    )
    await page.wait_for_timeout(300)


def title_card(subtitle: str, note: str) -> str:
    return (
        '<body style="margin:0;background:#0f172a;color:#f8fafc;font-family:system-ui;'
        'display:flex;align-items:center;justify-content:center;height:100vh">'
        '<div style="text-align:center"><div style="font-size:42px;font-weight:700">'
        'Биллинг онлайн-кинотеатра</div><div style="font-size:26px;margin-top:14px;'
        f'color:#93c5fd">{subtitle}</div>'
        f'<div style="font-size:18px;margin-top:10px;color:#94a3b8">{note}</div></div></body>'
    )


async def prepare_paid_user(browser, http: httpx.AsyncClient) -> dict:
    """Вне записи: регистрирует пользователя и оплачивает подписку на тестовой ЮKassa."""
    login, password = await demo_user(http)
    token = await api_login(http, login, password)
    headers = {'Authorization': f'Bearer {token}'}
    r = (await http.post(f'{BASE}/api/v1/billing/payments', headers=headers)).json()
    ctx = await browser.new_context(viewport={'width': WIDTH, 'height': HEIGHT}, locale='ru-RU')
    page = await ctx.new_page()
    await page.goto(r['confirmation_url'])
    await page.get_by_text('Банковская карта').click()
    number, month, year, cvc = TEST_CARD
    await page.locator('input[name=card-number]').fill(number)
    await page.locator('input[name=expiry-month]').fill(month)
    await page.locator('input[name=expiry-year]').fill(year)
    await page.locator('input[name=security-code]').fill(cvc)
    await page.get_by_role('button', name=re.compile('Заплатить')).click()
    await page.wait_for_url(re.compile(r'/success'), timeout=60000)
    await ctx.close()
    provider_id = order_id(r['confirmation_url'])
    for attempt in range(60):
        if attempt % 10 == 0:
            await http.post(
                f'{BASE}/api/v1/billing/webhooks/yookassa',
                json={
                    'type': 'notification',
                    'event': 'payment.succeeded',
                    'object': {'id': provider_id},
                },
            )
        summary = (await http.get(f'{BASE}/api/v1/billing/me/summary', headers=headers)).json()
        if summary['status_key'] == 'active' and await film_status(http, token) == 200:
            break
        await asyncio.sleep(3)
    else:
        raise RuntimeError('Подготовка: платёж не проведён')
    return {
        'login': login,
        'password': password,
        'token': token,
        'payment_id': r['payment_id'],
        'provider_id': provider_id,
    }


async def wait_page(
    page: Page, n: Narrator, done, *, quick: float = 3.0, factor: int = 4, timeout: float = 120
) -> bool:
    """Ждёт условие; если ожидание затянулось дольше quick секунд, показывает его ускоренно."""
    start = time.monotonic()
    while time.monotonic() - start < quick:
        if await done():
            return True
        await asyncio.sleep(0.5)
    return await n.fast_forward(factor, done, timeout=timeout)


async def scenario_refund(page: Page, n: Narrator, http: httpx.AsyncClient, u: dict) -> None:
    login, payment_id, token = u['login'], u['payment_id'], u['token']
    admin_login = os.environ.get('DEMO_ADMIN_LOGIN', 'admin')
    admin_password = os.environ.get('DEMO_ADMIN_PASSWORD', 'Cinema-Admin-2026!')
    page.on('load', lambda _: asyncio.ensure_future(n.render()))

    await page.set_content(
        title_card(
            'Сценарий 2. Возврат средств администратором',
            'ТЗ 9.2, шаг 3 · Django Admin и тестовый магазин ЮKassa',
        )
    )
    await n.say(
        'Сценарий 2',
        'Поддержка находит пользователя в админке, проверяет платёж и оформляет полный '
        'возврат с причиной. Доступ закрывается, письмо об окончании отменяется. '
        'Проверяем TASK-2 и TASK-3.',
    )

    # --- Исходное состояние: подписка оплачена (как в сценарии 1) ---
    await page.goto(f'{BASE}/billing/')
    await n.type(page.locator('#login'), login)
    await n.type(page.locator('#password'), u['password'], delay=20)
    await n.click(page.get_by_role('button', name='Войти'))
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Исходное состояние',
        f'Пользователь {login} уже оплатил «Премиум» на тестовой ЮKassa, как в сценарии 1: '
        'подписка активна, фильм-новинка доступен.',
        card=film_card(await film_status(http, token)),
    )

    # --- Шаг 3.1. Вход в админку и поиск пользователя ---
    await page.goto(f'{BASE}/admin/')
    await n.say(
        'Шаг 3.1 · Вход в Django Admin',
        'Сотрудник поддержки входит в админку своей учётной записью.',
        pause=2.5,
    )
    await n.type(page.locator('#id_username'), admin_login)
    await n.type(page.locator('#id_password'), admin_password, delay=20)
    await n.click(page.locator('input[type=submit], button[type=submit]').first)
    await page.wait_for_load_state('networkidle')
    await page.goto(f'{BASE}/admin/billing/subscriptions/')
    await n.say(
        'Шаг 3.2 · Раздел «Подписки» (А1)',
        'Админка не ходит в базу Billing напрямую: списки приходят из внутреннего API '
        'Billing, а логины пользователей — пакетным запросом в Auth.',
    )
    await n.type(page.locator('#id_q'), login)
    await n.click(page.get_by_role('button', name='Найти'))
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 3.2 · Пользователь найден по логину',
        'Активная подписка; даты показаны в двух колонках — в UTC и в часовом поясе '
        'пользователя (Europe/Moscow).',
    )

    # --- Шаг 3.3. Карточка платежа ---
    await n.click(page.get_by_text(re.compile(r'Карточка платежа')).first)
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 3.3 · Карточка платежа (А3)',
        'Все поля только для чтения: редактировать финансовые факты нельзя. '
        'Виден номер платежа в ЮKassa и связанная подписка.',
    )
    await show_section(page, '4. История переходов статусов')
    await n.say(
        'Шаг 3.3 · История переходов статусов',
        'Неизменяемый журнал operation_events: платёж создан (pending) и проведён '
        '(succeeded), указан источник итога и ответ ЮKassa.',
    )
    await show_section(page, '5. Доставка в Auth и Notifications')
    await n.say(
        'Шаг 3.3 · Доставка в Auth и Notifications',
        'Задачи outbox_tasks выполнены: доступ выдан в Auth, письма поставлены в Notifications. '
        'Если бы доставка застряла, здесь были бы кнопки «Повторить» и «Закрыть с причиной».',
    )
    await page.evaluate('window.scrollTo(0, 0)')
    await page.wait_for_timeout(300)

    # --- Шаг 3.4. Оформление возврата ---
    await n.click(page.get_by_role('link', name='Вернуть'))
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 3.4 · Подтверждение возврата (А4)',
        'Перед возвратом админка показывает сводку по платежу и предупреждает о '
        'последствиях: доступ закроется, письмо об окончании отменится, придёт письмо о возврате.',
    )
    await n.click(page.get_by_role('button', name='Подтвердить возврат'))
    await page.wait_for_load_state('networkidle')
    await page.evaluate('window.scrollTo(0, 0)')
    await n.say(
        'Шаг 3.5 · Без причины нельзя',
        'Попытка подтвердить возврат с пустой причиной отклонена: причина обязательна '
        'и попадёт в неизменяемый журнал действий.',
    )
    await n.type(page.locator('#id_reason'), 'Обращение клиента №101', delay=40)
    await n.click(page.get_by_role('button', name='Подтвердить возврат'))

    async def back_on_card() -> bool:
        return '/refund' not in page.url and '1. Основные данные платежа' in (await page.content())

    await wait_page(page, n, back_on_card)
    await page.wait_for_load_state('networkidle')
    message = (await page.locator('ul.messagelist').first.inner_text()).strip()
    await n.say(
        'Шаг 3.6 · Возврат отправлен в ЮKassa',
        'Billing создал возврат в ЮKassa с Idempotence-Key и сразу сверил его статус '
        'через GET — ответу на создание он не доверяет.',
        card=f'Сообщение админки:\n{message}',
    )

    async def refund_done() -> bool:
        await page.reload()
        return bool(re.search(r'Возвращ\w* \(<code>succeeded', await page.content()))

    if not await refund_done():
        await n.say(
            'Шаг 3.6 · ЮKassa ещё обрабатывает возврат',
            'Возврат в статусе «в обработке». Администратор может нажать «Проверить в ЮKassa» '
            'или дождаться фоновой сверки — повторного возврата не будет.',
        )
        button = page.locator('form[action*="/operations/refund/"] button').first
        if await button.count():
            await n.click(button)
            await page.wait_for_load_state('networkidle')
        await wait_page(page, n, refund_done, quick=0, factor=8, timeout=180)
    await show_section(page, '2. Связанная подписка')
    await n.say(
        'Шаг 3.6 · Возврат выполнен',
        'В карточке — возврат «Возвращён» с логином администратора и причиной, '
        'а подписка теперь вычисляется как «Возвращена».',
    )

    # --- Шаг 3.7. Последствия возврата ---
    async def access_closed() -> bool:
        return await film_status(http, token) == 403

    await wait_page(page, n, access_closed)
    await n.say(
        'Шаг 3.7 · Доступ к новинке закрыт',
        'Тот же запрос фильма снова получает отказ: Billing доставил в Auth новую версию '
        'доступа, кэш прав в Redis сброшен.',
        card=film_card(await film_status(http, token)),
    )
    await page.goto(f'{BASE}/billing/')
    await n.say(
        'Шаг 3.8 · Экран пользователя «Моя подписка»',
        'Пользователь видит статус «Возвращена», а в истории у платежа — отметку «Возвращён».',
    )

    async def refund_mail() -> bool:
        r = await http.get(
            f'{MAILPIT}/api/v1/search', params={'query': f'to:{login.replace("_", ".")}'}
        )
        return any('озврат' in m.get('Subject', '') for m in r.json().get('messages', []))

    await wait_page(page, n, refund_mail)
    await page.goto(MAILPIT)
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 3.9 · Письмо о возврате',
        'В почтовом ящике стенда (Mailpit) — новое письмо о возврате средств.',
        pause=2.5,
    )
    await n.click(page.get_by_text(re.compile('озврат', re.I)).first)
    await page.wait_for_load_state('networkidle')
    await n.say(
        'Шаг 3.9 · Письмо о возврате',
        'Notifications отправил транзакционное письмо о возврате 299 ₽ по подписке «Премиум».',
    )
    user_id = billing_sql(f"SELECT user_id FROM payments WHERE id = '{payment_id}'", rows=True)
    deliveries = psql(
        'notifications-db',
        'notifications',
        'notifications',
        'SELECT kind, status, terminal_reason, '
        "to_char(next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS planned_utc "
        f"FROM deliveries WHERE user_id = '{user_id}' ORDER BY next_attempt_at",
    )
    await n.say(
        'Шаг 3.10 · Письмо об окончании отменено',
        'Отложенное письмо об окончании подписки (на месяц вперёд) переведено в cancelled '
        'с причиной cancelled_by_sender — пользователь его не получит.',
        card='notifications-db › deliveries\n\n' + deliveries,
    )

    await page.goto(f'{BASE}/admin/billing/admin-actions/')
    await n.say(
        'Шаг 3.11 · Журнал действий администраторов (А5)',
        'Появилась неизменяемая запись refund_requested: кто, когда и с какой причиной '
        'оформил возврат. Удалить или изменить её из админки нельзя.',
    )
    await n.say(
        'Итог сценария 2',
        'Полный возврат через тестовую ЮKassa с обязательной причиной: доступ закрыт сразу, '
        'пользователь видит «Возвращена», письмо о возврате пришло, письмо об окончании '
        'отменено, действие админа записано в журнал.',
    )
    n.finish()


SCENARIOS = {
    '1': ('scenario-1-purchase', 'Сценарий 1. Покупка подписки «Премиум» (ТЗ 9.2, шаги 1–2)'),
    '2': ('scenario-2-refund', 'Сценарий 2. Возврат средств администратором (ТЗ 9.2, шаг 3)'),
}


async def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    exe = os.environ.get('DEMO_CHROMIUM_PATH') or None
    key = sys.argv[1] if len(sys.argv) > 1 else '1'
    name, title = SCENARIOS[key]
    async with async_playwright() as p, httpx.AsyncClient(timeout=20) as http:
        browser = await p.chromium.launch(executable_path=exe)
        ctx = await browser.new_context(
            viewport={'width': WIDTH, 'height': HEIGHT}, locale='ru-RU', bypass_csp=True
        )
        # Подготовка данных вне записи, чтобы в видео не было пустого экрана
        prepared = await prepare_paid_user(browser, http) if key == '2' else None
        page = await ctx.new_page()
        rec = Recorder(await ctx.new_cdp_session(page))
        await rec.start()
        n = Narrator(page, rec)
        try:
            if key == '1':
                await scenario_purchase(page, n, http)
            else:
                await scenario_refund(page, n, http, prepared)
        finally:
            n.finish()
            t_end = time.monotonic()
            await rec.stop()
            await ctx.close()
            await browser.close()
            rec.encode(n.started, t_end, OUT_DIR / f'{name}.webm')
            write_subtitles(
                n,
                OUT_DIR / f'{name}.vtt',
                OUT_DIR / f'{name}.md',
                title,
            )
            print('Saved to', OUT_DIR)


if __name__ == '__main__':
    asyncio.run(main())
