# Результаты DES сервиса уведомлений (seed=1)

Сценарии:
- **S1** — Кампания 50k + дайджест 8k в 10:00, регистрации 2/с, всплеск 50/с
- **S2** — S1 + отказ auth 10:05-10:20
- **S3** — S1 + троттлинг провайдера до 10/с 10:03-10:13
- **S4** — relay+scheduler лежат 07:00-10:00:05, накоплено ~10.8k, + кампания 50k
- **S5** — S1 + падение sender в 10:04 и audience-worker в 10:01 (посреди страницы)
- **S5b** — S1 + падение audience-worker сразу после COMMIT страницы, до publish
- **S6** — Ночь: 1 регистрация в 30 с в течение часа, без кампаний
- **S7** — S1 + медленный Postgres x10 10:05-10:10
- **S9** — 3 ежечасных кампании 20k + разовая 100k, 3 часа
- **S1-fix** — S1, AIMD max 45/с (90% лимита), bucket burst 0.2 с
- **S1-fix2** — S1-fix + bulk блокируется только ждущими токен critical
- **S2-fix** — S2, событие auth несёт email: transactional без users:batch
- **S3-fix** — S3 + AIMD max 45/с, bucket burst 0.2 с
- **S5b-fix** — S5b, следующая страница через outbox в той же транзакции

## Задержка транзакционных писем (событие → 250 OK от SMTP), секунды

| сценарий | класс | n | p50 | p95 | p99 | max | SLA p95≤60 и p99≤180 |
|---|---|---|---|---|---|---|---|
| S1 | critical | 676 | 1.4 | 11.3 | 11.7 | 12.2 | да |
| S1 | normal | 5950 | 1.5 | 28.6 | 29.6 | 30.1 | да |
| S2 | critical | 692 | 10.5 | 774 | 880 | 902 | **нет** |
| S2 | normal | 5956 | 22.0 | 782 | 878 | 957 | **нет** |
| S3 | critical | 373 | 10.3 | 39.2 | 53.7 | 56.0 | да |
| S3 | normal | 3352 | 20.5 | 146 | 159 | 170 | **нет** |
| S4 | critical | 180 | 1.1 | 10.7 | 16.8 | 24.9 | да |
| S4 | normal | 1542 | 1.1 | 10.4 | 22.5 | 47.7 | да |
| S4 | backlog | 9932 | 5501 | 10275 | 10683 | 10810 | — |
| S4 | expired | 862 |  |  |  |  |  |
| S5 | critical | 652 | 1.4 | 11.3 | 11.8 | 12.2 | да |
| S5 | normal | 5931 | 1.5 | 28.6 | 29.6 | 30.1 | да |
| S5b | critical | 626 | 1.9 | 20.3 | 24.2 | 25.9 | да |
| S5b | normal | 5944 | 1.5 | 86.3 | 92.6 | 94.6 | **нет** |
| S6 | critical | 12 | 0.9 | 1.3 | 1.4 | 1.4 | да |
| S6 | normal | 117 | 1.0 | 1.4 | 1.6 | 1.7 | да |
| S7 | critical | 684 | 1.4 | 11.3 | 11.7 | 12.2 | да |
| S7 | normal | 5981 | 1.5 | 28.6 | 29.6 | 30.1 | да |
| S9 | critical | 2198 | 1.0 | 1.5 | 1.7 | 2.0 | да |
| S9 | normal | 19451 | 1.0 | 1.5 | 1.7 | 3.0 | да |
| S1-fix | critical | 321 | 8.8 | 12.4 | 13.6 | 15.4 | да |
| S1-fix | normal | 3028 | 8.3 | 18.7 | 21.4 | 28.9 | да |
| S1-fix2 | critical | 692 | 1.4 | 11.0 | 11.3 | 14.4 | да |
| S1-fix2 | normal | 6011 | 1.4 | 37.1 | 38.1 | 38.6 | да |
| S2-fix | critical | 654 | 1.4 | 14.7 | 16.1 | 16.4 | да |
| S2-fix | normal | 5992 | 1.4 | 45.1 | 48.9 | 52.3 | да |
| S3-fix | critical | 346 | 9.5 | 52.3 | 65.3 | 65.9 | да |
| S3-fix | normal | 3232 | 9.0 | 81.4 | 95.5 | 100 | **нет** |
| S5b-fix | critical | 642 | 1.4 | 13.8 | 15.2 | 16.2 | да |
| S5b-fix | normal | 6044 | 1.5 | 41.6 | 44.6 | 46.1 | да |

`backlog` — события, случившиеся до восстановления relay (S4), SLA к ним не применяется.

## Массовые запуски: время рендера (мин), завершения и пропускная способность

| сценарий | запуск | размер | создано | отправлено | старт | рендер, мин | завершён | длительность, мин | писем/с |
|---|---|---|---|---|---|---|---|---|---|
| S1 | campaign:promo#0 | 50000 | 50000 | 48985 | 10:00:00 | 34.4 | 10:35:21 | 35.4 | 23.1 |
| S1 | digest:likes#1 | 8000 | 8000 | 7830 | 10:00:00 | 9.2 | 10:12:06 | 12.1 | 10.8 |
| S2 | campaign:promo#0 | 50000 | 50000 | 48986 | 10:00:00 | 45.3 | 10:46:31 | 46.5 | 17.6 |
| S2 | digest:likes#1 | 8000 | 8000 | 7829 | 10:00:00 | 24.1 | 10:25:59 | 26.0 | 5.0 |
| S3 | campaign:promo#0 | 50000 | 3000 | 1662 | 10:00:00 | — | **не завершён** | — | — |
| S3 | digest:likes#1 | 8000 | 3000 | 1451 | 10:00:00 | — | **не завершён** | — | — |
| S4 | campaign:promo#0 | 50000 | 50000 | 49012 | 10:00:05 | 31.7 | 10:32:49 | 32.7 | 25.0 |
| S5 | campaign:promo#0 | 50000 | 50000 | 49020 | 10:00:00 | 33.5 | 10:35:09 | 35.2 | 23.2 |
| S5 | digest:likes#1 | 8000 | 8000 | 7852 | 10:00:00 | 9.5 | 10:11:58 | 12.0 | 10.9 |
| S5b | campaign:promo#0 | 50000 | 2500 | 2441 | 10:00:00 | — | **не завершён** | — | — |
| S5b | digest:likes#1 | 8000 | 8000 | 7847 | 10:00:00 | 7.2 | 10:08:58 | 9.0 | 14.6 |
| S7 | campaign:promo#0 | 50000 | 50000 | 48985 | 10:00:00 | 35.0 | 10:36:06 | 36.1 | 22.6 |
| S7 | digest:likes#1 | 8000 | 8000 | 7832 | 10:00:00 | 9.4 | 10:12:23 | 12.4 | 10.5 |
| S9 | campaign:hourly#0 | 20000 | 20000 | 19583 | 10:00:00 | 23.6 | 10:25:41 | 25.7 | 12.7 |
| S9 | campaign:oneoff#1 | 100000 | 100000 | 97999 | 10:00:00 | 87.1 | 11:28:34 | 88.6 | 18.4 |
| S9 | campaign:hourly#2 | 20000 | 20000 | 19574 | 11:00:00 | 24.4 | 11:26:14 | 26.2 | 12.4 |
| S9 | campaign:hourly#3 | 20000 | 20000 | 19583 | 12:00:00 | 11.1 | 12:12:48 | 12.8 | 25.5 |
| S1-fix | campaign:promo#0 | 50000 | 4500 | 2246 | 10:00:00 | — | **не завершён** | — | — |
| S1-fix | digest:likes#1 | 8000 | 3000 | 1491 | 10:00:00 | — | **не завершён** | — | — |
| S1-fix2 | campaign:promo#0 | 50000 | 50000 | 49000 | 10:00:00 | 23.3 | 10:24:34 | 24.6 | 33.2 |
| S1-fix2 | digest:likes#1 | 8000 | 8000 | 7837 | 10:00:00 | 6.5 | 10:08:24 | 8.4 | 15.5 |
| S2-fix | campaign:promo#0 | 50000 | 50000 | 48994 | 10:00:00 | 45.0 | 10:46:35 | 46.6 | 17.5 |
| S2-fix | digest:likes#1 | 8000 | 8000 | 7821 | 10:00:00 | 22.6 | 10:25:57 | 25.9 | 5.0 |
| S3-fix | campaign:promo#0 | 50000 | 4500 | 2246 | 10:00:00 | — | **не завершён** | — | — |
| S3-fix | digest:likes#1 | 8000 | 3000 | 1491 | 10:00:00 | — | **не завершён** | — | — |
| S5b-fix | campaign:promo#0 | 50000 | 50000 | 49040 | 10:00:00 | 34.4 | 10:35:43 | 35.7 | 22.9 |
| S5b-fix | digest:likes#1 | 8000 | 8000 | 7833 | 10:00:00 | 10.7 | 10:12:34 | 12.6 | 10.4 |

## Пропускная способность bulk

| сценарий | bulk отправлено | средн. писем/с | пик мин., писем/с | интервал |
|---|---|---|---|---|
| S1 | 56815 | 26.8 | 37.5 | 10:00:00–10:35:21 |
| S2 | 56815 | 20.4 | 40.3 | 10:00:00–10:46:30 |
| S3 | 3113 | 25.3 | 28.2 | 10:00:00–10:02:02 |
| S4 | 49012 | 29.5 | 37.8 | 10:05:08–10:32:48 |
| S5 | 56872 | 27.0 | 35.6 | 10:00:00–10:35:09 |
| S5b | 10288 | 19.1 | 31.1 | 10:00:00–10:08:57 |
| S6 | 0 | — | — | — |
| S7 | 56817 | 26.2 | 41.6 | 10:00:00–10:36:06 |
| S9 | 156739 | 19.7 | 35.2 | 10:00:00–12:12:47 |
| S1-fix | 3737 | 30.6 | 35.8 | 10:00:00–10:02:01 |
| S1-fix2 | 56837 | 38.5 | 43.3 | 10:00:00–10:24:34 |
| S2-fix | 56815 | 20.3 | 39.8 | 10:00:00–10:46:34 |
| S3-fix | 3737 | 30.6 | 35.8 | 10:00:00–10:02:01 |
| S5b-fix | 56873 | 26.5 | 39.1 | 10:00:00–10:35:43 |

## Пиковые глубины очередей (по выборкам раз в 5 с)

| сценарий | render.transactional | audience.page | digest.page | retry.10s | dq queued transactional | dq queued bulk | send.email.transactional | send.email.bulk | outbox pending |
|---|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 1 | 0 | 15 | 666 | 3077 | 401 | 427 | 0 |
| S2 | 1883 | 1 | 8 | 15 | 1528 | 3385 | 401 | 467 | 0 |
| S3 | 0 | 0 | 1 | 15 | 2316 | 2969 | 58808 | 6149 | 0 |
| S4 | 8395 | 1 | 0 | 1 | 9012 | 2785 | 442 | 423 | 10786 |
| S5 | 0 | 1 | 0 | 15 | 666 | 3105 | 401 | 453 | 0 |
| S5b | 0 | 0 | 0 | 15 | 1428 | 2905 | 485 | 429 | 0 |
| S6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| S7 | 0 | 1 | 0 | 15 | 666 | 3056 | 401 | 477 | 0 |
| S9 | 0 | 1 | 0 | 2 | 0 | 3445 | 1 | 411 | 0 |
| S1-fix | 0 | 1 | 2 | 15 | 2828 | 3510 | 53410 | 6630 | 0 |
| S1-fix2 | 0 | 1 | 0 | 15 | 1009 | 3760 | 352 | 445 | 0 |
| S2-fix | 0 | 1 | 8 | 15 | 838 | 3883 | 423 | 476 | 0 |
| S3-fix | 0 | 1 | 1 | 15 | 2589 | 3510 | 53209 | 6630 | 0 |
| S5b-fix | 0 | 1 | 0 | 15 | 892 | 3642 | 428 | 457 | 0 |

Очереди RabbitMQ — ready+unacked; `dq queued` — строки dispatch_queue в статусе queued.

## auth internal API

| сценарий | вызовов trans | вызовов bulk | пик trans/с | пик bulk/с | пик всего/с | средн. RPS | заимствований | ошибок | проб | загрузка ср. | загрузка пик. мин |
|---|---|---|---|---|---|---|---|---|---|---|---|
| S1 | 2797 | 116 | 2 | 3 | 5 | 1.33 | 1186 | 0 | 0 | 0.4% | 0.6% |
| S2 | 1434 | 120 | 5 | 3 | 5 | 0.54 | 619 | 6 | 690 | 0.2% | 0.6% |
| S3 | 2752 | 12 | 2 | 3 | 5 | 0.13 | 1140 | 0 | 0 | 0.0% | 0.6% |
| S4 | 1671 | 100 | 5 | 3 | 5 | 0.14 | 505 | 0 | 0 | 0.0% | 0.9% |
| S5 | 2750 | 116 | 2 | 3 | 5 | 1.32 | 1149 | 0 | 0 | 0.4% | 0.6% |
| S5b | 2771 | 21 | 2 | 3 | 5 | 1.49 | 1155 | 0 | 0 | 0.4% | 0.6% |
| S6 | 129 | 0 | 2 | 0 | 2 | 0.04 | 2 | 0 | 0 | 0.0% | 0.0% |
| S7 | 2870 | 116 | 5 | 3 | 5 | 1.34 | 1178 | 0 | 0 | 0.4% | 0.6% |
| S9 | 15821 | 320 | 2 | 2 | 4 | 1.48 | 6472 | 0 | 0 | 0.4% | 0.5% |
| S1-fix | 2809 | 15 | 2 | 3 | 5 | 0.13 | 1189 | 0 | 0 | 0.0% | 0.6% |
| S1-fix2 | 2789 | 116 | 2 | 3 | 5 | 1.55 | 1176 | 0 | 0 | 0.5% | 0.6% |
| S2-fix | 0 | 120 | 0 | 3 | 3 | 0.04 | 20 | 4 | 456 | 0.1% | 0.2% |
| S3-fix | 2767 | 15 | 2 | 3 | 5 | 0.13 | 1149 | 0 | 0 | 0.0% | 0.6% |
| S5b-fix | 2786 | 116 | 2 | 3 | 5 | 1.32 | 1174 | 0 | 0 | 0.4% | 0.6% |

## Postgres notifications (смоделированные операторы)

| сценарий | операторов | операторов/с ср. | операторов/с пик | строк/с ср. | строк/с пик |
|---|---|---|---|---|---|
| S1 | 323868 | 148.2 | 642 | 239.9 | 29412 |
| S2 | 314916 | 110.3 | 642 | 183.6 | 29412 |
| S3 | 136333 | 6.3 | 642 | 12.5 | 29412 |
| S4 | 428774 | 33.6 | 1288 | 48.8 | 13522 |
| S5 | 322979 | 148.8 | 642 | 241.3 | 29412 |
| S5b | 132707 | 71.0 | 642 | 104.2 | 29412 |
| S6 | 2789 | 0.8 | 30 | 0.6 | 24 |
| S7 | 324977 | 145.7 | 642 | 235.2 | 29412 |
| S9 | 1033457 | 95.1 | 559 | 135.8 | 3869 |
| S1-fix | 137616 | 6.4 | 496 | 12.3 | 29266 |
| S1-fix2 | 322755 | 172.6 | 496 | 280.6 | 29266 |
| S2-fix | 322856 | 113.1 | 646 | 183.6 | 29416 |
| S3-fix | 138556 | 6.4 | 496 | 12.3 | 29266 |
| S5b-fix | 324636 | 147.2 | 643 | 238.2 | 29415 |

## Возраст снимка профиля в момент отправки, секунды

| сценарий | p50 | p95 | max |
|---|---|---|---|
| S1 | 78.0 | 145 | 267 |
| S2 | 72.5 | 151 | 266 |
| S3 | 30.5 | 126 | 169 |
| S4 | 77.8 | 206 | 315 |
| S5 | 76.9 | 164 | 510 |
| S5b | 56.4 | 257 | 282 |
| S6 | 0.0 | 0.0 | 0.0 |
| S7 | 76.6 | 167 | 263 |
| S9 | 80.3 | 130 | 199 |
| S1-fix | 16.1 | 84.0 | 91.9 |
| S1-fix2 | 70.0 | 178 | 226 |
| S2-fix | 73.5 | 170 | 236 |
| S3-fix | 26.7 | 86.2 | 98.4 |
| S5b-fix | 75.0 | 192 | 325 |

## Размеры микропачек transactional-worker и пачек outbox-relay

| сценарий | пачек TW | среднее | p50 | p95 | max | доля размера 1 | пачек relay | relay среднее | relay max |
|---|---|---|---|---|---|---|---|---|---|
| S1 | 2797 | 2.4 | 1.0 | 3.0 | 35 | 67% | 4053 | 1.6 | 7 |
| S2 | 1434 | 4.6 | 1.0 | 25.0 | 100 | 62% | 4035 | 1.7 | 7 |
| S3 | 2752 | 2.4 | 1.0 | 3.0 | 35 | 67% | 3985 | 1.6 | 7 |
| S4 | 1671 | 7.5 | 1.0 | 100 | 100 | 85% | 1620 | 7.7 | 500 |
| S5 | 2750 | 2.4 | 1.0 | 3.0 | 35 | 66% | 4010 | 1.6 | 7 |
| S5b | 2771 | 2.4 | 1.0 | 3.0 | 38 | 67% | 4016 | 1.6 | 7 |
| S6 | 129 | 1.0 | 1.0 | 1.0 | 1 | 100% | 129 | 1.0 | 1 |
| S7 | 2870 | 2.3 | 1.0 | 3.0 | 35 | 68% | 4192 | 1.6 | 7 |
| S9 | 15821 | 1.4 | 1.0 | 2.0 | 5 | 68% | 17256 | 1.3 | 4 |
| S1-fix | 2809 | 2.4 | 1.0 | 3.0 | 32 | 66% | 4042 | 1.6 | 7 |
| S1-fix2 | 2789 | 2.4 | 1.0 | 3.0 | 32 | 65% | 4070 | 1.7 | 7 |
| S2-fix | 2766 | 2.4 | 1.0 | 3.0 | 34 | 65% | 3993 | 1.7 | 6 |
| S3-fix | 2767 | 2.4 | 1.0 | 3.0 | 32 | 64% | 4017 | 1.7 | 7 |
| S5b-fix | 2786 | 2.4 | 1.0 | 3.0 | 35 | 65% | 4151 | 1.6 | 7 |

## Надёжность

| сценарий | создано | sent | expired | skipped | не завершено | не отрендерено | дубли | 429 | AIMD ↓/↑ | AIMD min | sweeper pub/lease | устаревших send-сообщ. | устаревших страниц | отложено в retry |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| S1 | 64626 | 63441 | 0 | 1185 | 0 | 0 | 0 | 89 | 20/202 | 17.4 | 0/0 | 0 | 0 | 643 |
| S2 | 64648 | 63463 | 0 | 1185 | 0 | 0 | 0 | 85 | 19/197 | 17.9 | 0/0 | 0 | 0 | 640 |
| S3 | 12541 | 6838 | 0 | 126 | 5577 | 52000 | 0 | 86 | 12/56 | 5.0 | 64257/100 | 0 | 0 | 23782 |
| S4 | 62516 | 60666 | 862 | 988 | 0 | 0 | 0 | 97 | 19/187 | 15.8 | 32/0 | 32 | 0 | 189 |
| S5 | 64583 | 63455 | 0 | 1128 | 0 | 0 | 2 | 85 | 20/201 | 14.8 | 0/25 | 25 | 0 | 636 |
| S5b | 17070 | 16858 | 0 | 212 | 0 | 47500 | 0 | 53 | 5/56 | 17.2 | 0/0 | 0 | 1 | 384 |
| S6 | 129 | 129 | 0 | 0 | 0 | 0 | 0 | 0 | 0/0 | 50.0 | 0/0 | 0 | 0 | 0 |
| S7 | 64665 | 63482 | 0 | 1183 | 0 | 0 | 0 | 96 | 21/206 | 14.7 | 0/0 | 0 | 0 | 647 |
| S9 | 181649 | 178388 | 0 | 3261 | 0 | 0 | 0 | 295 | 63/596 | 14.1 | 0/0 | 0 | 0 | 873 |
| S1-fix | 14127 | 7086 | 0 | 163 | 6878 | 50500 | 0 | 2 | 2/24 | 22.5 | 59400/100 | 0 | 0 | 23788 |
| S1-fix2 | 64703 | 63540 | 0 | 1163 | 0 | 0 | 0 | 2 | 2/24 | 22.5 | 0/0 | 0 | 0 | 435 |
| S2-fix | 64646 | 63461 | 0 | 1185 | 0 | 0 | 0 | 119 | 19/197 | 15.5 | 0/0 | 0 | 0 | 616 |
| S3-fix | 14117 | 7315 | 0 | 163 | 6639 | 50500 | 0 | 49 | 8/42 | 5.0 | 59199/100 | 0 | 0 | 23788 |
| S5b-fix | 64686 | 63559 | 0 | 1127 | 0 | 0 | 0 | 71 | 21/205 | 15.2 | 0/0 | 0 | 1 | 652 |

## Ввод-вывод на одно отправленное письмо

| сценарий | PG операторов | PG строк | сообщений RabbitMQ | HTTP-вызовов | операций Redis | SMTP-попыток |
|---|---|---|---|---|---|---|
| S1 | 5.11 | 8.26 | 1.13 | 0.0469 | 61.34 | 1.001 |
| S2 | 4.96 | 8.26 | 1.13 | 0.0364 | 57.08 | 1.001 |
| S3 | 19.94 | 39.50 | 18.43 | 0.4077 | 41.80 | 1.013 |
| S4 | 7.07 | 10.28 | 1.23 | 0.0292 | 59.28 | 1.002 |
| S5 | 5.09 | 8.25 | 1.13 | 0.0462 | 60.68 | 1.001 |
| S5b | 7.87 | 11.56 | 1.44 | 0.1694 | 59.36 | 1.003 |
| S6 | 21.62 | 17.69 | 2.00 | 1.0000 | 29.08 | 1.000 |
| S7 | 5.12 | 8.26 | 1.13 | 0.0480 | 62.58 | 1.002 |
| S9 | 5.79 | 8.28 | 1.13 | 0.0905 | 62.99 | 1.002 |
| S1-fix | 19.42 | 37.60 | 17.12 | 0.4019 | 34.81 | 1.000 |
| S1-fix2 | 5.08 | 8.26 | 1.12 | 0.0467 | 40.91 | 1.000 |
| S2-fix | 5.09 | 8.26 | 1.13 | 0.0101 | 57.31 | 1.002 |
| S3-fix | 18.94 | 36.51 | 16.60 | 0.3836 | 37.08 | 1.007 |
| S5b-fix | 5.11 | 8.26 | 1.13 | 0.0467 | 61.89 | 1.001 |

## Проверки дизайна

| сценарий | проверка | итог | детали |
|---|---|---|---|
| S1 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S1 | no lost deliveries | PASS | 0 created but not terminal at end |
| S1 | all runs fully rendered | PASS | 0 recipients never rendered |
| S1 | SLA transactional p95<=60s, p99<=180s | PASS | p95=28.4s p99=29.6s (fresh events, n=6626) |
| S1 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S1 | no self-inflicted 429 | **FAIL** | 89 x 429, 20 AIMD halvings |
| S2 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S2 | no lost deliveries | PASS | 0 created but not terminal at end |
| S2 | all runs fully rendered | PASS | 0 recipients never rendered |
| S2 | SLA transactional p95<=60s, p99<=180s | **FAIL** | p95=781.9s p99=878.9s (fresh events, n=6648) |
| S2 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S2 | no self-inflicted 429 | **FAIL** | 85 x 429, 19 AIMD halvings |
| S3 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S3 | no lost deliveries | **FAIL** | 5577 created but not terminal at end |
| S3 | all runs fully rendered | **FAIL** | 52000 recipients never rendered |
| S3 | SLA transactional p95<=60s, p99<=180s | **FAIL** | p95=143.3s p99=158.0s (fresh events, n=3725) |
| S3 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S3 | no self-inflicted 429 | PASS | 86 x 429, 12 AIMD halvings |
| S4 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S4 | no lost deliveries | PASS | 0 created but not terminal at end |
| S4 | all runs fully rendered | PASS | 0 recipients never rendered |
| S4 | SLA transactional p95<=60s, p99<=180s | PASS | p95=10.4s p99=22.2s (fresh events, n=1722) |
| S4 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S4 | no self-inflicted 429 | **FAIL** | 97 x 429, 19 AIMD halvings |
| S5 | no duplicates | PASS | 2 deliveries with >1 accepted SMTP call (crash: allowed) |
| S5 | no lost deliveries | PASS | 0 created but not terminal at end |
| S5 | all runs fully rendered | PASS | 0 recipients never rendered |
| S5 | SLA transactional p95<=60s, p99<=180s | PASS | p95=28.4s p99=29.6s (fresh events, n=6583) |
| S5 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S5 | no self-inflicted 429 | **FAIL** | 85 x 429, 20 AIMD halvings |
| S5b | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call (crash: allowed) |
| S5b | no lost deliveries | PASS | 0 created but not terminal at end |
| S5b | all runs fully rendered | **FAIL** | 47500 recipients never rendered |
| S5b | SLA transactional p95<=60s, p99<=180s | **FAIL** | p95=85.3s p99=92.2s (fresh events, n=6570) |
| S5b | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S5b | no self-inflicted 429 | **FAIL** | 53 x 429, 5 AIMD halvings |
| S6 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S6 | no lost deliveries | PASS | 0 created but not terminal at end |
| S6 | all runs fully rendered | PASS | 0 recipients never rendered |
| S6 | SLA transactional p95<=60s, p99<=180s | PASS | p95=1.4s p99=1.6s (fresh events, n=129) |
| S6 | auth within quota | PASS | peak 2 calls/s, quota 5 |
| S6 | no self-inflicted 429 | PASS | 0 x 429, 0 AIMD halvings |
| S7 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S7 | no lost deliveries | PASS | 0 created but not terminal at end |
| S7 | all runs fully rendered | PASS | 0 recipients never rendered |
| S7 | SLA transactional p95<=60s, p99<=180s | PASS | p95=28.4s p99=29.6s (fresh events, n=6665) |
| S7 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S7 | no self-inflicted 429 | **FAIL** | 96 x 429, 21 AIMD halvings |
| S9 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S9 | no lost deliveries | PASS | 0 created but not terminal at end |
| S9 | all runs fully rendered | PASS | 0 recipients never rendered |
| S9 | SLA transactional p95<=60s, p99<=180s | PASS | p95=1.5s p99=1.7s (fresh events, n=21649) |
| S9 | auth within quota | PASS | peak 4 calls/s, quota 5 |
| S9 | no self-inflicted 429 | **FAIL** | 295 x 429, 63 AIMD halvings |
| S1-fix | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S1-fix | no lost deliveries | **FAIL** | 6878 created but not terminal at end |
| S1-fix | all runs fully rendered | **FAIL** | 50500 recipients never rendered |
| S1-fix | SLA transactional p95<=60s, p99<=180s | PASS | p95=18.4s p99=21.3s (fresh events, n=3349) |
| S1-fix | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S1-fix | no self-inflicted 429 | **FAIL** | 2 x 429, 2 AIMD halvings |
| S1-fix2 | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S1-fix2 | no lost deliveries | PASS | 0 created but not terminal at end |
| S1-fix2 | all runs fully rendered | PASS | 0 recipients never rendered |
| S1-fix2 | SLA transactional p95<=60s, p99<=180s | PASS | p95=37.0s p99=38.1s (fresh events, n=6703) |
| S1-fix2 | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S1-fix2 | no self-inflicted 429 | **FAIL** | 2 x 429, 2 AIMD halvings |
| S2-fix | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S2-fix | no lost deliveries | PASS | 0 created but not terminal at end |
| S2-fix | all runs fully rendered | PASS | 0 recipients never rendered |
| S2-fix | SLA transactional p95<=60s, p99<=180s | PASS | p95=44.6s p99=48.7s (fresh events, n=6646) |
| S2-fix | auth within quota | PASS | peak 3 calls/s, quota 5 |
| S2-fix | no self-inflicted 429 | **FAIL** | 119 x 429, 19 AIMD halvings |
| S3-fix | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call |
| S3-fix | no lost deliveries | **FAIL** | 6639 created but not terminal at end |
| S3-fix | all runs fully rendered | **FAIL** | 50500 recipients never rendered |
| S3-fix | SLA transactional p95<=60s, p99<=180s | **FAIL** | p95=79.7s p99=94.9s (fresh events, n=3578) |
| S3-fix | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S3-fix | no self-inflicted 429 | PASS | 49 x 429, 8 AIMD halvings |
| S5b-fix | no duplicates | PASS | 0 deliveries with >1 accepted SMTP call (crash: allowed) |
| S5b-fix | no lost deliveries | PASS | 0 created but not terminal at end |
| S5b-fix | all runs fully rendered | PASS | 0 recipients never rendered |
| S5b-fix | SLA transactional p95<=60s, p99<=180s | PASS | p95=41.3s p99=44.5s (fresh events, n=6686) |
| S5b-fix | auth within quota | PASS | peak 5 calls/s, quota 5 |
| S5b-fix | no self-inflicted 429 | **FAIL** | 71 x 429, 21 AIMD halvings |

## Инварианты модели (assert, все прошли)

- **S1**: conservation: created 64626 = sent 63441 + expired 0 + skipped 1185 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63441 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:35:25, no cap hit; run sizes and micro-batch sizes within bounds
- **S2**: conservation: created 64648 = sent 63463 + expired 0 + skipped 1185 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63463 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:46:35, no cap hit; run sizes and micro-batch sizes within bounds
- **S3**: conservation: created 12541 = sent 6838 + expired 0 + skipped 126 + in-flight 5577; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 6838 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; run sizes and micro-batch sizes within bounds
- **S4**: conservation: created 62516 = sent 60666 + expired 862 + skipped 988 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 60666 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:32:50, no cap hit; run sizes and micro-batch sizes within bounds
- **S5**: conservation: created 64583 = sent 63455 + expired 0 + skipped 1128 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63457 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:35:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S5b**: conservation: created 17070 = sent 16858 + expired 0 + skipped 212 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 16858 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:30:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S6**: conservation: created 129 = sent 129 + expired 0 + skipped 0 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 4% of limit, 129 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 03:00:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S7**: conservation: created 64665 = sent 63482 + expired 0 + skipped 1183 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63482 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:36:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S9**: conservation: created 181649 = sent 178388 + expired 0 + skipped 3261 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 178388 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 13:00:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S1-fix**: conservation: created 14127 = sent 7086 + expired 0 + skipped 163 + in-flight 6878; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 7086 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; run sizes and micro-batch sizes within bounds
- **S1-fix2**: conservation: created 64703 = sent 63540 + expired 0 + skipped 1163 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63540 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:30:10, no cap hit; run sizes and micro-batch sizes within bounds
- **S2-fix**: conservation: created 64646 = sent 63461 + expired 0 + skipped 1185 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63461 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:46:35, no cap hit; run sizes and micro-batch sizes within bounds
- **S3-fix**: conservation: created 14117 = sent 7315 + expired 0 + skipped 163 + in-flight 6639; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 7315 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; run sizes and micro-batch sizes within bounds
- **S5b-fix**: conservation: created 64686 = sent 63559 + expired 0 + skipped 1127 + in-flight 0; queue/in-flight counters consistent and non-negative; provider limit never exceeded in any trailing 1 s window (max load 100% of limit, 63559 accepted); every sent delivery has >=1 accepted SMTP call, skipped/expired have none; drained: system idle at 10:35:45, no cap hit; run sizes and micro-batch sizes within bounds

## Журнал падений

- S5: 10:01:00 audience-worker-0 crashed (Interrupt), page redelivered: False
- S5: 10:04:00 sender replica 0 killed, 25 messages in hand
- S5b: 10:01:01 audience-worker-0 crashed (WorkerCrash), page redelivered: True
- S5b-fix: 10:01:01 audience-worker-0 crashed (WorkerCrash), page redelivered: True
