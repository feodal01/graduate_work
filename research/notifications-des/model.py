"""Discrete-event simulation (SimPy) of the notifications service, design v4.

Pipeline (docs/diagrams/notifications-arch/*.mmd):
sources -> auth outbox-relay -> Kafka -> event-bridge -> notifications PG (+outbox)
-> outbox-relay -> RabbitMQ -> transactional / audience / digest workers (+ auth, ugc, movies)
-> PG dispatch_queue -> dispatcher -> send.email.* -> sender-email (+ Redis bucket) -> provider.

Run: uv run python model.py --scenario S1|...|all --seed 1
"""

from __future__ import annotations

import argparse
import heapq
import math
from collections import Counter, defaultdict, deque
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, TypedDict

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import simpy  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

Proc = Generator[simpy.Event, Any, None]

H = 3600.0
M = 60.0
RESULTS_DIR = Path(__file__).resolve().parent / 'results'


def hms(h: int, m: int = 0, s: float = 0.0) -> float:
    return h * H + m * M + s


def clock(t: float) -> str:
    sec = int(round(t))
    return f'{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}'


# --------------------------------------------------------------------------------------
# Parameters
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Job:
    """A scheduled bulk job: campaign run or digest window."""

    key: str
    due: float  # clock time when it becomes due
    size: int  # recipients


@dataclass(frozen=True)
class Params:
    """Every number of the model. Durations in seconds, clock values in seconds since midnight.

    Service times are lognormal with the given mean (sigma = latency_sigma unless stated).
    """

    # Horizon
    start: float = hms(9, 59)  # simulation clock at start
    max_end: float = hms(16)  # hard stop if the system never drains
    sample_period_s: float = 5.0  # sampling period of queue depths for charts
    idle_check_s: float = 5.0  # run-until-drained monitor period

    # Workload
    signup_phases: tuple[tuple[float, float, float], ...] = (
        (hms(9, 59), hms(10, 2), 2.0),
        (hms(10, 2), hms(10, 3), 50.0),
        (hms(10, 3), hms(10, 30), 2.0),
    )  # (from, to, Poisson rate/s) of auth events (signup / password reset)
    critical_share: float = 0.10  # share of auth events that are critical (password reset)
    likes_rps: float = 5.0  # Poisson likes -> pending_digest inserts (PG load only)
    campaigns: tuple[Job, ...] = (Job('promo', hms(10), 50_000),)
    digests: tuple[Job, ...] = (Job('likes', hms(10), 8_000),)
    opt_out_share: float = 0.02  # bulk recipients filtered at render (status=skipped)
    likes_per_digest_recipient: int = 3  # pending_digest rows bound to a window per recipient
    expires_critical_s: float = 30 * M  # expires_at of password reset
    expires_normal_s: float = 24 * H  # expires_at of welcome
    latency_sigma: float = 0.25  # lognormal sigma for all service times except SMTP

    # auth outbox-relay -> Kafka -> event-bridge -> notifications PG
    auth_relay_poll_s: float = 1.0
    kafka_latency_s: float = 0.020
    bridge_tx_s: float = 0.005  # INSERT notification + outbox + offset, sequential consumer

    # notifications outbox-relay
    relay_poll_s: float = 5.0  # fallback poll; NOTIFY wakes it immediately
    relay_batch_max: int = 500
    relay_select_s: float = 0.002
    relay_mark_s: float = 0.002
    confirm_base_s: float = 0.010  # publisher-confirm latency per batch (relay, dispatcher)
    confirm_per_msg_s: float = 0.00005

    # scheduler
    scheduler_tick_s: float = 10.0
    scheduler_tx_s: float = 0.004

    # RabbitMQ
    retry_delay_s: float = 10.0  # retry.10s TTL before dead-lettering back

    # transactional-worker
    tw_replicas: int = 2
    tw_batch_max: int = 100  # micro-batch size = prefetch
    tw_max_wait_s: float = 1.0  # max age of the first message in a micro-batch
    tw_idle_flush_s: float = 0.050  # flush early when no message arrived for this long
    tw_tx_base_s: float = 0.005
    tw_tx_per_row_s: float = 0.00005

    # audience-worker / digest-worker
    aw_replicas: int = 2
    dw_replicas: int = 2
    page_size: int = 500  # audience page and digest chunk size
    gate_min_rows: int = 50  # backpressure: queued rows of a run < max(this, ewma*horizon)
    gate_horizon_s: float = 60.0
    bulk_check_s: float = 0.002  # cursor version + run backlog count
    bulk_tx_per_row_s: float = 0.00006  # 30 ms per 500 rows
    ugc_batch_s: float = 0.030
    movies_batch_s: float = 0.020
    movies_ids_per_call: int = 200

    # auth internal API
    auth_workers: int = 4  # uvicorn workers
    auth_base_s: float = 0.010
    auth_per_id_s: float = 0.00008
    auth_rps_trans: float = 3.0  # client-side token bucket, transactional lane
    auth_rps_bulk: float = 2.0  # client-side token bucket, bulk lane (borrowing allowed)
    auth_bucket_burst: float = 1.0  # bucket capacity in tokens per lane
    auth_quota_rps: float = 5.0  # agreed quota, used by the over-quota check
    auth_fail_s: float = 1.0  # client timeout when auth is down
    probe_period_s: float = 5.0  # breaker health probe period
    probe_jitter_s: float = 5.0  # + uniform(0, jitter)

    # dispatcher
    dispatcher_tick_s: float = 1.0  # wakes on NOTIFY or every tick
    dispatch_window_s: float = 10.0  # in-flight window = rate * this
    catchup_age_s: float = 600.0  # normal events older than this become 'catchup'
    bulk_min_cap_rps: float = 10.0  # bulk_min = min(cap, share*rate), 0 while critical queued
    bulk_min_share: float = 0.2
    drr_quantum: int = 50  # deficit round-robin quantum between bulk runs
    dispatch_select_s: float = 0.003
    dispatch_per_row_s: float = 0.00001

    # sender-email
    sender_replicas: int = 4
    sender_concurrency: int = 25
    claim_s: float = 0.003
    final_s: float = 0.003
    smtp_pre_s: float = 0.050  # connect/TLS/envelope until the provider counts the message
    smtp_post_s: float = 0.250  # DATA until 250 OK (SMTP total ~300 ms)
    smtp_sigma: float = 0.5
    smtp_429_s: float = 0.020  # remaining time of a 429 reply
    token_poll_s: float = 0.050  # sender Redis poll period (Redis-ops estimate, blocked bulk)
    bucket_burst_s: float = 1.0  # provider bucket capacity = max(1, rate * this)
    reserve_horizon_s: float = 1.0  # bulk token only if tokens >= max(1, trans_ewma * this)
    bulk_block_rule: Literal['critical_in_flight', 'critical_waiting'] = 'critical_in_flight'
    lease_trans_s: float = 60.0
    lease_bulk_s: float = 300.0

    # provider + AIMD (rate kept in Redis)
    provider_limit_rps: float = 50.0
    aimd_max: float = 50.0
    aimd_min: float = 5.0
    aimd_step: float = 2.0  # +step after aimd_increase_after_s without 429
    aimd_increase_after_s: float = 10.0
    aimd_decrease_gap_s: float = 2.0  # halve at most once per this
    ewma_tau_s: float = 10.0  # time constant of send-rate EWMAs

    # sweeper
    sweeper_period_s: float = 60.0
    sweep_published_trans_s: float = 120.0
    sweep_published_bulk_min_s: float = 120.0  # bulk: max(this, factor*inflight/bulk_rate)
    sweep_published_bulk_factor: float = 3.0
    sweep_s: float = 0.005

    # Failures, None = off. Windows are (from, to) clock values.
    auth_outage: tuple[float, float] | None = None
    provider_throttle: tuple[float, float, float] | None = None  # (from, to, limit rps)
    pg_slow: tuple[float, float, float] | None = None  # (from, to, latency multiplier)
    relay_down: tuple[float, float] | None = None  # notifications outbox-relay
    scheduler_down: tuple[float, float] | None = None
    sender_crash_at: float | None = None  # kill sender replica 0 (all its slots)
    audience_crash_at: float | None = None
    audience_crash_mode: Literal['mid_page', 'after_commit'] = 'mid_page'
    restart_s: float = 30.0  # container restart after a crash

    # Design switches for what-if variants
    next_page_via_outbox: bool = False  # next audience.page via outbox in the page tx
    welcome_profile_in_event: bool = False  # auth event carries email: no users:batch call

    # Reporting
    backlog_cutoff: float | None = None  # transactional events before this are 'backlog'
    sla_p95_s: float = 60.0
    sla_p99_s: float = 180.0


# --------------------------------------------------------------------------------------
# Entities
# --------------------------------------------------------------------------------------


class Lane(StrEnum):
    TRANS = 'transactional'
    BULK = 'bulk'


class St(StrEnum):
    QUEUED = 'queued'
    PUBLISHED = 'published'
    SENDING = 'sending'
    SENT = 'sent'
    EXPIRED = 'expired'
    SKIPPED = 'skipped'


TERMINAL = frozenset({St.SENT, St.EXPIRED, St.SKIPPED})


class AuthUnavailable(Exception):
    pass


class WorkerCrash(Exception):
    pass


@dataclass(eq=False)
class Run:
    """campaign_run or digest_window."""

    id: int
    kind: Literal['campaign', 'digest']
    key: str
    size: int
    started_at: float
    queued: deque[Delivery] = field(default_factory=deque)  # may hold stale entries
    queued_count: int = 0
    cursor_version: int = 0
    paged: int = 0  # recipients already rendered
    chunks_left: int = 0
    paging_done_at: float | None = None
    completed_at: float | None = None
    created: int = 0
    pending: int = 0  # non-terminal deliveries
    sent: int = 0
    sent_tick: int = 0
    ewma: float = 0.0
    deficit: int = 0
    last_sent_at: float | None = None


@dataclass(slots=True, eq=False)
class Delivery:
    id: int
    lane: Lane
    critical: bool
    run: Run | None
    event_time: float
    snapshot_time: float
    expires_at: float
    status: St
    published_at: float = 0.0
    lease_until: float = 0.0
    claim_seq: int = 0
    smtp_accepted: int = 0


@dataclass(slots=True)
class AuthEvent:
    id: int
    time: float
    critical: bool


@dataclass(slots=True)
class PageMsg:
    run: Run
    version: int


@dataclass(slots=True)
class ChunkMsg:
    run: Run
    size: int


QueueName = Literal['render', 'audience', 'digest']


class MQueue:
    """RabbitMQ queue: unbounded FIFO + unacked counter."""

    def __init__(self, env: simpy.Environment) -> None:
        self.store = simpy.Store(env)
        self.unacked = 0

    @property
    def depth(self) -> int:
        return len(self.store.items) + self.unacked


class Notifier:
    """PG LISTEN/NOTIFY-like wake-up for a single waiting process."""

    def __init__(self, env: simpy.Environment) -> None:
        self.env = env
        self.event = env.event()

    def notify(self) -> None:
        if not self.event.triggered:
            self.event.succeed()

    def wait(self, timeout: float | None) -> Proc:
        ev = self.event
        if timeout is None:
            yield ev
        else:
            yield ev | self.env.timeout(timeout)
        if ev.triggered:
            self.event = self.env.event()


class LaneBuckets:
    """Client-side auth token buckets per lane; a lane may borrow the other lane's tokens."""

    def __init__(self, rates: dict[Lane, float], burst: float, now: float) -> None:
        self.rates = rates
        self.burst = burst
        self.tokens = {lane: burst for lane in rates}
        self.last = now

    def _refill(self, now: float) -> None:
        dt = now - self.last
        for lane, rate in self.rates.items():
            self.tokens[lane] = min(self.burst, self.tokens[lane] + dt * rate)
        self.last = now

    def try_take(self, lane: Lane, now: float) -> Lane | None:
        self._refill(now)
        other = Lane.BULK if lane is Lane.TRANS else Lane.TRANS
        for source in (lane, other):
            if self.tokens[source] >= 1.0:
                self.tokens[source] -= 1.0
                return source
        return None

    def wait_time(self) -> float:
        return max(1e-4, min((1.0 - self.tokens[ln]) / r for ln, r in self.rates.items()))


@dataclass(slots=True, eq=False)
class Waiter:
    event: simpy.Event
    lane: Lane
    critical: bool
    t0: float
    cancelled: bool = False


class Provider:
    """Email provider: sliding 1 s window limit, 429 above it."""

    def __init__(self, p: Params) -> None:
        self.p = p
        self.window: deque[float] = deque()
        self.accepted: list[float] = []
        self.rejected = 0

    def limit(self, now: float) -> float:
        thr = self.p.provider_throttle
        if thr is not None and thr[0] <= now < thr[1]:
            return thr[2]
        return self.p.provider_limit_rps

    def request(self, now: float) -> bool:
        while self.window and self.window[0] <= now - 1.0:
            self.window.popleft()
        if len(self.window) >= self.limit(now):
            self.rejected += 1
            return False
        self.window.append(now)
        self.accepted.append(now)
        return True


class ProviderGate:
    """One provider token bucket in Redis, refill = AIMD rate, reserve for transactional."""

    def __init__(self, sim: Sim) -> None:
        self.sim = sim
        p = sim.p
        self.rate = p.aimd_max
        self.tokens = self.capacity()
        self.last = sim.env.now
        self.trans: deque[Waiter] = deque()
        self.bulk: deque[Waiter] = deque()
        self.wake = Notifier(sim.env)
        self.last_429 = -math.inf
        self.last_decrease = -math.inf
        self.last_increase = sim.env.now
        self.decreases = 0
        self.increases = 0

    def capacity(self) -> float:
        return max(1.0, self.rate * self.sim.p.bucket_burst_s)

    def reserve(self) -> float:
        return max(1.0, self.sim.trans_rate_ewma * self.sim.p.reserve_horizon_s)

    def _refill(self, now: float) -> None:
        self.tokens = min(self.capacity(), self.tokens + (now - self.last) * self.rate)
        self.last = now

    def acquire(self, d: Delivery) -> Waiter:
        w = Waiter(self.sim.env.event(), d.lane, d.critical, self.sim.env.now)
        (self.trans if d.lane is Lane.TRANS else self.bulk).append(w)
        self.wake.notify()
        return w

    def bulk_blocked(self) -> bool:
        if self.sim.p.bulk_block_rule == 'critical_in_flight':
            return self.sim.crit_inflight > 0
        return any(w.critical and not w.cancelled for w in self.trans)

    def on_429(self, now: float) -> None:
        p = self.sim.p
        self.last_429 = now
        if now - self.last_decrease >= p.aimd_decrease_gap_s:
            self.rate = max(p.aimd_min, self.rate / 2)
            self.tokens = min(self.tokens, self.capacity())
            self.last_decrease = now
            self.decreases += 1
            self.sim.m.redis_ops += 1

    def aimd_ticker(self) -> Proc:
        p = self.sim.p
        env = self.sim.env
        while True:
            yield env.timeout(1.0)
            now = env.now
            quiet = now - self.last_429 >= p.aimd_increase_after_s
            if quiet and now - self.last_increase >= p.aimd_increase_after_s:
                if self.rate < p.aimd_max:
                    self.rate = min(p.aimd_max, self.rate + p.aimd_step)
                    self.increases += 1
                    self.sim.m.redis_ops += 1
                    self.wake.notify()
                self.last_increase = now

    def run(self) -> Proc:
        poll = self.sim.p.token_poll_s
        env = self.sim.env
        while True:
            self._refill(env.now)
            while self.trans and self.trans[0].cancelled:
                self.trans.popleft()
            while self.bulk and self.bulk[0].cancelled:
                self.bulk.popleft()
            while self.trans and self.tokens >= 1.0:
                w = self.trans.popleft()
                if not w.cancelled:
                    self.tokens -= 1.0
                    w.event.succeed()
            if not self.trans:
                while self.bulk and not self.bulk_blocked() and self.tokens >= self.reserve():
                    w = self.bulk.popleft()
                    if not w.cancelled:
                        self.tokens -= 1.0
                        w.event.succeed()
            dt: float | None
            if self.trans:
                dt = (1.0 - self.tokens) / self.rate
            elif self.bulk:
                need = self.reserve()
                if self.bulk_blocked() or need > self.capacity():
                    dt = poll
                else:
                    dt = (need - self.tokens) / self.rate
            else:
                dt = None
            yield from self.wake.wait(None if dt is None else max(dt, 1e-4))


# --------------------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------------------


@dataclass
class Metrics:
    latencies: list[tuple[float, float, str]] = field(default_factory=list)  # (t, lat, class)
    snapshot_ages: list[float] = field(default_factory=list)
    tw_batches: list[int] = field(default_factory=list)
    relay_batches: list[int] = field(default_factory=list)
    auth_calls: list[tuple[float, Lane, int]] = field(default_factory=list)
    auth_failures: int = 0
    auth_probes: int = 0
    auth_borrowed: int = 0
    auth_busy_by_min: dict[int, float] = field(default_factory=lambda: defaultdict(float))
    ugc_calls: int = 0
    movies_calls: int = 0
    pg_stmts: Counter[int] = field(default_factory=Counter)  # per second
    pg_rows: Counter[int] = field(default_factory=Counter)
    redis_ops: int = 0
    rabbit_msgs: int = 0
    rabbit_redeliveries: int = 0
    smtp_attempts: int = 0
    defers: Counter[str] = field(default_factory=Counter)
    stale_pages: int = 0
    stale_send_msgs: int = 0
    lease_lost_sends: int = 0
    swept_published: Counter[Lane] = field(default_factory=Counter)
    swept_lease: Counter[Lane] = field(default_factory=Counter)
    requeued_429: int = 0
    sent_bins: dict[Lane, Counter[int]] = field(
        default_factory=lambda: {Lane.TRANS: Counter(), Lane.BULK: Counter()}
    )
    sample_t: list[float] = field(default_factory=list)
    series: dict[str, list[float]] = field(default_factory=dict)
    crash_log: list[str] = field(default_factory=list)
    skipped_occurrences: int = 0


# --------------------------------------------------------------------------------------
# Simulation
# --------------------------------------------------------------------------------------


class Sim:
    def __init__(self, p: Params, seed: int) -> None:
        self.p = p
        self.env = simpy.Environment(initial_time=p.start)
        self.rng = np.random.default_rng(seed)
        self.m = Metrics()
        env = self.env

        self.queues: dict[QueueName, MQueue] = {
            'render': MQueue(env),
            'audience': MQueue(env),
            'digest': MQueue(env),
        }
        self.send_store = simpy.PriorityStore(env)
        self.send_ready: dict[Lane, int] = {Lane.TRANS: 0, Lane.BULK: 0}
        self.send_unacked: dict[Lane, int] = {Lane.TRANS: 0, Lane.BULK: 0}
        self.send_seq = 0
        self.retry_depth = 0

        self.auth_outbox: list[AuthEvent] = []
        self.bridge_store = simpy.Store(env)
        self.kafka_inflight = 0
        self.outbox: deque[tuple[QueueName, PageMsg | ChunkMsg | AuthEvent]] = deque()
        self.relay_wake = Notifier(env)
        self.dispatch_wake = Notifier(env)
        self.next_event_id = 0

        self.deliveries: list[Delivery] = []
        self.status_count: Counter[St] = Counter()
        self.queued_count: dict[Lane, int] = {Lane.TRANS: 0, Lane.BULK: 0}
        self.crit_heap: list[tuple[float, int, Delivery]] = []
        self.normal_heap: list[tuple[float, int, Delivery]] = []
        self.catchup_heap: list[tuple[float, int, Delivery]] = []
        self.published: dict[Lane, dict[int, Delivery]] = {Lane.TRANS: {}, Lane.BULK: {}}
        self.sending: dict[Lane, dict[int, Delivery]] = {Lane.TRANS: {}, Lane.BULK: {}}
        self.crit_inflight = 0
        self.runs: list[Run] = []
        self.drr_offset = 0

        self.auth_server = simpy.Resource(env, capacity=p.auth_workers)
        self.auth_buckets = LaneBuckets(
            {Lane.TRANS: p.auth_rps_trans, Lane.BULK: p.auth_rps_bulk},
            p.auth_bucket_burst,
            env.now,
        )
        self.trans_rate_ewma = 0.0
        self.bulk_rate_ewma = 0.0
        self.tick_sent: dict[Lane, int] = {Lane.TRANS: 0, Lane.BULK: 0}
        self.provider = Provider(p)
        self.gate = ProviderGate(self)

        self.aw_holding: list[PageMsg | ChunkMsg | None] = [None] * p.aw_replicas
        self.aw_procs: list[simpy.Process] = []
        self.aw_crash_armed = False
        self.sender_down_until = [0.0] * p.sender_replicas
        self.slot_item: dict[tuple[int, int], simpy.PriorityItem | None] = {}
        self.slot_waiter: dict[tuple[int, int], Waiter | None] = {}
        self.slot_procs: dict[int, list[simpy.Process]] = {}

        self.stop = env.event()
        self.hit_cap = False
        self.sources_end = self._sources_end()

    # ---------------------------------------------------------------- helpers

    def _sources_end(self) -> float:
        p = self.p
        ends = [p.start]
        ends += [ph[1] for ph in p.signup_phases]
        ends += [j.due for j in p.campaigns + p.digests]
        for w in (p.auth_outage, p.relay_down, p.scheduler_down):
            if w is not None:
                ends.append(w[1])
        for w3 in (p.provider_throttle, p.pg_slow):
            if w3 is not None:
                ends.append(w3[1])
        for t in (p.sender_crash_at, p.audience_crash_at):
            if t is not None:
                ends.append(t + p.restart_s)
        return max(ends) + p.scheduler_tick_s

    @staticmethod
    def in_window(w: tuple[float, float] | tuple[float, float, float] | None, t: float) -> bool:
        return w is not None and w[0] <= t < w[1]

    def lat(self, mean: float, sigma: float | None = None) -> float:
        s = self.p.latency_sigma if sigma is None else sigma
        return mean * math.exp(s * float(self.rng.standard_normal()) - 0.5 * s * s)

    def pg(self, mean_s: float, stmts: int, rows: int) -> Proc:
        now = self.env.now
        self.m.pg_stmts[int(now)] += stmts
        self.m.pg_rows[int(now)] += rows
        mult = self.p.pg_slow[2] if self.p.pg_slow and self.in_window(self.p.pg_slow, now) else 1
        yield self.env.timeout(self.lat(mean_s) * mult)

    def confirm_time(self, n: int) -> float:
        return self.lat(self.p.confirm_base_s + self.p.confirm_per_msg_s * n)

    def publish(self, name: QueueName, msg: PageMsg | ChunkMsg | AuthEvent) -> None:
        self.m.rabbit_msgs += 1
        self.queues[name].store.put(msg)

    def defer(self, name: QueueName, msg: PageMsg | ChunkMsg | AuthEvent) -> None:
        self.m.rabbit_msgs += 1
        self.m.defers[name] += 1
        self.retry_depth += 1
        self.env.process(self._dead_letter(name, msg))

    def _dead_letter(self, name: QueueName, msg: PageMsg | ChunkMsg | AuthEvent) -> Proc:
        yield self.env.timeout(self.p.retry_delay_s)
        self.retry_depth -= 1
        self.publish(name, msg)

    def wait_until_up(self, w: tuple[float, float] | None) -> Proc:
        if w is not None and self.in_window(w, self.env.now):
            yield self.env.timeout(w[1] - self.env.now)

    def gate_limit(self, run: Run) -> float:
        return max(self.p.gate_min_rows, run.ewma * self.p.gate_horizon_s)

    def bulk_rate(self) -> float:
        return max(self.bulk_rate_ewma, 1.0)

    # ---------------------------------------------------------------- delivery state

    def create_delivery(
        self, lane: Lane, critical: bool, run: Run | None, event_time: float, snapshot: float
    ) -> None:
        p = self.p
        now = self.env.now
        if lane is Lane.TRANS:
            expires = event_time + (p.expires_critical_s if critical else p.expires_normal_s)
            status = St.QUEUED
        else:
            expires = math.inf
            status = St.SKIPPED if self.rng.random() < p.opt_out_share else St.QUEUED
        d = Delivery(
            len(self.deliveries), lane, critical, run, event_time, snapshot, expires, status
        )
        self.deliveries.append(d)
        self._enter(d)
        if run is not None:
            run.created += 1
            if status is not St.SKIPPED:
                run.pending += 1
        if status is St.QUEUED:
            self._push(d, requeue=False, now=now)

    def _push(self, d: Delivery, requeue: bool, now: float) -> None:
        if d.lane is Lane.BULK:
            assert d.run is not None
            if requeue:
                d.run.queued.appendleft(d)
            else:
                d.run.queued.append(d)
            return
        entry = (d.event_time, d.id, d)
        if d.critical:
            heapq.heappush(self.crit_heap, entry)
        elif d.event_time >= now - self.p.catchup_age_s:
            heapq.heappush(self.normal_heap, entry)
        else:
            heapq.heappush(self.catchup_heap, entry)

    def _enter(self, d: Delivery) -> None:
        s = d.status
        self.status_count[s] += 1
        if s is St.QUEUED:
            self.queued_count[d.lane] += 1
            if d.run is not None:
                d.run.queued_count += 1
        elif s is St.PUBLISHED or s is St.SENDING:
            (self.published if s is St.PUBLISHED else self.sending)[d.lane][d.id] = d
            if d.critical:
                self.crit_inflight += 1

    def _leave(self, d: Delivery) -> None:
        s = d.status
        self.status_count[s] -= 1
        if s is St.QUEUED:
            self.queued_count[d.lane] -= 1
            if d.run is not None:
                d.run.queued_count -= 1
        elif s is St.PUBLISHED or s is St.SENDING:
            del (self.published if s is St.PUBLISHED else self.sending)[d.lane][d.id]
            if d.critical:
                self.crit_inflight -= 1
                if self.crit_inflight == 0:
                    self.gate.wake.notify()

    def set_status(self, d: Delivery, new: St) -> None:
        old = d.status
        assert old not in TERMINAL, f'transition from terminal {old}'
        now = self.env.now
        self._leave(d)
        d.status = new
        self._enter(d)
        if new is St.QUEUED:
            self._push(d, requeue=True, now=now)
        if new in TERMINAL and d.run is not None:
            d.run.pending -= 1
            self._check_run_done(d.run)
        if new is St.SENT:
            self.tick_sent[d.lane] += 1
            self.m.sent_bins[d.lane][int(now)] += 1
            if d.run is not None:
                d.run.sent += 1
                d.run.sent_tick += 1
                d.run.last_sent_at = now

    def _check_run_done(self, run: Run) -> None:
        if run.paging_done_at is not None and run.pending == 0 and run.completed_at is None:
            run.completed_at = self.env.now

    # ---------------------------------------------------------------- sources

    def signup_source(self) -> Proc:
        env = self.env
        for start, end, rate in self.p.signup_phases:
            if env.now < start:
                yield env.timeout(start - env.now)
            while True:
                dt = float(self.rng.exponential(1.0 / rate))
                if env.now + dt >= end:
                    break
                yield env.timeout(dt)
                self.next_event_id += 1
                critical = bool(self.rng.random() < self.p.critical_share)
                self.auth_outbox.append(AuthEvent(self.next_event_id, env.now, critical))
            if env.now < end:
                yield env.timeout(end - env.now)

    def likes_source(self) -> Proc:
        env = self.env
        end = max(ph[1] for ph in self.p.signup_phases)
        while True:
            yield env.timeout(float(self.rng.exponential(1.0 / self.p.likes_rps)))
            if env.now >= end:
                return
            self.m.pg_stmts[int(env.now)] += 2
            self.m.pg_rows[int(env.now)] += 2

    def auth_relay(self) -> Proc:
        while True:
            yield self.env.timeout(self.p.auth_relay_poll_s)
            if self.auth_outbox:
                batch, self.auth_outbox = self.auth_outbox, []
                self.kafka_inflight += len(batch)
                self.env.process(self._kafka(batch))

    def _kafka(self, batch: list[AuthEvent]) -> Proc:
        yield self.env.timeout(self.lat(self.p.kafka_latency_s))
        for ev in batch:
            self.bridge_store.put(ev)

    def event_bridge(self) -> Proc:
        while True:
            ev = yield self.bridge_store.get()
            yield from self.pg(self.p.bridge_tx_s, stmts=3, rows=3)
            self.kafka_inflight -= 1
            self.outbox.append(('render', ev))
            self.relay_wake.notify()

    def outbox_relay(self) -> Proc:
        p = self.p
        while True:
            yield from self.wait_until_up(p.relay_down)
            while self.outbox and not self.in_window(p.relay_down, self.env.now):
                n = min(len(self.outbox), p.relay_batch_max)
                rows = [self.outbox.popleft() for _ in range(n)]
                self.m.relay_batches.append(n)
                yield from self.pg(p.relay_select_s, stmts=1, rows=0)
                yield self.env.timeout(self.confirm_time(n))
                for name, msg in rows:
                    self.publish(name, msg)
                yield from self.pg(p.relay_mark_s, stmts=1, rows=n)
            yield from self.relay_wake.wait(p.relay_poll_s)

    def scheduler(self) -> Proc:
        p = self.p
        env = self.env
        campaigns = sorted(p.campaigns, key=lambda j: j.due)
        digests = sorted(p.digests, key=lambda j: j.due)
        while campaigns or digests:
            yield from self.wait_until_up(p.scheduler_down)
            now = env.now
            while campaigns and campaigns[0].due <= now:
                job = campaigns.pop(0)
                running = any(r.key == job.key and r.completed_at is None for r in self.runs)
                if running:
                    self.m.skipped_occurrences += 1
                    continue
                yield from self.pg(p.scheduler_tx_s, stmts=4, rows=4)
                run = Run(len(self.runs), 'campaign', job.key, job.size, env.now)
                self.runs.append(run)
                self.outbox.append(('audience', PageMsg(run, 0)))
                self.relay_wake.notify()
            while digests and digests[0].due <= now:
                job = digests.pop(0)
                run = Run(len(self.runs), 'digest', job.key, job.size, env.now)
                self.runs.append(run)
                chunks = math.ceil(job.size / p.page_size)
                run.chunks_left = chunks
                for i in range(chunks):
                    size = min(p.page_size, job.size - i * p.page_size)
                    rows = 2 + size * p.likes_per_digest_recipient
                    yield from self.pg(p.scheduler_tx_s, stmts=3, rows=rows)
                    self.outbox.append(('digest', ChunkMsg(run, size)))
                    self.relay_wake.notify()
            yield env.timeout(p.scheduler_tick_s)

    # ---------------------------------------------------------------- auth

    def auth_call(self, lane: Lane, n_ids: int) -> Proc:
        env = self.env
        while True:
            source = self.auth_buckets.try_take(lane, env.now)
            if source is not None:
                if source is not lane:
                    self.m.auth_borrowed += 1
                break
            yield env.timeout(self.auth_buckets.wait_time())
        self.m.auth_calls.append((env.now, lane, n_ids))
        if self.in_window(self.p.auth_outage, env.now):
            self.m.auth_failures += 1
            yield env.timeout(self.p.auth_fail_s)
            raise AuthUnavailable
        with self.auth_server.request() as req:
            yield req
            service = self.lat(self.p.auth_base_s + self.p.auth_per_id_s * n_ids)
            self.m.auth_busy_by_min[int(env.now // M)] += service
            yield env.timeout(service)

    def wait_auth_healthy(self) -> Proc:
        while True:
            yield self.env.timeout(
                self.p.probe_period_s + self.rng.uniform(0, self.p.probe_jitter_s)
            )
            self.m.auth_probes += 1
            if not self.in_window(self.p.auth_outage, self.env.now):
                return

    # ---------------------------------------------------------------- workers

    def transactional_worker(self) -> Proc:
        p = self.p
        env = self.env
        q = self.queues['render']
        while True:
            first = yield q.store.get()
            q.unacked += 1
            batch: list[AuthEvent] = [first]
            deadline = env.now + p.tw_max_wait_s
            while len(batch) < p.tw_batch_max:
                remaining = deadline - env.now
                if remaining <= 0:
                    break
                get = q.store.get()
                yield get | env.timeout(min(p.tw_idle_flush_s, remaining))
                if not get.triggered:
                    get.cancel()
                    break
                event = get.value
                assert isinstance(event, AuthEvent)
                batch.append(event)
                q.unacked += 1
            self.m.tw_batches.append(len(batch))
            if not p.welcome_profile_in_event:
                try:
                    yield from self.auth_call(Lane.TRANS, len(batch))
                except AuthUnavailable:
                    for ev in batch:
                        self.defer('render', ev)
                    q.unacked -= len(batch)
                    yield from self.wait_auth_healthy()
                    continue
            snapshot = env.now
            n = len(batch)
            yield from self.pg(p.tw_tx_base_s + p.tw_tx_per_row_s * n, stmts=4, rows=4 * n)
            for ev in batch:
                snap = ev.time if p.welcome_profile_in_event else snapshot
                self.create_delivery(Lane.TRANS, ev.critical, None, ev.time, snap)
            q.unacked -= n
            self.dispatch_wake.notify()

    def bulk_worker(self, name: Literal['audience', 'digest'], i: int) -> Proc:
        q = self.queues[name]
        while True:
            try:
                with q.store.get() as get:
                    msg = yield get
                q.unacked += 1
                if name == 'audience':
                    self.aw_holding[i] = msg
                try:
                    if isinstance(msg, PageMsg):
                        yield from self.handle_page(msg)
                    else:
                        yield from self.handle_chunk(msg)
                except AuthUnavailable:
                    self.defer(name, msg)
                    q.unacked -= 1
                    if name == 'audience':
                        self.aw_holding[i] = None
                    yield from self.wait_auth_healthy()
                    continue
                q.unacked -= 1
                if name == 'audience':
                    self.aw_holding[i] = None
            except (simpy.Interrupt, WorkerCrash) as exc:
                held = self.aw_holding[i] if name == 'audience' else None
                if held is not None:
                    q.unacked -= 1
                    q.store.put(held)
                    self.m.rabbit_redeliveries += 1
                    self.aw_holding[i] = None
                self.m.crash_log.append(
                    f'{clock(self.env.now)} {name}-worker-{i} crashed '
                    f'({type(exc).__name__}), page redelivered: {held is not None}'
                )
                yield self.env.timeout(self.p.restart_s)

    def handle_page(self, msg: PageMsg) -> Proc:
        p = self.p
        run = msg.run
        yield from self.pg(p.bulk_check_s, stmts=2, rows=0)
        if msg.version != run.cursor_version:
            self.m.stale_pages += 1
            return
        if run.queued_count >= self.gate_limit(run):
            self.defer('audience', msg)
            return
        n = min(p.page_size, run.size - run.paged)
        yield from self.auth_call(Lane.BULK, n)
        snapshot = self.env.now
        via_outbox = p.next_page_via_outbox
        yield from self.pg(
            p.bulk_tx_per_row_s * n, stmts=5 + int(via_outbox), rows=3 * n + 1 + int(via_outbox)
        )
        for _ in range(n):
            self.create_delivery(Lane.BULK, False, run, run.started_at, snapshot)
        run.paged += n
        run.cursor_version += 1
        last = run.paged >= run.size
        nxt = PageMsg(run, run.cursor_version)
        if last:
            run.paging_done_at = self.env.now
            self._check_run_done(run)
        elif via_outbox:
            self.outbox.append(('audience', nxt))
            self.relay_wake.notify()
        self.dispatch_wake.notify()
        if self.aw_crash_armed:
            self.aw_crash_armed = False
            raise WorkerCrash
        if not last and not via_outbox:
            self.publish('audience', nxt)

    def handle_chunk(self, msg: ChunkMsg) -> Proc:
        p = self.p
        run = msg.run
        yield from self.pg(p.bulk_check_s, stmts=2, rows=0)
        if run.queued_count >= self.gate_limit(run):
            self.defer('digest', msg)
            return
        n = msg.size
        yield from self.auth_call(Lane.BULK, n)
        snapshot = self.env.now
        self.m.ugc_calls += 1
        yield self.env.timeout(self.lat(p.ugc_batch_s))
        for _ in range(math.ceil(n / p.movies_ids_per_call)):
            self.m.movies_calls += 1
            yield self.env.timeout(self.lat(p.movies_batch_s))
        yield from self.pg(p.bulk_tx_per_row_s * n, stmts=4, rows=3 * n)
        for _ in range(n):
            self.create_delivery(Lane.BULK, False, run, run.started_at, snapshot)
        run.chunks_left -= 1
        if run.chunks_left == 0:
            run.paging_done_at = self.env.now
            self._check_run_done(run)
        self.dispatch_wake.notify()

    # ---------------------------------------------------------------- dispatcher

    def _pop_valid(self, heap: list[tuple[float, int, Delivery]]) -> Delivery | None:
        while heap:
            d = heapq.heappop(heap)[2]
            if d.status is St.QUEUED:
                return d
        return None

    def take_trans(self, n: int) -> list[Delivery]:
        cutoff = self.env.now - self.p.catchup_age_s
        while self.normal_heap and self.normal_heap[0][0] < cutoff:
            heapq.heappush(self.catchup_heap, heapq.heappop(self.normal_heap))
        out: list[Delivery] = []
        for heap in (self.crit_heap, self.normal_heap, self.catchup_heap):
            while len(out) < n:
                d = self._pop_valid(heap)
                if d is None:
                    break
                out.append(d)
        return out

    def take_bulk(self, n: int) -> list[Delivery]:
        out: list[Delivery] = []
        runs = [r for r in self.runs if r.queued_count > 0]
        if not runs:
            return out
        k = self.drr_offset % len(runs)
        runs = runs[k:] + runs[:k]
        self.drr_offset += 1
        while n > 0 and runs:
            for run in list(runs):
                run.deficit += self.p.drr_quantum
                while run.deficit > 0 and n > 0 and run.queued:
                    d = run.queued.popleft()
                    if d.status is not St.QUEUED:
                        continue
                    out.append(d)
                    run.deficit -= 1
                    n -= 1
                if not run.queued:
                    run.deficit = 0
                    runs.remove(run)
                if n == 0:
                    break
        return out

    def dispatcher(self) -> Proc:
        p = self.p
        env = self.env
        while True:
            yield from self.dispatch_wake.wait(p.dispatcher_tick_s)
            self.m.redis_ops += 1
            rate = self.gate.rate
            window = rate * p.dispatch_window_s
            infl_t = len(self.published[Lane.TRANS]) + len(self.sending[Lane.TRANS])
            infl_b = len(self.published[Lane.BULK]) + len(self.sending[Lane.BULK])
            trans = self.take_trans(max(0, int(window - infl_t)))
            critical_left = self.queued_count[Lane.TRANS] > 0 and any(
                e[2].status is St.QUEUED for e in self.crit_heap
            )
            bulk_min = 0.0 if critical_left else min(p.bulk_min_cap_rps, p.bulk_min_share * rate)
            bulk_cap = max(bulk_min * p.dispatch_window_s, window - infl_t - len(trans))
            bulk = self.take_bulk(max(0, int(bulk_cap - infl_b)))
            batch = trans + bulk
            if not batch:
                continue
            stmts = 2 * (int(bool(trans)) + int(bool(bulk)))
            yield from self.pg(
                p.dispatch_select_s + p.dispatch_per_row_s * len(batch), stmts, len(batch)
            )
            yield env.timeout(self.confirm_time(len(batch)))
            for d in batch:
                self.set_status(d, St.PUBLISHED)
                d.published_at = env.now
                self.send_seq += 1
                prio = (0 if d.lane is Lane.TRANS else 1, self.send_seq)
                self.send_store.put(simpy.PriorityItem(prio, d))
                self.send_ready[d.lane] += 1
                self.m.rabbit_msgs += 1

    # ---------------------------------------------------------------- sender

    def sender_slot(self, r: int, s: int) -> Proc:
        env = self.env
        slot = (r, s)
        while True:
            try:
                if self.sender_down_until[r] > env.now:
                    yield env.timeout(self.sender_down_until[r] - env.now)
                with self.send_store.get() as get:
                    item = yield get
                d: Delivery = item.item
                self.send_ready[d.lane] -= 1
                self.send_unacked[d.lane] += 1
                self.slot_item[slot] = item
                yield from self.send_one(d, slot)
                self.slot_item[slot] = None
                self.send_unacked[d.lane] -= 1
            except simpy.Interrupt:
                held = self.slot_item.get(slot)
                if held is not None:
                    lane = held.item.lane
                    self.send_unacked[lane] -= 1
                    self.send_ready[lane] += 1
                    self.send_store.put(held)
                    self.m.rabbit_redeliveries += 1
                    self.slot_item[slot] = None
                waiter = self.slot_waiter.get(slot)
                if waiter is not None:
                    waiter.cancelled = True
                    self.slot_waiter[slot] = None

    def send_one(self, d: Delivery, slot: tuple[int, int]) -> Proc:
        p = self.p
        env = self.env
        yield from self.pg(p.claim_s, stmts=2, rows=1)
        now = env.now
        claimable = d.status is St.PUBLISHED or (d.status is St.SENDING and d.lease_until <= now)
        if not claimable:
            self.m.stale_send_msgs += 1
            return
        self.set_status(d, St.SENDING)
        d.claim_seq += 1
        mine = d.claim_seq
        d.lease_until = now + (p.lease_trans_s if d.lane is Lane.TRANS else p.lease_bulk_s)
        if now >= d.expires_at:
            self.set_status(d, St.EXPIRED)
            yield from self.pg(p.final_s, stmts=2, rows=2)
            return
        waiter = self.gate.acquire(d)
        self.slot_waiter[slot] = waiter
        yield waiter.event
        self.slot_waiter[slot] = None
        self.m.redis_ops += 1 + int((env.now - waiter.t0) / p.token_poll_s)
        self.m.smtp_attempts += 1
        self.m.snapshot_ages.append(env.now - d.snapshot_time)
        yield env.timeout(self.lat(p.smtp_pre_s, p.smtp_sigma))
        if not self.provider.request(env.now):
            yield env.timeout(self.lat(p.smtp_429_s))
            self.gate.on_429(env.now)
            yield from self.pg(p.final_s, stmts=2, rows=2)
            self.m.requeued_429 += 1
            if d.status is St.SENDING and d.claim_seq == mine:
                self.set_status(d, St.QUEUED)
            return
        d.smtp_accepted += 1
        yield env.timeout(self.lat(p.smtp_post_s, p.smtp_sigma))
        accepted_at = env.now
        yield from self.pg(p.final_s, stmts=2, rows=2)
        if d.claim_seq != mine:
            self.m.lease_lost_sends += 1
        if d.status in TERMINAL:
            return
        if d.lane is Lane.TRANS:
            if p.backlog_cutoff is not None and d.event_time < p.backlog_cutoff:
                cls = 'backlog'
            else:
                cls = 'critical' if d.critical else 'normal'
            self.m.latencies.append((accepted_at, accepted_at - d.event_time, cls))
        self.set_status(d, St.SENT)

    # ---------------------------------------------------------------- background

    def sweeper(self) -> Proc:
        p = self.p
        env = self.env
        while True:
            yield env.timeout(p.sweeper_period_s)
            now = env.now
            infl_b = len(self.published[Lane.BULK]) + len(self.sending[Lane.BULK])
            thresholds = {
                Lane.TRANS: p.sweep_published_trans_s,
                Lane.BULK: max(
                    p.sweep_published_bulk_min_s,
                    p.sweep_published_bulk_factor * infl_b / self.bulk_rate(),
                ),
            }
            stale: list[tuple[Delivery, St]] = []
            for lane, thr in thresholds.items():
                stale += [
                    (d, St.PUBLISHED)
                    for d in self.published[lane].values()
                    if now - d.published_at > thr
                ]
                stale += [
                    (d, St.SENDING) for d in self.sending[lane].values() if d.lease_until <= now
                ]
            yield from self.pg(p.sweep_s, stmts=2, rows=len(stale))
            for d, was in stale:
                if d.status is not was:
                    continue
                if was is St.SENDING and d.lease_until > env.now:
                    continue
                (self.m.swept_published if was is St.PUBLISHED else self.m.swept_lease)[d.lane] += 1
                self.set_status(d, St.QUEUED)
            if stale:
                self.dispatch_wake.notify()

    def ewma_ticker(self) -> Proc:
        alpha = 1.0 - math.exp(-1.0 / self.p.ewma_tau_s)
        while True:
            yield self.env.timeout(1.0)
            self.trans_rate_ewma += alpha * (self.tick_sent[Lane.TRANS] - self.trans_rate_ewma)
            self.bulk_rate_ewma += alpha * (self.tick_sent[Lane.BULK] - self.bulk_rate_ewma)
            self.tick_sent = {Lane.TRANS: 0, Lane.BULK: 0}
            for run in self.runs:
                if run.completed_at is None:
                    run.ewma += alpha * (run.sent_tick - run.ewma)
                    run.sent_tick = 0

    def sampler(self) -> Proc:
        m = self.m
        while True:
            q = self.queues
            values = {
                'render.transactional': q['render'].depth,
                'audience.page': q['audience'].depth,
                'digest.page': q['digest'].depth,
                'retry.10s': self.retry_depth,
                'send.email.transactional': self.send_ready[Lane.TRANS]
                + self.send_unacked[Lane.TRANS],
                'send.email.bulk': self.send_ready[Lane.BULK] + self.send_unacked[Lane.BULK],
                'dq queued transactional': self.queued_count[Lane.TRANS],
                'dq queued bulk': self.queued_count[Lane.BULK],
                'outbox pending': len(self.outbox),
                'aimd rate': self.gate.rate,
                'provider limit': self.provider.limit(self.env.now),
            }
            m.sample_t.append(self.env.now)
            for key, val in values.items():
                m.series.setdefault(key, []).append(float(val))
            yield self.env.timeout(self.p.sample_period_s)

    def nonterminal(self) -> int:
        return sum(self.status_count[s] for s in (St.QUEUED, St.PUBLISHED, St.SENDING))

    def idle(self) -> bool:
        return (
            not self.auth_outbox
            and self.kafka_inflight == 0
            and not self.outbox
            and all(q.depth == 0 for q in self.queues.values())
            and self.retry_depth == 0
            and sum(self.send_ready.values()) + sum(self.send_unacked.values()) == 0
            and self.nonterminal() == 0
        )

    def monitor(self) -> Proc:
        while True:
            yield self.env.timeout(self.p.idle_check_s)
            if self.env.now >= self.p.max_end:
                self.hit_cap = True
                self.stop.succeed()
                return
            if self.env.now >= self.sources_end and self.idle():
                self.stop.succeed()
                return

    # ---------------------------------------------------------------- faults

    def sender_crash(self, at: float) -> Proc:
        yield self.env.timeout(at - self.env.now)
        r = 0
        self.sender_down_until[r] = self.env.now + self.p.restart_s
        busy = sum(1 for s in range(self.p.sender_concurrency) if self.slot_item.get((r, s)))
        self.m.crash_log.append(
            f'{clock(self.env.now)} sender replica {r} killed, {busy} messages in hand'
        )
        for proc in self.slot_procs[r]:
            proc.interrupt('crash')

    def audience_crash(self, at: float) -> Proc:
        yield self.env.timeout(at - self.env.now)
        if self.p.audience_crash_mode == 'after_commit':
            self.aw_crash_armed = True
            return
        holders = [i for i, msg in enumerate(self.aw_holding) if msg is not None]
        self.aw_procs[holders[0] if holders else 0].interrupt('crash')

    # ---------------------------------------------------------------- run

    def run(self) -> None:
        p = self.p
        env = self.env
        env.process(self.signup_source())
        env.process(self.likes_source())
        env.process(self.auth_relay())
        env.process(self.event_bridge())
        env.process(self.outbox_relay())
        env.process(self.scheduler())
        for _ in range(p.tw_replicas):
            env.process(self.transactional_worker())
        self.aw_procs = [env.process(self.bulk_worker('audience', i)) for i in range(p.aw_replicas)]
        for i in range(p.dw_replicas):
            env.process(self.bulk_worker('digest', i))
        env.process(self.dispatcher())
        for r in range(p.sender_replicas):
            self.slot_procs[r] = [
                env.process(self.sender_slot(r, s)) for s in range(p.sender_concurrency)
            ]
        env.process(self.gate.run())
        env.process(self.gate.aimd_ticker())
        env.process(self.sweeper())
        env.process(self.ewma_ticker())
        env.process(self.sampler())
        env.process(self.monitor())
        if p.sender_crash_at is not None:
            env.process(self.sender_crash(p.sender_crash_at))
        if p.audience_crash_at is not None:
            env.process(self.audience_crash(p.audience_crash_at))
        env.run(until=self.stop)


# --------------------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    name: str
    title: str
    params: Params
    crash: bool = False  # duplicates are expected (at-least-once after a crash)


def scenarios() -> dict[str, Scenario]:
    s1 = Params()
    s4 = replace(
        s1,
        start=hms(7),
        signup_phases=((hms(7), hms(10, 30), 1.0),),
        digests=(),
        relay_down=(hms(7), hms(10, 0, 5)),
        scheduler_down=(hms(7), hms(10, 0, 5)),
        backlog_cutoff=hms(10, 0, 5),
    )
    s9 = replace(
        s1,
        max_end=hms(18),
        signup_phases=((hms(9, 59), hms(13), 2.0),),
        campaigns=(
            Job('hourly', hms(10), 20_000),
            Job('oneoff', hms(10), 100_000),
            Job('hourly', hms(11), 20_000),
            Job('hourly', hms(12), 20_000),
        ),
        digests=(),
    )
    items = [
        Scenario('S1', 'Кампания 50k + дайджест 8k в 10:00, регистрации 2/с, всплеск 50/с', s1),
        Scenario(
            'S2', 'S1 + отказ auth 10:05-10:20', replace(s1, auth_outage=(hms(10, 5), hms(10, 20)))
        ),
        Scenario(
            'S3',
            'S1 + троттлинг провайдера до 10/с 10:03-10:13',
            replace(s1, provider_throttle=(hms(10, 3), hms(10, 13), 10.0)),
        ),
        Scenario(
            'S4', 'relay+scheduler лежат 07:00-10:00:05, накоплено ~10.8k, + кампания 50k', s4
        ),
        Scenario(
            'S5',
            'S1 + падение sender в 10:04 и audience-worker в 10:01 (посреди страницы)',
            replace(s1, sender_crash_at=hms(10, 4), audience_crash_at=hms(10, 1)),
            crash=True,
        ),
        Scenario(
            'S5b',
            'S1 + падение audience-worker сразу после COMMIT страницы, до publish',
            replace(s1, audience_crash_at=hms(10, 1), audience_crash_mode='after_commit'),
            crash=True,
        ),
        Scenario(
            'S6',
            'Ночь: 1 регистрация в 30 с в течение часа, без кампаний',
            replace(
                s1,
                start=hms(2),
                signup_phases=((hms(2), hms(3), 1 / 30),),
                campaigns=(),
                digests=(),
                likes_rps=0.1,
            ),
        ),
        Scenario(
            'S7',
            'S1 + медленный Postgres x10 10:05-10:10',
            replace(s1, pg_slow=(hms(10, 5), hms(10, 10), 10.0)),
        ),
        Scenario('S9', '3 ежечасных кампании 20k + разовая 100k, 3 часа', s9),
    ]
    return {s.name: s for s in items}


class SmoothingParams(TypedDict):
    aimd_max: float
    bucket_burst_s: float


def variants(base: dict[str, Scenario]) -> dict[str, Scenario]:
    """What-if design changes, compared with their base scenario in the README."""
    s1, s2, s3 = base['S1'].params, base['S2'].params, base['S3'].params
    no_hol = replace(s1, bulk_block_rule='critical_waiting')
    smooth: SmoothingParams = {'aimd_max': 45.0, 'bucket_burst_s': 0.1}
    items = [
        Scenario('S1-A', 'S1, bulk ждёт только ждущих токен critical (не «в полёте»)', no_hol),
        Scenario(
            'S1-B', 'S1, AIMD max 45/с и burst бакета 0.1 с (без фикса A)', replace(s1, **smooth)
        ),
        Scenario('S1-AB', 'S1 + фиксы A и B', replace(no_hol, **smooth)),
        Scenario(
            'S2-fix',
            'S2, событие auth несёт email: transactional без users:batch',
            replace(s2, welcome_profile_in_event=True),
        ),
        Scenario(
            'S3-AB',
            'S3 + фиксы A и B',
            replace(s3, bulk_block_rule='critical_waiting', **smooth),
        ),
        Scenario(
            'S5b-fix',
            'S5b, следующая страница через outbox в той же транзакции',
            replace(base['S5b'].params, next_page_via_outbox=True),
            crash=True,
        ),
    ]
    return {s.name: s for s in items}


# --------------------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------------------


def pct(values: Sequence[float] | np.ndarray, q: float) -> float:
    return float(np.percentile(values, q)) if len(values) else math.nan


def peak_per_second(times: list[float]) -> int:
    return max(Counter(int(t) for t in times).values(), default=0)


@dataclass
class Result:
    scenario: Scenario
    sim: Sim
    end: float
    checks: list[tuple[str, bool, str]]  # design checks: (name, ok, detail)
    invariants: list[str]  # model invariants that passed


def model_invariants(sim: Sim) -> list[str]:
    """Hard sanity assertions of the model itself."""
    p = sim.p
    passed: list[str] = []
    counted = Counter(d.status for d in sim.deliveries)
    created = len(sim.deliveries)
    term = counted[St.SENT] + counted[St.EXPIRED] + counted[St.SKIPPED]
    inflight = counted[St.QUEUED] + counted[St.PUBLISHED] + counted[St.SENDING]
    assert created == term + inflight, 'conservation'
    assert all(counted[s] == sim.status_count[s] for s in St), 'status counters drift'
    passed.append(
        f'conservation: created {created} = sent {counted[St.SENT]} + expired '
        f'{counted[St.EXPIRED]} + skipped {counted[St.SKIPPED]} + in-flight {inflight}'
    )
    for lane in Lane:
        assert len(sim.published[lane]) == sum(
            1 for d in sim.deliveries if d.lane is lane and d.status is St.PUBLISHED
        )
        assert sim.send_ready[lane] >= 0 and sim.send_unacked[lane] >= 0
    assert all(q.unacked >= 0 for q in sim.queues.values()) and sim.retry_depth >= 0
    passed.append('queue/in-flight counters consistent and non-negative')
    accepted = sim.provider.accepted
    times = np.array(accepted)
    worst = 0.0
    for j, t in enumerate(accepted):
        i = int(np.searchsorted(times, t - 1.0, side='right'))
        count = j - i + 1
        worst = max(worst, count / sim.provider.limit(t))
        assert count <= sim.provider.limit(t), f'provider limit exceeded at {clock(t)}'
    passed.append(
        f'provider limit never exceeded in any trailing 1 s window '
        f'(max load {worst:.0%} of limit, {len(accepted)} accepted)'
    )
    for d in sim.deliveries:
        if d.status is St.SENT:
            assert d.smtp_accepted >= 1
        if d.status in (St.SKIPPED, St.EXPIRED):
            assert d.smtp_accepted == 0
    passed.append('every sent delivery has >=1 accepted SMTP call, skipped/expired have none')
    if not sim.hit_cap:
        assert sim.idle(), 'stopped while not idle'
        passed.append(f'drained: system idle at {clock(sim.env.now)}, no cap hit')
    for run in sim.runs:
        assert run.created <= run.size
    assert sum(1 for ln in sim.m.tw_batches if ln > p.tw_batch_max) == 0
    passed.append('run sizes and micro-batch sizes within bounds')
    return passed


def design_checks(scn: Scenario, sim: Sim) -> list[tuple[str, bool, str]]:
    p = sim.p
    m = sim.m
    dup = sum(1 for d in sim.deliveries if d.smtp_accepted > 1)
    lost = sim.nonterminal()
    not_created = sum(r.size - r.created for r in sim.runs)
    checks: list[tuple[str, bool, str]] = []
    checks.append(
        (
            'no duplicates',
            dup == 0 or scn.crash,
            f'{dup} deliveries with >1 accepted SMTP call'
            + (' (crash: allowed)' if scn.crash else ''),
        )
    )
    checks.append(('no lost deliveries', lost == 0, f'{lost} created but not terminal at end'))
    checks.append(
        ('all runs fully rendered', not_created == 0, f'{not_created} recipients never rendered')
    )
    fresh = [lat for _, lat, cls in m.latencies if cls != 'backlog']
    p95, p99 = pct(fresh, 95), pct(fresh, 99)
    checks.append(
        (
            'SLA transactional p95<=60s, p99<=180s',
            bool(fresh) and p95 <= p.sla_p95_s and p99 <= p.sla_p99_s,
            f'p95={p95:.1f}s p99={p99:.1f}s (fresh events, n={len(fresh)})',
        )
    )
    peak = peak_per_second([t for t, _, _ in m.auth_calls])
    checks.append(
        (
            'auth within quota',
            peak <= p.auth_quota_rps,
            f'peak {peak} calls/s, quota {p.auth_quota_rps:g}',
        )
    )
    checks.append(
        (
            'no self-inflicted 429',
            sim.provider.rejected == 0 or p.provider_throttle is not None,
            f'{sim.provider.rejected} x 429, {sim.gate.decreases} AIMD halvings',
        )
    )
    return checks


def run_scenario(scn: Scenario, seed: int) -> Result:
    sim = Sim(scn.params, seed)
    sim.run()
    return Result(scn, sim, sim.env.now, design_checks(scn, sim), model_invariants(sim))


# ---------------------------------------------------------------- tables


def fmt_s(x: float) -> str:
    if math.isnan(x):
        return '—'
    return f'{x:.1f}' if x < 100 else f'{x:.0f}'


def md_table(header: list[str], rows: list[list[str]]) -> str:
    lines = ['| ' + ' | '.join(header) + ' |', '|' + '---|' * len(header)]
    lines += ['| ' + ' | '.join(r) + ' |' for r in rows]
    return '\n'.join(lines)


def latency_rows(res: Result) -> list[list[str]]:
    rows = []
    p = res.sim.p
    for cls in ('critical', 'normal', 'backlog'):
        vals = [lat for _, lat, c in res.sim.m.latencies if c == cls]
        if not vals:
            continue
        p95, p99 = pct(vals, 95), pct(vals, 99)
        ok = 'да' if p95 <= p.sla_p95_s and p99 <= p.sla_p99_s else '**нет**'
        rows.append(
            [res.scenario.name, cls, str(len(vals))]
            + [fmt_s(pct(vals, q)) for q in (50, 95, 99, 100)]
            + [ok if cls != 'backlog' else '—']
        )
    expired = sum(1 for d in res.sim.deliveries if d.status is St.EXPIRED)
    if expired:
        rows.append([res.scenario.name, 'expired', str(expired), '', '', '', '', ''])
    return rows


def run_rows(res: Result) -> list[list[str]]:
    rows = []
    for run in res.sim.runs:
        dur = (run.completed_at - run.started_at) if run.completed_at is not None else math.nan
        paging = (run.paging_done_at - run.started_at) if run.paging_done_at else math.nan
        thr = run.sent / dur if run.completed_at is not None and dur > 0 else math.nan
        rows.append(
            [
                res.scenario.name,
                f'{run.kind}:{run.key}#{run.id}',
                str(run.size),
                str(run.created),
                str(run.sent),
                clock(run.started_at),
                fmt_s(paging / M) if not math.isnan(paging) else '—',
                clock(run.completed_at) if run.completed_at is not None else '**не завершён**',
                fmt_s(dur / M),
                fmt_s(thr),
            ]
        )
    return rows


QUEUE_KEYS = [
    'render.transactional',
    'audience.page',
    'digest.page',
    'retry.10s',
    'dq queued transactional',
    'dq queued bulk',
    'send.email.transactional',
    'send.email.bulk',
    'outbox pending',
]


def queue_row(res: Result) -> list[str]:
    s = res.sim.m.series
    return [res.scenario.name] + [f'{max(s[k]):.0f}' for k in QUEUE_KEYS]


def auth_row(res: Result) -> list[str]:
    m = res.sim.m
    span = res.end - res.sim.p.start
    by_lane = {lane: [t for t, ln, _ in m.auth_calls if ln is lane] for lane in Lane}
    peak_util = max(m.auth_busy_by_min.values(), default=0.0) / (res.sim.p.auth_workers * M)
    total_util = sum(m.auth_busy_by_min.values()) / (res.sim.p.auth_workers * span)
    return [
        res.scenario.name,
        str(len(by_lane[Lane.TRANS])),
        str(len(by_lane[Lane.BULK])),
        str(peak_per_second(by_lane[Lane.TRANS])),
        str(peak_per_second(by_lane[Lane.BULK])),
        str(peak_per_second([t for t, _, _ in m.auth_calls])),
        f'{len(m.auth_calls) / span:.2f}',
        str(m.auth_borrowed),
        str(m.auth_failures),
        str(m.auth_probes),
        f'{total_util:.1%}',
        f'{peak_util:.1%}',
    ]


def pg_row(res: Result) -> list[str]:
    m = res.sim.m
    span = res.end - res.sim.p.start
    stmts = sum(m.pg_stmts.values())
    rows = sum(m.pg_rows.values())
    return [
        res.scenario.name,
        str(stmts),
        f'{stmts / span:.1f}',
        str(max(m.pg_stmts.values(), default=0)),
        f'{rows / span:.1f}',
        str(max(m.pg_rows.values(), default=0)),
    ]


def snapshot_row(res: Result) -> list[str]:
    a = res.sim.m.snapshot_ages
    return [res.scenario.name] + [fmt_s(pct(a, q)) for q in (50, 95, 100)]


def batch_row(res: Result) -> list[str]:
    b = res.sim.m.tw_batches
    r = res.sim.m.relay_batches
    ones = sum(1 for x in b if x == 1) / len(b) if b else math.nan
    return [
        res.scenario.name,
        str(len(b)),
        f'{np.mean(b):.1f}' if b else '—',
        fmt_s(pct(b, 50)),
        fmt_s(pct(b, 95)),
        str(max(b, default=0)),
        f'{ones:.0%}' if b else '—',
        str(len(r)),
        f'{np.mean(r):.1f}' if r else '—',
        str(max(r, default=0)),
    ]


def reliability_row(res: Result) -> list[str]:
    sim = res.sim
    m = sim.m
    c = Counter(d.status for d in sim.deliveries)
    dup = sum(1 for d in sim.deliveries if d.smtp_accepted > 1)
    not_created = sum(r.size - r.created for r in sim.runs)
    return [
        res.scenario.name,
        str(len(sim.deliveries)),
        str(c[St.SENT]),
        str(c[St.EXPIRED]),
        str(c[St.SKIPPED]),
        str(sim.nonterminal()),
        str(not_created),
        str(dup),
        str(sim.provider.rejected),
        f'{sim.gate.decreases}/{sim.gate.increases}',
        f'{min(res.sim.m.series["aimd rate"]):.1f}',
        f'{sum(m.swept_published.values())}/{sum(m.swept_lease.values())}',
        str(m.stale_send_msgs),
        str(m.stale_pages),
        str(sum(m.defers.values())),
    ]


def io_row(res: Result) -> list[str]:
    sim = res.sim
    m = sim.m
    sent = max(1, sum(1 for d in sim.deliveries if d.status is St.SENT))
    http = len(m.auth_calls) + m.auth_probes + m.ugc_calls + m.movies_calls
    return [
        res.scenario.name,
        f'{sum(m.pg_stmts.values()) / sent:.2f}',
        f'{sum(m.pg_rows.values()) / sent:.2f}',
        f'{(m.rabbit_msgs + m.rabbit_redeliveries) / sent:.2f}',
        f'{http / sent:.4f}',
        f'{m.redis_ops / sent:.2f}',
        f'{m.smtp_attempts / sent:.3f}',
    ]


def throughput_row(res: Result) -> list[str]:
    sim = res.sim
    bins = sim.m.sent_bins[Lane.BULK]
    if not bins:
        return [res.scenario.name, '0', '—', '—', '—']
    first, last = min(bins), max(bins)
    total = sum(bins.values())
    per_min = Counter(t // 60 for t in bins.elements())
    return [
        res.scenario.name,
        str(total),
        f'{total / max(1, last - first + 1):.1f}',
        f'{max(per_min.values()) / 60:.1f}',
        f'{clock(first)}–{clock(last)}',
    ]


def write_summary(results: list[Result], seed: int, path: Path) -> None:
    out = [f'# Результаты DES сервиса уведомлений (seed={seed})', '']
    out.append('Сценарии:')
    out += [f'- **{r.scenario.name}** — {r.scenario.title}' for r in results]
    out += ['', '## Задержка транзакционных писем (событие → 250 OK от SMTP), секунды', '']
    out.append(
        md_table(
            ['сценарий', 'класс', 'n', 'p50', 'p95', 'p99', 'max', 'SLA p95≤60 и p99≤180'],
            [row for r in results for row in latency_rows(r)],
        )
    )
    out += [
        '',
        '`backlog` — события, случившиеся до восстановления relay (S4), SLA к ним не применяется.',
    ]
    out += ['', '## Массовые запуски: время рендера (мин), завершения и пропускная способность', '']
    out.append(
        md_table(
            [
                'сценарий',
                'запуск',
                'размер',
                'создано',
                'отправлено',
                'старт',
                'рендер, мин',
                'завершён',
                'длительность, мин',
                'писем/с',
            ],
            [row for r in results for row in run_rows(r)],
        )
    )
    out += ['', '## Пропускная способность bulk', '']
    out.append(
        md_table(
            ['сценарий', 'bulk отправлено', 'средн. писем/с', 'пик мин., писем/с', 'интервал'],
            [throughput_row(r) for r in results],
        )
    )
    out += ['', '## Пиковые глубины очередей (по выборкам раз в 5 с)', '']
    out.append(md_table(['сценарий'] + QUEUE_KEYS, [queue_row(r) for r in results]))
    out += [
        '',
        'Очереди RabbitMQ — ready+unacked; `dq queued` — строки dispatch_queue в статусе queued.',
    ]
    out += ['', '## auth internal API', '']
    out.append(
        md_table(
            [
                'сценарий',
                'вызовов trans',
                'вызовов bulk',
                'пик trans/с',
                'пик bulk/с',
                'пик всего/с',
                'средн. RPS',
                'заимствований',
                'ошибок',
                'проб',
                'загрузка ср.',
                'загрузка пик. мин',
            ],
            [auth_row(r) for r in results],
        )
    )
    out += ['', '## Postgres notifications (смоделированные операторы)', '']
    out.append(
        md_table(
            [
                'сценарий',
                'операторов',
                'операторов/с ср.',
                'операторов/с пик',
                'строк/с ср.',
                'строк/с пик',
            ],
            [pg_row(r) for r in results],
        )
    )
    out += ['', '## Возраст снимка профиля в момент отправки, секунды', '']
    out.append(md_table(['сценарий', 'p50', 'p95', 'max'], [snapshot_row(r) for r in results]))
    out += ['', '## Размеры микропачек transactional-worker и пачек outbox-relay', '']
    out.append(
        md_table(
            [
                'сценарий',
                'пачек TW',
                'среднее',
                'p50',
                'p95',
                'max',
                'доля размера 1',
                'пачек relay',
                'relay среднее',
                'relay max',
            ],
            [batch_row(r) for r in results],
        )
    )
    out += ['', '## Надёжность', '']
    out.append(
        md_table(
            [
                'сценарий',
                'создано',
                'sent',
                'expired',
                'skipped',
                'не завершено',
                'не отрендерено',
                'дубли',
                '429',
                'AIMD ↓/↑',
                'AIMD min',
                'sweeper pub/lease',
                'устаревших send-сообщ.',
                'устаревших страниц',
                'отложено в retry',
            ],
            [reliability_row(r) for r in results],
        )
    )
    out += ['', '## Ввод-вывод на одно отправленное письмо', '']
    out.append(
        md_table(
            [
                'сценарий',
                'PG операторов',
                'PG строк',
                'сообщений RabbitMQ',
                'HTTP-вызовов',
                'операций Redis',
                'SMTP-попыток',
            ],
            [io_row(r) for r in results],
        )
    )
    out += ['', '## Проверки дизайна', '']
    rows = [
        [r.scenario.name, name, 'PASS' if ok else '**FAIL**', detail]
        for r in results
        for name, ok, detail in r.checks
    ]
    out.append(md_table(['сценарий', 'проверка', 'итог', 'детали'], rows))
    out += ['', '## Инварианты модели (assert, все прошли)', '']
    for r in results:
        out.append(f'- **{r.scenario.name}**: ' + '; '.join(r.invariants))
    crashes = [(r.scenario.name, line) for r in results for line in r.sim.m.crash_log]
    if crashes:
        out += ['', '## Журнал падений', '']
        out += [f'- {name}: {line}' for name, line in crashes]
    path.write_text('\n'.join(out) + '\n', encoding='utf-8')


# ---------------------------------------------------------------- chart


def plot(res: Result, path: Path) -> None:
    m = res.sim.m
    t = np.array(m.sample_t) / H
    fig, axes = plt.subplots(4, 1, figsize=(12, 13), sharex=True)
    hours = FuncFormatter(lambda x, _pos: clock(x * H)[:5])

    ax = axes[0]
    for key in (
        'render.transactional',
        'audience.page',
        'digest.page',
        'retry.10s',
        'send.email.transactional',
        'send.email.bulk',
    ):
        ax.plot(t, m.series[key], label=key, lw=1.2)
    ax.set_yscale('symlog', linthresh=10)
    ax.set_ylabel('сообщений')
    ax.set_title('Очереди RabbitMQ (ready+unacked)')
    ax.legend(fontsize=8, ncol=3)

    ax = axes[1]
    for key in ('dq queued transactional', 'dq queued bulk', 'outbox pending'):
        ax.plot(t, m.series[key], label=key, lw=1.2)
    ax.set_yscale('symlog', linthresh=10)
    ax.set_ylabel('строк')
    ax.set_title('Postgres: dispatch_queue (queued) и outbox')
    ax.legend(fontsize=8)

    ax = axes[2]
    for lane, color in ((Lane.TRANS, 'tab:red'), (Lane.BULK, 'tab:blue')):
        bins = m.sent_bins[lane]
        if bins:
            lo = int(res.sim.p.start)
            hi = int(res.end) + 1
            counts = np.array([bins.get(s, 0) for s in range(lo, hi)], dtype=float)
            win = 10
            smooth = np.convolve(counts, np.ones(win) / win, mode='same')
            ax.plot(np.arange(lo, hi) / H, smooth, color=color, lw=1, label=f'sent/с {lane} (10 с)')
    ax.plot(t, m.series['aimd rate'], color='black', lw=1.2, label='AIMD rate')
    ax.plot(t, m.series['provider limit'], color='grey', ls='--', lw=1, label='лимит провайдера')
    ax.set_ylabel('писем/с')
    ax.set_title('Отправка и скорость AIMD')
    ax.legend(fontsize=8)

    ax = axes[3]
    colors = {'critical': 'tab:red', 'normal': 'tab:blue', 'backlog': 'tab:grey'}
    for cls, color in colors.items():
        pts = [(ts, lat) for ts, lat, c in m.latencies if c == cls]
        if pts:
            xs, ys = zip(*pts, strict=True)
            ax.scatter(np.array(xs) / H, ys, s=3, color=color, label=cls, alpha=0.5)
    ax.axhline(res.sim.p.sla_p95_s, color='orange', ls='--', lw=1, label='60 с')
    ax.axhline(res.sim.p.sla_p99_s, color='red', ls='--', lw=1, label='180 с')
    ax.set_yscale('log')
    ax.set_ylabel('секунд')
    ax.set_title('Задержка транзакционных писем по времени отправки')
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(hours)
    ax.set_xlabel('время')

    fig.suptitle(f'{res.scenario.name}: {res.scenario.title}')
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


# ---------------------------------------------------------------- CLI


def all_scenarios() -> dict[str, Scenario]:
    base = scenarios()
    return base | variants(base)


def main() -> None:
    catalogue = all_scenarios()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', default='all', choices=[*catalogue, 'all'])
    parser.add_argument('--seed', type=int, default=1)
    args = parser.parse_args()
    names = list(catalogue) if args.scenario == 'all' else [args.scenario]
    RESULTS_DIR.mkdir(exist_ok=True)
    results: list[Result] = []
    for name in names:
        res = run_scenario(catalogue[name], args.seed)
        results.append(res)
        plot(res, RESULTS_DIR / f'{name}.png')
        sim = res.sim
        sent = sum(1 for d in sim.deliveries if d.status is St.SENT)
        fails = [c for c in res.checks if not c[1]]
        print(
            f'{name:8s} end={clock(res.end)} created={len(sim.deliveries)} sent={sent} '
            f'429={sim.provider.rejected} design-FAIL={len(fails)}'
        )
        for line in res.invariants:
            print(f'    invariant OK: {line}')
        for check, ok, detail in res.checks:
            print(f'    {"PASS" if ok else "FAIL"}: {check} — {detail}')
    write_summary(results, args.seed, RESULTS_DIR / 'summary.md')
    print(
        f'ALL MODEL INVARIANTS PASSED for {len(results)} scenario(s); summary: results/summary.md'
    )


if __name__ == '__main__':
    main()
