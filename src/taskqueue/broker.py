"""Redis broker. Holds only what is waiting to run or currently leased.

Keys (with the configured prefix, default "tq:"):
  queue       list  job ids ready to run (RPUSH in, LPOP out: FIFO)
  processing  zset  job id -> lease expiry (ms); a worker holds the lease
  delayed     zset  job id -> run-at time (ms); retries waiting out backoff
  dead        zset  job id -> time it died; the dead-letter queue
  workers     zset  worker id -> last heartbeat (s)

Every multi-key move is a Lua script so it is atomic, and all times come from
the Redis clock so workers with skewed clocks agree on lease expiry."""

import time

import redis

_NOW_MS = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# Pop the next job and lease it in one step, so a worker that dies right after
# taking a job still leaves it in `processing` for the reaper to find.
_CLAIM = _NOW_MS + """
local id = redis.call('LPOP', KEYS[1])
if not id then return false end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), id)
return id
"""

# Extend a lease only if it still exists; returns 0 when the lease was reaped.
_EXTEND = _NOW_MS + """
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
return 1
"""

# Move every member of a zset whose score is due onto a list. Used both to
# reap expired leases (processing -> queue) and to promote retries
# (delayed -> queue).
_MOVE_DUE = _NOW_MS + """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('RPUSH', KEYS[2], id)
end
return ids
"""

_RETRY = _NOW_MS + """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
return 1
"""

# NX keeps the first death time, so a job lands in the DLQ exactly once.
_DEAD = _NOW_MS + """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], 'NX', now, ARGV[1])
return 1
"""

_REPLAY = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('RPUSH', KEYS[2], ARGV[1])
return 1
"""

# Re-push a job MySQL says is unfinished, unless Redis already tracks it.
_REQUEUE_IF_UNKNOWN = _NOW_MS + """
if redis.call('ZSCORE', KEYS[2], ARGV[1]) or redis.call('ZSCORE', KEYS[3], ARGV[1]) then return 0 end
if redis.call('LPOS', KEYS[1], ARGV[1]) then return 0 end
local delay = tonumber(ARGV[2])
if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, ARGV[1])
else
  redis.call('RPUSH', KEYS[1], ARGV[1])
end
return 1
"""


class Broker:
    def __init__(self, url: str, prefix: str = "tq:"):
        self.r = redis.Redis.from_url(
            url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5, health_check_interval=30,
        )
        self.prefix = prefix
        self.queue = f"{prefix}queue"
        self.processing = f"{prefix}processing"
        self.delayed = f"{prefix}delayed"
        self.dead = f"{prefix}dead"
        self.workers = f"{prefix}workers"
        self._claim = self.r.register_script(_CLAIM)
        self._extend = self.r.register_script(_EXTEND)
        self._move_due = self.r.register_script(_MOVE_DUE)
        self._retry = self.r.register_script(_RETRY)
        self._dead = self.r.register_script(_DEAD)
        self._replay = self.r.register_script(_REPLAY)
        self._requeue_if_unknown = self.r.register_script(_REQUEUE_IF_UNKNOWN)

    def ping(self) -> None:
        self.r.ping()

    def enqueue(self, job_id: str) -> None:
        self.r.rpush(self.queue, job_id)

    def claim(self, lease_ms: int) -> str | None:
        return self._claim(keys=[self.queue, self.processing], args=[lease_ms])

    def extend_lease(self, job_id: str, lease_ms: int) -> bool:
        return bool(self._extend(keys=[self.processing], args=[job_id, lease_ms]))

    def ack(self, job_id: str) -> None:
        self.r.zrem(self.processing, job_id)

    def retry(self, job_id: str, delay_ms: int) -> None:
        self._retry(keys=[self.processing, self.delayed], args=[job_id, delay_ms])

    def bury(self, job_id: str) -> None:
        self._dead(keys=[self.processing, self.dead], args=[job_id])

    def replay(self, job_id: str) -> None:
        self._replay(keys=[self.dead, self.queue], args=[job_id])

    def forget_dead(self, ids: list[str]) -> None:
        if ids:
            self.r.zrem(self.dead, *ids)

    def reap_expired(self, limit: int = 500) -> list[str]:
        return self._move_due(keys=[self.processing, self.queue], args=[limit])

    def promote_due(self, limit: int = 500) -> list[str]:
        return self._move_due(keys=[self.delayed, self.queue], args=[limit])

    def requeue_if_unknown(self, job_id: str, delay_ms: int = 0) -> bool:
        return bool(self._requeue_if_unknown(keys=[self.queue, self.processing, self.delayed], args=[job_id, delay_ms]))

    def depths(self) -> dict[str, int]:
        p = self.r.pipeline()
        p.llen(self.queue)
        p.zcard(self.processing)
        p.zcard(self.delayed)
        p.zcard(self.dead)
        queue, processing, delayed, dead = p.execute()
        return {"queue": queue, "processing": processing, "delayed": delayed, "dead": dead}

    def worker_heartbeat(self, worker_id: str) -> None:
        self.r.zadd(self.workers, {worker_id: time.time()})

    def worker_gone(self, worker_id: str) -> None:
        self.r.zrem(self.workers, worker_id)

    def active_workers(self, window_seconds: float) -> int:
        cutoff = time.time() - window_seconds
        self.r.zremrangebyscore(self.workers, "-inf", cutoff - 3600)
        return self.r.zcount(self.workers, cutoff, "+inf")
