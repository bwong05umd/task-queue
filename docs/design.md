# Design decisions and tradeoffs

**Redis + MySQL.** MySQL is the source of truth for every job's state, result and
attempt history; Redis only holds ids waiting to run or currently leased. Redis
gives cheap atomic queue operations; MySQL gives durability and queryable history.
If the two disagree, MySQL wins: the scheduler's reconcile pass re-pushes any
unfinished job that Redis has lost track of (for example, the API crashed between
the INSERT and the RPUSH, or Redis lost its last second of AOF writes).

**At-least-once delivery.** A claim is a Lua script that pops the id and adds it
to the `processing` zset with a lease expiry in one step, so a crash right after
claiming cannot drop the job. A worker that dies stops heartbeating; the reaper
moves expired leases back to the queue. The cost is that a job can run twice
(e.g. a worker stalls past its lease, then wakes up), so handlers must be
idempotent. `email.send` shows the pattern: `SET NX` on the job id before the
side effect.

**Fencing tokens.** Each claim writes a fresh `run_token` to the job row. Every
later write (`succeeded`, retry, `dead`) is `WHERE run_token = :mine AND
status = 'running'`, so a stale worker cannot overwrite a newer attempt's result.
A stale worker also leaves Redis alone, because the lease now belongs to the new
claimer. Duplicate deliveries of an already-finished job fail the claim's
`status IN ('queued','running')` guard and are dropped.

**Write order.** Workers write MySQL first, then Redis. A crash in between leaves
the job leased in Redis; the lease expires, the job is redelivered, and the claim
guard (or fencing) makes the redelivery harmless.

**Lease length.** 15s lease, 5s heartbeat by default: a crashed worker's job is
retried within ~15s, and a worker can miss two heartbeats (GC pause, Redis blip)
before it loses the lease. Longer leases mean slower recovery; shorter ones mean
more duplicate runs. Jobs that lose their lease count as an attempt, so a job that
crashes its worker every time still ends up in the DLQ.

**Backoff.** `base * 2^(attempt-1)` capped at `BACKOFF_CAP`, plus up to 20%
random jitter so a burst of failures does not retry in lockstep. Delayed retries
wait in a Redis zset scored by run time; the scheduler promotes due ones.

**Dead-letter queue.** A `dead` status in MySQL plus a Redis zset (added with
`NX`, so a job is buried exactly once). Permanent errors skip straight to the DLQ
because retrying a bad payload cannot help, but a human may want to fix and
replay it.

**Polling claim.** Lua scripts cannot block, so idle workers poll every 100ms
(`POLL_INTERVAL`). That adds up to 100ms of pickup latency when the queue is idle.
`BLMOVE` into a per-worker list would remove it but complicates lease tracking.

**Known limits.** Redis loses up to ~1s of writes on a hard crash (`appendfsync
everysec`); reconcile recovers those jobs after `RECONCILE_AFTER_SECONDS`. If
Redis loses a `processing` entry while the job is still running, reconcile may
start a duplicate run of it, which fencing then resolves.
