"""Job states and the transitions allowed between them."""

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
DEAD = "dead"

ALL = (QUEUED, RUNNING, SUCCEEDED, DEAD)

# A worker may claim a job that is queued, or one still marked running whose
# previous worker lost its lease (crashed or stalled) and was reaped.
CLAIMABLE = (QUEUED, RUNNING)
TERMINAL = (SUCCEEDED, DEAD)

TRANSITIONS = {
    QUEUED: {RUNNING},
    RUNNING: {RUNNING, QUEUED, SUCCEEDED, DEAD},
    SUCCEEDED: set(),
    DEAD: {QUEUED},  # manual replay from the dead-letter queue
}


def can_transition(current: str, new: str) -> bool:
    return new in TRANSITIONS.get(current, set())
