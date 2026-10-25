import pytest

from taskqueue import states


@pytest.mark.parametrize(
    "current, new",
    [
        (states.QUEUED, states.RUNNING),
        (states.RUNNING, states.SUCCEEDED),
        (states.RUNNING, states.QUEUED),  # retry scheduled
        (states.RUNNING, states.DEAD),
        (states.RUNNING, states.RUNNING),  # re-claimed after a lost lease
        (states.DEAD, states.QUEUED),  # replay
    ],
)
def test_allowed_transitions(current, new):
    assert states.can_transition(current, new)


@pytest.mark.parametrize(
    "current, new",
    [
        (states.SUCCEEDED, states.RUNNING),
        (states.SUCCEEDED, states.QUEUED),
        (states.DEAD, states.RUNNING),
        (states.QUEUED, states.SUCCEEDED),
    ],
)
def test_forbidden_transitions(current, new):
    assert not states.can_transition(current, new)


def test_terminal_states_are_not_claimable():
    assert not set(states.TERMINAL) & set(states.CLAIMABLE)
    assert set(states.TERMINAL) | set(states.CLAIMABLE) == set(states.ALL)
